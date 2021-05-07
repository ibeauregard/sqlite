import collections
import itertools
import operator
import re
from abc import ABC, abstractmethod
from pathlib import Path

from config.config import Config
from my_sqlite.conversion import converted
from my_sqlite.error import NoSuchTableError, AmbiguousColumnNameError, NoSuchColumnError, translate_key_error, \
    InsertError, UpdateError


class AbstractQuery(ABC):
    _database_path = Config.database_path
    _file_extension = Config.table_filename_extension
    _unit_sep = Config.unit_separator
    _record_sep = Config.record_separator

    def __init__(self):
        self.table_map = {}

    @abstractmethod
    def run(self):
        pass

    def append_table(self, table):
        table_path = Path(self._database_path) / f'{table}{self._file_extension}'
        if not table_path.is_file():
            raise NoSuchTableError(table)
        with open(table_path) as table_file:
            headers = self.strip_and_split(table_file.read().partition(self._record_sep)[0])
        if table in self.table_map:
            table += f'__{len(self.table_map)}'
        self.table_map[table] = Table(index=len(self.table_map),
                                      path=table_path,
                                      header_map={header.lower(): i for i, header in enumerate(headers)},
                                      header_map_case_preserved={header: i for i, header in enumerate(headers)},
                                      filter=lambda row: True)

    @classmethod
    def _parse_table(cls, table_file):
        split_file = table_file.read().split(cls._record_sep)
        records = itertools.islice(split_file, 1, len(split_file) - 1)
        return map(cls.strip_and_split, records)

    def _serialize_table(self, entries):
        header_map = next(iter(self.table_map.values())).header_map_case_preserved
        return f"{self._unit_sep.join(header_map)}{self._record_sep}{self._serialize_rows(entries)}"

    @classmethod
    def _serialize_rows(cls, entries):
        serialized_rows = cls._record_sep.join(cls._unit_sep.join(entry) for entry in entries)
        return f"{serialized_rows}{cls._record_sep if serialized_rows else ''}"

    @classmethod
    def strip_and_split(cls, line):
        return line.rstrip(cls._record_sep).split(cls._unit_sep)

    def get_tables_in_query_order(self):
        return sorted(self.table_map.values(), key=lambda t: t.index)

    def get_table_by_index(self, index):
        return next(table for table in self.table_map.values() if table.index == index)


Table = collections.namedtuple('Table', ['index', 'path', 'header_map', 'header_map_case_preserved', 'filter'])


class FilteredQuery(AbstractQuery):
    @abstractmethod
    def __init__(self):
        super().__init__()
        self._where_filter = lambda row: True

    def where(self, column, *, condition):
        [key] = self._map_keys(column)
        self._where_filter = lambda row: condition(converted(row[key.table][key.column]))

    def _map_keys(self, *keys):
        return tuple(self._map_key(key) for key in keys)

    def _map_key(self, key):
        key_parts = key.split('.', maxsplit=1)
        if len(key_parts) == 1:
            return self._map_one_part_key(key)
        else:  # len == 2
            return self._map_two_part_key(key_parts)

    def _map_one_part_key(self, key):
        lower_key = key.lower()
        matches = tuple((table.index, table.header_map[lower_key])
                        for table in self.table_map.values() if lower_key in table.header_map)
        if len(matches) > 1:
            raise AmbiguousColumnNameError(key)
        if not matches:
            raise NoSuchColumnError(key)
        return Key(*matches[0])

    def _map_two_part_key(self, parts):
        try:
            table = self.table_map[parts[0]]
            return Key(table=table.index, column=table.header_map[parts[1].lower()])
        except KeyError:
            raise NoSuchColumnError('.'.join(parts))

    def _get_key_set(self, table):
        try:
            return (Key(*key) for key in
                    (self._get_full_key_set() if table is None else self._get_one_table_key_set(self.table_map[table])))
        except KeyError:
            raise NoSuchTableError(table)

    def _get_full_key_set(self):
        return itertools.chain.from_iterable(self._get_one_table_key_set(table)
                                             for table in self.get_tables_in_query_order())

    @staticmethod
    def _get_one_table_key_set(table):
        return itertools.product((table.index,), range(len(table.header_map)))


Key = collections.namedtuple('Key', ['table', 'column'])


class Delete(FilteredQuery):
    def __init__(self):
        super().__init__()

    def from_(self, table):
        self.append_table(table)

    def run(self):
        table = next(iter(self.table_map.values()))
        with open(table.path) as table_file:
            entries = self._parse_table(table_file)
            non_deleted_entries = [entry for entry in entries if not self._where_filter((entry,))]
        with open(table.path, 'w') as table_file:
            table_file.write(self._serialize_table(non_deleted_entries))


class Insert(AbstractQuery):
    def __init__(self):
        super().__init__()
        self._value_indices = None
        self._inserted_rows = None

    def into(self, table, *, columns=None):
        self.append_table(table)
        self._translate_columns(columns)

    @translate_key_error
    def _translate_columns(self, columns):
        table_header = next(iter(self.table_map.values())).header_map
        if columns is None:
            self._value_indices = tuple(range(len(table_header)))
        else:
            self._value_indices = {table_header[column.lower()]: i for i, column in enumerate(columns)}
            if 0 not in self._value_indices:
                raise InsertError("the value of the column at index 0 must be specified")

    def values(self, rows):
        row_len, num_columns = len(rows[0]), len(self._value_indices)
        if row_len != num_columns:
            raise InsertError(f"table {next(iter(self.table_map))} has {num_columns} columns "
                              f"but {row_len} values were supplied")
        self._inserted_rows = rows

    def run(self):
        table = next(iter(self.table_map.values()))
        with open(table.path) as table_file:
            rows = self._parse_table(table_file)
        existing_ids = {row[0] for row in rows}
        row_len = len(table.header_map)
        rows_to_insert = []
        for row in self._inserted_rows:
            row_id = row[self._value_indices[0]]
            if row_id in existing_ids:
                raise InsertError(f"attempting to store more than one record with id '{row_id}'; aborting the insert")
            existing_ids.add(row_id)
            rows_to_insert.append(
                [row[self._value_indices[i]] if i in self._value_indices else '' for i in range(row_len)])
        with open(table.path, 'a') as table_file:
            table_file.write(self._serialize_rows(rows_to_insert))


class Select(FilteredQuery):
    def __init__(self):
        super().__init__()
        self._on_keys = None
        self._select_keys = None
        self._order_keys = []
        self._order_ascending = True
        self._limit = None

    def from_(self, table):
        self.append_table(table)

    def join(self, table, *, on):
        self.append_table(table)
        if on is not None:
            self._on(on)

    def _on(self, join_keys):
        key1, key2 = self._map_keys(*join_keys)
        if key1.table == key2.table:
            self.get_table_by_index(key1.table).filter = lambda row: row[key1.column] == row[key2.column]
        else:
            self._on_keys = tuple(key.column for key in sorted((key1, key2), key=lambda k: k.table))

    def select(self, columns):
        key_groups = []
        for column in columns:
            matches = re.match(r'(|(?P<table>.+)\.)(?=\*$)', column)
            key_groups.append(self._map_keys(column) if matches is None
                              else self._get_key_set(matches.groupdict()['table']))
        self._select_keys = tuple(itertools.chain.from_iterable(key_groups))

    def order_by(self, ordering_terms):
        columns, ascendings = zip(*ordering_terms)
        self._order_keys = tuple(zip(self._map_keys(*columns), map(operator.not_, ascendings)))

    def limit(self, limit):
        self._limit = limit if limit >= 0 else None

    def run(self):
        filtered_rows = (row for row in self._get_rows() if self._where_filter(row))
        result = ((row[table][column] for table, column in self._select_keys)
                  for row in self._order_and_limit(list(filtered_rows)))
        print(*('|'.join(entry) for entry in result), sep='\n')

    def _get_rows(self):
        tables = []
        for table in self.get_tables_in_query_order():
            with open(table.path) as table_file:
                rows = filter(table.filter, self._parse_table(table_file))
            tables.append(rows)
        if self._on_keys is None:
            return itertools.product(*tables)
        groups0 = collections.defaultdict(list)
        for record in tables[0]:
            groups0[record[self._on_keys[0]]].append(record)
        groups1 = collections.defaultdict(list)
        for record in tables[1]:
            groups1[record[self._on_keys[1]]].append(record)
        return itertools.chain.from_iterable(itertools.product(group, groups1[key]) for key, group in groups0.items())

    def _order_and_limit(self, rows):
        for key, reverse in reversed(self._order_keys):
            def sort_key(row):
                value = converted(row[key.table][key.column])
                return (value != '' if reverse else value == ''), value

            rows.sort(key=sort_key, reverse=reverse)
        return itertools.islice(rows, self._limit)


class Update(FilteredQuery):
    def __init__(self, table):
        super().__init__()
        self.append_table(table)
        self._update_dict = None

    @translate_key_error
    def set(self, update_dict):
        header_map = next(iter(self.table_map.values())).header_map
        self._update_dict = {header_map[col.lower()]: value for col, value in update_dict.items()}

    def run(self):
        table = next(iter(self.table_map.values()))
        with open(table.path) as table_file:
            entries = self._parse_table(table_file)
            updated_entries, seen_ids = [], set()
            for entry in entries:
                updated_entries.append(self._update(entry))
                if entry[0] in seen_ids:
                    raise UpdateError(f"Attempting to store more than one row with id '{entry[0]}'; refusing to update")
                seen_ids.add(entry[0])
        with open(table.path, 'w') as table_file:
            table_file.write(self._serialize_table(updated_entries))

    def _update(self, entry):
        if self._where_filter((entry,)):
            for column, value in self._update_dict.items():
                entry[column] = value
        return entry


class Describe(AbstractQuery):
    def __init__(self, table):
        super().__init__()
        self.append_table(table)

    def run(self):
        print(*next(iter(self.table_map.values())).header_map_case_preserved.keys())

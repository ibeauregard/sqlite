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
        self.tables = []
        self.table_map = {}

    @abstractmethod
    def run(self):
        pass

    def append_table(self, name):
        table_path = Path(self._database_path) / f'{name}{self._file_extension}'
        if not table_path.is_file():
            raise NoSuchTableError(name)
        with open(table_path) as table_file:
            headers = self.strip_and_split(table_file.read().partition(self._record_sep)[0])
        self.tables.append(Table(index=len(self.tables),
                                 name=name,
                                 path=table_path,
                                 header_map={header.lower(): i for i, header in enumerate(headers)},
                                 headers=headers,
                                 filters=[]))
        if name in self.table_map:
            name += f'__{len(self.table_map)}'
        self.table_map[name] = len(self.table_map)

    @classmethod
    def _parse_table(cls, table_file):
        split_file = table_file.read().split(cls._record_sep)
        records = itertools.islice(split_file, 1, len(split_file) - 1)
        return map(cls.strip_and_split, records)

    def _serialize_table(self, records):
        return f"{self._unit_sep.join(self.tables[0].headers)}{self._record_sep}{self._serialize_records(records)}"

    @classmethod
    def _serialize_records(cls, records):
        serialized_records = cls._record_sep.join(cls._unit_sep.join(record) for record in records)
        return f"{serialized_records}{cls._record_sep if serialized_records else ''}"

    @classmethod
    def strip_and_split(cls, line):
        return line.rstrip(cls._record_sep).split(cls._unit_sep)


Table = collections.namedtuple('Table', ['index', 'name', 'path', 'header_map', 'headers', 'filters'])


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
        lowercase_column = key_parts[-1].lower()
        matches = tuple((table.index, table.header_map[lowercase_column])
                        for table in self.tables
                        if lowercase_column in table.header_map and (len(key_parts) == 1 or table.name == key_parts[0]))
        if len(matches) > 1:
            raise AmbiguousColumnNameError(key)
        if not matches:
            raise NoSuchColumnError(key)
        return Key(*matches[0])

    def _get_key_set(self, table):
        try:
            return (Key(*key) for key in
                    (self._get_full_key_set() if table is None
                     else self._get_one_table_key_set(self.tables[self.table_map[table]])))
        except KeyError:
            raise NoSuchTableError(table)

    def _get_full_key_set(self):
        return itertools.chain.from_iterable(self._get_one_table_key_set(table) for table in self.tables)

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
        table = self.tables[0]
        with open(table.path) as table_file:
            records = self._parse_table(table_file)
            non_deleted_records = [record for record in records if not self._where_filter((record,))]
        with open(table.path, 'w') as table_file:
            table_file.write(self._serialize_table(non_deleted_records))


class Insert(AbstractQuery):
    def __init__(self):
        super().__init__()
        self._value_indices = None
        self._insertions = None

    def into(self, table, *, columns=None):
        self.append_table(table)
        self._translate_columns(columns)

    @translate_key_error
    def _translate_columns(self, columns):
        table_header = self.tables[0].header_map
        if columns is None:
            self._value_indices = tuple(range(len(table_header)))
        else:
            self._value_indices = {table_header[column.lower()]: i for i, column in enumerate(columns)}
            if 0 not in self._value_indices:
                raise InsertError("the value of the column at index 0 must be specified")

    def values(self, records):
        record_len, num_columns = len(records[0]), len(self._value_indices)
        if record_len != num_columns:
            raise InsertError(f"table {self.tables[0].name} has {num_columns} columns "
                              f"but {record_len} values were supplied")
        self._insertions = records

    def run(self):
        table = self.tables[0]
        with open(table.path) as table_file:
            records = self._parse_table(table_file)
        existing_ids = {record[0] for record in records}
        record_len = len(table.header_map)
        records_to_insert = []
        for insertion in self._insertions:
            insertion_id = insertion[self._value_indices[0]]
            if insertion_id in existing_ids:
                raise InsertError(f"attempting to store more than one record with id '{insertion_id}'; aborting the insert")
            existing_ids.add(insertion_id)
            records_to_insert.append(
                [insertion[self._value_indices[i]] if i in self._value_indices else '' for i in range(record_len)])
        with open(table.path, 'a') as table_file:
            table_file.write(self._serialize_records(records_to_insert))


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
            self.tables[key1.table].filter.append(lambda record: record[key1.column] == record[key2.column])
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
        joined_rows = (row for row in self._get_rows() if self._where_filter(row))
        result = ((row[table][column] for table, column in self._select_keys)
                  for row in self._order_and_limit(list(joined_rows)))
        print(*('|'.join(row) for row in result), sep='\n')

    def _get_rows(self):
        tables = []
        for table in self.tables:
            with open(table.path) as table_file:
                tables.append(
                    row for row in self._parse_table(table_file) if not table.filters or table.filters[0](row))
        if self._on_keys is None:
            return itertools.product(*tables)
        groups0, groups1 = self._get_groups(tables)
        return itertools.chain.from_iterable(itertools.product(group, groups1[key]) for key, group in groups0.items())

    def _get_groups(self, tables):
        groups = [collections.defaultdict(list), collections.defaultdict(list)]
        for index in range(2):
            for record in tables[index]:
                groups[index][record[self._on_keys[index]]].append(record)
        return groups

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
        header_map = self.tables[0].header_map
        self._update_dict = {header_map[col.lower()]: value for col, value in update_dict.items()}

    def run(self):
        table = self.tables[0]
        with open(table.path) as table_file:
            records = self._parse_table(table_file)
            updated_records, seen_ids = [], set()
            for record in records:
                updated_records.append(self._update(record))
                if record[0] in seen_ids:
                    raise UpdateError(
                        f"Attempting to store more than one record with id '{record[0]}'; refusing to update")
                seen_ids.add(record[0])
        with open(table.path, 'w') as table_file:
            table_file.write(self._serialize_table(updated_records))

    def _update(self, record):
        if self._where_filter((record,)):
            for column, value in self._update_dict.items():
                record[column] = value
        return record


class Describe(AbstractQuery):
    def __init__(self, table):
        super().__init__()
        self.append_table(table)

    def run(self):
        print(*self.tables[0].headers)

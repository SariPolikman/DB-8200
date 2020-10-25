import csv
import json
import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List
from dataclasses_json import dataclass_json
import db_api

'''
TODO:
make a different metadata json file and local variable for each table,
check block size,
take care of date type,
create index : give a 3 options create by it a key_index in the begin...
'''

BLOCK_SIZE = 25
DB_ROOT = Path('db_files')


def update_json_file(path, object_):
    with path.open('w') as _file:
        _file.seek(0)
        _file.write(json.dumps(object_))
        _file.truncate()


def get_table_data_files(table_name):
    for index_file in range((db_metadata[table_name]["count_rows"] // BLOCK_SIZE) + 1):
        yield Path(DB_ROOT / f'{table_name}{index_file}.csv')


def validate_fields(fields, given_fields) -> bool:
    fields_names = [field.name for field in fields]

    for given_filed in given_fields:
        if given_filed not in fields_names:
            break
    else:
        return True
    return False


def get_num_of_file(num):  # TODO call this func
    return num // BLOCK_SIZE


def get_row_index_in_file(num):
    return num % BLOCK_SIZE


def get_row_from_file(file, row_index):
    with open(DB_ROOT / file) as fp:
        reader = csv.reader(fp)
        lines = [x for x in reader if x]
    return lines[row_index - 1]


def write_list_to_csv(file_name, _list):
    with (DB_ROOT / file_name).open("w") as file:
        writer = csv.writer(file)
        writer.writerows(_list)


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[db_api.DBField]
    key_field_name: str
    key_index: dict = field(default_factory=dict)

    # metadata = {"list deleted" :[],  "count_rows":0 , "indexes": ["key"]}  # {list deleted :[],  "count_rows": , "indexes": ["key"]}  -> TableName_metadata.json

    def __post_init__(self):
        my_file = Path(DB_ROOT / f'{self.key_field_name}_index_{self.name}.json')
        if my_file.is_file():
            with (DB_ROOT / f'{self.key_field_name}_index_{self.name}.json').open('r') as metadata_file:
                self.key_index = json.load(metadata_file)

        else:
            with (DB_ROOT / f'{self.key_field_name}_index_{self.name}.json').open('w') as metadata_file:
                json.dump({}, metadata_file)

    def count(self) -> int:
        return db_metadata[self.name]["count_rows"] - len(db_metadata[self.name]["deleted_rows"])

    def insert_record(self, values: Dict[str, Any]) -> None:
        if not self.check_fields_valid(values):
            raise ValueError("Field name not valid")

        if self.key_exist(values[self.key_field_name]):
            raise ValueError("Key exist")

        row_in_file = self.get_free_row()
        file, row_index = self.get_file_and_row(row_in_file)
        self.insert_to_file(file, row_index, values)
        self.add_key_to_key_index(values[self.key_field_name], row_in_file)

    def delete_record(self, key: Any) -> None:
        if not self.key_exist(key):
            raise ValueError("Key not exist")

        location = self.get_row_index_by_key(key)
        self.add_to_deleted_rows_list(location)
        self.delete_keys_from_key_index([key])

    def delete_records(self,
                       criteria: List[db_api.SelectionCriteria]) -> None:  # TODO do not call update fot each line...
        rows = self.query_table(criteria)

        rows_keys = self.get_rows_keys(rows)
        deleted_keys_path = [self.get_row_index_by_key(key) for key in rows_keys]
        db_metadata[self.name]["deleted_rows"] += deleted_keys_path

        self.delete_keys_from_key_index(rows_keys)

        update_json_file(DB_ROOT / 'metadata.json', db_metadata)

    def get_record(self, key: Any) -> Dict[str, Any]:
        if not self.key_exist(key):
            return None

        row_in_file = self.get_row_index_by_key(key)

        file, row_index = self.get_file_and_row(row_in_file)
        row = get_row_from_file(file, row_index)
        row_as_dict = self.get_row_as_dict(row)
        return row_as_dict

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        if not self.key_exist(key):
            raise ValueError("Key not exist")

        if not validate_fields(self.fields, values.keys()):
            raise ValueError("Field not exist")

        row_in_file = self.get_row_index_by_key(key)
        file, row_index = self.get_file_and_row(row_in_file)

        self.update_row_in_file(file, row_index, values)

    def query_table(self, criteria: List[db_api.SelectionCriteria]) -> List[Dict[str, Any]]:
        suitable = []
        for row in self.get_rows_of_first_query(criteria[0]):  # TODO check if there is index everywhere
            for query in criteria[1:]:
                if not self.row_is_suitable(row, query):
                    break
            else:
                suitable.append(row)
        return suitable

    def get_rows_keys(self, rows):
        rows_keys = [row[self.key_field_name] for row in rows]
        return rows_keys

    def check_fields_valid(self, values):
        if len(values) != len(db_metadata[self.name]["DBtable"]["fields"]):
            return False

        return validate_fields(self.fields, values.keys())

    def key_exist(self, key) -> bool:
        if self.key_index.get(str(key)) is None:
            return False
        return True

    def get_file_and_row(self, row_in_file):
        _file = f'{self.name}{get_num_of_file(row_in_file)}.csv'
        return _file, get_row_index_in_file(row_in_file)

    def get_free_row(self):
        global db_metadata
        deleted = db_metadata[self.name]["deleted_rows"]
        if deleted:
            db_metadata[self.name]["deleted_rows"] = deleted[1:]
            row = deleted[0]
        else:
            db_metadata[self.name]["count_rows"] += 1
            row = db_metadata[self.name]["count_rows"]

        update_json_file(DB_ROOT / 'metadata.json', db_metadata)
        return row

    def insert_to_file(self, _file, row_index, values):
        my_file = Path(DB_ROOT / _file)
        if my_file.is_file():
            with (DB_ROOT / _file).open("r") as file:
                reader = csv.reader(file)
                r = [x for x in reader if x]
                if len(r) <= row_index:
                    r.append(self.list_data_dict_in_order(values))
                else:
                    r[row_index - 1] = self.list_data_dict_in_order(values)
        else:
            r = [self.list_data_dict_in_order(values)]

        write_list_to_csv(_file, r)

    def list_data_dict_in_order(self, values):
        data_list = []
        for field in self.fields:
            data_list.append(values[field.name])

        return data_list

    def get_row_index_by_key(self, key):
        return self.key_index[str(key)]

    def delete_keys_from_key_index(self, keys):
        for key in keys:
            del self.key_index[str(key)]

        update_json_file(DB_ROOT / f'{self.key_field_name}_index_{self.name}.json', self.key_index)

    def add_key_to_key_index(self, key, row_in_file):
        self.key_index[str(key)] = row_in_file
        update_json_file(DB_ROOT / f'{self.key_field_name}_index_{self.name}.json', self.key_index)

    def add_to_deleted_rows_list(self, location):
        db_metadata[self.name]["deleted_rows"].append(location)
        update_json_file(DB_ROOT / 'metadata.json', db_metadata)

    def index_exist(self, field: str) -> bool:
        return field in db_metadata[self.name]["indexes"]

    def get_rows_by_index(self, criteria):
        if criteria.field_name == self.key_field_name:
            for key in self.key_index:
                if self.row_is_suitable({self.key_field_name: key}, criteria):
                    row_counter = self.get_row_index_by_key(key)
                    file, row_index = self.get_file_and_row(row_counter)
                    row = get_row_from_file(file, row_index)
                    yield row

    def get_rows_by_full_scan(self, criteria):  # TODO check deleted arr
        for block in get_table_data_files(self.name):
            with block.open("r") as file:
                reader = csv.reader(file)
                for x in reader:
                    if x:
                        row_as_dict = self.get_row_as_dict(x)
                        if self.key_exist(row_as_dict[self.key_field_name]):
                            if self.row_is_suitable(row_as_dict, criteria):
                                yield x

    def get_rows_of_first_query(self, criteria):
        if self.index_exist(criteria.field_name):
            return [self.get_row_as_dict(x) for x in self.get_rows_by_index(criteria)]
        else:
            return [self.get_row_as_dict(x) for x in self.get_rows_by_full_scan(criteria)]

    def row_is_suitable(self, row, query):
        if query.operator == '=':
            query.operator = '=='
        val1 = row[query.field_name]
        val2 = str(query.value)
        return eval('val1' + query.operator + 'val2')

    def get_row_as_dict(self, row):
        return {field.name: column for field, column in zip(self.fields, row)}

    def update_row_in_file(self, _file, row_index, values):
        with (DB_ROOT / _file).open("r") as file:
            reader = csv.reader(file)
            r = [x for x in reader if x]
            new_row = self.get_row_as_dict(r[row_index - 1])

            for key, value in values.items():
                new_row[key] = value

            r[row_index - 1] = self.list_data_dict_in_order(new_row)
        write_list_to_csv(_file, r)

    def create_index(self, field_to_index: str) -> None:
        pass


db_metadata = {}
table_metadata = {}
types = {"str": str, "int": int, "datetime": datetime}


# with (DB_ROOT / "metadata.json").open() as metafile:
# metadata = json.load(metafile)

def create_hash_table(_list, keys_of_hash):
    hash_table = defaultdict(list)
    for row in _list:
        key = ''.join([row[fields] for fields in keys_of_hash])
        hash_table[key].append(row)
    return hash_table


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):

    def __post_init__(self):
        global db_metadata

        my_file = Path(DB_ROOT / 'metadata.json')
        if my_file.is_file():
            with (DB_ROOT / 'metadata.json').open('r') as metadata_file:
                db_metadata = json.load(metadata_file)

        else:

            with (DB_ROOT / 'metadata.json').open('w') as metadata_file:
                json.dump({}, metadata_file)

    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> DBTable:
        global db_metadata
        if self.table_exist(table_name):
            raise ValueError("Table already exist")

        if not validate_fields(fields, [key_field_name]):
            raise ValueError("key field not exist in fields")

        db_metadata[table_name] = {"DBtable": {"fields": [(field.name, field.type.__name__) for field in fields],
                                               "key_field_name": key_field_name},
                                   "count_rows": 0,
                                   "deleted_rows": [],
                                   "indexes": [key_field_name]}
        update_json_file(DB_ROOT / 'metadata.json', db_metadata)
        table = DBTable(table_name, fields, key_field_name)
        return table

    def num_tables(self) -> int:
        return len(self.get_tables_names())

    def get_table(self, table_name: str) -> DBTable:
        if not self.table_exist(table_name):
            raise ValueError("Table not exist")

        DBtable_data = db_metadata[table_name]["DBtable"]
        DBField_data = [db_api.DBField(iter[0], types[iter[1]]) for iter in DBtable_data["fields"]]
        table = DBTable(table_name, DBField_data, DBtable_data["key_field_name"])

        with (DB_ROOT / f'{table.key_field_name}_index_{table.name}.json').open('r') as metadata_file:
            table.key_index = json.load(metadata_file)
        return table

    def delete_table(self, table_name: str) -> None:  # TODO if table empty
        if not self.table_exist(table_name):
            raise ValueError("table not exist")

        global db_metadata

        if db_metadata[table_name]["count_rows"]:
            for file_to_rem in get_table_data_files(table_name):
                file_to_rem.unlink()

        for index in db_metadata[table_name]["indexes"]:
            file_to_rem = Path(DB_ROOT / f'{index}_index_{table_name}.json')
            file_to_rem.unlink()

        del db_metadata[table_name]
        update_json_file(DB_ROOT / 'metadata.json', db_metadata)

    def get_tables_names(self) -> List[Any]:
        return list(db_metadata.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]) -> List[Dict[str, Any]]:

        tables_after_query = [self.get_table(table).query_table(fields_and_values_list[index])
                              for index, table in enumerate(tables)]
        shortest_list = min(tables_after_query, key=len)
        tables_after_query.remove(shortest_list)
        hash_table = create_hash_table(shortest_list, fields_to_join_by)
        print(hash_table)
        for table in tables_after_query:
            for row in table:
                self.merge_to_hash(hash_table, row, fields_to_join_by)

        return [row for row_in_hash in hash_table.values() for row in row_in_hash]

    def table_exist(self, name: str) -> bool:
        return name in self.get_tables_names()

    def merge_to_hash(self, hash_table, row, fields_to_join_by):
        if hash_table.get(''.join([row[fields] for fields in fields_to_join_by])):
            for dict_ in hash_table[''.join([row[fields] for fields in fields_to_join_by])]:
                dict_.update(row)

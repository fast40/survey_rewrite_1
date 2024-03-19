from __future__ import annotations

from typing import TYPE_CHECKING, IO
import pathlib
import zipfile
import csv
import shutil
import random

from config import FILE_DATASETS_FOLDER, TABLE_DATASETS_FOLDER, DATABASE, FILE_DATASETS_COLLECTION, TABLE_DATASETS_COLLECTION

if TYPE_CHECKING:
    import pymongo


def _get_file_dataset_path(dataset_name: str) -> pathlib.Path:
    return FILE_DATASETS_FOLDER.joinpath(dataset_name)


def _get_table_dataset_path(dataset_name: str) -> pathlib.Path:
    return TABLE_DATASETS_FOLDER.joinpath(dataset_name)


def _create_on_disk(dataset_path: pathlib.Path, zip_file: str | IO) -> None:
    if dataset_path.exists():
        raise FileExistsError(f'A dataset already exists at \'{dataset_path}\'')
    
    with zipfile.ZipFile(zip_file) as zf:
        # TODO: always extract to top level dataset folder, not a subfolder
        zf.extractall(dataset_path)  # this will create all necessary parent directories


# thought process with passing dataset name and dataset path is that dataset_path will be used many times in the function that calls this function, so
# no need to generate it inside this function using dataset_name. Same thing with dataset_name. Kind of annoying.
def _create_file_dataset_on_mongo(dataset_name: str, dataset_path: pathlib.Path, client: pymongo.MongoClient) -> None:
    file_paths = [file_path for file_path in dataset_path.rglob('*') if file_path.is_file() and file_path.name[0] != '.']
    random.shuffle(file_paths)

    client[DATABASE][FILE_DATASETS_COLLECTION].insert_many({
        'dataset_name': dataset_name,
        'file_path': str(file_path.relative_to(FILE_DATASETS_FOLDER)),
        'file_path_parts': file_path.relative_to(FILE_DATASETS_FOLDER).parts,
        'loop_number': str(i + 1)  # TODO: get this out of here (should be an extension)
    } for i, file_path in enumerate(file_paths))


def _create_table_dataset_on_mongo(dataset_name: str, dataset_path: pathlib.Path, client: pymongo.MongoClient) -> None:
    csv_path = list(dataset_path.glob('*.csv'))[0]  # TODO: Properly deal with this. I think this should be recorded on file save and passed as a parameter. The issue here is that if there are multiple csv files in the dataset_path folder or something it just seems messy

    # NOTE: I am storing the column names with every row. This seems wasteful and expensive to update.
    # NOTE: I am storing each cell as a dictionary {colname: value}. Some of this code almost certainly depends on these dictionaries having only one key-value pair. There might be a more elegant solution to this.
    with open(csv_path) as csv_file:
        reader = csv.reader(csv_file)

        column_names = next(reader)

        client[DATABASE][TABLE_DATASETS_COLLECTION].insert_many({
            'dataset_name': dataset_name,
            'row': [{column_name: value} for column_name, value in zip(column_names, row)],
            'row_number': str(i + 1)  # TODO: figure out if it would be better to start at 0 or something else. Right now I'm liking 1 corresponding to row 1
        } for i, row in enumerate(reader))


def create_file_dataset(dataset_name: str, zip_file: str | IO, client: pymongo.MongoClient):
    dataset_path = _get_file_dataset_path(dataset_name)

    _create_on_disk(dataset_path, zip_file)  # NOTE: if the dataset already exists, this should throw an error. However, the following line will work fine, which might not be the best way of handling things
    _create_file_dataset_on_mongo(dataset_name, dataset_path, client)


def create_table_dataset(dataset_name: str, zip_file: str | IO, client: pymongo.MongoClient):
    dataset_path = _get_table_dataset_path(dataset_name)

    _create_on_disk(dataset_path, zip_file)  # NOTE: if the dataset already exists, this should throw an error. However, the following line will work fine, which might not be the best way of handling things
    _create_table_dataset_on_mongo(dataset_name, dataset_path, client)


def get_file_datasets(client: pymongo.MongoClient):
    return client[DATABASE][FILE_DATASETS_COLLECTION].distinct('dataset_name')


def get_table_datasets(client: pymongo.MongoClient):
    return client[DATABASE][TABLE_DATASETS_COLLECTION].distinct('dataset_name')


def delete_file_dataset(dataset_name: str, client: pymongo.MongoClient):
    dataset_path = _get_file_dataset_path(dataset_name)

    client[DATABASE][FILE_DATASETS_COLLECTION].delete_many({'dataset_name': dataset_name})
    shutil.rmtree(dataset_path, ignore_errors=True)


def delete_table_dataset(dataset_name: str, client: pymongo.MongoClient):
    dataset_path = _get_table_dataset_path(dataset_name)

    client[DATABASE][TABLE_DATASETS_COLLECTION].delete_many({'dataset_name': dataset_name})
    shutil.rmtree(dataset_path, ignore_errors=True)

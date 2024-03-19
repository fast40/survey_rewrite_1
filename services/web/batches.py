from __future__ import annotations

from typing import TYPE_CHECKING, IO
import pathlib
import zipfile
import random

from config import DATABASE, FILE_DATASETS_COLLECTION, RESPONSES_COLLECTION, FILE_DATASETS_FOLDER

if TYPE_CHECKING:
    import pymongo


def _get_file_dataset_path(dataset_name: str) -> pathlib.Path:
    return FILE_DATASETS_FOLDER.joinpath(dataset_name)


def _create_on_disk(dataset_path: pathlib.Path, zip_file: str | IO) -> None:
    if dataset_path.exists():
        raise FileExistsError(f'A dataset already exists at \'{dataset_path}\'')
    
    with zipfile.ZipFile(zip_file) as zf:
        zf.extractall(dataset_path)


def _create_in_mongo(dataset_name: str, dataset_path: pathlib.Path, client: pymongo.MongoClient):
    for container_folder in dataset_path.glob('*'):
        for batch in container_folder.glob('*'):
            files = [str(path) for path in batch.rglob('*')]
            random.shuffle(files)

            client[DATABASE][FILE_DATASETS_COLLECTION].insert_one({
                'dataset_name': dataset_name,
                'batch_name': batch.name,
                'files': files,
                'uses': 0
            })


def create(dataset_name: str, zip_file: str | IO, client: pymongo.MongoClient):
    dataset_path = _get_file_dataset_path(dataset_name)

    _create_on_disk(dataset_path, zip_file)
    _create_in_mongo(dataset_name, dataset_path, client)

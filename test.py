import pathlib
import pymongo
import random
client = pymongo.MongoClient('mongodb://mongo:27017')
DATABASE = 'soft_hummingbird'
FILE_DATASETS_COLLECTION = 'file_datasets'


def _get_file_dataset_path(dataset_name: str) -> pathlib.Path:
    return pathlib.Path('datasets/file').joinpath(dataset_name)


def _create_in_mongo(dataset_name: str, dataset_path: pathlib.Path, client: pymongo.MongoClient):
    print(dataset_path)
    for container_folder in dataset_path.glob('*'):
        for batch in container_folder.glob('*'):
            files = [str(path) for path in batch.rglob('*')]
            random.shuffle(files)
            print(files[0])

            # client[DATABASE][FILE_DATASETS_COLLECTION].insert_one({
            #     'dataset_name': dataset_name,
            #     'batch_name': batch.name,
            #     'files': files,
            #     'uses': 0
            # })
        
        break


_create_in_mongo('test', _get_file_dataset_path('test'), client)
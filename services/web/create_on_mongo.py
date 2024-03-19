import argparse
import datasets
from config import MONGO_URL
import pymongo


def main():
    parser = argparse.ArgumentParser(description='Create dataset on mongo')

    parser.add_argument('dataset_name', help='Dataset name')

    args = parser.parse_args()

    dataset_path = datasets._get_file_dataset_path(args.dataset_name)

    client = pymongo.MongoClient(MONGO_URL)

    datasets._create_file_dataset_on_mongo(args.dataset_name, dataset_path, client)


if __name__ == '__main__':
    main()

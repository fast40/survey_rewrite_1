from __future__ import annotations

from typing import TYPE_CHECKING

from flask import Response, redirect as flask_redirect

from config import DATABASE, FILE_DATASETS_COLLECTION, RESPONSES_COLLECTION

if TYPE_CHECKING:
    import flask
    import pymongo


def get_file(request: flask.Request, client: pymongo.MongoClient):
    response_id = request.args.get('response_id', str)
    dataset_name = request.args.get('dataset_name', str)
    loop_number = request.args.get('loop_number', int)
    redirect = request.args.get('redirect', str)

    if response_id is None or dataset_name is None or loop_number is None:
        return 'invalid request'

    response = client[DATABASE][RESPONSES_COLLECTION].find_one({'dataset_name': dataset_name, 'response_id': response_id})

    if response is None:
        pipeline = [
            {'$match': {'dataset_name': dataset_name}},
            {'$group': {'_id': None, 'max_uses': {'$max': '$uses'}}}
        ]

        max_uses, = list(client[DATABASE][FILE_DATASETS_COLLECTION].aggregate(pipeline))
        max_uses = max_uses['max_uses']

        pipeline = [
            {"$match": {'dataset_name': dataset_name, "uses": {"$lt": max_uses}}},
            {"$sample": {"size": 1}}
        ]

        documents = list(client[DATABASE][FILE_DATASETS_COLLECTION].aggregate(pipeline))

        if len(documents) == 0:
            print('test')
            pipeline = [
                {"$match": {'dataset_name': dataset_name, "uses": {"$lte": max_uses}}},
                {"$sample": {"size": 1}}
            ]

            documents = list(client[DATABASE][FILE_DATASETS_COLLECTION].aggregate(pipeline))

        document = documents[0]
        
        client[DATABASE][FILE_DATASETS_COLLECTION].update_one({'_id': document['_id']}, {'$inc': {'uses': 1}})

        client[DATABASE][RESPONSES_COLLECTION].insert_one({
            'dataset_name': dataset_name,
            'response_id': response_id,
            'batch': document
        })

        file = document['files'][int(loop_number) - 1]
    else:
        file =  response['batch']['files'][int(loop_number) - 1]

    if redirect.lower() == 'true' or redirect == '1':
        return flask_redirect(file)
    else:
        response = Response(file)
        response.headers.add('Access-Control-Allow-Origin', '*')

        return response

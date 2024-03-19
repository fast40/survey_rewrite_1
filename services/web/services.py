from __future__ import annotations

from typing import TYPE_CHECKING
import random

from flask import jsonify

from config import DATABASE, FILE_DATASETS_COLLECTION, TABLE_DATASETS_COLLECTION, RESPONSES_COLLECTION
from helpers import url_bool, create_file_path_response

if TYPE_CHECKING:
    import pymongo
    import flask



def j2(request: flask.Request, client: pymongo.MongoClient):
    # NOTE: For now, responses must go with exactly one dataset

    '''
    responses take the following form:

    {
        'response_id': 'test',
        'dataset_name': 'test',
        'question_numbers': {
            '1': ['some junk in here'],
            '2': ['etc']
        }
    }
    '''

    response_id = request.args.get('response_id', type=str)
    question_number = request.args.get('question_number', type=int)
    dataset_name = request.args.get('dataset_name', type=str)

    # get response document
    response = client[DATABASE][RESPONSES_COLLECTION].find_one({'response_id': response_id})

    # return the previously generated response if it exists
    if response is not None and str(question_number) in response:
        return response[question_number]

    # if there is no existing reponse, create one
    if response is None:
        client[DATABASE][RESPONSES_COLLECTION].insert_one({
            'response_id': response_id,
            'dataset_name': dataset_name,
            'question_numbers': {}
        })

        row_numbers = client[DATABASE][TABLE_DATASETS_COLLECTION].find({'dataset_name': dataset_name}).distinct('row_number')
    else:
        previously_seen_question_numbers = response['question_numbers'].keys()

        row_numbers = client[DATABASE][TABLE_DATASETS_COLLECTION].find({'dataset_name': dataset_name, 'row_number': {'$nin': previously_seen_question_numbers}}).distinct('row_number')

    pipeline = [
        {'$match': {'row_number': {'$in': row_numbers}}},
        {'$group': {'_id': '$row_number', 'count': {'$sum': 1}}},
        {'$project': {'_id': 0, 'row_number': '$_id', 'count': '$count'}},
        # {'$group': {'_id': None, 'batch_counts': {'$push': {'k': '$_id', 'v': '$count'}}}},
        # {'$replaceRoot': {'newRoot': {'$arrayToObject': '$batch_counts'}}}
    ]

    row_uses = list(client[DATABASE][RESPONSES_COLLECTION].aggregate(pipeline))

    collected_rows = [row['row_number'] for row in row_uses]

    for row_number in row_numbers:
        if row_number not in collected_rows:
            row_uses.append({'row_number': row_number, 'count': 0})

    min_count_row = min(row_uses, key=lambda x: x['count'])

    row_uses = [row for row in row_uses if row['count'] == min_count_row['count']]

    row_choice = random.choice(row_uses)['row_number']



    question_number = request.args.get('question_number', type=int)

    pipeline = [
        {'$match': {'dataset_name': 'jerry'}},
        {'$sample': {'size': 2}},
        {'$project': {
            'row_number': 1,
            # objectToArray does this: {'key1': 'value1', 'key2': 'value2'} -> [{'k': 'key1', 'v': 'value1'}, {'k': 'key2', 'v': 'value2'}]
            'value': {'$objectToArray': {'$arrayElemAt': ['$row', question_number - 1]}},
            'rank': {'$objectToArray': {'$arrayElemAt': ['$row', question_number - 1 + 14]}},
            'error': {'$objectToArray': {'$arrayElemAt': ['$row', question_number - 1 + 28]}},
        }},
        {'$project': {
            '_id': 0,
            'row_number': 1,
            # syntax is $arrayElemAt: [array, index] idk why $value.v is an array but it is. (it's an array of length one)
            'value': {'$arrayElemAt': ['$value.v', 0]},
            'rank': {'$arrayElemAt': ['$rank.v', 0]},
            'error': {'$arrayElemAt': ['$error.v', 0]},
        }}
    ]

    rows = list(client[DATABASE][TABLE_DATASETS_COLLECTION].aggregate(pipeline))

    # return str(row_choice)

    # row_uses_included_rows = [row_use['_id']]

    # for row_number in row_numbers:
    #     if row_number not in

    # try:
    # row_uses = list(client[DATABASE][RESPONSES_COLLECTION].aggregate(pipeline))[0]
    # except IndexError:
    #     row_uses = {}

    # row_uses = {key: row_uses.get(key, 0) for key in row_numbers}

    # pipeline = [
    #     {'$match': {'batch': {'$in': batch_names}}},
    #     {'$group': {'_id': '$batch', 'count': {'$sum': 1}}},
    #     {'$group': {'_id': None, 'batch_counts': {'$push': {'k': '$_id', 'v': '$count'}}}},
    #     {'$replaceRoot': {'newRoot': {'$arrayToObject': '$batch_counts'}}}
    # ]

    # response = client[DATABASE][RESPONSES_COLLECTION].find_one({'response_id': response_id})

    # if response is None:
    #     print(1)
    #     dataset_name = pick_response_dataset(survey_id, client)  # this is the only time survey id is used. It just is used to put the response into a dataset_name
    #     client[DATABASE][RESPONSES_COLLECTION].insert_one({'response_id': response_id, 'dataset_name': dataset_name})
    # elif loop_number in response:
    #     print(2)
    #     return response[loop_number]
    # else:
    #     print(3)
    #     dataset_name = response['dataset_name']

    # print('---------------------')
    # print(dataset_name)
    # print(loop_number)
    # print('---------------------')
    # file_path = client[DATABASE][FILE_DATASETS_COLLECTION].find_one({'dataset_name': dataset_name, 'loop_number': loop_number})['file_path']

    # client[DATABASE][RESPONSES_COLLECTION].update_one({'response_id': response_id}, {'$set': {loop_number: file_path}})

    # return file_path


# initial_answer.csv, meta_abs_error_rank.csv, meta_meta_abs_error.csv
def j(request: flask.Request, client: pymongo.MongoClient):
    question_number = request.args.get('question_number', type=int)

    pipeline = [
        {'$match': {'dataset_name': 'jerry'}},
        {'$sample': {'size': 2}},
        {'$project': {
            'row_number': 1,
            # objectToArray does this: {'key1': 'value1', 'key2': 'value2'} -> [{'k': 'key1', 'v': 'value1'}, {'k': 'key2', 'v': 'value2'}]
            'value': {'$objectToArray': {'$arrayElemAt': ['$row', question_number - 1]}},
            'rank': {'$objectToArray': {'$arrayElemAt': ['$row', question_number - 1 + 14]}},
            'error': {'$objectToArray': {'$arrayElemAt': ['$row', question_number - 1 + 28]}},
        }},
        {'$project': {
            '_id': 0,
            'row_number': 1,
            # syntax is $arrayElemAt: [array, index] idk why $value.v is an array but it is. (it's an array of length one)
            'value': {'$arrayElemAt': ['$value.v', 0]},
            'rank': {'$arrayElemAt': ['$rank.v', 0]},
            'error': {'$arrayElemAt': ['$error.v', 0]},
        }}
    ]

    rows = list(client[DATABASE]
                [TABLE_DATASETS_COLLECTION].aggregate(pipeline))

    # wow this sucks
    # the person at index 0 is 1 if the most accurate person is person 1
    # the person at index 0 is 2 if the most accurate person is person 2
    if int(rows[0]['rank']) < int(rows[1]['rank']):
        rows[0]['person'] = '1'
        rows[1]['person'] = '2'
    else:
        rows[0]['person'] = '2'
        rows[1]['person'] = '1'

    response = jsonify(rows)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


# def m(request: flask.Request, client: pymongo.MongoClient):


def m_pilot(request: flask.Request, client: pymongo.MongoClient):
    batch_name = 'pilot_study_5'

    dataset_name = request.args.get('dataset_name')
    loop_number = request.args.get('loop_number', type=int)  # needs to be passed to mongo as a str, at least at the time of writing this
    redirect = request.args.get('redirect', type=url_bool)

    file_path = client[DATABASE][FILE_DATASETS_COLLECTION].find_one({'dataset_name': dataset_name, 'loop_number': str(loop_number), 'file_path_parts.1': batch_name})['file_path']

    return create_file_path_response(file_path, redirect)


def pick_response_dataset(request: flask.Request, client: pymongo.MongoClient):
    dataset_name = request.args.get('dataset_name')

    batch_names = client[DATABASE][FILE_DATASETS_COLLECTION].find({'dataset_name': dataset_name}).distinct('file_path_parts.1')
    # return str(batch_names)

    pipeline = [
        {'$match': {'dataset_name': dataset_name, 'batch': {'$in': batch_names}}},
        {'$group': {'_id': '$batch', 'count': {'$sum': 1}}},
        {'$project': {'_id': 0, 'k': '$_id', 'v': '$count'}}
        # {'$group': {'_id': None, 'batch_counts': {'$push': {'k': '$_id', 'v': '$count'}}}},
        # {'$replaceRoot': {'newRoot': {'$arrayToObject': '$batch_counts'}}}
    ]

    batches = list(client[DATABASE][RESPONSES_COLLECTION].aggregate(pipeline))

    return str(batches)

    batches = {} if len(batches) == 0 else batches[0]
    batches = {key: batches.get(key, 0) for key in batch_names}

    # min_uses = 

    # candidate_batches = 

    # # aggregation pipeline that gets a random batch from the list of batches with the fewest uses in the responses

    # # TODO: this can and probably should be done with an aggregation pipeline
    # # this basically gets the number of uses on each dataset as indicated by the responses
    # # the code goes on to only include the datasets with the minimum number of views
    # dataset_uses = [{
    #     'dataset_name': dataset_name,
    #     'uses': client[DATABASE][RESPONSES_COLLECTION].count_documents({'dataset_name': dataset_name})
    # } for dataset_name in dataset_names]

    # min_uses = min(dataset_uses, key=lambda x: x['uses'])['uses']

    # candidate_datasets = [dataset['dataset_name'] for dataset in dataset_uses if dataset['uses'] == min_uses]

    # dataset_choice = random.choice(candidate_datasets)

    # return dataset_choice


def get_file_path(response_id, loop_number: str, survey_id: str, client):
    response = client[DATABASE][RESPONSES_COLLECTION].find_one({'response_id': response_id})

    if response is None:
        print(1)
        # this is the only time survey id is used. It just is used to put the response into a dataset_name
        dataset_name = pick_response_dataset(survey_id, client)
        client[DATABASE][RESPONSES_COLLECTION].insert_one(
            {'response_id': response_id, 'dataset_name': dataset_name})
    elif loop_number in response:
        print(2)
        return response[loop_number]
    else:
        print(3)
        dataset_name = response['dataset_name']

    print('---------------------')
    print(dataset_name)
    print(loop_number)
    print('---------------------')
    file_path = client[DATABASE][FILE_DATASETS_COLLECTION].find_one(
        {'dataset_name': dataset_name, 'loop_number': loop_number})['file_path']

    client[DATABASE][RESPONSES_COLLECTION].update_one(
        {'response_id': response_id}, {'$set': {loop_number: file_path}})

    return file_path

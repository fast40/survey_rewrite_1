from flask import Flask, request, render_template, redirect, abort, url_for
import pymongo

import datasets
import services2
import batches
from config import MONGO_URL

client = pymongo.MongoClient(MONGO_URL, connect=False)

app = Flask(__name__)


@app.route('/')
def dataset_dashboard():
    return render_template('dashboard.html',
        file_datasets=datasets.get_file_datasets(client),
        table_datasets=datasets.get_table_datasets(client)
    )


@app.route('/create-dataset', methods=['POST'])
def create_dataset():
    dataset_name = request.form.get('dataset_name')
    dataset_type = request.form.get('dataset_type')

    zip_file = request.files.get('zip_file')  

    if dataset_type == 'file':
        datasets.create_file_dataset(dataset_name, zip_file, client)
    elif dataset_type == 'table':
        datasets.create_table_dataset(dataset_name, zip_file, client)
    elif dataset_type == 'batch':
        batches.create(dataset_name, zip_file, client)
    else:
        abort(400, 'Incorrect dataset type. This should never happen; if it does please notify someone.')

    return redirect(url_for('dataset_dashboard'))


@app.route('/delete-dataset')
def delete_dataset():
    dataset_name = request.args.get('dataset_name')
    dataset_type = request.args.get('dataset_type')

    if dataset_type == 'file':
        datasets.delete_file_dataset(dataset_name, client)
    elif dataset_type == 'table':
        datasets.delete_table_dataset(dataset_name, client)
    else:
        abort(400, 'Incorrect dataset type. This should never happen; if it does please notify someone.')

    return redirect(url_for('dataset_dashboard'))


@app.route('/service/<service_name>')
def service(service_name):
    service = getattr(services2, service_name)

    return service(request, client)

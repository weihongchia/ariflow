#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import glob
import os
import json
from google.cloud import storage


def upload_directory_to_gcs(directory, bucket):
    for local_file in glob.glob(directory + '/**'):
        if not os.path.isfile(local_file):
            upload_directory_to_gcs(local_file, bucket)
        else:
            blob = bucket.blob(local_file)
            print(f'Uploading {local_file}')
            blob.upload_from_filename(local_file)


def remove_unused_files_from_gcs(directory, bucket):
    blobs = bucket.list_blobs(prefix=f"{directory}/")
    files = [blob.name for blob in blobs]
    for file in files:
        if not os.path.exists(file):
            print(f"Removing {file}")
            bucket.delete_blob(file)


def deploy_directory(directory, bucket):
    upload_directory_to_gcs(directory, bucket)
    remove_unused_files_from_gcs(directory, bucket)


if __name__ == '__main__':
    if not os.path.exists('dags'):
        raise Exception('Please run this script at the root of the project.')

    bucket_name = os.environ.get('BUCKET_NAME')
    service_account = os.environ.get('SERVICE_ACCOUNT')
    if not bucket_name or not service_account:
        raise Exception('Please set BUCKET_NAME and SERVICE_ACCOUNT environment variables.')

    service_account = json.loads(service_account)
    client = storage.Client.from_service_account_info(service_account)
    bucket = storage.Bucket(client, bucket_name)
    deploy_directory('plugins', bucket)
    deploy_directory('dags', bucket)

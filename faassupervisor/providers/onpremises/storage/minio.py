# Copyright (C) GRyCAP - I3M - UPV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3
import os
from urllib.parse import unquote_plus
import faassupervisor.utils as utils
from faassupervisor.interfaces.dataprovider import DataProviderInterface

logger = utils.get_logger()

class Minio(DataProviderInterface):

    @utils.lazy_property
    def client(self):
        client = boto3.client('s3', endpoint_url='http://minio-service.minio:9000',
                              aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                              aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        return client

    def __init__(self, event, output_folder):
        self.event = event
        self.os_tmp_folder = utils.get_tmp_dir()
        self.output_folder = output_folder
    
    @classmethod    
    def is_minio_event(cls, event):
        if utils.is_key_and_value_in_dictionary('Records', event):
            return event['Records'][0]['eventSource'] == 'minio:s3'
        return False
    
    def get_record(self):
        return self.event['Records'][0]['s3']    
    
    def download_input(self):
        '''Downloads the file from the minio bucket and returns the path were the download is placed'''
        record_info = self.get_record()
        bucket = record_info['bucket']['name']
        key = unquote_plus(record_info['object']['key'])
        file_name = os.path.splitext(key)[0]
        file_download_path = "{0}/{1}".format(self.os_tmp_folder, file_name) 
        print("Downloading item from bucket '{0}' with key '{1}'".format(bucket, key))
        utils.create_folder(self.os_tmp_folder)
        with open(file_download_path, 'wb') as data:
            self.client.download_fileobj(bucket, key, data)
        print("Successful download of file '{0}' from bucket '{1}' in path '{2}'".format(key, bucket, file_download_path))
        return file_download_path
    
    def upload_output(self):
        output_files_path = utils.get_all_files_in_directory(self.output_folder)
        output_bucket = os.environ['OUTPUT_BUCKET']
        print("UPLOADING FILES {0}".format(output_files_path))
        for file_path in output_files_path:
            file_name = file_path.replace("{0}/".format(self.output_folder), "")
            output_file_name = "{0}-out{1}".format(os.path.splitext(file_name)[0],''.join(os.path.splitext(file_name)[1:]))
            self.upload_file(output_bucket, file_path, output_file_name)
    
    def upload_file(self, bucket_name, file_path, file_key):
        print("Uploading file  '{0}' to bucket '{1}'".format(file_key, bucket_name))
        with open(file_path, 'rb') as data:
            self.client.upload_fileobj(data, bucket_name, file_key)

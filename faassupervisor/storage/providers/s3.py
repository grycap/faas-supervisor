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

from faassupervisor.storage.storage import DefaultStorageProvider
from faassupervisor.utils import lazy_property, join_paths, get_all_files_in_directory, get_environment_variable
import boto3
import faassupervisor.logger as logger

class S3(DefaultStorageProvider):
    
    @lazy_property
    def client(self):
        client = boto3.client('s3')
        return client
    
    def __init__(self, **kwargs):
        self.storage_auth = kwargs['Auth']
        # This is the output bucket in case of OUTPUT storage
        # Contains at least BUCKET_NAME/FUNCTION_NAME
        self.storage_path = kwargs['Path']    

    def download_input(self, event, input_dir_path):
        file_download_path = join_paths(input_dir_path, event.data.file_name)
        '''Downloads the file from the S3 bucket and returns the path were the download is placed'''
        logger.get_logger().info("Downloading item from bucket '{0}' with key '{1}'".format(event.data.bucket_name, event.data.object_key))
        with open(file_download_path, 'wb') as data:
            self.client.download_fileobj(event.data.bucket_name, event.data.object_key, data)
        logger.get_logger().info("Successful download of file '{0}' from bucket '{1}' in path '{2}'".format(event.data.object_key, 
                                                                                               event.data.bucket_name,
                                                                                               file_download_path))
        return file_download_path
  
    def _get_file_key(self, file_name):
        storage_path = self.storage_path.path.split('/')
        #Path format => storage_path.path: bucket/<folder-path> 
        #Last part is optional
        if len(storage_path) > 1:
            # There is a folder defined
            # Set the folder in the file path
            folder = "{0}".format("/".join(storage_path[1:]))
            file_key = "{0}/{1}".format(folder, file_name)
        else:
            # Set the default file path
            file_key = "{0}/{1}/{2}/{3}".format(get_environment_variable("AWS_LAMBDA_FUNCTION_NAME"),
                                                'output',
                                                get_environment_variable("AWS_LAMBDA_REQUEST_ID"),
                                                file_name)
        return file_key

    def _get_bucket_name(self):
        return self.storage_path.path.split("/")[0]

    def upload_output(self, output_dir_path):
        output_files = get_all_files_in_directory(output_dir_path)
        logger.get_logger().info("Found the following files to upload: {0}".format(output_files))
        for file_path in output_files:
            file_name = file_path.replace("{0}/".format(output_dir_path), "")
            file_key = self._get_file_key(file_name)
            self._upload_file(file_path, file_key)
            
    def _upload_file(self, file_path, file_key):
        bucket_name = self._get_bucket_name()
        logger.get_logger().info("Uploading file  '{0}' to bucket '{1}'".format(file_key, bucket_name))
        with open(file_path, 'rb') as data:
            self.client.upload_fileobj(data, bucket_name, file_key)
        logger.get_logger().info("Changing ACLs for public-read for object in bucket {0} with key {1}".format(bucket_name, file_key))
        obj = boto3.resource('s3').Object(bucket_name, file_key)
        obj.Acl().put(ACL='public-read')
        
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
""" Module containing all the classes and methods
related with the Minio storage provider. """

import boto3
from faassupervisor.logger import get_logger
from faassupervisor.storage.providers.default import DefaultStorageProvider
from faassupervisor.utils import lazy_property, SysUtils


class Minio(DefaultStorageProvider):
    """ Class that manages downloads and uploads from Minio. """

    _DEFAULT_MINIO_ENDPOINT = 'http://minio-service.minio:9000'

    @lazy_property
    def client(self):
        """ Return Minio client with user configuration. """
        client = boto3.client('s3', endpoint_url=self._DEFAULT_MINIO_ENDPOINT,
                              aws_access_key_id=self.storage_auth.get_auth_var('USER'),
                              aws_secret_access_key=self.storage_auth.get_auth_var('PASS'))
        return client

    def download_file(self, event, input_dir_path):
        """ Downloads a file from a minio bucket. """
        file_download_path = SysUtils.join_paths(input_dir_path, event.event.file_name)
        get_logger().info("Downloading item from bucket '%s' with key '%s'",
                          event.event.bucket_name,
                          event.event.file_name)

        with open(file_download_path, 'wb') as data:
            self.client.download_fileobj(event.event.bucket_name, event.event.file_name, data)
        get_logger().info("Successful download of file '%s' from bucket '%s' in path '%s'",
                          event.event.file_name,
                          event.event.bucket_name,
                          file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name):
        get_logger().info("Uploading file '%s' to bucket '%s'", file_name, self.storage_path.path)
        with open(file_path, 'rb') as data:
            self.client.upload_fileobj(data, self.storage_path.path, file_name)

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
related with the S3 storage provider. """

import boto3
from faassupervisor.logger import get_logger
from faassupervisor.storage.providers import DefaultStorageProvider, \
    get_bucket_name, get_file_key
from faassupervisor.utils import FileUtils, SysUtils


class S3(DefaultStorageProvider):
    """Class that manages downloads and uploads from S3."""

    _TYPE = 'S3'

    def __init__(self, stg_auth):
        super().__init__(stg_auth)
        self.client = self._get_client()

    def _get_client(self):
        """Returns S3 client with default configuration."""
        if self.stg_auth.creds is None:
            return boto3.client('s3')
        region = self.stg_auth.get_credential('region')
        if region == '':
            region = None
        return boto3.client('s3',
                            region_name=region,
                            aws_access_key_id=self.stg_auth.get_credential('access_key'),
                            aws_secret_access_key=self.stg_auth.get_credential('secret_key'))

    def download_file(self, parsed_event, input_dir_path):
        """Downloads the file from the S3 bucket and
        returns the path were the download is placed."""
        file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
        get_logger().info('Downloading item from bucket \'%s\' with key \'%s\'',
                          parsed_event.bucket_name,
                          parsed_event.object_key)
        with open(file_download_path, 'wb') as data:
            self.client.download_fileobj(parsed_event.bucket_name,
                                         parsed_event.object_key,
                                         data)
        get_logger().info('Successful download of file \'%s\' from bucket \'%s\' in path \'%s\'',
                          parsed_event.object_key,
                          parsed_event.bucket_name,
                          file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name, output_path):
        """Uploads the file to the S3 output path."""
        file_key = get_file_key(output_path, file_name)
        bucket_name = get_bucket_name(output_path)
        get_logger().info('Uploading file \'%s\' to bucket \'%s\'', file_key, bucket_name)
        with open(file_path, 'rb') as data:
            self.client.upload_fileobj(data, bucket_name, file_key)

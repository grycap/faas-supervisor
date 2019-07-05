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
from faassupervisor.storage.providers import DefaultStorageProvider
from faassupervisor.utils import SysUtils


class Minio(DefaultStorageProvider):
    """ Class that manages downloads and uploads from Minio. """

    _DEFAULT_MINIO_ENDPOINT = 'http://minio-service.minio:9000'
    _TYPE = 'MINIO'

    def _get_client(self):
        """Return Minio client with user configuration."""
        endpoint = SysUtils.get_env_var('MINIO_ENDPOINT')
        if not endpoint:
            endpoint = self._DEFAULT_MINIO_ENDPOINT
        return boto3.client('s3', endpoint_url=endpoint,
                            aws_access_key_id=self.stg_auth.get_credential('USER'),
                            aws_secret_access_key=self.stg_auth.get_credential('PASS'))

    def download_file(self, parsed_event, input_dir_path):
        """Downloads a file from a minio bucket."""
        file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
        get_logger().info("Downloading item from bucket '%s' with key '%s'",
                          parsed_event.bucket_name,
                          parsed_event.file_name)

        with open(file_download_path, 'wb') as data:
            self._get_client().download_fileobj(parsed_event.bucket_name,
                                                parsed_event.file_name,
                                                data)
        get_logger().info("Successful download of file '%s' from bucket '%s' in path '%s'",
                          parsed_event.file_name,
                          parsed_event.bucket_name,
                          file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name):
        """Uploads a file to a minio bucket."""
        get_logger().info("Uploading file '%s' to bucket '%s'", file_name, self.stg_path)
        with open(file_path, 'rb') as data:
            self._get_client().upload_fileobj(data, self.stg_path, file_name)

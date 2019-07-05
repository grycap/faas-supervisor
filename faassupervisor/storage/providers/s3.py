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
from faassupervisor.storage.providers import DefaultStorageProvider
from faassupervisor.utils import SysUtils


def _get_client():
    """Returns S3 client with default configuration."""
    return boto3.client('s3')


def _set_file_acl(bucket_name, file_key):
    obj = boto3.resource('s3').Object(bucket_name, file_key)
    obj.Acl().put(ACL='public-read')


class S3(DefaultStorageProvider):
    """Class that manages downloads and uploads from S3."""

    _TYPE = 'S3'

    def _get_file_key(self, file_name):
        stg_path = self.stg_path.split('/', 1)
        # Path format => stg_path: bucket/<folder-path>
        # Last part is optional
        if len(stg_path) > 1:
            # There is a folder defined
            # Set the folder in the file path
            file_key = f"{stg_path[1]}/{file_name}"
        else:
            # Set the default file path
            file_key = (f"{SysUtils.get_env_var('AWS_LAMBDA_FUNCTION_NAME')}/output/"
                        f"{SysUtils.get_env_var('AWS_LAMBDA_REQUEST_ID')}/{file_name}")
        return file_key

    def _get_bucket_name(self):
        return self.stg_path.split("/")[0]

    def download_file(self, parsed_event, input_dir_path):
        """ Downloads the file from the S3 bucket and
        returns the path were the download is placed. """
        file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
        get_logger().info("Downloading item from bucket '%s' with key '%s'",
                          parsed_event.bucket_name,
                          parsed_event.object_key)
        with open(file_download_path, 'wb') as data:
            _get_client().download_fileobj(parsed_event.bucket_name,
                                           parsed_event.object_key,
                                           data)
        get_logger().info("Successful download of file '%s' from bucket '%s' in path '%s'",
                          parsed_event.object_key,
                          parsed_event.bucket_name,
                          file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name):
        file_key = self._get_file_key(file_name)
        bucket_name = self._get_bucket_name()
        get_logger().info("Uploading file '%s' to bucket '%s'", file_key, bucket_name)
        with open(file_path, 'rb') as data:
            _get_client().upload_fileobj(data, bucket_name, file_key)

        get_logger().info("Changing ACLs for public-read for object in bucket '%s' with key '%s'",
                          bucket_name,
                          file_key)
        _set_file_acl(bucket_name, file_key)

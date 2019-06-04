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
from faassupervisor.storage.providers.default import DefaultStorageProvider
from faassupervisor.utils import SysUtils


def s3_client():
    """ Return S3 client with default configuration. """
    return boto3.client('s3')


class S3(DefaultStorageProvider):
    """ Class that manages downloads and uploads from S3. """

    def _get_file_key(self, file_name):
        # The storage_path contains at least BUCKET_NAME/FUNCTION_NAME
        storage_path = self.storage_path.path.split('/')
        # Path format => storage_path.path: bucket/<folder-path>
        # Last part is optional
        if len(storage_path) > 1:
            # There is a folder defined
            # Set the folder in the file path
            folder = "{0}".format("/".join(storage_path[1:]))
            file_key = "{0}/{1}".format(folder, file_name)
        else:
            # Set the default file path
            file_key = "{0}/{1}/{2}/{3}".format(SysUtils.get_env_var("AWS_LAMBDA_FUNCTION_NAME"),
                                                'output',
                                                SysUtils.get_env_var("AWS_LAMBDA_REQUEST_ID"),
                                                file_name)
        return file_key

    def _get_bucket_name(self):
        return self.storage_path.path.split("/")[0]

    def download_file(self, event, input_dir_path):
        """ Downloads the file from the S3 bucket and
        returns the path were the download is placed. """
        file_download_path = SysUtils.join_paths(input_dir_path, event.event.file_name)
        get_logger().info("Downloading item from bucket '%s' with key '%s'",
                          event.event.bucket_name,
                          event.event.object_key)
        with open(file_download_path, 'wb') as data:
            s3_client().download_fileobj(event.event.bucket_name, event.event.object_key, data)
        get_logger().info("Successful download of file '%s' from bucket '%s' in path '%s'",
                          event.event.object_key,
                          event.event.bucket_name,
                          file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name):
        file_key = self._get_file_key(file_name)
        bucket_name = self._get_bucket_name()
        get_logger().info("Uploading file '%s' to bucket '%s'", file_key, bucket_name)
        with open(file_path, 'rb') as data:
            s3_client().upload_fileobj(data, bucket_name, file_key)

        get_logger().info("Changing ACLs for public-read for object in bucket '%s' with key '%s'",
                          bucket_name,
                          file_key)
        obj = boto3.resource('s3').Object(bucket_name, file_key)
        obj.Acl().put(ACL='public-read')

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
""" This module provides a generic class
used to define storage providers."""

import abc


def get_bucket_name(output_path):
    """Returns the bucket name from a defined output path."""
    return output_path.split('/')[0]


def get_file_key(output_path, file_name):
    """Returns the correct 'file_key' required for uploading files."""
    stg_path = output_path.split('/', 1)
    # Path format => stg_path: bucket/<folder-path>
    # Last part is optional
    if len(stg_path) > 1:
        # There is a folder defined
        # Set the folder in the file path
        return f'{stg_path[1]}/{file_name}'
    return file_name


class DefaultStorageProvider(metaclass=abc.ABCMeta):
    """All the different data providers must inherit from this class
    to ensure that the commands are defined consistently."""

    _TYPE = 'DEFAULT'

    def __init__(self, stg_auth):
        self.stg_auth = stg_auth

    @abc.abstractmethod
    def download_file(self, parsed_event, input_dir_path):
        """Generic method to be implemented by all the storage providers."""

    @abc.abstractmethod
    def upload_file(self, file_path, file_name, output_path):
        """Generic method to be implemented by all the storage providers."""

    def get_type(self):
        """Returns the storage type.
        Can be LOCAL, MINIO, ONEDATA, S3."""
        return self._TYPE

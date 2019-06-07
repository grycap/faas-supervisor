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


class DefaultStorageProvider(metaclass=abc.ABCMeta):
    """All the different data providers must inherit from this class
    to ensure that the commands are defined consistently."""

    _TYPE = 'DEFAULT'

    def __init__(self, stg_auth, stg_path=None):
        self.stg_auth = stg_auth
        self.stg_path = stg_path

    @abc.abstractmethod
    def download_file(self, parsed_event, input_dir_path):
        """Generic method to be implemented by all the storage providers."""

    @abc.abstractmethod
    def upload_file(self, file_path, file_name):
        """Generic method to be implemented by all the storage providers."""

    def get_type(self):
        """Returns the storage type.
        Can be LOCAL, MINIO, ONEDATA, S3."""
        return self._TYPE

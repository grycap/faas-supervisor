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
"""This module defines a generic storage
provider to be used by the generic supervisor."""

from faassupervisor.storage.providers.minio import Minio
from faassupervisor.storage.providers.onedata import Onedata
from faassupervisor.storage.providers.s3 import S3
from faassupervisor.logger import get_logger
from faassupervisor.utils import FileUtils
from faassupervisor.exceptions import InvalidStorageProviderError


def _get_storage_provider(storage_auth, storage_path):
    """ Returns the storage provider needed based on the authentication type defined. """
    provider = ""
    if storage_auth.type == 'MINIO':
        provider = Minio(storage_auth, storage_path)
    elif storage_auth.type == 'ONEDATA':
        provider = Onedata(storage_auth, storage_path)
    elif storage_auth.type == 'S3':
        provider = S3(storage_auth, storage_path)
    else:
        raise InvalidStorageProviderError(storage_type=storage_auth.type)
    return provider


class StorageProvider():
    """ Generic storage provider that manages all the common
    calls used by the specific providers. """

    def __init__(self, storage_auth, storage_path):
        self.provider = _get_storage_provider(storage_auth, storage_path)

    def download_input(self, event, input_dir_path):
        """Receives the event where the file information is and
        the tmp_dir_path where to store the downloaded file.

        Returns the file path where the file is downloaded."""

        return self.provider.download_file(event, input_dir_path)

    def upload_output(self, output_dir_path):
        """Receives the tmp_dir_path where the files to upload are stored and
        uploads all the files found there."""

        get_logger().info("Searching for files to upload in folder '%s'", output_dir_path)
        output_files = FileUtils.get_all_files_in_dir(output_dir_path)
        get_logger().info("Found the following files to upload: '%s'", output_files)
        for file_path in output_files:
            file_name = file_path.replace("{0}/".format(output_dir_path), "")
            self.provider.upload_file(file_path, file_name)

    def get_type(self):
        """Returns the name of the class of the created provider."""
        
        return self.provider.__class__.__name__

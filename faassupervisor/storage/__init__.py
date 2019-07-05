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

from collections import namedtuple
from faassupervisor.utils import SysUtils
from faassupervisor.storage.providers.minio import Minio
from faassupervisor.storage.providers.onedata import Onedata
from faassupervisor.storage.providers.local import Local
from faassupervisor.storage.providers.s3 import S3
from faassupervisor.logger import get_logger
from faassupervisor.utils import FileUtils
from faassupervisor.exceptions import InvalidStorageProviderError


def create_provider(storage_auth, storage_path=None):
    """Returns the storage provider needed
    based on the authentication type defined.

    If not storage auth provided, use local storage."""
    if not storage_auth:
        provider = Local(storage_auth, storage_path)
    elif storage_auth.type == 'MINIO':
        provider = Minio(storage_auth, storage_path)
    elif storage_auth.type == 'ONEDATA':
        provider = Onedata(storage_auth, storage_path)
    elif storage_auth.type == 'S3':
        provider = S3(storage_auth, storage_path)
    else:
        raise InvalidStorageProviderError(storage_type=storage_auth.type)
    return provider


def download_input(storage_provider, parsed_event, input_dir_path):
    """Receives the event where the file information is and
    the tmp_dir_path where to store the downloaded file.

    Returns the file path where the file is downloaded."""
    return storage_provider.download_file(parsed_event, input_dir_path)


def upload_output(storage_provider, output_dir_path):
    """Receives the tmp_dir_path where the files to upload are stored and
    uploads all the files found there."""

    get_logger().info("Searching for files to upload in folder '%s'", output_dir_path)
    output_files = FileUtils.get_all_files_in_dir(output_dir_path)
    get_logger().info("Found the following files to upload: '%s'", output_files)
    for file_path in output_files:
        file_name = file_path.replace(f"{output_dir_path}/", "")
        storage_provider.upload_file(file_path, file_name)


def get_output_paths():
    """Returns the defined output providers.

    Reads the global variables to create the providers needed.
    Variable schema: STORAGE_PATH_$1_$2
                     $1: INPUT | OUTPUT
                     $2: STORAGE_ID (Specified in the function definition file,
                                     is unique for each storage defined)
    e.g.: STORAGE_PATH_INPUT_12345
    """
    get_logger().info("Reading output path variables")
    env_vars = SysUtils.get_filtered_env_vars("STORAGE_PATH_")
    storage_path = namedtuple('storage_path', ['id', 'path'])
    # Remove the storage type 'OUTPUT_' and store only the id and the path
    # Store a tuple, so the information can't be modified
    return [storage_path(env_key[7:], env_val) for env_key, env_val in env_vars.items()
            if env_key.startswith('OUTPUT_')]

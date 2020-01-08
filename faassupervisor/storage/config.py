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
"""Class to parse, store and manage storage information."""
from faassupervisor.utils import ConfigUtils, FileUtils
from faassupervisor.exceptions import StorageAuthError, \
    InvalidStorageProviderError, exception
from faassupervisor.storage.providers.local import Local
from faassupervisor.storage.providers.minio import Minio
from faassupervisor.storage.providers.onedata import Onedata
from faassupervisor.storage.providers.s3 import S3
from faassupervisor.logger import get_logger


def create_provider(storage_auth):
    """Returns the storage provider needed
    based on the authentication type defined.

    If not storage auth provided, use local storage."""
    if not storage_auth:
        provider = Local(storage_auth)
    elif storage_auth.type == 'MINIO':
        provider = Minio(storage_auth)
    elif storage_auth.type == 'ONEDATA':
        provider = Onedata(storage_auth)
    elif storage_auth.type == 'S3':
        provider = S3(storage_auth)
    else:
        raise InvalidStorageProviderError(storage_type=storage_auth.type)
    return provider


class AuthData():
    """Stores provider authentication values."""

    def __init__(self, storage_type, credentials):
        self.type = storage_type
        self.creds = credentials

    def get_credential(self, key):
        """Return authentication credentials previously stored."""
        return self.creds.get(key, '')


class StorageConfig():
    """Parses providers authentication variables and defined outputs."""

    def __init__(self):
        # Create s3_auth with empty credentials
        self.s3_auth = AuthData('S3', None)
        self.minio_auth = None
        self.onedata_auth = None
        self.output = []
        self._parse_config()

    @exception()
    def _parse_config(self):
        output = ConfigUtils.read_cfg_var('output')
        # Output list
        if output != '':
            self.output = output
        else:
            get_logger().warning('There is no output defined for this function.')
        storage_providers = ConfigUtils.read_cfg_var('storage_providers')
        if (storage_providers and
                storage_providers is not ''):
            # s3 storage provider auth
            if ('s3' in storage_providers
                    and storage_providers['s3']):
                self._validate_s3_creds(storage_providers['s3'])
            # minio storage provider auth
            if ('minio' in storage_providers
                    and storage_providers['minio']):
                self._validate_minio_creds(storage_providers['minio'])
            # onedata storage provider auth
            if ('onedata' in storage_providers
                    and storage_providers['onedata']):
                self._validate_onedata_creds(storage_providers['onedata'])
        else:
            get_logger().warning('There is no storage provider defined for this function.')

    def _validate_minio_creds(self, minio_creds):
        if (isinstance(minio_creds, dict)
                and 'access_key' in minio_creds
                and minio_creds['access_key'] is not None
                and minio_creds['access_key'] != ''
                and 'secret_key' in minio_creds
                and minio_creds['secret_key'] is not None
                and minio_creds['secret_key'] != ''):
            self.minio_auth = AuthData('MINIO', minio_creds)
        else:
            raise StorageAuthError(auth_type='MINIO')

    def _validate_s3_creds(self, s3_creds):
        if (isinstance(s3_creds, dict)
                and 'access_key' in s3_creds
                and s3_creds['access_key'] is not None
                and s3_creds['access_key'] != ''
                and 'secret_key' in s3_creds
                and s3_creds['secret_key'] is not None
                and s3_creds['secret_key'] != ''):
            self.s3_auth = AuthData('S3', s3_creds)
        else:
            raise StorageAuthError(auth_type='S3')

    def _validate_onedata_creds(self, onedata_creds):
        if (isinstance(onedata_creds, dict)
                and 'oneprovider_host' in onedata_creds
                and onedata_creds['oneprovider_host'] is not None
                and onedata_creds['oneprovider_host'] != ''
                and 'token' in onedata_creds
                and onedata_creds['token'] is not None
                and onedata_creds['token'] != ''
                and 'space' in onedata_creds
                and onedata_creds['space'] is not None
                and onedata_creds['space'] != ''):
            self.onedata_auth = AuthData('ONEDATA', onedata_creds)
        else:
            raise StorageAuthError(auth_type='ONEDATA')

    def get_auth_data_by_stg_type(self, storage_type):
        """Returns the authentication credentials by its type."""
        if storage_type == 'S3':
            return self.s3_auth
        elif storage_type == 'MINIO':
            return self.minio_auth
        elif storage_type == 'ONEDATA':
            return self.onedata_auth
        return None

    def download_input(self, parsed_event, input_dir_path):
        """Receives the event where the file information is and
        the tmp_dir_path where to store the downloaded file.

        Returns the file path where the file is downloaded."""
        event_type = parsed_event.get_type()
        auth_data = self.get_auth_data_by_stg_type(event_type)
        stg_provider = create_provider(auth_data)
        get_logger().info('Found \'%s\' input provider', stg_provider.get_type())
        return stg_provider.download_file(parsed_event, input_dir_path)

    def upload_output(self, output_dir_path):
        """Receives the tmp_dir_path where the files to upload are stored and
        uploads files whose name matches the prefixes and suffixes specified
        in 'output'."""
        get_logger().info('Searching for files to upload in folder \'%s\'', output_dir_path)
        output_files = FileUtils.get_all_files_in_dir(output_dir_path)
        stg_providers = {}
        # Filter files by prefix and suffix
        for output in self.output:
            get_logger().info('Checking files for uploading to \'%s\' on path: \'%s\'',
                              output['storage_provider'].upper(),
                              output['path'])
            for file_path in output_files:
                file_name = file_path.replace(f'{output_dir_path}/', '')
                prefix_ok = False
                suffix_ok = False
                # Check prefixes
                if ('prefix' not in output
                        or len(output['prefix']) == 0):
                    prefix_ok = True
                else:
                    for pref in output['prefix']:
                        if file_name.startswith(pref):
                            prefix_ok = True
                            break
                if prefix_ok:
                    # Check suffixes
                    if ('suffix' not in output
                            or len(output['suffix']) == 0):
                        suffix_ok = True
                    else:
                        for suff in output['suffix']:
                            if file_name.endswith(suff):
                                suffix_ok = True
                                break
                    # Only upload file if name matches the prefixes and suffixes
                    if suffix_ok:
                        out_type = output['storage_provider'].upper()
                        if out_type not in stg_providers:
                            auth_data = self.get_auth_data_by_stg_type(out_type)
                            stg_providers[out_type] = create_provider(auth_data)
                        stg_providers[out_type].upload_file(file_path, file_name, output['path'])

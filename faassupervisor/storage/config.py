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

from faassupervisor.utils import ConfigUtils, FileUtils, StrUtils
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
        self.s3_auth = {'default': AuthData('S3', None)}
        self.minio_auth = {}
        self.onedata_auth = {}
        self.input = []
        self.output = []
        self._parse_config()

    @exception()
    def _parse_config(self):
        # Read output list
        output = ConfigUtils.read_cfg_var('output')
        if output != '':
            self.output = output
        else:
            get_logger().warning('There is no output defined for this function.')
        # Read input list
        input = ConfigUtils.read_cfg_var('input')
        if input != '':
            self.input = input
        else:
            get_logger().warning('There is no input defined for this function.')
        # Read storage_providers dict
        storage_providers = ConfigUtils.read_cfg_var('storage_providers')
        if (storage_providers and
                storage_providers != ''):
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
        if isinstance(minio_creds, dict):
            for provider_id in minio_creds:
                if ('access_key' in minio_creds[provider_id]
                        and minio_creds[provider_id]['access_key'] is not None
                        and minio_creds[provider_id]['access_key'] != ''
                        and 'secret_key' in minio_creds[provider_id]
                        and minio_creds[provider_id]['secret_key'] is not None
                        and minio_creds[provider_id]['secret_key'] != ''):
                    self.minio_auth[provider_id] = AuthData('MINIO', minio_creds[provider_id])
                else:
                    raise StorageAuthError(auth_type='MINIO')
        else:
            raise StorageAuthError(auth_type='MINIO')

    def _validate_s3_creds(self, s3_creds):
        if isinstance(s3_creds, dict):
            for provider_id in s3_creds:
                if ('access_key' in s3_creds[provider_id]
                        and s3_creds[provider_id]['access_key'] is not None
                        and s3_creds[provider_id]['access_key'] != ''
                        and 'secret_key' in s3_creds[provider_id]
                        and s3_creds[provider_id]['secret_key'] is not None
                        and s3_creds[provider_id]['secret_key'] != ''):
                    self.s3_auth[provider_id] = AuthData('S3', s3_creds[provider_id])
                else:
                    raise StorageAuthError(auth_type='S3')
        else:
            raise StorageAuthError(auth_type='S3')

    def _validate_onedata_creds(self, onedata_creds):
        if isinstance(onedata_creds, dict):
            for provider_id in onedata_creds:
                if ('oneprovider_host' in onedata_creds[provider_id]
                        and onedata_creds[provider_id]['oneprovider_host'] is not None
                        and onedata_creds[provider_id]['oneprovider_host'] != ''
                        and 'token' in onedata_creds[provider_id]
                        and onedata_creds[provider_id]['token'] is not None
                        and onedata_creds[provider_id]['token'] != ''
                        and 'space' in onedata_creds[provider_id]
                        and onedata_creds[provider_id]['space'] is not None
                        and onedata_creds[provider_id]['space'] != ''):
                    self.onedata_auth[provider_id] = AuthData('ONEDATA', onedata_creds[provider_id])
                else:
                    raise StorageAuthError(auth_type='ONEDATA')
        else:
            raise StorageAuthError(auth_type='ONEDATA')

    def _get_auth_data(self, storage_type, provider_id='default'):
        """Returns the authentication credentials by its type and id."""
        if storage_type == 'S3':
            return self.s3_auth.get(provider_id, None)
        elif storage_type == 'MINIO':
            return self.minio_auth.get(provider_id, None)
        elif storage_type == 'ONEDATA':
            return self.onedata_auth.get(provider_id, None)
        return None

    def _get_input_auth_data(self, parsed_event):
        """Return the proper auth data from a storage_provider based on the event.

        This methods allows to filter ONEDATA provider when multiple inputs are defined."""
        storage_type = parsed_event.get_type()
        if storage_type == 'ONEDATA':
            # Check input path and event object_key
            if hasattr(parsed_event, 'object_key'):
                # Get the onedata space from the event object_key
                event_space = parsed_event.object_key.strip('/').split('/', maxsplit=1)[0]
            for input in self.input:
                provider_type = StrUtils.get_storage_type(input.get('storage_provider'))
                if provider_type == storage_type:
                    provider_id = StrUtils.get_storage_id(input.get('storage_provider'))
                    if self.onedata_auth[provider_id].get_credential('space') == event_space:
                        return self._get_auth_data(storage_type, provider_id)
            raise StorageAuthError(auth_type='ONEDATA')
        else:
            return self._get_auth_data(storage_type)

    def download_input(self, parsed_event, input_dir_path):
        """Receives the event where the file information is and
        the tmp_dir_path where to store the downloaded file.

        Returns the file path where the file is downloaded."""
        auth_data = self._get_input_auth_data(parsed_event)
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
                              output['storage_provider'],
                              output['path'])
            provider_type = StrUtils.get_storage_type(output['storage_provider'])
            provider_id = StrUtils.get_storage_id(output['storage_provider'])
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
                        if provider_type not in stg_providers:
                            stg_providers[provider_type] = {}
                        if provider_id not in stg_providers[provider_type]:
                            auth_data = self._get_auth_data(provider_type, provider_id)
                            stg_providers[provider_type][provider_id] = create_provider(auth_data)
                        stg_providers[provider_type][provider_id].upload_file(file_path,
                                                                              file_name,
                                                                              output['path'])

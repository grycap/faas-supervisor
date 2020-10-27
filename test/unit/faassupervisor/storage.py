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
"""Unit tests for the faassupervisor.storage module and classes."""

import unittest
from unittest import mock
from unittest.mock import call
from collections import namedtuple
from faassupervisor.storage.providers import get_bucket_name, get_file_key
from faassupervisor.storage.config import StorageConfig, AuthData, create_provider
from faassupervisor.storage.providers.local import Local
from faassupervisor.storage.providers.minio import Minio
from faassupervisor.storage.providers.onedata import Onedata
from faassupervisor.storage.providers.s3 import S3
from faassupervisor.events import parse_event
from faassupervisor.events.minio import MinioEvent
from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.events.s3 import S3Event
from faassupervisor.events.onedata import OnedataEvent
from faassupervisor.utils import StrUtils

# pylint: disable=missing-docstring
# pylint: disable=no-self-use
# pylint: disable=protected-access

CONFIG_FILE_OK = """
name: test-func
input:
- storage_provider: onedata.test_onedata
  path: files
- storage_provider: onedata.test_onedata2
  path: files
output:
- storage_provider: s3
  path: bucket/folder
- storage_provider: minio.test_minio
  path: bucket
  suffix: ['txt', 'jpg']
  prefix: ['result-']
storage_providers:
  minio:
    test_minio:
        access_key: test_minio_access
        secret_key: test_minio_secret
  onedata:
    test_onedata:
        oneprovider_host: test_oneprovider.host
        token: test_onedata_token
        space: test_onedata_space
    test_onedata2:
        oneprovider_host: test_oneprovider.host
        token: test_onedata_token
        space: space_ok
"""

CONFIG_FILE_NO_OUTPUT = """
name: test-func
storage_providers:
  minio:
    test_minio:
        access_key: test_minio_access
        secret_key: test_minio_secret
"""

CONFIG_FILE_NO_STORAGE_PROVIDER = """
name: test-func
output:
- storage_provider: s3
  path: bucket/folder
"""

CONFIG_FILE_INVALID_MINIO = """
name: test-func
storage_providers:
  minio:
    secret_key: test_minio_secret
"""

CONFIG_FILE_INVALID_ONEDATA = """
name: test-func
storage_providers:
  onedata:
    token: test_onedata_token
    space: test_onedata_space
"""

CONFIG_FILE_INVALID_S3 = """
name: test-func
storage_providers:
  s3:
    user: test
"""

ONEDATA_EVENT = """
{
    "Key": "/space_ok/files/file.txt",
    "Records": [
        {
            "objectKey": "file.txt",
            "objectId": "1234",
            "eventTime": "2019-02-07T09:51:04.347823",
            "eventSource": "OneTrigger"
        }
    ]
}
"""

class AuthDataTest(unittest.TestCase):

    def test_create_auth_data(self):
        auth = AuthData('MINIO', {'access_key': 'test'})
        self.assertEqual(auth.type, 'MINIO')
        self.assertEqual(auth.creds, {'access_key': 'test'})

    def test_get_auth_data_credential(self):
        auth = AuthData('MINIO', {'access_key': 'test'})
        self.assertEqual(auth.get_credential('access_key'), 'test')
        self.assertEqual(auth.get_credential('k'), '')


class StorageConfigTest(unittest.TestCase):

    def test_parse_config_valid(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_OK)},
                             clear=True):
            config = StorageConfig()
            expected_output = [
                {
                    'storage_provider': 's3',
                    'path': 'bucket/folder'
                }, {
                    'storage_provider': 'minio.test_minio',
                    'path': 'bucket',
                    'suffix': ['txt', 'jpg'],
                    'prefix': ['result-']
                }
            ]
            self.assertEqual(config.output, expected_output)
            self.assertEqual(config.minio_auth['test_minio'].type, 'MINIO')
            self.assertEqual(config.minio_auth['test_minio'].get_credential('access_key'), 'test_minio_access')
            self.assertEqual(config.minio_auth['test_minio'].get_credential('secret_key'), 'test_minio_secret')
            self.assertEqual(config.onedata_auth['test_onedata'].type, 'ONEDATA')
            self.assertEqual(config.onedata_auth['test_onedata'].get_credential('oneprovider_host'), 'test_oneprovider.host')
            self.assertEqual(config.onedata_auth['test_onedata'].get_credential('token'), 'test_onedata_token')
            self.assertEqual(config.onedata_auth['test_onedata'].get_credential('space'), 'test_onedata_space')
            self.assertEqual(config.s3_auth['default'].type, 'S3')

    def test_parse_config_no_output(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_NO_OUTPUT)},
                             clear=True):
            StorageConfig()
            self.assertLogs('There is no output defined for this function.',
                             level='WARNING')

    def test_parse_config_no_storage_provider(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_NO_STORAGE_PROVIDER)},
                             clear=True):
            StorageConfig()
            self.assertLogs('There is no storage provider defined for this function.',
                             level='WARNING')

    def test_parse_config_invalid_minio(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_INVALID_MINIO)},
                             clear=True):
            with self.assertRaises(SystemExit):
                StorageConfig()
                self.assertLogs('The storage authentication of \'MINIO\' is not well-defined.',
                                level='ERROR')

    def test_parse_config_invalid_s3(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_INVALID_S3)},
                             clear=True):
            with self.assertRaises(SystemExit):
                StorageConfig()
                self.assertLogs('The storage authentication of \'S3\' is not well-defined.',
                                level='ERROR')

    def test_parse_config_invalid_onedata(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_INVALID_ONEDATA)},
                             clear=True):
            with self.assertRaises(SystemExit):
                StorageConfig()
                self.assertLogs('The storage authentication of \'ONEDATA\' is not well-defined.',
                                level='ERROR')

    def test_get_minio_auth(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_OK)},
                             clear=True):
            minio_auth = StorageConfig()._get_auth_data('MINIO', 'test_minio')
            self.assertEqual(minio_auth.type, 'MINIO')
            self.assertEqual(minio_auth.get_credential('access_key'),
                             'test_minio_access')
            self.assertEqual(minio_auth.get_credential('secret_key'),
                             'test_minio_secret')

    def test_get_s3_auth(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_OK)},
                             clear=True):
            s3_auth = StorageConfig()._get_auth_data('S3')
            self.assertEqual(s3_auth.type, 'S3')

    def test_get_onedata_auth(self):
        # TODO: check _get_input_auth_data() with custom Onedata event
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_OK)},
                             clear=True):
            onedata_auth = StorageConfig()._get_auth_data('ONEDATA', 'test_onedata')
            self.assertEqual(onedata_auth.type, 'ONEDATA')
            self.assertEqual(onedata_auth.get_credential('oneprovider_host'),
                             'test_oneprovider.host')
            self.assertEqual(onedata_auth.get_credential('token'),
                             'test_onedata_token')
            self.assertEqual(onedata_auth.get_credential('space'),
                             'test_onedata_space')
            # Test _get_input_auth_data() for getting the proper auth data from an event
            parsed_event = parse_event(ONEDATA_EVENT)
            onedata2_auth = StorageConfig()._get_input_auth_data(parsed_event)
            self.assertEqual(onedata2_auth.get_credential('space'), 'space_ok')


    def test_get_invalid_auth(self):
        invalid_auth = StorageConfig()._get_auth_data('INVALID_TYPE')
        self.assertIsNone(invalid_auth)

    @mock.patch('faassupervisor.storage.providers.local.Local.download_file')
    def test_download_input(self, mock_download_file):
        event = UnknownEvent('')
        StorageConfig().download_input(event, '/tmp/test')
        mock_download_file.assert_called_once_with(event, '/tmp/test')

    @mock.patch('faassupervisor.utils.FileUtils.get_all_files_in_dir')
    @mock.patch('faassupervisor.storage.providers.s3.S3.upload_file')
    @mock.patch('faassupervisor.storage.providers.minio.Minio.upload_file')
    def test_upload_output(self, mock_minio, mock_s3, mock_get_files):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_OK)},
                             clear=True):
            files = [
                '/tmp/test/file1.txt',
                '/tmp/test/file1.jpg',
                '/tmp/test/result-file.txt',
                '/tmp/test/result-file.out',
                '/tmp/test/file2.txt',
                '/tmp/test/file2.out'
            ]
            mock_get_files.return_value = files
            StorageConfig().upload_output('/tmp/test')
            self.assertEqual(mock_minio.call_count, 1)
            self.assertEqual(mock_minio.call_args,
                             call('/tmp/test/result-file.txt',
                                  'result-file.txt',
                                  'bucket'))
            self.assertEqual(mock_s3.call_count, 6)
            for i, f in enumerate(files):
                self.assertEqual(mock_s3.call_args_list[i],
                                 call(f, f.split('/')[3], 'bucket/folder'))


class ProvidersModuleTest(unittest.TestCase):

    def test_get_bucket_name(self):
        self.assertEqual(get_bucket_name('bucket/folder1/folder2'), 'bucket')
        self.assertEqual(get_bucket_name('bucket'), 'bucket')

    def test_get_file_key(self):
        self.assertEqual(get_file_key('bucket/folder1/folder2', 'file'),
                         'folder1/folder2/file')
        self.assertEqual(get_file_key('bucket', 'file'), 'file')


class LocalProviderTest(unittest.TestCase):

    def test_create_local_provider(self):
        provider = create_provider(None)
        self.assertEqual(provider.get_type(), 'LOCAL')

    def test_download_file(self):
        provider = Local(None)
        parsed_event = mock.Mock(spec=UnknownEvent)
        provider.download_file(parsed_event, '/tmp/local')
        parsed_event.save_event.assert_called_once_with('/tmp/local')


class MinioProviderTest(unittest.TestCase):

    MINIO_CREDS = {
            'access_key': 'test_minio_access',
            'secret_key': 'test_minio_secret'
    }

    def test_create_minio_provider(self):
        minio_auth = AuthData('MINIO', self.MINIO_CREDS)
        provider = create_provider(minio_auth)
        self.assertEqual(provider.get_type(), 'MINIO')
        self.assertEqual(provider.stg_auth.creds, self.MINIO_CREDS)

    @mock.patch('boto3.client')
    def test_get_client_default_endpoint(self, mock_boto):
        Minio(AuthData('MINIO', self.MINIO_CREDS))
        mock_boto.assert_called_once_with('s3',
                                           endpoint_url='http://minio-service.minio:9000',
                                           region_name=None,
                                           verify=True,
                                           aws_access_key_id='test_minio_access',
                                           aws_secret_access_key='test_minio_secret')

    @mock.patch('boto3.client')
    def test_get_client_custom_endpoint(self, mock_boto):
        Minio(AuthData('MINIO', {**self.MINIO_CREDS,
                                 'endpoint': 'https://test.endpoint'}))
        mock_boto.assert_called_once_with('s3',
                                           endpoint_url='https://test.endpoint',
                                           region_name=None,
                                           verify=True,
                                           aws_access_key_id='test_minio_access',
                                           aws_secret_access_key='test_minio_secret')

    @mock.patch('boto3.client')
    def test_download_file(self, mock_boto):
        minio_provider = Minio(AuthData('MINIO', self.MINIO_CREDS))
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            # Create mock event and event properties
            event = mock.Mock(spec=MinioEvent)
            type(event).bucket_name = mock.PropertyMock(return_value='minio_bucket')
            type(event).file_name = mock.PropertyMock(return_value='minio_file')
            type(event).object_key = mock.PropertyMock(return_value='minio_file')
            file_path = minio_provider.download_file(event, '/tmp/input')
            # Check returned file path
            self.assertEqual(file_path, '/tmp/input/minio_file')
            # Check file writing call
            mopen.assert_called_once_with('/tmp/input/minio_file', 'wb')
            # Check boto client download call
            self.assertEqual(mock_boto.mock_calls[1],
                             call().download_fileobj('minio_bucket',
                                                     'minio_file',
                                                     mopen.return_value))

    @mock.patch('boto3.client')
    def test_upload_file(self, mock_boto):
        minio_provider = Minio(AuthData('MINIO', self.MINIO_CREDS))
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            minio_provider.upload_file('/tmp/output/processed.jpg', 'processed.jpg', 'minio_bucket')
            # Check file reading call
            mopen.assert_called_once_with('/tmp/output/processed.jpg', 'rb')
            # Check boto client upload call
            self.assertEqual(mock_boto.mock_calls[1],
                             call().upload_fileobj(mopen.return_value,
                                                   'minio_bucket',
                                                   'processed.jpg'))


class OnedataProviderTest(unittest.TestCase):

    ONEDATA_CREDS = {
        'oneprovider_host': 'test_oneprovider.host',
        'token': 'test_onedata_token',
        'space': 'test_onedata_space'
    }
    def test_create_onedata_provider(self):
        onedata_auth = AuthData('ONEDATA', self.ONEDATA_CREDS)
        provider = create_provider(onedata_auth)
        self.assertEqual(provider.get_type(), 'ONEDATA')
        self.assertEqual(provider.stg_auth.creds, self.ONEDATA_CREDS)

    @mock.patch('requests.get')
    def test_download_file(self, mock_requests):
        onedata_provider = Onedata(AuthData('ONEDATA', self.ONEDATA_CREDS))
        # Mock requests.get response
        response = namedtuple('response', ['content', 'status_code'])
        mock_requests.return_value = response('test response', 200)
        # Create mock event
        event = mock.Mock(spec=OnedataEvent)
        type(event).file_name = mock.PropertyMock(return_value='onedata_file')
        type(event).object_key = mock.PropertyMock(return_value='/onedata_file_key')
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            file_path = onedata_provider.download_file(event, '/tmp/input')
            # Check returned file path
            self.assertEqual(file_path, '/tmp/input/onedata_file')
            # Check request to onedata endpoint
            mock_requests.assert_called_once_with('https://test_oneprovider.host/cdmi/onedata_file_key',
                                                  headers={'X-Auth-Token': 'test_onedata_token'})
            # Check file writing
            mopen.assert_called_once_with('/tmp/input/onedata_file', 'wb')

    @mock.patch('requests.get')
    @mock.patch('requests.put')
    def test_upload_file(self, mock_put, mock_get):
        onedata_provider = Onedata(AuthData('ONEDATA', self.ONEDATA_CREDS))
        response = namedtuple('response', ['status_code'])
        # Mock requests.get response (check if folder exists)
        mock_get.return_value = response(200)
        # Mock requests.put response
        mock_put.return_value = response(202)
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            onedata_provider.upload_file('/tmp/output/onedata_file', 'onedata_file', 'onedata_path')
            # Check request to onedata endpoint
            mock_get.assert_called_once_with(
                'https://test_oneprovider.host/cdmi/test_onedata_space/onedata_path/',
                headers={**onedata_provider._CDMI_VERSION_HEADER,
                         'X-Auth-Token': 'test_onedata_token'})
            mock_put.assert_called_once_with(
                'https://test_oneprovider.host/cdmi/test_onedata_space/onedata_path/onedata_file',
                data=mopen.return_value,
                headers={'X-Auth-Token': 'test_onedata_token'})
            # Check file writing
            mopen.assert_called_once_with('/tmp/output/onedata_file', 'rb')


class S3ProviderTest(unittest.TestCase):

    S3_CREDS = {
        'access_key': 'test_s3_access',
        'secret_key': 'test_s3_secret'
    }

    def test_create_s3_provider(self):
        minio_auth = AuthData('S3', None)
        provider = create_provider(minio_auth)
        self.assertEqual(provider.get_type(), 'S3')
        self.assertIsNone(provider.stg_auth.creds)

    @mock.patch('boto3.client')
    def test_get_client_without_creds(self, mock_boto):
        S3(AuthData('S3', None))
        mock_boto.assert_called_once_with('s3')

    @mock.patch('boto3.client')
    def test_get_client_with_creds(self, mock_boto):
        S3(AuthData('S3', self.S3_CREDS))
        mock_boto.assert_called_once_with('s3',
                                           region_name=None,
                                           aws_access_key_id='test_s3_access',
                                           aws_secret_access_key='test_s3_secret')

    @mock.patch('boto3.client')
    def test_download_file(self, mock_boto):
        s3_provider = S3(AuthData('S3', None))
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            # Create mock event and event properties
            event = mock.Mock(spec=S3Event)
            type(event).bucket_name = mock.PropertyMock(return_value='s3_bucket')
            type(event).file_name = mock.PropertyMock(return_value='s3_file')
            type(event).object_key = mock.PropertyMock(return_value='s3_folder/s3_file')
            file_path = s3_provider.download_file(event, '/tmp/input')
            # Check returned file path
            self.assertEqual(file_path, '/tmp/input/s3_file')
            # Check file writing call
            mopen.assert_called_once_with('/tmp/input/s3_file', 'wb')
            # Check boto client download call
            self.assertEqual(mock_boto.mock_calls[1],
                             call().download_fileobj('s3_bucket',
                                                     's3_folder/s3_file',
                                                     mopen.return_value))

    @mock.patch('boto3.client')
    def test_upload_file(self, mock_boto):
        s3_provider = S3(AuthData('S3', None))
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            s3_provider.upload_file('/tmp/output/processed.jpg', 'processed.jpg', 's3_bucket/s3_folder')
            # Check file reading call
            mopen.assert_called_once_with('/tmp/output/processed.jpg', 'rb')
            # Check boto client upload call
            self.assertEqual(mock_boto.mock_calls[1],
                             call().upload_fileobj(mopen.return_value,
                                                   's3_bucket',
                                                   's3_folder/processed.jpg'))

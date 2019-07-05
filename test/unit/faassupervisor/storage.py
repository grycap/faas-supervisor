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
import faassupervisor.storage as storage
from faassupervisor.exceptions import InvalidStorageProviderError
from faassupervisor.storage.auth import AuthData, StorageAuth
from faassupervisor.storage.providers.local import Local
from faassupervisor.storage.providers.minio import Minio
from faassupervisor.events.minio import MinioEvent
from faassupervisor.storage.providers.onedata import Onedata
from faassupervisor.storage.providers.s3 import S3, _get_client
from faassupervisor.events.apigateway import ApiGatewayEvent
from faassupervisor.events.s3 import S3Event
from faassupervisor.events.onedata import OnedataEvent

# pylint: disable=missing-docstring
# pylint: disable=no-self-use
# pylint: disable=protected-access


class StorageModuleTest(unittest.TestCase):

    auth = namedtuple("auth", ["type"])

    def test_create_provider_local(self):
        prov = storage.create_provider(())
        self.assertEqual(prov.get_type(), "LOCAL")

    def test_create_provider_minio(self):
        prov = storage.create_provider(self.auth("MINIO"))
        self.assertEqual(prov.get_type(), "MINIO")

    @mock.patch("faassupervisor.storage.providers.onedata.Onedata._set_onedata_environment")
    def test_create_provider_onedata(self, onepatch):
        prov = storage.create_provider(self.auth("ONEDATA"))
        onepatch.assert_called_once()
        self.assertEqual(prov.get_type(), "ONEDATA")

    def test_create_provider_s3(self):
        prov = storage.create_provider(self.auth("S3"))
        self.assertEqual(prov.get_type(), "S3")

    def test_create_provider_invalid(self):
        with self.assertRaises(InvalidStorageProviderError):
            storage.create_provider(self.auth("ERROR"))

    @mock.patch("faassupervisor.storage.providers.s3.S3.download_file")
    def test_download_input(self, mock_s3):
        prov = storage.create_provider(self.auth("S3"))
        storage.download_input(prov, {}, '/tmp/test')
        mock_s3.assert_called_once_with({}, '/tmp/test')

    @mock.patch("faassupervisor.utils.FileUtils.get_all_files_in_dir")
    @mock.patch("faassupervisor.storage.providers.s3.S3.upload_file")
    def test_upload_output(self, mock_s3, mock_utils):
        mock_utils.return_value = ['/tmp/test/f1', '/tmp/test/k1/f2']
        prov = storage.create_provider(self.auth("S3"))
        storage.upload_output(prov, '/tmp/test')
        mock_s3.call_count = 2
        mock_s3.mock_call()[0] = call('/tmp/test/f1', 'f1')
        mock_s3.mock_call()[1] = call('/tmp/test/k1/f2', 'k1/f2')

    def test_get_output_paths(self):
        # Mock environment variables
        with mock.patch.dict('os.environ',
                             {"STORAGE_PATH_OUTPUT_1" : "tmp1", "STORAGE_PATH_OUTPUT_2" : "tmp1"},
                             clear=True):
            result = storage.get_output_paths()
            storage_path = namedtuple('storage_path', ['id', 'path'])
            self.assertEqual(result,
                             [storage_path(id='1', path='tmp1'),
                              storage_path(id='2', path='tmp1')])


class AuthDataTest(unittest.TestCase):

    def test_create_auth_data(self):
        auth = AuthData('1', 'LOCAL')
        self.assertEqual(auth.storage_id, '1')
        self.assertEqual(auth.type, 'LOCAL')
        self.assertEqual(auth.creds, {})

    def test_set_auth_data_credential(self):
        auth = AuthData('1', 'LOCAL')
        auth.set_credential('K1', 'V1')
        self.assertEqual(auth.creds, {'K1':'V1'})

    def test_get_auth_data_credential(self):
        auth = AuthData('1', 'LOCAL')
        auth.set_credential('K1', 'V1')
        self.assertEqual(auth.get_credential('K1'), 'V1')
        self.assertEqual(auth.get_credential('K11'), '')


class StorageAuthTest(unittest.TestCase):

    def test_create_storage_auth(self):
        stga = StorageAuth()
        self.assertEqual(stga.auth_id, {})
        self.assertEqual(stga.auth_type, {})

    @mock.patch('faassupervisor.storage.auth.AuthData')
    def test_read_storage_providers(self, mock_auth):
        # Mock environment variables
        with mock.patch.dict('os.environ',
                             {"STORAGE_AUTH_S3_USER_1" : "u1", "STORAGE_AUTH_S3_PASS_1" : "p1"},
                             clear=True):
            stga = StorageAuth()
            stga.read_storage_providers()
            self.assertEqual(stga.auth_id, {'1' : 'S3'})
            mock_auth.assert_called_once()
            self.assertEqual(mock_auth.mock_calls[0], call('1', 'S3'))
            self.assertEqual(mock_auth.mock_calls[1], call().set_credential('USER', 'u1'))
            self.assertEqual(mock_auth.mock_calls[2], call().set_credential('PASS', 'p1'))

    def test_get_auth_data_by_stg_type(self):
        # Mock environment variables
        with mock.patch.dict('os.environ',
                             {"STORAGE_AUTH_S3_USER_1" : "u1", "STORAGE_AUTH_S3_PASS_1" : "p1"},
                             clear=True):
            stga = StorageAuth()
            stga.read_storage_providers()
            self.assertEqual(stga.get_auth_data_by_stg_type('S3').get_credential('USER'), 'u1')
            self.assertEqual(stga.get_auth_data_by_stg_type('S3').get_credential('PASS'), 'p1')

    def test_get_data_by_stg_id(self):
        # Mock environment variables
        with mock.patch.dict('os.environ',
                             {"STORAGE_AUTH_S3_USER_1" : "u1", "STORAGE_AUTH_S3_PASS_1" : "p1"},
                             clear=True):
            stga = StorageAuth()
            stga.read_storage_providers()
            self.assertEqual(stga.get_data_by_stg_id('1').get_credential('USER'), 'u1')
            self.assertEqual(stga.get_data_by_stg_id('1').get_credential('PASS'), 'p1')


class LocalStorageTest(unittest.TestCase):

    def test_create_local_storage(self):
        stg = Local('auth', 'path')
        self.assertEqual(stg.stg_auth, 'auth')
        self.assertEqual(stg.stg_path, 'path')
        self.assertEqual(stg.get_type(), 'LOCAL')

    def test_download_file(self):
        stg = Local('auth', 'path')
        parsed_event = mock.Mock(spec=ApiGatewayEvent)
        stg.download_file(parsed_event, '/tmp/local')
        parsed_event.save_event.assert_called_once_with('/tmp/local')


class MinioStorageTest(unittest.TestCase):

    def _get_minio_class_and_auth(self):
        auth = mock.Mock(spec=AuthData)
        auth.get_credential.return_value = 'test'
        return (Minio(auth, 'minio_path'), auth)

    def test_create_minio_storage(self):
        stg, auth = self._get_minio_class_and_auth()
        self.assertEqual(stg.stg_auth, auth)
        self.assertEqual(stg.stg_path, 'minio_path')
        self.assertEqual(stg.get_type(), 'MINIO')

    @mock.patch('boto3.client')
    def test_get_client_default_endpoint(self, mock_boto):
        stg, auth = self._get_minio_class_and_auth()
        stg._get_client()
        self.assertEqual(auth.get_credential.mock_calls[0], call('USER'))
        self.assertEqual(auth.get_credential.mock_calls[1], call('PASS'))
        mock_boto.assert_called_once_with('s3',
                                          aws_access_key_id='test',
                                          aws_secret_access_key='test',
                                          endpoint_url='http://minio-service.minio:9000')

    @mock.patch('boto3.client')
    def test_get_client(self, mock_boto):
        # Mock environment variables
        with mock.patch.dict('os.environ', {"MINIO_ENDPOINT" : "test_endpoint"}, clear=True):
            stg, auth = self._get_minio_class_and_auth()
            stg._get_client()
            self.assertEqual(auth.get_credential.mock_calls[0], call('USER'))
            self.assertEqual(auth.get_credential.mock_calls[1], call('PASS'))
            mock_boto.assert_called_once_with('s3',
                                              aws_access_key_id='test',
                                              aws_secret_access_key='test',
                                              endpoint_url='test_endpoint')

    @mock.patch('boto3.client')
    def test_download_file(self, mock_boto):
        stg, _ = self._get_minio_class_and_auth()
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            # Create mock event and event properties
            event = mock.Mock(spec=MinioEvent)
            type(event).bucket_name = mock.PropertyMock(return_value='minio_bucket')
            type(event).file_name = mock.PropertyMock(return_value='minio_file')
            file_path = stg.download_file(event, '/tmp/input')
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
        stg, _ = self._get_minio_class_and_auth()
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            stg.upload_file('/tmp/output/processed.jpg', 'processed.jpg')
            # Check file reading call
            mopen.assert_called_once_with('/tmp/output/processed.jpg', 'rb')
            # Check boto client upload call
            self.assertEqual(mock_boto.mock_calls[1],
                             call().upload_fileobj(mopen.return_value,
                                                   'minio_path',
                                                   'processed.jpg'))


class S3StorageTest(unittest.TestCase):

    def _get_s3_class_and_auth(self, path=None):
        auth = mock.Mock(spec=AuthData)
        auth.get_credential.return_value = 'test'
        s3_path = 's3_path'
        if path:
            s3_path = path
        return (S3(auth, s3_path), auth)

    def test_create_s3_storage(self):
        stg, auth = self._get_s3_class_and_auth()
        self.assertEqual(stg.stg_auth, auth)
        self.assertEqual(stg.stg_path, 's3_path')
        self.assertEqual(stg.get_type(), 'S3')

    def test_get_file_key_wo_folder(self):
        stg, _ = self._get_s3_class_and_auth()
        # Mock environment variables
        with mock.patch.dict('os.environ',
                             {"AWS_LAMBDA_FUNCTION_NAME" : "func_name",
                              "AWS_LAMBDA_REQUEST_ID" : "req_id"},
                             clear=True):
            res = stg._get_file_key('test.jpg')
            self.assertEqual(res, "func_name/output/req_id/test.jpg")

    def test_get_file_key_w_folder(self):
        stg, _ = self._get_s3_class_and_auth(path='bucket/f1/f2')
        res = stg._get_file_key('test.jpg')
        self.assertEqual(res, "f1/f2/test.jpg")

    def test_get_bucket_name_wo_folder(self):
        stg, _ = self._get_s3_class_and_auth()
        res = stg._get_bucket_name()
        self.assertEqual(res, 's3_path')

    def test_get_bucket_name_w_folder(self):
        stg, _ = self._get_s3_class_and_auth(path='bucket/f1/f2')
        res = stg._get_bucket_name()
        self.assertEqual(res, 'bucket')

    @mock.patch('boto3.client')
    def test_get_client(self, mock_boto):
        _get_client()
        mock_boto.assert_called_once_with('s3')

    @mock.patch('boto3.client')
    def test_download_file(self, mock_boto):
        stg, _ = self._get_s3_class_and_auth()
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            # Mock S3 event and event properties
            event = mock.Mock(spec=S3Event)
            type(event).bucket_name = mock.PropertyMock(return_value='s3_bucket')
            type(event).file_name = mock.PropertyMock(return_value='s3_file')
            type(event).object_key = mock.PropertyMock(return_value='s3_file_key')
            file_path = stg.download_file(event, '/tmp/input')
            # Check returned file path
            self.assertEqual(file_path, '/tmp/input/s3_file')
            # Check file writing call
            mopen.assert_called_once_with('/tmp/input/s3_file', 'wb')
            # Check boto client download call
            self.assertEqual(mock_boto.mock_calls[1],
                             call().download_fileobj('s3_bucket',
                                                     's3_file_key',
                                                     mopen.return_value))

    @mock.patch('boto3.resource')
    @mock.patch('boto3.client')
    def test_upload_file(self, mock_boto_client, mock_boto_resource):
        stg, _ = self._get_s3_class_and_auth()
        # Mock environment variables
        with mock.patch.dict('os.environ',
                             {"AWS_LAMBDA_FUNCTION_NAME" : "func_name",
                              "AWS_LAMBDA_REQUEST_ID" : "req_id"},
                             clear=True):
            # Mock file management
            mopen = mock.mock_open()
            with mock.patch('builtins.open', mopen, create=True):
                stg.upload_file('/tmp/output/processed.jpg', 'processed.jpg')
                # Check file read
                mopen.assert_called_once_with('/tmp/output/processed.jpg', 'rb')
                # Check boto client upload call
                self.assertEqual(mock_boto_client.mock_calls[1],
                                 call().upload_fileobj(mopen.return_value,
                                                       's3_path',
                                                       'func_name/output/req_id/processed.jpg'))
                # Check resource creation
                mock_boto_resource.assert_called_once_with('s3')
                # Check resource call
                self.assertEqual(mock_boto_resource.mock_calls[1],
                                 call().Object('s3_path', 'func_name/output/req_id/processed.jpg'))
                # Check setting resource properties
                self.assertEqual(mock_boto_resource.mock_calls[3],
                                 call().Object().Acl().put(ACL='public-read'))


class OnedataStorageTest(unittest.TestCase):

    def _get_onedata_class_and_auth(self, path=None):
        auth = mock.Mock(spec=AuthData)
        auth.get_credential.return_value = 'test'
        one_path = 'onedata_path'
        if path:
            one_path = path
        return (Onedata(auth, one_path), auth)

    def test_create_onedata_storage(self):
        stg, auth = self._get_onedata_class_and_auth()
        self.assertEqual(stg.stg_auth, auth)
        self.assertEqual(stg.stg_auth.mock_calls[0], call.get_credential('SPACE'))
        self.assertEqual(stg.stg_auth.mock_calls[1], call.get_credential('HOST'))
        self.assertEqual(stg.stg_auth.mock_calls[2], call.get_credential('TOKEN'))
        self.assertEqual(stg.stg_path, 'onedata_path')
        self.assertEqual(stg.get_type(), 'ONEDATA')

    @mock.patch('requests.get')
    def test_download_file(self, mock_requests):
        stg, _ = self._get_onedata_class_and_auth()
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
            file_path = stg.download_file(event, '/tmp/input')
            # Check returned file path
            self.assertEqual(file_path, '/tmp/input/onedata_file')
            # Check request to onedata endpoint
            mock_requests.assert_called_once_with('https://test/cdmi/onedata_file_key',
                                                  headers={'X-Auth-Token': 'test'})
            # Check file writing
            mopen.assert_called_once_with('/tmp/input/onedata_file', 'wb')

    @mock.patch('requests.put')
    def test_upload_file(self, mock_requests):
        stg, _ = self._get_onedata_class_and_auth()
        # Mock requests.put response
        response = namedtuple('response', ['status_code'])
        mock_requests.return_value = response(202)
        # Mock file management
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            stg.upload_file('/tmp/output/onedata_file', 'onedata_file')
            # Check request to onedata endpoint
            mock_requests.assert_called_once_with(
                'https://test/cdmi/test/onedata_path/onedata_file',
                data=mopen.return_value,
                headers={'X-Auth-Token': 'test'})
            # Check file writing
            mopen.assert_called_once_with('/tmp/output/onedata_file', 'rb')

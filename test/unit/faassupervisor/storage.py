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
from unittest.mock import call
"""Unit tests for the faassupervisor.events module and classes."""

import sys
import io
import os
import unittest
from unittest import mock
from collections import namedtuple
import faassupervisor.storage as storage
from faassupervisor.storage.providers.s3 import S3
from faassupervisor.storage.providers.minio import Minio
from faassupervisor.storage.providers.onedata import Onedata
from faassupervisor.storage.providers.local import Local
from faassupervisor.exceptions import InvalidStorageProviderError

# pylint: disable=missing-docstring
# pylint: disable=no-self-use

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
        with mock.patch.dict('os.environ',
                             {"STORAGE_PATH_OUTPUT_1" : "tmp1", "STORAGE_PATH_OUTPUT_2" : "tmp1"},
                             clear=True):
            result = storage.get_output_paths()
            storage_path = namedtuple('storage_path', ['id', 'path'])
            self.assertEqual(result,
                             [storage_path(id='1', path='tmp1'),
                              storage_path(id='2', path='tmp1')])
        
        

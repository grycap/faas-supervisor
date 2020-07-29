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
"""Unit tests for the faassupervisor.utils module."""

import sys
import io
import os
import unittest
from unittest import mock
from faassupervisor.utils import SysUtils, StrUtils, FileUtils, ConfigUtils

# pylint: disable=missing-docstring
# pylint: disable=no-self-use

CONFIG_FILE = """
name: test-func
"""

class SysUtilsTest(unittest.TestCase):

    def test_get_stdin(self):
        sys.stdin = io.StringIO('It works!')
        self.assertEqual(SysUtils.get_stdin(), 'It works!')

    def test_join_paths(self):
        paths = ['It', 'works', '!']
        # Works in Linux systems
        self.assertEqual(SysUtils.join_paths(*paths), "/".join(paths))

    def test_is_var_in_env(self):
        with mock.patch.dict('os.environ', {"K1":"V1"}, clear=True):
            self.assertTrue(SysUtils.is_var_in_env('K1'))

    def test_set_env_var(self):
        with mock.patch.dict('os.environ', {}, clear=True):
            SysUtils.set_env_var('TEST_VAR', 'TEST_VAL')
            self.assertTrue('TEST_VAR' in os.environ)
            self.assertEqual(os.environ['TEST_VAR'], 'TEST_VAL')

    def test_get_all_env_vars(self):
        with mock.patch.dict('os.environ', {"K1":"V1", "K2":"V2"}, clear=True):
            self.assertEqual(SysUtils.get_all_env_vars(), {"K1":"V1", "K2":"V2"})

    def test_get_env_var(self):
        with mock.patch.dict('os.environ', {"K1":"V1", "K2":"V2"}, clear=True):
            self.assertEqual(SysUtils.get_env_var("K2"), "V2")
            self.assertEqual(SysUtils.get_env_var("K3"), "")

#     def test_get_cont_env_vars(self):
#         with mock.patch.dict('os.environ',
#                              {"K1":"V1", "CONT_VAR_C1":"VC1", "CONT_VAR_C2":"VC2"},
#                              clear=True):
#             # Variables without the prefix
#             self.assertEqual(SysUtils.get_cont_env_vars(), {"C1":"VC1", "C2":"VC2"})
#  
#         with mock.patch.dict('os.environ', {"K1":"V1"}, clear=True):
#             self.assertEqual(SysUtils.get_cont_env_vars(), {})

    @mock.patch('subprocess.call')
    def test_execute_cmd(self, mock_call):
        SysUtils.execute_cmd(["ls", "-la"])
        mock_call.assert_called_once_with(["ls", "-la"])

    @mock.patch('subprocess.check_output')
    def test_execute_command_and_return_output(self, mock_check_output):
        mock_check_output.return_value = b'testing\nreturn\ndecode.'
        out = SysUtils.execute_cmd_and_return_output(["ls", "-la"])
        mock_check_output.assert_called_once_with(["ls", "-la"])
        self.assertTrue(out, "testing\nreturn\ndecode")


class FileUtilsTest(unittest.TestCase):

    @mock.patch('os.stat')
    @mock.patch('os.chmod')
    def test_set_file_execution_rights(self, mock_chmod, mock_stat):
        mock_stat().st_mode = 0o0000
        FileUtils.set_file_execution_rights('/tmp/invented_file')
        # 73 is = 0o000 | 0o0111 in decimal
        mock_chmod.assert_called_once_with('/tmp/invented_file', 73)

    @mock.patch('shutil.copyfile')
    def test_cp_file(self, mock_cp):
        FileUtils.cp_file('/tmp/src_file', '/tmp/dst_file')
        mock_cp.assert_called_once_with('/tmp/src_file', '/tmp/dst_file')

    @mock.patch('os.makedirs')
    def test_create_folder(self, mock_dir):
        FileUtils.create_folder('/tmp/folder')
        mock_dir.assert_called_once_with('/tmp/folder', exist_ok=True)

    def test_create_file_with_content(self):
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            FileUtils.create_file_with_content('/tmp/file', 'fifayfofum')
            mopen.assert_called_once_with('/tmp/file', 'w')
            mopen().write.assert_called_once_with('fifayfofum')

    def test_create_file_with_json_content(self):
        mopen = mock.mock_open()
        with mock.patch('builtins.open', mopen, create=True):
            FileUtils.create_file_with_content('/tmp/file', {"k1":"v1", "k2":"v2"})
            mopen.assert_called_once_with('/tmp/file', 'w')
            mopen().write.assert_called_once_with('{"k1": "v1", "k2": "v2"}')

    def test_read_file(self):
        mopen = mock.mock_open(read_data='fifayfofum')
        with mock.patch('builtins.open', mopen, create=True):
            content = FileUtils.read_file('/tmp/file')
            mopen.assert_called_once_with('/tmp/file', mode='r', encoding='utf-8')
            self.assertEqual(content, 'fifayfofum')

    def test_read_binary_file(self):
        mopen = mock.mock_open(read_data='fifayfofum')
        with mock.patch('builtins.open', mopen, create=True):
            content = FileUtils.read_file('/tmp/file', file_mode='rb')
            mopen.assert_called_once_with('/tmp/file', mode='rb', encoding=None)
            self.assertEqual(content, 'fifayfofum')

    @mock.patch('tempfile.TemporaryDirectory')
    def test_create_tmp_dir(self, mock_tmp):
        FileUtils.create_tmp_dir()
        mock_tmp.assert_called_once()

    @mock.patch('tempfile.gettempdir')
    def test_get_tmp_dir(self, mock_tmp):
        FileUtils.get_tmp_dir()
        mock_tmp.assert_called_once()

    @mock.patch('os.walk')
    def test_get_all_files_in_dir(self, mock_os):
        mock_os.return_value = [('/tmp', ['t1'], ['f1', 'f2']),
                                ('/tmp/t1', [], ['f3'])]
        files = FileUtils.get_all_files_in_dir('/tmp')
        mock_os.assert_called_once_with('/tmp')
        self.assertEqual(files, ['/tmp/f1', '/tmp/f2', '/tmp/t1/f3'])

    @mock.patch('os.path.isfile')
    def test_is_file(self, mock_os):
        FileUtils.is_file('/tmp/invented_file')
        mock_os.assert_called_with('/tmp/invented_file')

    @mock.patch('os.path.basename')
    def test_get_file_name(self, mock_os):
        FileUtils.get_file_name('/tmp/invented_file.jpg')
        mock_os.assert_called_with('/tmp/invented_file.jpg')


class StrUtilsTest(unittest.TestCase):

    def test_bytes_to_base64str(self):
        self.assertEqual(StrUtils.bytes_to_base64str(b'testing\nencode.'), "dGVzdGluZwplbmNvZGUu")

    def test_dict_to_base64str(self):
        self.assertEqual(StrUtils.dict_to_base64str({"k1":"v1", "k2":"v2"}),
                         "eyJrMSI6ICJ2MSIsICJrMiI6ICJ2MiJ9")

    def test_base64_to_str(self):
        self.assertEqual(StrUtils.base64_to_str("dGVzdGluZwplbmNvZGUu"), "testing\nencode.")

    def test_utf8_to_base64_string(self):
        self.assertEqual(StrUtils.utf8_to_base64_string("testing\nencode."), "dGVzdGluZwplbmNvZGUu")

    def test_get_storage_id(self):
        self.assertEqual(StrUtils.get_storage_id('bad.good'), 'good')
        self.assertEqual(StrUtils.get_storage_id('bad.good.with.dots'), 'good.with.dots')

    def test_get_storage_type(self):
        self.assertEqual(StrUtils.get_storage_type('good.bad.asdf'), 'GOOD')


class ConfigUtilsTest(unittest.TestCase):

    def test_read_cfg_var_environment(self):
        with mock.patch.dict('os.environ', {'LOG_LEVEL': 'TEST'}, clear=True):
            self.assertEqual(ConfigUtils.read_cfg_var('log_level'), 'TEST')

    def test_read_cfg_var_config_file(self):
        with mock.patch.dict('os.environ',
                             {'AWS_EXECUTION_ENV': 'AWS_Lambda_'},
                             clear=True):
            mopen = mock.mock_open(read_data=CONFIG_FILE)
            with mock.patch('builtins.open', mopen, create=True):
                var = ConfigUtils.read_cfg_var('name')
                mopen.assert_called_once_with('/var/task/function_config.yaml')
                self.assertEqual(var, 'test-func')

    def test_read_cfg_var_config_encoded(self):
        with mock.patch.dict('os.environ',
                             {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE)},
                             clear=True):
            self.assertEqual(ConfigUtils.read_cfg_var('name'), 'test-func')

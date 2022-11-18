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
"""Unit tests for the faassupervisor.faas module and classes."""

import unittest
from unittest import mock
import os
import subprocess
# from faassupervisor.events.minio import MinioEvent
from faassupervisor.faas.binary.supervisor import BinarySupervisor
from faassupervisor.faas.aws_lambda.supervisor import LambdaSupervisor, \
                                                      is_batch_execution, \
                                                      _is_lambda_batch_execution
from faassupervisor.exceptions import NoLambdaContextError
# from faassupervisor.storage.config import StorageConfig
# from faassupervisor.supervisor import Supervisor
# from faassupervisor.utils import FileUtils, StrUtils
# from faassupervisor.utils import ConfigUtils

# pylint: disable=missing-docstring
# pylint: disable=no-self-use
# pylint: disable=protected-access

MINIO_EVENT = {"Key": "images/nature-wallpaper-229.jpg",
               "Records": [{"s3": {"object": {"key": "nature-wallpaper-229.jpg"},
                                   "bucket": {"name": "images",
                                              "arn": "arn:aws:s3:::images"}},
                            "eventSource": "minio:s3",
                            "eventTime": "2018-06-29T10:23:44Z"}]}

DELEGATED_MINIO_EVENT = {"storage_provider": "minio.cluster2",
                         "event": MINIO_EVENT
                        }
CONFIG_FILE_OK = """
name: test-func
input:
- storage_provider: minio.test_minio
  path: files
output:
- storage_provider: s3
  path: bucket/folder
- storage_provider: minio.test_minio
  path: bucket
storage_providers:
  minio:
    test_minio:
        access_key: test_minio_access
        secret_key: test_minio_secret    
    cluster2:
        access_key: test_minio_access
        secret_key: test_minio_secret
"""

class BinarySupervisorTest(unittest.TestCase):

    @mock.patch('subprocess.Popen')
    @mock.patch('faassupervisor.utils.FileUtils.create_file_with_content')
    def test_execute_function(self, mock_create, mock_popen):
        supervisor = BinarySupervisor('UNKNOWN')
        with mock.patch.dict('os.environ', {'SCRIPT': 'ZmFrZSBzY3JpcHQh',
                                            'TMP_INPUT_DIR': '/tmp/input'}, clear=True):
            supervisor.execute_function()
            # Check script file creation
            mock_create.assert_called_once_with('/tmp/input/script.sh', 'fake script!')
            # Check process execution
            mock_popen.assert_called_once_with(['/bin/sh', '/tmp/input/script.sh'],
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.STDOUT,
                                               encoding='utf-8',
                                               errors='ignore')


class LambdaSupervisorTest(unittest.TestCase):

    def _get_context(self):
        mock_context = mock.Mock()
        type(mock_context).aws_request_id = mock.PropertyMock(return_value='123')
        mock_context.get_remaining_time_in_millis.return_value = 123
        return mock_context

    def test_is_batch_execution(self):
        with mock.patch.dict('os.environ', {'EXECUTION_MODE': 'batch'}, clear=True):
            self.assertTrue(is_batch_execution())
        with mock.patch.dict('os.environ', {}, clear=True):
            self.assertFalse(is_batch_execution())

    def test_is_lambda_batch_execution(self):
        with mock.patch.dict('os.environ', {'EXECUTION_MODE': 'lambda-batch'}, clear=True):
            self.assertTrue(_is_lambda_batch_execution())
        with mock.patch.dict('os.environ', {}, clear=True):
            self.assertFalse(_is_lambda_batch_execution())

    def test_create_lambda_supervisor(self):
        with self.assertRaises(NoLambdaContextError):
            LambdaSupervisor(None, None)
        with mock.patch.dict('os.environ', {'EXECUTION_MODE': 'lambda-batch',
                                            'TMP_INPUT_DIR': '/tmp/input',
                                            'TMP_OUTPUT_DIR': '/tmp/output'}, clear=True):

            LambdaSupervisor('event', self._get_context())

    @mock.patch('subprocess.Popen')
    @mock.patch('faassupervisor.utils.FileUtils.cp_file')
    @mock.patch('faassupervisor.utils.SysUtils.execute_cmd')
    @mock.patch('faassupervisor.utils.SysUtils.execute_cmd_and_return_output')
    @mock.patch('faassupervisor.faas.aws_lambda.udocker.get_function_ip')
    @mock.patch('faassupervisor.utils.ConfigUtils.read_cfg_var')
    def test_execute_function(self, mock_read_cfg_var, mock_get_function_ip, mock_execute_out,
                              mock_execute, mock_cp_file, mock_popen):
        mock_read_cfg_var.side_effect = ["1", "init_script.sh", "3", {"image": "image"},
                                         {"image": "image"}, {"timeout_threshold": 10}]
        mock_execute_out.return_value = "22"
        mock_get_function_ip.return_value = "127.0.0.1"
        with mock.patch.dict('os.environ', {'EXECUTION_MODE': 'lambda-batch',
                                            'TMP_INPUT_DIR': '/tmp/input',
                                            'UDOCKER_DIR': '/tmp/udocker',
                                            'UDOCKER_EXEC': '/udocker.py',
                                            'TMP_OUTPUT_DIR': '/tmp/output'}, clear=True):
            supervisor = LambdaSupervisor('event', self._get_context())
            supervisor.execute_function()

        res = ['/udocker.py', '--quiet', 'run', '-v', '/tmp/input', '-v', '/tmp/output', '-v',
               '/dev', '-v', '/proc', '-v', '/etc/hosts', '--nosysdirs', '--env', 'REQUEST_ID=123',
               '--env', 'INSTANCE_IP=127.0.0.1', '--env', 'TMP_OUTPUT_DIR=/tmp/output',
               '--entrypoint=/bin/sh /tmp/input/init_script.sh', 'udocker_container']
        self.assertEqual(mock_popen.call_args_list[0][0][0], res)

    @mock.patch('subprocess.Popen')
    @mock.patch('os.path.isfile')
    @mock.patch('faassupervisor.utils.ConfigUtils.read_cfg_var')
    @mock.patch('faassupervisor.utils.FileUtils.cp_file')
    def test_execute_function_container(self, mock_cp_file, mock_read_cfg_var, mock_is_file, mock_popen):
        mock_read_cfg_var.side_effect = ["1", "init_script.sh", {"timeout_threshold": 10}, {}]
        with mock.patch.dict('os.environ', {'AWS_EXECUTION_ENV': 'AWS_Lambda_Image',
                                            'TMP_INPUT_DIR': '/tmp/input',
                                            'TMP_OUTPUT_DIR': '/tmp/output'}, clear=True):
            supervisor = LambdaSupervisor('event', self._get_context())
            supervisor.execute_function()

        res = ['/bin/sh', '/tmp/input/init_script.sh']
        self.assertEqual(mock_popen.call_args_list[0][0][0], res)
        res =  {'AWS_EXECUTION_ENV': 'AWS_Lambda_Image',
                'TMP_INPUT_DIR': '/tmp/input',
                'TMP_OUTPUT_DIR': '/tmp/output',
                'AWS_LAMBDA_REQUEST_ID': '123'}
        self.assertEqual(mock_popen.call_args_list[0][1]['env'], res)

# class SupervisorTest(unittest.TestCase):
#     @mock.patch('faassupervisor.utils.ConfigUtils.read_cfg_var')
#     @mock.patch('faassupervisor.supervisor._create_supervisor')
#     @mock.patch('faassupervisor.supervisor.Supervisor._parse_input')
#     @mock.patch('faassupervisor.storage.config.StorageConfig._parse_config')
#     @mock.patch('faassupervisor.storage.config.StorageConfig.download_input')
#     def test_parse_input(self, mock_download_input, mock_parse_config , mock_parse_input, mock_create_supervisor, mock_read_cfg_var):
#             with mock.patch.dict('os.environ', {'FUNCTION_CONFIG': StrUtils.utf8_to_base64_string(CONFIG_FILE_OK),
#                                             'DOWNLOAD_INPUT': ''}, clear=True):

#                 supervisor = Supervisor(DELEGATED_MINIO_EVENT)
#                 parsed_event = MinioEvent(MINIO_EVENT, provider_id = 'cluster2')
#                 supervisor._parse_input()
#                 mock_create_supervisor.assert_called_with(DELEGATED_MINIO_EVENT, None, 'MINIO')
#                 mock_parse_config.assert_called()
#                 mock_read_cfg_var.assert_called_with('output')
#                 mock_parse_input.assert_called()
#                 self.assertEqual(supervisor.parsed_event.get_type(), parsed_event.get_type())
#                 mock_download_input.assert_called_with(parsed_event, '/tmp/input')
#                 #mock_read_cfg_var.assert_called_with('storage_providers')
# """             minio_auth = StorageConfig()._get_auth_data('MINIO', 'cluster2')
#                 self.assertEqual(minio_auth['test_minio'].type, 'MINIO') """

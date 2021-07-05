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
import subprocess
from faassupervisor.faas.binary.supervisor import BinarySupervisor
from faassupervisor.faas.aws_lambda.supervisor import LambdaSupervisor, \
                                                      is_batch_execution, \
                                                      _is_lambda_batch_execution
from faassupervisor.exceptions import NoLambdaContextError

# pylint: disable=missing-docstring
# pylint: disable=no-self-use
# pylint: disable=protected-access


class BinarySupervisorTest(unittest.TestCase):

    @mock.patch('subprocess.Popen')
    @mock.patch('faassupervisor.utils.FileUtils.create_file_with_content')
    def test_execute_function(self, mock_create, mock_popen):
        supervisor = BinarySupervisor('UNKNOWN')
        with mock.patch.dict('os.environ', {'SCRIPT':'ZmFrZSBzY3JpcHQh',
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
        return  mock_context

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

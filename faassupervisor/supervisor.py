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

""" Module with all the generic supervisor classes and methods.
Also entry point of the faassupervisor package."""

import distutils.util
from faassupervisor.events import parse_event
from faassupervisor.exceptions import exception, FaasSupervisorError
from faassupervisor.storage.config import StorageConfig
from faassupervisor.utils import SysUtils, FileUtils, ConfigUtils
from faassupervisor.logger import configure_logger, get_logger
from faassupervisor.faas.aws_lambda.supervisor import LambdaSupervisor, is_batch_execution
from faassupervisor.faas.binary.supervisor import BinarySupervisor


class Supervisor():
    """Generic supervisor used to create the required supervisors
    based on the environment variable 'SUPERVISOR_TYPE'."""

    # pylint: disable=too-few-public-methods

    def __init__(self, event, context=None):
        self._create_tmp_dirs()
        # Parse the event_info data
        self.parsed_event = parse_event(event)
        # Read storage config
        self._read_storage_config()
        # Create the supervisor
        self.supervisor = _create_supervisor(event, context, self.parsed_event.get_type())

    def _create_tmp_dirs(self):
        """Creates the temporal directories where the
        input/output data is going to be stored.

        The folders are deleted automatically
        when the execution finishes.
        """
        self.input_tmp_dir = FileUtils.create_tmp_dir()
        self.output_tmp_dir = FileUtils.create_tmp_dir()
        SysUtils.set_env_var("TMP_INPUT_DIR", self.input_tmp_dir.name)
        SysUtils.set_env_var("TMP_OUTPUT_DIR", self.output_tmp_dir.name)

    def _read_storage_config(self):
        get_logger().info("Reading storage configuration")
        self.stg_config = StorageConfig()

    @exception()
    def _parse_input(self):
        """Download input data from storage provider
        or save data from POST request.

        A function can have information from several storage providers
        but one event always represents only one file (so far), so only
        one provider is going to be used for each event received.
        """
        # Parse the 'download_input' config var
        download_input = ConfigUtils.read_cfg_var('download_input')
        if download_input == '':
            download_input = True
        else:
            try:
                download_input = bool(distutils.util.strtobool(download_input))
            except ValueError:
                download_input = True
        # Parse input file
        if download_input is False:
            get_logger().info('Skipping download of input file.')
        else:
            input_file_path = self.stg_config.download_input(self.parsed_event,
                                                             self.input_tmp_dir.name)
            if input_file_path and FileUtils.is_file(input_file_path):
                SysUtils.set_env_var('INPUT_FILE_PATH', input_file_path)
                get_logger().info('INPUT_FILE_PATH variable set to \'%s\'', input_file_path)

    @exception()
    def _parse_output(self):
        self.stg_config.upload_output(self.output_tmp_dir.name)

    @exception()
    def run(self):
        """Generic method to launch the supervisor execution."""
        try:
            if is_batch_execution() and SysUtils.is_lambda_environment():
                # Only delegate to batch
                self.supervisor.execute_function()
            else:
                self._parse_input()
                self.supervisor.execute_function()
                self._parse_output()
            get_logger().info('Creating response')
            return self.supervisor.create_response()
        except FaasSupervisorError as fse:
            get_logger().exception(fse)
            get_logger().error('Creating error response')
            return self.supervisor.create_error_response()


@exception()
def _create_supervisor(event, context=None, event_type=None):
    """Returns a new supervisor based on the
    environment.
    Binary mode by default"""
    supervisor = None
    if SysUtils.is_lambda_environment():
        supervisor = LambdaSupervisor(event, context)
    else:
        supervisor = BinarySupervisor(event_type)
    return supervisor


def main(event, context=None):
    """Initializes the generic supervisor
    and launches its execution."""
    configure_logger()
    get_logger().debug("EVENT received: %s", event)
    if context:
        get_logger().debug("CONTEXT received: %s", context)
    supervisor = Supervisor(event, context)
    return supervisor.run()


if __name__ == "__main__":
    # If supervisor is running as a binary
    # receive the input from stdin.
    ret = main(SysUtils.get_stdin())
    if ret is not None:
        print(ret)

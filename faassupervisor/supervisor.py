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

from faassupervisor.events.events import EventProvider
from faassupervisor.exceptions import exception, InvalidSupervisorTypeError, \
    FaasSupervisorError
from faassupervisor.storage.auth import StorageAuth
from faassupervisor.storage.path import StoragePath
from faassupervisor.storage.storage import StorageProvider
from faassupervisor.utils import SysUtils, FileUtils
from faassupervisor.logger import configure_logger, get_logger
from faassupervisor.faas.aws.lambda_.supervisor import LambdaSupervisor
from faassupervisor.faas.aws.batch.supervisor import BatchSupervisor
from faassupervisor.faas.openfaas.supervisor import OpenfaasSupervisor


class Supervisor():
    """Generic supervisor used to create the required supervisors
    based on the environment variable 'SUPERVISOR_TYPE'."""

    # pylint: disable=too-few-public-methods

    def __init__(self, **kwargs):
        self._create_tmp_dirs()
        # Parse the event data
        self.event = EventProvider(kwargs['event'], self._get_input_dir())
        self.input_data_providers = []
        self.output_data_providers = []
        # Create the supervisor
        self.supervisor = _create_supervisor(**kwargs)

    def _create_tmp_dirs(self):
        """Creates the temporal directories where the
        input/output data is going to be stored.

        The folders are deleted automatically
        when the execution finishes.

        In Batch mode the folders have to be created
        only at the INIT step.
        """
        if _is_batch_environment():
            if SysUtils.get_env_var("STEP") == "INIT":
                FileUtils.create_folder(SysUtils.get_env_var("TMP_INPUT_DIR"))
                FileUtils.create_folder(SysUtils.get_env_var("TMP_OUTPUT_DIR"))
        else:
            self.input_tmp_dir = FileUtils.create_tmp_dir()
            self.output_tmp_dir = FileUtils.create_tmp_dir()
            SysUtils.set_env_var("TMP_INPUT_DIR", self.input_tmp_dir.name)
            SysUtils.set_env_var("TMP_OUTPUT_DIR", self.output_tmp_dir.name)

    def _get_input_dir(self):
        if _is_batch_environment():
            return SysUtils.get_env_var("TMP_INPUT_DIR")
        return self.input_tmp_dir.name

    def _get_output_dir(self):
        if _is_batch_environment():
            return SysUtils.get_env_var("TMP_OUTPUT_DIR")
        return self.output_tmp_dir.name

    def _create_storage_providers(self):
        get_logger().info("Reading STORAGE_AUTH variables")
        storage_auths = StorageAuth()
        get_logger().info("Reading STORAGE_PATH variables")
        storage_paths = StoragePath()
        # Create input data providers
        for storage_id, storage_path in storage_paths.get_input_data():
            self.input_data_providers.append(
                StorageProvider(storage_auths.get_auth_data(storage_id), storage_path))
            get_logger().info("Found '%s' input provider",
                              self.input_data_providers[-1].get_type())
        # Create output data providers
        for storage_id, storage_path in storage_paths.get_output_data():
            self.output_data_providers.append(
                StorageProvider(storage_auths.get_auth_data(storage_id), storage_path))
            get_logger().info("Found '%s' output provider",
                              self.output_data_providers[-1].get_type())

    @exception()
    def _parse_input(self):
        """Download input data from storage provider
        or save data from POST request."""

        if _is_batch_environment():
            # Don't download anything if not INIT step
            if SysUtils.get_env_var("STEP") != "INIT":
                return
            # Manage batch extra steps
            self.supervisor.parse_input()

        # event_type could be: 'APIGATEWAY'|'MINIO'|'ONEDATA'|'S3'|'UNKNOWN'
        event_type = self.event.get_event_type()
        if event_type not in ('APIGATEWAY', 'UNKNOWN'):
            get_logger().info("Downloading input file from event type '%s'", event_type)
            for data_provider in self.input_data_providers:
                # data_provider.get_type() could be: 'MINIO'|'ONEDATA'|'S3'
                # Match the received event with the data provider
                if data_provider.get_type().upper() == event_type.upper():
                    input_file_path = data_provider.download_input(self.event,
                                                                   self._get_input_dir())
                    if input_file_path:
                        SysUtils.set_env_var("INPUT_FILE_PATH", input_file_path)
                        get_logger().info("INPUT_FILE_PATH variable set to '%s'", input_file_path)
                    break

    @exception()
    def _parse_output(self):
        # Don't upload anything if not END step
        if _is_batch_environment() and SysUtils.get_env_var("STEP") != "END":
            return

        for data_provider in self.output_data_providers:
            data_provider.upload_output(self._get_output_dir())

    @exception()
    def run(self):
        """Generic method to launch the supervisor execution."""
        try:
            self._create_storage_providers()
            self._parse_input()
            self.supervisor.execute_function()
            self._parse_output()
            get_logger().info('Creating response')
            return self.supervisor.create_response()
        except FaasSupervisorError as fse:
            get_logger().exception(fse)
            get_logger().error('Creating error response')
            return self.supervisor.create_error_response()


def _is_batch_environment():
    return _get_supervisor_type() == 'BATCH'


def _get_supervisor_type():
    return SysUtils.get_env_var("SUPERVISOR_TYPE")


@exception()
def _create_supervisor(**kwargs):
    """Returns a new supervisor based on the
    environment variable SUPERVISOR_TYPE."""
    supervisor = ""
    sup_type = _get_supervisor_type()
    if sup_type == 'LAMBDA':
        supervisor = LambdaSupervisor(**kwargs)
    elif sup_type == 'BATCH':
        supervisor = BatchSupervisor()
    elif sup_type == 'OPENFAAS':
        supervisor = OpenfaasSupervisor()
    else:
        raise InvalidSupervisorTypeError(sup_typ=sup_type)
    return supervisor


def _start_supervisor(**kwargs):
    configure_logger()
    get_logger().debug("EVENT Received: %s", kwargs['event'])
    if 'context' in kwargs:
        get_logger().debug("CONTEXT Received: %s", kwargs['context'])
    supervisor = Supervisor(**kwargs)
    return supervisor.run()


def python_main(**kwargs):
    """Called when running from a Python environment.
    Receives the input from the method arguments."""
    return _start_supervisor(**kwargs)


def main():
    """Called when running as binary.
    Receives the input from stdin."""
    kwargs = {'event': SysUtils.get_stdin()}
    return _start_supervisor(**kwargs)


if __name__ == "__main__":
    main()

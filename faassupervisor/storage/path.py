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
""" Module with class to parse STORAGE_PATH_ variables """

from collections import namedtuple
from faassupervisor.utils import SysUtils
from faassupervisor.exceptions import InvalidStoragePathTypeError


class StoragePath():
    """ Stores the input and output provider path information. """

    def __init__(self):
        self.input = {}
        self.output = {}
        self._read_storage_path_envs()

    def get_input_data(self):
        """ Return the defined input providers. """
        return self.input.items()

    def get_output_data(self):
        """ Return the defined output providers. """
        return self.output.items()

    def _read_storage_path_envs(self):
        """
        Reads the global variables to create the providers needed.
        Variable schema: STORAGE_PATH_$1_$2
                         $1: INPUT | OUTPUT
                         $2: STORAGE_ID (Specified in the function definition file,
                                         is unique for each storage defined)
        e.g.: STORAGE_PATH_INPUT_12345
        """
        for env_key, env_val in SysUtils.get_all_env_vars().items():
            if env_key.startswith("STORAGE_PATH_"):
                self._parse_storage_path(env_key, env_val)

    def _parse_storage_path(self, env_key, env_val):
        provider_key = namedtuple('provider_key', ['type', 'id'])
        storage_path = namedtuple('storage_path', ['id', 'path'])
        # Remove 'STORAGE_PATH_' and don't split past the id
        # STORAGE_PATH_INPUT_123_45 -> STORAGE_PATH_*[INPUT, 123_45]
        prov_key = provider_key(*env_key[13:].split("_", 1))
        # Store a tuple, so the information can't be modified
        path = storage_path(prov_key.id, env_val)
        if prov_key.type == 'INPUT':
            self.input[prov_key.id] = path
        elif prov_key.type == 'OUTPUT':
            self.output[prov_key.id] = path
        else:
            raise InvalidStoragePathTypeError(storage_type=prov_key.type)

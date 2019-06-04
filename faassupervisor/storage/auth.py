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
"""Classes to parse, store and manage storage authentication information."""
from faassupervisor.utils import SysUtils


class AuthData():
    """Stores provider authentication values."""

    def __init__(self, storage_id):
        self.storage_id = storage_id
        self.type = 'UNKNOWN'
        self.event = {}

    def set_auth_var(self, auth_var, val):
        """Store authentication credentials like USER|PASS|TOKEN|SPACE|HOST."""
        self.event[auth_var] = val

    def get_auth_var(self, val):
        """Return authentication credentials previously stored."""
        return self.event.get(val, "")


class StorageAuth():
    """Parses all the provider authentication variables."""

    # pylint: disable=too-few-public-methods

    def __init__(self):
        self.auth_data = {}
        self._read_storage_providers_envs()

    def _read_storage_providers_envs(self):
        """Reads the global variables to create the providers needed.

        Variable schema:  STORAGE_AUTH_$1_$2_$3
        $1: MINIO | S3 | ONEDATA
        $2: STORAGE_ID (Specified in the function definition file,
                        is unique for each storage defined)
        $3: USER | PASS | TOKEN | SPACE | HOST

        e.g.: STORAGE_AUTH_MINIO_12345_USER
        """
        env_vars = SysUtils.get_all_env_vars()
        for env_key, env_val in env_vars.items():
            if env_key.startswith("STORAGE_AUTH_"):
                self._parse_storage_auth(env_key[13:].split("_"), env_val)

    def _parse_storage_auth(self, env_key, env_val):
        """ Creates the classes needed to initialize the storage providers.

        The provider_id can be composed by several fields:
        Two different cases:
          - key1 = "STORAGE_AUTH_MINIO_123_456_USER"
            ['STORAGE', 'AUTH', 'MINIO', '123', '456', 'USER']
          - key2 = "STORAGE_AUTH_MINIO_123-456_USER"
            ['STORAGE', 'AUTH', 'MINIO', '123-456', 'USER']
        """
        storage_prov_id = "_".join(env_key[1:-1])
        if storage_prov_id not in self.auth_data:
            self.auth_data[storage_prov_id] = AuthData(storage_prov_id)
            self.auth_data[storage_prov_id].type = env_key[0]
        self.auth_data[storage_prov_id].set_auth_var(env_key[-1], env_val)

    def get_auth_data(self, storage_prov_id):
        """Returns the authentication credentials previously stored."""
        return self.auth_data.get(storage_prov_id)

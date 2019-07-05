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
from collections import namedtuple
from faassupervisor.utils import SysUtils


class AuthData():
    """Stores provider authentication values."""

    def __init__(self, storage_id, storage_type):
        self.storage_id = storage_id
        self.type = storage_type
        self.creds = {}

    def set_credential(self, key, val):
        """Store authentication credentials like USER|PASS|TOKEN|SPACE|HOST."""
        self.creds[key] = val

    def get_credential(self, key):
        """Return authentication credentials previously stored."""
        return self.creds.get(key, "")


class StorageAuth():
    """Parses all the provider authentication variables."""

    def __init__(self):
        self.auth_id = {}
        self.auth_type = {}

    def read_storage_providers(self):
        """Reads the global variables to create the providers needed.

        Variable schema:  STORAGE_AUTH_$1_$2_$3
        $1: MINIO | S3 | ONEDATA
        $2: USER | PASS | TOKEN | SPACE | HOST
        $3: STORAGE_ID (Specified in the function definition file,
                        is unique for each storage defined)

        e.g.: STORAGE_AUTH_MINIO_USER_12345
        """
        # Remove the prefix 'STORAGE_AUTH_'
        env_vars = SysUtils.get_filtered_env_vars("STORAGE_AUTH_")
        # type = MINIO | S3 | ONEDATA ...
        # cred = USER | PASS | TOKEN ...
        provider_info = namedtuple('provider_info', ['type', 'cred', 'id'])
        for env_key, env_val in env_vars.items():
            # Don't split past the id
            # MINIO_USER_123_45 -> *[MINIO, USER, 123_45]
            prov_info = provider_info(*env_key.split("_", 2))
            # Link ID with TYPE
            if prov_info.id not in self.auth_id:
                self.auth_id[prov_info.id] = prov_info.type
            if prov_info.type not in self.auth_type:
                # Link TYPE with AUTH data
                self.auth_type[prov_info.type] = AuthData(prov_info.id, prov_info.type)
            self.auth_type[prov_info.type].set_credential(prov_info.cred, env_val)

    def get_auth_data_by_stg_type(self, storage_type):
        """Returns the authentication credentials previously stored."""
        return self.auth_type.get(storage_type)

    def get_data_by_stg_id(self, storage_id):
        """Returns the authentication credentials previously stored."""
        prov_type = self.auth_id.get(storage_id)
        return self.get_auth_data_by_stg_type(prov_type)

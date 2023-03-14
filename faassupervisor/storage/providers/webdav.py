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
""" Module containing all the classes and methods
related with the WebDav storage provider. """

from faassupervisor.storage.providers import DefaultStorageProvider
from webdav3.client import Client
from faassupervisor.utils import SysUtils

class WebDav(DefaultStorageProvider):
    """Class that manages downloads and uploads from providers that use WebDav."""

    _TYPE = "WEBDAV"

    def __init__(self, stg_auth):
        super().__init__(stg_auth)
        self.client = self._get_client()

    def _get_client(self):
        """Returns a WebDav client to connect to the https endpoint of the storage provider"""
        options = {
            'webdav_hostname': 'https://'+self.stg_auth.get_credential('hostname'),
            'webdav_login':    self.stg_auth.get_credential('login'),
            'webdav_password': self.stg_auth.get_credential('password')
        }
        return Client(options=options)

    def download_file(self, parsed_event, input_dir_path):
        file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
        self.client.download_sync(remote_path=parsed_event.object_key, local_path=file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name, output_path):
        if self.client.check(output_path):
            self.client.upload_sync(remote_path=output_path+"/"+file_name, local_path=file_path)
        else:
            self.client.mkdir(output_path)
            self.client.upload_sync(remote_path=output_path+"/"+file_name, local_path=file_path)

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
related with the Rucio storage provider. """

import os
import tempfile
from faassupervisor.logger import get_logger
from faassupervisor.storage.providers import DefaultStorageProvider
from faassupervisor.utils import SysUtils
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.client.downloadclient import DownloadClient
from rucio.common.exception import DataIdentifierAlreadyExists, NoFilesUploaded
from faassupervisor.exceptions import RucioDataIdentifierAlreadyExists


class Rucio(DefaultStorageProvider):
    """ Class that manages downloads and uploads from Rucio. """

    _TYPE = 'RUCIO'
    _OIDC_SCOPE = 'openid profile offline_access eduperson_entitlement'
    _CA_CERT = '/etc/ssl/certs/ca-certificates.crt'
    _FOLDER_SEPARATOR = '__'

    def __init__(self, stg_auth):
        super().__init__(stg_auth)
        self._set_rucio_environment()

    def __del__(self):
        try:
            for file in self.tmp_files:
                os.remove(file)
        except Exception as exc:
            get_logger().warning('Error removing temporary files: %s', exc)

    def _set_rucio_environment(self):
        self.tmp_files = []
        self.rucio_host = self.stg_auth.get_credential('host')
        self.scope = self.stg_auth.get_credential('account')
        self.rse = self.stg_auth.get_credential('rse')
        token_temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_files.append(token_temp_file.name)
        token_temp_file.write(self.stg_auth.get_credential('token').encode())
        token_temp_file.close()
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_files.append(temp_file.name)
        temp_file.write(b'[client]\n')
        temp_file.write(b'rucio_host = %s\n' % self.stg_auth.get_credential('host').encode())
        temp_file.write(b'auth_host = %s\n' % self.stg_auth.get_credential('auth_host').encode())
        temp_file.write(b'ca_cert = %s\n' % self._CA_CERT.encode())
        temp_file.write(b'auth_type = oidc\n')
        temp_file.write(b'account = %s\n' % self.stg_auth.get_credential('account').encode())
        temp_file.write(b'auth_token_file_path = %s\n' % token_temp_file.name.encode())
        temp_file.write(b'oidc_scope = %s\n' % self._OIDC_SCOPE.encode())
        temp_file.close()
        os.environ['RUCIO_CONFIG'] = temp_file.name
        self.client = Client()
        self.upload_client = UploadClient(self.client)
        self.download_client = DownloadClient(self.client)

    def download_file(self, parsed_event, input_dir_path):
        """Downloads the file from Rucio and
        returns the path were the download is placed. """
        get_logger().info("Downloading item from host '%s' with key '%s'",
                          self.rucio_host,
                          parsed_event.object_key)
        did_name = parsed_event.object_key.replace('/', self._FOLDER_SEPARATOR)
        file = {'did': '%s:%s' % (self.scope, did_name)}

        download = self.download_client.download_dids([file])
        get_logger().debug('Downloaded file info: %s', download)
        file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
        os.rename(SysUtils.join_paths(self.scope, did_name), file_download_path)
        return file_download_path

    def upload_file(self, file_path, file_name, output_path):
        """Uploads the file to the Rucio output path.
        It fakes the folder structure by using a separator in the file name."""
        file_name = file_name.strip('/')
        upload_path = f'{output_path}/{file_name}'

        file = {
            'path': file_path,
            'did_scope': self.scope,
            # For some reason, the did_name cannot contain slashes
            'did_name': upload_path.replace('/', self._FOLDER_SEPARATOR)
        }
        if self.rse:
            file['rse'] = self.rse

        get_logger().info("Uploading file '%s' to host '%s'",
                          file_path,
                          self.rucio_host)
        try:
            upload = self.upload_client.upload([file])
            get_logger().debug('Uploaded file info: %s', upload)
        except DataIdentifierAlreadyExists:
            raise RucioDataIdentifierAlreadyExists(scope=self.scope, file_name=file_name)
        except NoFilesUploaded:
            get_logger().info('File %s not uploaded. It already exists. Ignore.' % file_path)

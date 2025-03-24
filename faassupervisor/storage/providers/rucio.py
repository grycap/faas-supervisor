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

# Import classes to force pyinstaller to add them to the package
try:
    import dogpile.cache.backends.memory # noqa pylint: disable=unused-import
    from rucio.rse.protocols import bittorrent, cache, dummy, globus, gsiftp, http_cache, mock, ngarc, posix, protocol, rclone, rfio, srm, ssh, storm, webdav, xrootd # noqa pylint: disable=unused-import
    # from rucio.rse.protocols import gfal  # noqa pylint: disable=unused-import
    # import gfal2  # noqa pylint: disable=unused-import
except Exception:  # nosec pylint: disable=broad-except
    pass


from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.client.downloadclient import DownloadClient
from rucio.client.rseclient import RSEClient
from rucio.common.exception import DataIdentifierAlreadyExists, NoFilesUploaded
from faassupervisor.exceptions import RucioDataIdentifierAlreadyExists, RucioNotRSE
from faassupervisor.logger import get_logger
from faassupervisor.storage.providers import DefaultStorageProvider
from faassupervisor.utils import SysUtils


class Rucio(DefaultStorageProvider):
    """ Class that manages downloads and uploads from Rucio. """

    _TYPE = 'RUCIO'
    _OIDC_SCOPE = 'openid profile offline_access eduperson_entitlement'
    _FOLDER_SEPARATOR = '__'
    _CONFIG_FILE = 1
    _TOKEN_FILE = 0

    def __init__(self, stg_auth):
        super().__init__(stg_auth)
        self.tmp_files = []
        self._set_rucio_environment()

    def __del__(self):
        try:
            for file in self.tmp_files:
                os.remove(file)
        except Exception as exc:
            get_logger().warning('Error removing temporary files: %s', exc)

    def _set_rucio_environment(self):
        self.rucio_host = self.stg_auth.get_credential('host')
        self.auth_host = self.stg_auth.get_credential('auth_host')
        self.scope = self.stg_auth.get_credential('account')
        self.rse = self.stg_auth.get_credential('rse')
        self.token = self.stg_auth.get_credential('token')

    def _get_rucio_client(self, client_type=None):
        for file in self.tmp_files:
            os.remove(file)
        self.tmp_files = []
        token_temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_files.append(token_temp_file.name)
        token_temp_file.write(self.stg_auth.get_credential('token').encode())
        token_temp_file.close()
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_files.append(temp_file.name)
        temp_file.write(b'[client]\n')
        temp_file.write(b'rucio_host = %s\n' % self.rucio_host.encode())
        temp_file.write(b'auth_host = %s\n' % self.auth_host.encode())
        temp_file.write(b'auth_type = oidc\n')
        temp_file.write(b'account = %s\n' % self.scope.encode())
        temp_file.write(b'auth_token_file_path = %s\n' % token_temp_file.name.encode())
        temp_file.write(b'oidc_scope = %s\n' % self._OIDC_SCOPE.encode())
        temp_file.close()
        os.environ['RUCIO_CONFIG'] = temp_file.name
        client = Client()
        if not client_type:
            return client
        elif client_type == "upload":
            return UploadClient(client)
        elif client_type == "download":
            return DownloadClient(client)

    def download_file(self, parsed_event, input_dir_path):
        """Downloads the file from Rucio and
        returns the path were the download is placed. """
        get_logger().info("Downloading item from host '%s' with key '%s'",
                          self.rucio_host,
                          parsed_event.object_key)
        did_name = parsed_event.object_key.replace('/', self._FOLDER_SEPARATOR)
        file = {'did': '%s:%s' % (parsed_event.scope, did_name)}

        if parsed_event.token:
            self.token = parsed_event.token

        downloadc = self._get_rucio_client("download")
        download = downloadc.download_dids([file])
        get_logger().debug('Downloaded file info: %s', download)
        file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
        os.rename(SysUtils.join_paths(parsed_event.scope, did_name), file_download_path)
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
        else:
            # if not set, get the first RSE available
            try:
                rses = RSEClient().list_rses()
                file['rse'] = list(rses)[0]['rse']
            except Exception as exc:
                raise RucioNotRSE(msg=str(exc))

        get_logger().info("Uploading file '%s' to host '%s'",
                          file_path,
                          self.rucio_host)
        try:
            uploadc = self._get_rucio_client("upload")
            upload = uploadc.upload([file])
            get_logger().debug('Uploaded file info: %s', upload)
        except DataIdentifierAlreadyExists:
            raise RucioDataIdentifierAlreadyExists(scope=self.scope, file_name=file_name)
        except NoFilesUploaded:
            get_logger().info('File %s not uploaded. It already exists. Ignore.' % file_path)

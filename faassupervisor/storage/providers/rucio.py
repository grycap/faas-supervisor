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
from faassupervisor.utils import FileUtils

# Import classes to force pyinstaller to add them to the package
try:
    import dogpile.cache.backends.memory # noqa pylint: disable=unused-import
    from rucio.rse.protocols import bittorrent, cache, dummy, globus, gsiftp, http_cache, mock, ngarc, posix, protocol, rclone, rfio, srm, ssh, storm, webdav, xrootd # noqa pylint: disable=unused-import
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
from faassupervisor.utils import SysUtils, OIDCUtils, FileUtils


class Rucio(DefaultStorageProvider):
    """ Class that manages downloads and uploads from Rucio. """

    _TYPE = 'RUCIO'
    _OIDC_SCOPE = 'openid profile offline_access eduperson_entitlement'

    def __init__(self, stg_auth):
        super().__init__(stg_auth)
        # Initializes the Rucio storage provider
        self.client_id = self.stg_auth.get_credential('client_id')
        if not self.client_id:
            self.client_id = 'token-portal'
        self.oidc_token = self.stg_auth.get_credential('access_token')
        self.rucio_host = self.stg_auth.get_credential('host')
        self.auth_host = self.stg_auth.get_credential('auth_host')
        self.scope = self.stg_auth.get_credential('account')
        self.rse = self.stg_auth.get_credential('rse')
        self.refresh_token = self.stg_auth.get_credential('refresh_token')
        self.token_endpoint = self.stg_auth.get_credential('token_endpoint')
        self.oidc_audience = self.stg_auth.get_credential('oidc_audience')
        if not self.token_endpoint:
            self.token_endpoint = OIDCUtils.DEFAULT_TOKEN_ENDPOINT
        self.scopes = self.stg_auth.get_credential('scopes')
        if not self.scopes:
            self.scopes = self._OIDC_SCOPE.split()
        self.token_temp_file = tempfile.mktemp(prefix='rucio_token_',
                                               suffix='.token')  # nosec
        self._create_config_file()

    def __del__(self):
        try:
            if os.path.exists(self.token_temp_file):
                os.remove(self.token_temp_file)
            if os.path.exists(self.cfg_temp_file):
                os.remove(self.cfg_temp_file)
        except Exception as exc:
            get_logger().warning('Error removing temporary file: %s', exc)

    def _get_access_token(self):
        """Returns the access token for Rucio."""
        if OIDCUtils.is_access_token_expired(self.oidc_token):
            get_logger().debug("Access token expired, refreshing...")
            if not self.refresh_token:
                raise ValueError("Refresh token is not set in the storage authentication.")

            self.oidc_token = OIDCUtils.refresh_access_token(self.refresh_token,
                                                             self.scopes,
                                                             self.token_endpoint,
                                                             self.oidc_audience,
                                                             self.client_id)
        return self.oidc_token

    def _create_config_file(self):
        cfg_temp_file = tempfile.NamedTemporaryFile(delete=False)
        cfg_temp_file.write(b'[client]\n')
        cfg_temp_file.write(b'rucio_host = %s\n' % self.rucio_host.encode())
        cfg_temp_file.write(b'auth_host = %s\n' % self.auth_host.encode())
        cfg_temp_file.write(b'auth_type = oidc\n')
        cfg_temp_file.write(b'account = %s\n' % self.scope.encode())
        cfg_temp_file.write(b'auth_token_file_path = %s\n' % self.token_temp_file.encode())
        cfg_temp_file.write(b'oidc_scope = %s\n' % self._OIDC_SCOPE.encode())
        self.cfg_temp_file = cfg_temp_file.name

    def _get_rucio_client(self, client_type=None):
        # Create token file
        with open(self.token_temp_file, 'w') as f:
            f.write(self._get_access_token())
        os.environ['RUCIO_CONFIG'] = self.cfg_temp_file
        client = Client()
        if not client_type:
            return client
        elif client_type == "upload":
            return UploadClient(client)
        elif client_type == "download":
            return DownloadClient(client)

    def download_file(self, parsed_event, input_dir_path):
        """Downloads the dataset from Rucio and
        returns the path were the all the downloaded
        files are placed. """
        get_logger().info("Downloading item from host '%s' with key '%s'",
                          self.rucio_host,
                          parsed_event.object_key)
        dataset_name = parsed_event.object_key

        rcli = self._get_rucio_client()
        files = rcli.list_files(parsed_event.scope, dataset_name)

        dids = [{'did': '%s:%s' % (f['scope'], f['name'])} for f in files]

        output_dir = SysUtils.join_paths(input_dir_path, dataset_name)
        FileUtils.create_folder(output_dir)

        downloadc = self._get_rucio_client("download")
        download = downloadc.download_dids(dids)
        get_logger().debug('Downloaded file info: %s', download)
        try:
            for file in download[0]["dest_file_paths"]:
                basename = os.path.basename(file)
                FileUtils.cp_file(file, SysUtils.join_paths(output_dir,
                                                            basename))
        except Exception as e:
            print("An exception occurred" + e)
        return output_dir

    def upload_file(self, file_path, file_name, output_path):
        """Uploads the file to Rucio.
        In this case the output path is ignored.
        """
        file_name = file_name.strip('/')
        upload_path = file_name

        file = {
            'path': file_path,
            'did_scope': self.scope,
            'did_name': upload_path
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

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
related with the Onedata storage provider. """

import requests
from faassupervisor.logger import get_logger
from faassupervisor.storage.providers import DefaultStorageProvider
from faassupervisor.utils import SysUtils, FileUtils
from faassupervisor.exceptions import OnedataDownloadError, \
    OnedataUploadError, OnedataFolderCreationError


class Onedata(DefaultStorageProvider):
    """Class that manages downloads and uploads from Onedata. """

    _TYPE = 'ONEDATA'
    _CDMI_PATH = '/cdmi'
    _CDMI_VERSION_HEADER = {'X-CDMI-Specification-Version': '1.1.1'}

    def __init__(self, stg_auth):
        super().__init__(stg_auth)
        self._set_onedata_environment()

    def _set_onedata_environment(self):
        self.oneprovider_space = self.stg_auth.get_credential('space')
        self.oneprovider_host = self.stg_auth.get_credential('oneprovider_host')
        self.headers = {'X-Auth-Token': self.stg_auth.get_credential('token')}

    def _create_folder(self, folder_name):
        url = (f'https://{self.oneprovider_host}{self._CDMI_PATH}/'
               f'{self.oneprovider_space}/{folder_name}/')
        response = requests.put(url, headers=self.headers)
        if response.status_code != 201:
            raise OnedataFolderCreationError(folder_name=folder_name,
                                             status_code=response.status_code)

    def _folder_exists(self, folder_name):
        url = (f'https://{self.oneprovider_host}{self._CDMI_PATH}/'
               f'{self.oneprovider_space}/{folder_name}/')
        headers = {**self._CDMI_VERSION_HEADER, **self.headers}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True
        return False

    def download_file(self, parsed_event, input_dir_path):
        """Downloads the file from the space of Onedata and
        returns the path were the download is placed. """
        file_download_path = ""
        url = f'https://{self.oneprovider_host}{self._CDMI_PATH}{parsed_event.object_key}'
        get_logger().info('Downloading item from host \'%s\' with key \'%s\'',
                          self.oneprovider_host,
                          parsed_event.object_key)
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            file_download_path = SysUtils.join_paths(input_dir_path, parsed_event.file_name)
            FileUtils.create_file_with_content(file_download_path, response.content, mode='wb')

            get_logger().info('Successful download of file \'%s\' with key \'%s\' in path \'%s\'',
                              parsed_event.file_name,
                              parsed_event.object_key,
                              file_download_path)
        else:
            raise OnedataDownloadError(file_name=parsed_event.object_key,
                                       status_code=response.status_code)
        return file_download_path

    def upload_file(self, file_path, file_name, output_path):
        """Uploads the file to the Onedata output path."""
        file_name = file_name.strip('/')
        output_path = output_path.strip('/')
        upload_path = f'{output_path}/{file_name}'
        upload_folder = FileUtils.get_dir_name(upload_path)
        # Create output folder (and subfolders) if it does not exists
        if not self._folder_exists(upload_folder):
            folders = upload_folder.split('/')
            path = ''
            for folder in folders:
                path = f'{path}/{folder}'
                if not self._folder_exists(path):
                    self._create_folder(path)
        # Upload the file
        url = (f'https://{self.oneprovider_host}{self._CDMI_PATH}/'
               f'{self.oneprovider_space}/{upload_path}')
        get_logger().info('Uploading file \'%s\' to space \'%s\'',
                          upload_path,
                          self.oneprovider_space)
        with open(file_path, 'rb') as data:
            response = requests.put(url, data=data, headers=self.headers)
            if response.status_code not in [201, 202, 204]:
                raise OnedataUploadError(file_name=file_name,
                                         status_code=response.status_code)

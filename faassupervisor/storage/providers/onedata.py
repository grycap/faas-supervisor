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

from faassupervisor.storage.storage import DefaultStorageProvider
import faassupervisor.logger as logger
import faassupervisor.utils as utils
import requests

class Onedata(DefaultStorageProvider):

    CDMI_PATH = 'cdmi'

    def __init__(self, **kwargs):
        self.auth = kwargs['Auth']
        # This is the output bucket in case of OUTPUT storage
        self.storage_path = kwargs['Path']
        self._set_onedata_environment()

    def _set_onedata_environment(self):
        self.oneprovider_space = self.auth.get('SPACE')
        self.oneprovider_host = self.auth.get('HOST')
        self.headers = { 'X-Auth-Token': self.auth.get('TOKEN') }

    def download_input(self, event, input_dir_path):
        '''Downloads the file from the Onedata space and
        returns the path were the download is placed'''
        url = 'https://{0}/{1}{2}'.format(self.oneprovider_host, self.CDMI_PATH, event.object_key)
        logger.info("Downloading item from '{0}' with key '{1}'".format(event.bucket_name, event.object_key))
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            file_download_path = utils.join_paths(input_dir_path, event.file_name)
            utils.create_file_with_content(file_download_path, response.content, mode='wb')
            logger.info("Successful download of file '{0}' from Oneprovider '{1}' in path '{2}'".format(event.file_name,
                                                                                                         event.object_key,
                                                                                                         file_download_path))
            return file_download_path
        else:
            logger.error("File download from Onedata failed!")

    def upload_output(self, output_dir_path):
        output_files = utils.get_all_files_in_directory(output_dir_path)
        logger.info("Found the following files to upload: {0}".format(output_files))
        for file_path in output_files:
            file_name = file_path.replace("{0}/".format(output_dir_path), "")
            self.upload_file(file_path, file_name)

    def upload_file(self, file_path, file_name):
        url = 'https://{0}/{1}/{2}/{3}/{4}'.format(self.oneprovider_host, self.CDMI_PATH, self.oneprovider_space, self.storage_path, file_name)
        logger.info("Uploading file '{0}' to '{1}/{2}'".format(file_name, self.oneprovider_space, self.storage_path))
        with open(file_path, 'rb') as data:
            response = requests.put(url, data=data, headers=self.headers)
            if response.status_code not in [201, 202, 204]:
                logger.error("Upload failed. Status code: {0}".format(response.status_code))

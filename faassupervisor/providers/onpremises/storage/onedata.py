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

import os
import requests
import faassupervisor.utils as utils
from faassupervisor.interfaces.dataprovider import DataProviderInterface

class Onedata(DataProviderInterface):

    CDMI_PATH = '/cdmi'

    def __init__(self, event, output_folder):
        self.event = event
        self.os_tmp_folder = os.path.dirname(output_folder)
        self.output_folder = output_folder
        # Onedata settings
        self.onedata_access_token = os.environ.get('ONEDATA_ACCESS_TOKEN')
        self.oneprovider_host = os.environ.get('ONEPROVIDER_HOST')
        self.oneprovider_space = os.environ.get('ONEDATA_SPACE')
        self.headers = {
            'X-Auth-Token': self.onedata_access_token
        }

    @classmethod    
    def is_onedata_event(cls, event):
        if utils.is_key_and_value_in_dictionary('eventSource', event):
            return event['eventSource'] == 'OneTrigger'
        return False

    def download_input(self):
        '''Downloads the file from the Onedata space and returns the path were the download is placed'''
        file_path = self.event['path']
        file_name = self.event['file']
        url = 'https://{0}{1}{2}'.format(self.oneprovider_host, self.CDMI_PATH, file_path)
        print("Downloading item from '{0}' with key '{1}'".format(file_path, file_name))
        req = requests.get(url, headers=self.headers)
        if req.status_code == 200:
            file_download_path = "{0}/{1}".format(self.os_tmp_folder, file_name) 
            utils.create_folder(self.os_tmp_folder)
            print(file_name)
            with open(file_download_path, 'wb') as f:
                f.write(req.content)
            print("Successful download of file '{0}' from Oneprovider '{1}' in path '{2}'".format(file_name, file_path, file_download_path))
            return file_download_path
        else:
            print("Download failed!")
            return None

    def upload_output(self):
        output_files_path = utils.get_all_files_in_directory(self.output_folder)
        print("UPLOADING FILES {0}".format(output_files_path))
        for file_path in output_files_path:
            file_name = os.path.basename(file_path)
            output_file_name = "{0}-out{1}".format(os.path.splitext(file_name)[0],''.join(os.path.splitext(file_name)[1:]))
            self.upload_file(file_path, output_file_name)

    def upload_file(self, file_path, file_name):
        # Get OUTPUT_BUCKET environment variable to use as output folder in Onedata Space
        # If not set upload files to space root
        output_bucket = os.environ.get('OUTPUT_BUCKET')
        if output_bucket == None:
            url = 'https://{0}{1}/{2}/{3}'.format(self.oneprovider_host, self.CDMI_PATH, self.oneprovider_space, file_name)
        else:
            url = 'https://{0}{1}/{2}/{3}/{4}'.format(self.oneprovider_host, self.CDMI_PATH, self.oneprovider_space, output_bucket, file_name)
        print("Uploading file  '{0}' to '{1}/{2}'".format(file_name, self.oneprovider_space, output_bucket))
        with open(file_path, 'rb') as f:
            req = requests.put(url, data=f, headers=self.headers)
        if req.status_code not in [201, 202, 204]:
            print("Upload failed. Status code: {0}".format(req.status_code))
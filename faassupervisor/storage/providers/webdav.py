from faassupervisor.storage.providers import DefaultStorageProvider
from faassupervisor.utils import SysUtils, FileUtils
from webdav3.client import Client


class WebDav(DefaultStorageProvider):
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
        self.client.download_async(remote_path=parsed_event.object_key, local_path=file_download_path)

    def upload_file(self, file_path, file_name, output_path):
        if self.client.check(output_path):
            self.client.upload_sync(remote_path=output_path+"/"+file_name, local_path=file_path)
        else:
            self.client.mkdir(output_path)
            self.client.upload_sync(remote_path=output_path+"/"+file_name, local_path=file_path)

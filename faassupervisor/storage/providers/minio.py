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
related with the Minio storage provider. """

import boto3
from faassupervisor.storage.providers.s3 import S3


class Minio(S3):
    """Class that manages downloads and uploads from Minio. """

    _DEFAULT_MINIO_ENDPOINT = 'http://minio-service.minio:9000'
    _TYPE = 'MINIO'

    def _get_client(self):
        """Return Minio client with user configuration."""
        endpoint = self.stg_auth.get_credential('endpoint')
        if endpoint == '':
            endpoint = self._DEFAULT_MINIO_ENDPOINT
        verify = self.stg_auth.get_credential('verify')
        if verify == '':
            verify = True
        region = self.stg_auth.get_credential('region')
        if region == '':
            region = None
        return boto3.client('s3',
                            endpoint_url=endpoint,
                            region_name=region,
                            verify=verify,
                            aws_access_key_id=self.stg_auth.get_credential('access_key'),
                            aws_secret_access_key=self.stg_auth.get_credential('secret_key'))


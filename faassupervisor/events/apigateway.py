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
""" Module to define classes and methods related
with the API Gateway event.

API Gateway event example:

{'body': '/9j/4AAQSkZJRgAB5Wp//Z',
 'headers': {'Accept': '*/*',
             'Accept-Encoding': 'gzip, deflate',
             'Content-Type': 'application/octet-stream',
             'Host': 'xxxxxx.execute-api.us-east-1.amazonaws.com',
             'User-Agent': 'python-requests/2.21.0',
             'X-Amzn-Trace-Id': 'Root=1-xxx-xxxxx',
             'X-Forwarded-For': '84.111.4.22',
             'X-Forwarded-Port': '443',
             'X-Forwarded-Proto': 'https'},
 'httpMethod': 'POST',
 'isBase64Encoded': False,
 'multiValueHeaders': {'Accept': ['*/*'],
                       'Accept-Encoding': ['gzip, deflate'],
                       'Content-Type': ['application/octet-stream'],
                       'Host': ['xxxxxx.execute-api.us-east-1.amazonaws.com'],
                       'User-Agent': ['python-requests/2.21.0'],
                       'X-Amzn-Trace-Id': ['Root=1-xxx-xxxxx'],
                       'X-Forwarded-For': ['84.111.4.22'],
                       'X-Forwarded-Port': ['443'],
                       'X-Forwarded-Proto': ['https']},
 'multiValueQueryStringParameters': None,
 'path': '/launch',
 'pathParameters': {'proxy': 'launch'},
 'queryStringParameters': None,
 'requestContext': {'accountId': '123456789012',
                    'apiId': 'xxxxxx',
                    'domainName': 'xxxxxx.execute-api.us-east-1.amazonaws.com',
                    'domainPrefix': 'xxxxxx',
                    'extendedRequestId': 'VjR22Hr2oAMFnIw=',
                    'httpMethod': 'POST',
                    'identity': {'accessKey': None,
                                 'accountId': None,
                                 'caller': None,
                                 'cognitoAuthenticationProvider': None,
                                 'cognitoAuthenticationType': None,
                                 'cognitoIdentityId': None,
                                 'cognitoIdentityPoolId': None,
                                 'sourceIp': '84.111.4.22',
                                 'user': None,
                                 'userAgent': 'python-requests/2.21.0',
                                 'userArn': None},
                    'path': '/scar/launch',
                    'protocol': 'HTTP/1.1',
                    'requestId': '385ed319-375f-11e9-aa95-df027d8eef05',
                    'requestTime': '23/Feb/2019:11:36:11 +0000',
                    'requestTimeEpoch': 1550921771868,
                    'resourceId': 'asdfg',
                    'resourcePath': '/{proxy+}',
                    'stage': 'scar'},
 'resource': '/{proxy+}',
 'stageVariables': None}
"""

import base64
from faassupervisor.utils import FileUtils, SysUtils
from faassupervisor.events.unknown import UnknownEvent

_JSON_TYPE = 'application/json'


class ApiGatewayEvent(UnknownEvent):
    """ Parse the API Gateway event and saves the body
    (if exists) and the request parameters (if exists). """

    _TYPE = 'APIGATEWAY'
    _FILE_NAME = 'api_event_file'

    def _set_event_params(self):
        """If has a JSON body, it can contain
        storage provider information so we save
        it for further parsing.

        Also check for request parameters."""
        self.body = self.event.get('body')
        if self._is_request_with_parameters():
            self._save_request_parameters()

    def has_json_body(self):
        """Returns true if the type of the request is JSON"""
        return self.event['headers']['Content-Type'].strip() == _JSON_TYPE

    def _is_request_with_parameters(self):
        return "queryStringParameters" in self.event \
                and self.event["queryStringParameters"]

    def _save_request_parameters(self):
        # Add passed HTTP parameters to container variables
        for key, value in self.event["queryStringParameters"].items():
            SysUtils.set_env_var(f"CONT_VAR_{key}", value)

    def save_event(self, input_dir_path):
        file_path = SysUtils.join_paths(input_dir_path, self._FILE_NAME)
        if self.has_json_body():
            FileUtils.create_file_with_content(file_path, self.body)
        else:
            FileUtils.create_file_with_content(file_path,
                                               base64.b64decode(self.body),
                                               mode='wb')
        return file_path

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

'''
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
 '''
import base64
import faassupervisor.utils as utils
import faassupervisor.logger as logger

class ApiGatewayEvent():
     
    def __init__(self, event_info, tmp_dir_path):
        self.event_info = event_info
        self.tmp_dir_path = tmp_dir_path
        self._process_api_event()
        
    def _process_api_event(self):
        if self._is_post_request_with_body():
            self.file_path = self._save_post_body()
        if self._is_request_with_parameters():
            self._save_request_parameters()
        if hasattr(self, 'file_path'):
            utils.set_environment_variable("INPUT_FILE_PATH", self.file_path)
        
    def _is_post_request_with_body(self):
        return self.event_info['httpMethod'] == 'POST' and \
               'body' in self.event_info and self.event_info['body']

    def _has_json_body(self):
        return self.event_info['headers']['Content-Type'].strip() == 'application/json'

    def _save_post_body(self):
        '''
        The received body must be a json or a base64 encoded file 
        '''
        if self._has_json_body():
            file_path = self._save_json_body()
        else:
            file_path = self._save_body()
        return file_path

    def _save_json_body(self):
        file_path = utils.join_paths(self.tmp_dir_path, "api_event.json")
        logger.get_logger().info("Received JSON from POST request and saved it in path '{0}'".format(file_path))
        utils.create_file_with_content(file_path, self.event_info['body'])
        return file_path

    def _save_body(self):
        file_path = utils.join_paths(self.tmp_dir_path, "api_event_file")
        logger.get_logger().info("Received file from POST request and saved it in path '{0}'".format(file_path))
        utils.create_file_with_content(file_path, base64.b64decode(self.event_info['body']), mode='wb')
        return file_path
        
    def _is_request_with_parameters(self):
        return "queryStringParameters" in self.event_info and self.event_info["queryStringParameters"]
        
    def _save_request_parameters(self):
        # Add passed HTTP parameters to container variables
        for key, value in self.event_info["queryStringParameters"].items():
            utils.set_environment_variable("CONT_VAR_{}".format(key), value)

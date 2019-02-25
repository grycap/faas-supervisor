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
API Gateway event

{'body': '/9j/4AAQSkZJRgAB5Wp//Z',
 'headers': {'Accept': '*/*',
             'Accept-Encoding': 'gzip, deflate',
             'Content-Type': 'application/octet-stream',
             'Host': 'xxxxxx.execute-api.us-east-1.amazonaws.com',
             'User-Agent': 'python-requests/2.21.0',
             'X-Amzn-Trace-Id': 'Root=1-xxx-xxxxx',
             'X-Forwarded-For': '84.123.4.23',
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
                       'X-Forwarded-For': ['84.123.4.23'],
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
                                 'sourceIp': '84.123.4.23',
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

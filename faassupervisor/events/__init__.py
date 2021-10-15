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
"""EventProvider identification flow:

       Create
      unknown
       event
         |
         V
    Is APIGateway? --(yes)--> Read Body, Parameters
         |                             |
        (no)                         (then)
         |-----------------------------|
         V
     Read EventProvider.
     Storage keys? --(yes)-->  Read keys (minio, s3, onedata)
         |                             |
        (no)                           |
         |-----------------------------|
         V
    Return parsed
       event
"""

import json
from faassupervisor.events.apigateway import ApiGatewayEvent
from faassupervisor.events.minio import MinioEvent
from faassupervisor.events.onedata import OnedataEvent
from faassupervisor.events.s3 import S3Event
from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.logger import get_logger
from faassupervisor.exceptions import exception, UnknowStorageEventWarning
from faassupervisor.utils import SysUtils

_S3_EVENT = "aws:s3"
_MINIO_EVENT = "minio:s3"
_ONEDATA_EVENT = "OneTrigger"


def _is_api_gateway_event(event_info):
    return 'httpMethod' in event_info


def _is_storage_event(event_info):
    if 'Records' in event_info \
           and event_info['Records'] \
           and 'eventSource' in event_info['Records'][0]:
        return event_info['Records'][0]['eventSource'] == _S3_EVENT \
            or event_info['Records'][0]['eventSource'] == _MINIO_EVENT \
            or event_info['Records'][0]['eventSource'] == _ONEDATA_EVENT
    return False


@exception()
def _parse_storage_event(event):
    record = event['Records'][0]['eventSource']
    if record == _S3_EVENT:
        parsed_event = S3Event(event)
        get_logger().info("S3 event created")
    elif record == _MINIO_EVENT:
        parsed_event = MinioEvent(event)
        get_logger().info("MINIO event created")
    elif record == _ONEDATA_EVENT:
        parsed_event = OnedataEvent(event)
        get_logger().info("ONEDATA event created")
    else:
        raise UnknowStorageEventWarning()
    return parsed_event


def parse_event(event):
    """Parses the received event and
    returns the appropriate event class."""
    # Make sure the event is always stored
    parsed_event = None
    if not isinstance(event, dict):
        try:
            event = json.loads(event)
        except ValueError:
            return UnknownEvent(event)
    # Applies the event identification flow
    if _is_api_gateway_event(event):
        get_logger().info("API Gateway event found.")
        parsed_event = ApiGatewayEvent(event)
        # Update event info with API request event body
        # to be further processed (if needed)
        if parsed_event.has_json_body():
            event = parsed_event.body
            if not isinstance(parsed_event.body, dict):
                event = json.loads(parsed_event.body)
    if _is_storage_event(event):
        get_logger().info("Storage event found.")
        parsed_event = _parse_storage_event(event)
        # Store 'object_key' in environment variable
        SysUtils.set_env_var("STORAGE_OBJECT_KEY", parsed_event.object_key)
        # Store 'event_time' in environment variable
        SysUtils.set_env_var("EVENT_TIME", parsed_event.event_time)
        # Store the raw event in environment variable
        SysUtils.set_env_var("EVENT", json.dumps(event))
    return parsed_event if parsed_event else UnknownEvent(event)

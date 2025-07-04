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
"""rucio event example:
{"event_type": "close",
 "payload": {"scope": "username",
             "name": "dataset_name"}
}
"""

from faassupervisor.events.unknown import UnknownEvent


class RucioEvent(UnknownEvent):
    """Class to parse the Rucio event."""

    _TYPE = 'RUCIO'

    def __init__(self, event, provider_id='rucio'):
        super().__init__(event.get('event') or event)
        self.provider_id = provider_id

    def _set_event_params(self):
        self.object_key = self.event['payload']['name']
        self.scope = self.event['payload']['scope']
        self.event_time = None

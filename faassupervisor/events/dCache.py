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
"""dCache event example:
{"event":
	{"name":"image2.jpg",
	"mask":["IN_CREATE"]},
"subscription":"https://prometheus.desy.de:3880/api/v1/events/channels/oyGcraV_6abmXQU0_yMApQ/subscriptions/inotify/AACvM"}
"""

from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.utils import SysUtils

class DCacheEvent(UnknownEvent):
    """Class to parse the dCache event."""

    _TYPE = 'DCACHE'

    def __init__(self, event, provider_id='dcache'):
        super().__init__(event.get('event') or event)
        self.provider_id = provider_id

    def _set_event_params(self):
        self.file_name = self.event['name']
        self.event_time = None

    def set_path(self, path):
        self.object_key = SysUtils.join_paths(path, self.file_name)

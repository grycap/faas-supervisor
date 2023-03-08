from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.utils import SysUtils

""" dCache event example:
{"event":
	{"name":"image2.jpg",
	"mask":["IN_CREATE"]},
"subscription":"https://prometheus.desy.de:3880/api/v1/events/channels/oyGcraV_6abmXQU0_yMApQ/subscriptions/inotify/AACvM"
}
"""

class DCacheEvent(UnknownEvent):
    
    _TYPE = 'DCACHE'
    
    def __init__(self, event, provider_id='dcache'):
        super().__init__(event.get('event') or event)
        self.provider_id = provider_id

    def _set_event_params(self):
        self.file_name = self.event['name']
        self.event_time = None
    
    def set_path(self, path):
        self.object_key = SysUtils.join_paths(path, self.file_name)

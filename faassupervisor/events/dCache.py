from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.utils import FileUtils

class DCacheEvent(UnknownEvent):
    
    _TYPE = 'DCACHE'
    
    def __init__(self, event, provider_id='default'):
        super().__init__(event.get('event') or event)
        self.provider_id = provider_id

    def _set_event_params(self):
        self.object_key = self.event["file_path"]
        self.file_name = FileUtils.get_file_name(self.object_key)
        self.event_time = self.event["timestamp"]
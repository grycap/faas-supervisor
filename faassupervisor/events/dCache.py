from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.utils import FileUtils
from faassupervisor.logger import get_logger

""" dCache event example:
{ "Records": [{"file_path": "/Users/calarcon/gray/input/image1.jpg",
                "timestamp": "1677592091",
                "eventSource": "dcacheTrigger"}]}


"""
class DCacheEvent(UnknownEvent):
    
    _TYPE = 'DCACHE'
    
    def __init__(self, event, provider_id='default'):
        super().__init__(event.get('event') or event)
        self.provider_id = provider_id

    def _set_event_params(self):
        get_logger().info("Setting event params:\n \
                            object_key: %s \n", self.event_records['file_path'])
        self.object_key = self.event_records['file_path']
        self.file_name = FileUtils.get_file_name(self.object_key)
        self.event_time = self.event_records['timestamp']
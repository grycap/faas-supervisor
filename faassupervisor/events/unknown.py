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
""" Module used to define classes and
methods related with unknown events. """

from faassupervisor.logger import get_logger
from faassupervisor.utils import SysUtils, FileUtils


class UnknownEvent():
    """ Class to manage unknown events. """

    # pylint: disable=too-few-public-methods

    _UNKNOWN_FILE_NAME = "event_file"

    def __init__(self, event, tmp_dir_path):
        self.event = event
        self.tmp_dir_path = tmp_dir_path
        self.file_path = self._save_unknown_event()
        SysUtils.set_env_var("INPUT_FILE_PATH", self.file_path)
        get_logger().info("INPUT_FILE_PATH set to '%s'", self.file_path)

    def _save_unknown_event(self):
        """ Stores the unknown event and return the file path where the file is stored. """
        file_path = SysUtils.join_paths(self.tmp_dir_path, self._UNKNOWN_FILE_NAME)
        FileUtils.create_file_with_content(file_path, self.event)
        get_logger().info("Received unknown event and saved it in path '%s'", file_path)
        return file_path

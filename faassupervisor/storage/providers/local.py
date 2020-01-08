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
""" Module containing all the classes and methods
related with the local storage provider. """

from faassupervisor.storage.providers import DefaultStorageProvider


class Local(DefaultStorageProvider):
    """Class to manage saving files in local storage."""

    _TYPE = 'LOCAL'

    def download_file(self, parsed_event, input_dir_path):
        """Delegates the 'download' and local storage to the event."""
        return parsed_event.save_event(input_dir_path)

    def upload_file(self, file_path, file_name, output_path):
        pass

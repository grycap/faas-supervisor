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
""" Module with a generic supervisor definition."""

import abc


class DefaultSupervisor(metaclass=abc.ABCMeta):
    """All the different supervisors must inherit from this class
    to ensure that the commands are defined."""

    @abc.abstractmethod
    def execute_function(self):
        """Executes the function code
        (udocker container or user script)."""

    @abc.abstractmethod
    def create_response(self):
        """Creates the function response."""

    @abc.abstractmethod
    def create_error_response(self):
        """Creates an error response when something fails."""

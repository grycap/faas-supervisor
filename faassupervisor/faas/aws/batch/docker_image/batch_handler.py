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
"""Module in charge of loading and executing the batch supervisor."""

import json
import faassupervisor.supervisor as supervisor
from faassupervisor.utils import SysUtils

if __name__ == "__main__":

    supervisor.python_main(event=json.loads(SysUtils.get_env_var('AWS_LAMBDA_EVENT')),
                           context=json.loads(SysUtils.get_env_var('AWS_LAMBDA_CONTEXT')))

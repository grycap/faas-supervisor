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
"""Module to define log configuration and management."""

import logging
from faassupervisor.utils import SysUtils


def _get_log_level():
    loglevel = logging.INFO
    if SysUtils.is_var_in_env("LOG_LEVEL"):
        loglevel = logging.getLevelName(SysUtils.get_env_var("LOG_LEVEL"))
    return loglevel


def _get_stream_handler():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    str_hdl = logging.StreamHandler()
    str_hdl.setFormatter(formatter)
    return str_hdl


def configure_logger():
    """Configure the global logger used by all the classes."""
    logger = logging.getLogger('supervisor')
    # Avoid initializing the logger several times
    if not logger.handlers:
        # Set logger configuration
        logger.propagate = 0
        logger.setLevel(_get_log_level())
        logger.addHandler(_get_stream_handler())


def get_logger():
    """Returns the configured logger."""
    return logging.getLogger('supervisor')

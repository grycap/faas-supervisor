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
"""Module with methods shared by all the classes."""

import base64
import json
import os
import subprocess
import sys
import tempfile
import shutil


class SysUtils():
    """Common methods for system management."""

    @staticmethod
    def get_stdin():
        """Returns system stdin."""
        return sys.stdin.read()

    @staticmethod
    def join_paths(*paths):
        """Returns the strings passed joined as a system path."""
        return os.path.join(*paths)

    @staticmethod
    def is_var_in_env(variable):
        """Checks if a variable is in the system environment."""
        return variable in os.environ

    @staticmethod
    def set_env_var(key, value):
        """Sets a system environment variable."""
        if key and value:
            os.environ[key] = value

    @staticmethod
    def get_all_env_vars():
        """Returns all the system environment variables."""
        return os.environ

    @staticmethod
    def get_env_var(variable):
        """Returns the value of system environment variable
        or an empty string if not found."""
        return os.environ.get(variable, "")

    @staticmethod
    def get_cont_env_vars():
        """Returns the defined container environment variables."""
        return SysUtils.get_filtered_env_vars("CONT_VAR_")

    @staticmethod
    def get_filtered_env_vars(key_filter):
        """Returns the global variables that start with the
        key_filter provided and removes the filter used."""

        size = len(key_filter)
        env_vars = {}
        for key, val in SysUtils.get_all_env_vars().items():
            # Find global variables with the specified prefix
            if key.startswith(key_filter):
                env_vars[key[size:]] = val
        return env_vars

    @staticmethod
    def execute_cmd(command):
        """Executes a bash command."""
        subprocess.call(command)

    @staticmethod
    def execute_cmd_and_return_output(command, encoding='utf-8'):
        """Executes a bash command and returns the console output."""
        return subprocess.check_output(command).decode(encoding)


class FileUtils():
    """Common methods for file and directory management."""

    @staticmethod
    def set_file_execution_rights(file_path):
        """Makes a file executable."""
        # Execution rights for user, group and others
        mode = os.stat(file_path).st_mode | 0o0111
        os.chmod(file_path, mode)

    @staticmethod
    def cp_file(file_src, file_dst):
        """Copy file to specified destination."""
        shutil.copyfile(file_src, file_dst)

    @staticmethod
    def create_folder(folder_name):
        """Creates a system folder.
        Does nothing if the folder exists."""
        os.makedirs(folder_name, exist_ok=True)

    @staticmethod
    def create_file_with_content(path, content, mode='w'):
        """Creates a new file with the passed content.
        If the content is a dictionary, first is converted to a string."""
        with open(path, mode) as fwc:
            if isinstance(content, dict):
                content = json.dumps(content)
            fwc.write(content)

    @staticmethod
    def read_file(file_path, file_mode="r", file_encoding="utf-8"):
        """Reads the whole specified file and returns the content."""
        content = ''
        if file_mode == 'rb':
            file_encoding = None
        with open(file_path, mode=file_mode, encoding=file_encoding) as content_file:
            content = content_file.read()
        return content

    @staticmethod
    def create_tmp_dir():
        """Creates a directory in the temporal folder of the system.
        When the context is finished, the folder is automatically deleted."""
        return tempfile.TemporaryDirectory()

    @staticmethod
    def get_tmp_dir():
        """Gets the directory where the temporal
        folder of the system is located."""
        return tempfile.gettempdir()

    @staticmethod
    def get_all_files_in_dir(dir_path):
        """Returns a list with all the file paths in
        the specified directory and subdirectories."""
        files = []
        for dirpath, _, filenames in os.walk(dir_path):
            for filename in filenames:
                files.append(SysUtils.join_paths(dirpath, filename))
        return files

    @staticmethod
    def is_file(file_path):
        """Test whether a path is a regular file."""
        return os.path.isfile(file_path)

    @staticmethod
    def get_file_name(file_path):
        """Returns the filename."""
        return os.path.basename(file_path)


class StrUtils():
    """Common methods for string management."""

    @staticmethod
    def bytes_to_base64str(value, encoding='utf-8'):
        """Encodes string to base64 and returns another string."""
        return base64.b64encode(value).decode(encoding)

    @staticmethod
    def dict_to_base64str(value, encoding='utf-8'):
        """Encodes a dictionary to base64 and returns a string."""
        return base64.b64encode(json.dumps(value).encode(encoding)).decode(encoding)

    @staticmethod
    def base64_to_str(value, encoding='utf-8'):
        """Decodes from base64 and returns a string."""
        return base64.b64decode(value).decode(encoding)

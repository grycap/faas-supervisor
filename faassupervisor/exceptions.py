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
""" Module containing all the custom exceptions for the supervisor. """

import functools
import sys
from requests.exceptions import RequestException
from botocore.exceptions import ClientError
from faassupervisor.logger import get_logger


def exception():
    """A decorator that wraps the passed in function and logs exceptions."""

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)

            except ClientError as cerr:
                print(f"There was an exception in {func.__name__}")
                print(cerr.response['Error']['Message'])
                get_logger().error(cerr)
                sys.exit(1)

            except RequestException as rexc:
                print(f"There was an exception in {func.__name__}")
                get_logger().error(rexc)
                sys.exit(1)

            except FaasSupervisorError as fse:
                # print(fse.args[0])
                # get_logger().error(fse)
                if 'Warning' in fse.__class__.__name__:
                    get_logger().warning(fse)
                # Finish the execution if it's an error
                if 'Error' in fse.__class__.__name__:
                    get_logger().error(fse)
                    sys.exit(1)

        return wrapper

    return decorator


class FaasSupervisorError(Exception):
    """
    The base exception class for exceptions.

    :ivar msg: The descriptive message associated with the error.
    """
    fmt = 'An unspecified error occurred'

    def __init__(self, **kwargs):
        msg = self.fmt.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs


################################################
##             GENERAL EXCEPTIONS             ##
################################################
class InvalidPlatformError(FaasSupervisorError):
    """
    The binary is not launched on a Linux platform

    """
    fmt = "This binary only works on a Linux Platform.\nTry executing the Python version."


class InvalidSupervisorTypeError(FaasSupervisorError):
    """
    The supervisor type is not in the allowed list

    """
    fmt = "The supervisor type '{sup_typ}' is not allowed."


class InvalidStoragePathTypeError(FaasSupervisorError):
    """
    The storage path type is not in the allowed list

    """
    fmt = "The storage path type '{storage_type}' is not allowed."


class ContainerImageNotFoundError(FaasSupervisorError):
    """
    The container image is no specified

    """
    fmt = "Container image id is not specified."


class ContainerTimeoutExpiredWarning(FaasSupervisorError):
    """
    The udocker containers has exceeded the defined execution time.

    """
    fmt = "Container timeout expired.\nContainer execution stopped."


class NoLambdaContextError(FaasSupervisorError):
    """
    No context was provided for the lambda instance.

    """
    fmt = "No context found in the Lambda environment."


class UnknowStorageEventWarning(FaasSupervisorError):
    """
    Unknown storage event detected

    """
    fmt = "Unknown storage event detected."


################################################
##        STORAGE PROVIDER EXCEPTIONS         ##
################################################
class InvalidStorageProviderError(FaasSupervisorError):
    """
    The storage provider type is not valid.

    """
    fmt = "Invalid storage provider type defined: '{storage_type}'."


class NoStorageProviderDefinedWarning(FaasSupervisorError):
    """
    There is no storage provider defined.

    """
    fmt = "There is no storage provider defined for this function execution."


class StorageTypeError(FaasSupervisorError):
    """
    The storage type defined is not allowed

    """
    fmt = "The storage type '{auth_type}' is not allowed."


class StorageAuthError(FaasSupervisorError):
    """
    The storage authentication is not well-defined.

    """
    fmt = "The storage authentication of '{auth_type}' is not well-defined."


################################################
##        ONEDATA PROVIDER EXCEPTIONS         ##
################################################
class OnedataUploadError(FaasSupervisorError):
    """
    Uploading file to Onedata failed.
    """
    fmt = ("Uploading file '{file_name}' to Onedata failed. "
           "Status code: {status_code}")


class OnedataDownloadError(FaasSupervisorError):
    """
    Downloading file from Onedata failed.
    """
    fmt = ("Downloading file '{file_name}' from Onedata failed. "
           "Status code: {status_code}")


class OnedataFolderCreationError(FaasSupervisorError):
    """
    Folder creation in Onedata failed.
    """
    fmt = ("Folder '{folder_name}' creation in Onedata failed. "
           "Status code: {status_code}")

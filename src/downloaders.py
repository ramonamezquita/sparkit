import importlib
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from types import ModuleType
from urllib.parse import urlparse as parse_url
from urllib.parse import uses_netloc, uses_params, uses_relative

import boto3

_VALID_URLS = set(uses_relative + uses_netloc + uses_params)
_VALID_URLS.discard("")


class Downloader(ABC):
    def download(self, filepath: str, dest: str | None = None) -> Path:
        """Downloads file.

        Parameters
        ----------
        filepath : str
            Path to the file to download.

        dest : str or None, default None
            Download destination. If None, cwd is used.

        Returns
        -------
        path : Path
            Path of the downloaded file.
        """
        return self._download(filepath, dest)

    @abstractmethod
    def _download(filepath: str, dest: str) -> None:
        """Method to handle the file download."""
        pass


def is_url(url: object) -> bool:
    """
    Check to see if a URL has a valid protocol.

    Parameters
    ----------
    url : str or unicode

    Returns
    -------
    isurl : bool
        If `url` has a valid protocol return True otherwise False.
    """
    if not isinstance(url, str):
        return False
    return parse_url(url).scheme in _VALID_URLS


def import_from_filepath(filepath: str) -> ModuleType:
    """Imports a Python module from a given file path.

    Parameters
    ----------
    filepath : str
        The path to the Python file to be imported. This should be a valid file
        path ending with `.py`.

    Returns
    -------
    module: The imported module object.
        You can access the attributes, functions, and classes defined in the
        module using this object.

    Raises
    ------
    - FileNotFoundError: If the specified file does not exist.
    - ImportError: If the module cannot be imported for any reason.

    Example
    -------
    # Assuming a Python file "example.py" exists with a function `hello_world()`:
    # example.py content:
    # def hello_world():
    #     print("Hello, world!")

    module = import_from_filepath("example.py")
    module.hello_world()  # Output: Hello, world!

    Notes
    -----
    - This function uses `importlib.util` to dynamically load a Python module
    from a file path at runtime.
    - The `module_name` is derived from the filename without the extension, so
    ensure the file name is unique if multiple modules are imported in this
    manner.
    - The imported module is not added to `sys.modules` unless explicitly done
    by the user.
    """
    module_name = os.path.splitext(os.path.basename(filepath))[0]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # Load the module
    return module


def download_and_import(filepath: str, dest: str | None = None):
    """Download Python module and load it to current namespace.

    Parameters
    ----------
    filepath : str
        Path of the file to download. Supported schemas are: s3://,

    dest : str or None
        Download destination in the current host. If None, cwd is used.
    """

    download_path = download_file(filepath, dest)
    module = import_from_filepath(download_path)
    return module


def download_file(filepath: str, dest: str | None = None):
    """Downloads file based on its filepath schema.

    Parameters
    ----------
    filepath : str
        Path of the file to download. Supported schemas are: s3://,

    dest : str or None
        Download destination in the current host. If None, cwd is used.
    """
    downloader = get_downloader_from_filepath(filepath)
    return downloader.download(filepath=filepath, dest=dest)


def get_downloader_from_filepath(filepath: str) -> Downloader:
    """Infers :class:`FileHandler` from the given `filepath`

    Parameters
    ----------
    filepath : str
        Filepath
    """

    # Mapping of protocol prefixes to handler classes
    handler_mapping = {"s3://": S3Downloader}

    # Match protocol and return handler instance
    for prefix, handler_class in handler_mapping.items():
        if filepath.startswith(prefix):
            return handler_class()

    # Default to a local file handler if no match
    return LocalDownloader()


def get_downloader(handler_name: str) -> Downloader:
    """Factory for derived :class:`FileHandler` instances.

    Parameters
    ----------
    handler_name : str
        Handler name.
    """
    handlers = {"s3": S3Downloader}

    if handler_name not in handlers:
        raise ValueError(f"Unsupported handler: {handler_name}")

    # Instantiate the handler and return it.
    return handlers[handler_name]()


class LocalDownloader(Downloader):
    def _download(self, filepath: str, dest: str = None) -> None:

        # If `dest` is None, only check `filepath` exists and return it.
        # Otherwise, copy the file to `dest`.

        if dest is None:
            if not os.path.isfile(filepath):
                raise ValueError(f"Filepath '{filepath}' does not exist.")

            return filepath

        full_dest = os.path.join(dest, os.path.basename(filepath))
        return shutil.copy(filepath, full_dest)


class S3Downloader(Downloader):

    def __init__(self):
        self.s3_client = boto3.client("s3")

    def _download(self, file_path: str, dest: str | None = None) -> Path:
        """Download file from S3 to the specified directory."""
        dest = dest or os.getcwd()
        bucket_name, object_key = self._parse_s3_url(file_path)
        downloaded_path = os.path.join(dest, object_key.split("/")[-1])
        self.s3_client.download_file(bucket_name, object_key, downloaded_path)
        return Path(downloaded_path)

    def _parse_s3_url(self, s3_url: str) -> tuple:
        """Parse the S3 URL to get bucket and object key."""
        s3_url = s3_url.replace("s3://", "")
        bucket_name, object_key = s3_url.split("/", 1)
        return bucket_name, object_key

from contextlib import contextmanager
from dataclasses import dataclass
from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
import multiprocessing
import os
import os.path
from pathlib import Path
import re
import time
from typing import List

from datalad.api import Dataset, clone
import pytest
import requests

DATA_DIR = Path(__file__).with_name("data")

lgr = logging.getLogger("datalad.fuse.tests")

# We need large files that exceed the blocksize of 1 MiB
BIG_URLS = [
    # (dataset path, URL, SHA256 digest)
    (
        "APL.pdf",
        "http://www.softwarepreservation.org/projects/apl/Books/APROGRAMMING%20LANGUAGE",
        "c65ccc2a97cdb6042641847112dc6e4d4d6e75fdedcd476fdd61f855711bbaf4",
    ),
    (
        "gameboy.pdf",
        "https://archive.org/download/GameBoyProgManVer1.1/GameBoyProgManVer1.1.pdf",
        "5263e6c1f5fa51fc6813d2ed71c738c887ec78554eef633ad72c6285f7ff9197",
    ),
    (
        "libpython3.10-stdlib_3.10.4-3_i386.deb",
        "http://nyc3.clouds.archive.ubuntu.com/ubuntu/pool/main/p/python3.10/libpython3.10-stdlib_3.10.4-3_i386.deb",
        "e79c1416ec792b61ad9770f855bf6889e57be5f6511ea814d81ef5f9b1a3eec9",
    ),
]


@pytest.fixture(autouse=True)
def capture_all_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="datalad.fuse")


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--libfuse",
        action="store_true",
        default=False,
        help="Enable fuse tests",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--libfuse"):
        skip_no_libfuse = pytest.mark.skip(reason="Only run when --libfuse is given")
        for item in items:
            if "libfuse" in item.keywords:
                item.add_marker(skip_no_libfuse)


@pytest.fixture()
def tmp_home(monkeypatch, tmp_path_factory):
    home = tmp_path_factory.mktemp("tmp_home")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(home))
    (home / ".gitconfig").write_text(
        '[annex "security"]\n'
        "allowed-url-schemes = http https file\n"
        "allowed-http-addresses = all\n"
        "\n"
        "[user]\n"
        "name = Test User\n"
        "email = test@test.nil\n"
    )
    return home


def serve_path_via_http(hostname, path, queue):
    os.chdir(path)
    httpd = HTTPServer((hostname, 0), SimpleHTTPRequestHandler)
    queue.put(httpd.server_port)
    httpd.serve_forever()


@contextmanager
def local_server(directory):
    hostname = "127.0.0.1"
    queue = multiprocessing.Queue()
    p = multiprocessing.Process(
        target=serve_path_via_http, args=(hostname, directory, queue)
    )
    p.start()
    try:
        port = queue.get(timeout=300)
        url = f"http://{hostname}:{port}"
        lgr.debug("HTTP: serving %s at %s", directory, url)
        with pytest.MonkeyPatch().context() as m:
            m.delenv("http_proxy", raising=False)
            for _ in range(10):
                try:
                    requests.get(url, timeout=1)
                except requests.RequestException:
                    time.sleep(0.1)
                else:
                    break
            else:
                raise RuntimeError("Server did not come up in time")
            yield url
    finally:
        lgr.debug("HTTP: stopping server")
        p.terminate()


@dataclass
class DataFile:
    url: str
    path: str
    content: bytes


@pytest.fixture(scope="session")
def served_files():
    with local_server(DATA_DIR) as url:
        files = []
        for p in DATA_DIR.iterdir():
            if p.is_file():
                files.append(
                    DataFile(
                        url=f"{url}/{p.name}",
                        path=p.name,
                        content=p.read_bytes(),
                    )
                )
        yield files


def initdataset(ds, data_files: List[DataFile], is_remote: bool) -> None:
    for dfile in data_files:
        if is_remote:
            ds.repo.add_url_to_file(dfile.path, dfile.url, options=["--relaxed"])
        else:
            ds.download_url(urls=dfile.url, path=dfile.path)
        # Add an invalid URL in order to test bad-URL-fallback.
        # Add it after `download_url()` is called in order to not cause an
        #  error on filesystems without symlinks (in which case doing
        #  `add_url_to_file()` would cause the file to be created as a
        #  non-symlink, which `download_url()` would then see as the file
        #  already being present, leading to an error).
        # It appears that git annex returns files' URLs in lexicographic order,
        #  so in order for the bad URL to be tried first, we insert a '0' at
        #  the start of the URL path.
        ds.repo.add_url_to_file(
            dfile.path, re.sub(r"(:\d+/)", r"\g<1>0", dfile.url), options=["--relaxed"]
        )


# @pytest.mark.usefixtures("tmp_home")  # Doesn't work on fixture functions
@pytest.fixture(params=["remote", "local", "cloned"])
def url_dataset(served_files, request, tmp_home, tmp_path_factory):  # noqa: U100
    workpath = tmp_path_factory.mktemp("url_dataset")
    ds = Dataset(workpath / "ds").create()
    initdataset(ds, served_files, request.param == "remote")
    if request.param == "cloned":
        ds.repo.call_git(["update-server-info"])
        with local_server(ds.path) as origin_url:
            clone_ds = clone(origin_url, workpath / "clone")
            for dfile in served_files:
                clone_ds.repo.rm_url(dfile.path, dfile.url)
            yield (clone_ds, {df.path: df.content for df in served_files})
    else:
        yield (ds, {df.path: df.content for df in served_files})


@pytest.fixture(params=["remote", "local"])
def superdataset(served_files, request, tmp_home, tmp_path_factory):  # noqa: U100
    dspath = tmp_path_factory.mktemp("superdataset")
    ds = Dataset(dspath).create()
    sub = ds.create(dspath / "sub")
    initdataset(sub, served_files, request.param == "remote")
    return (ds, {os.path.join("sub", df.path): df.content for df in served_files})


@pytest.fixture
def big_url_dataset(tmp_home, tmp_path_factory):  # noqa: U100
    workpath = tmp_path_factory.mktemp("big_url_dataset")
    ds = Dataset(workpath / "ds").create()
    for path, url, _ in BIG_URLS:
        ds.repo.add_url_to_file(path, url, options=["--relaxed"])
    yield (ds, {path: digest for path, _, digest in BIG_URLS})

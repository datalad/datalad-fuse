from contextlib import contextmanager
from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
import multiprocessing
import os
from pathlib import Path
import time

from datalad.api import Dataset, clone
import pytest
import requests

DATA_DIR = Path(__file__).with_name("data")

lgr = logging.getLogger("datalad_fuse.tests")


@pytest.fixture(scope="session")
def data_files():
    return {p.name: p.read_bytes() for p in DATA_DIR.iterdir() if p.is_file()}


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


@pytest.fixture(scope="session")
def data_server():
    with local_server(DATA_DIR) as url:
        yield url


# @pytest.mark.usefixtures("tmp_home")  # Doesn't work on fixture functions
@pytest.fixture(params=["remote", "local", "cloned"])
def url_dataset(data_files, data_server, request, tmp_home, tmp_path):  # noqa: U100
    ds = Dataset(str(tmp_path / "ds")).create()
    for fname in data_files:
        if request.param == "remote":
            ds.repo.add_url_to_file(
                fname, f"{data_server}/{fname}", options=["--relaxed"]
            )
        else:
            ds.download_url(urls=f"{data_server}/{fname}", path=fname)
        # Add an invalid URL in order to test bad-URL-fallback.
        # Add it after `download_url()` is called in order to not cause an
        #  error on filesystems without symlinks (in which case doing
        #  `add_url_to_file()` would cause the file to be created as a
        #  non-symlink, which `download_url()` would then see as the file
        #  already being present, leading to an error).
        # It appears that git annex returns files' URLs in lexicographic
        #  order, so in order for the bad URL to be tried first, we insert
        #  a '0'.
        ds.repo.add_url_to_file(fname, f"{data_server}/0{fname}", options=["--relaxed"])
    if request.param == "cloned":
        ds.repo.call_git(["update-server-info"])
        with local_server(ds.path) as origin_url:
            clone_ds = clone(origin_url, str(tmp_path / "clone"))
            for fname in data_files:
                clone_ds.repo.rm_url(fname, f"{data_server}/{fname}")
            yield clone_ds
    else:
        yield ds

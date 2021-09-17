from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
import multiprocessing
import os
from pathlib import Path
import time

from datalad.distribution.dataset import Dataset
import pytest
import requests

DATA_DIR = Path(__file__).with_name("data")

lgr = logging.getLogger("datalad_fuse.tests")


def serve_path_via_http(hostname, path, queue):
    os.chdir(path)
    httpd = HTTPServer((hostname, 0), SimpleHTTPRequestHandler)
    queue.put(httpd.server_port)
    httpd.serve_forever()


@pytest.fixture(scope="session")
def local_server():
    hostname = "127.0.0.1"
    queue = multiprocessing.Queue()
    p = multiprocessing.Process(
        target=serve_path_via_http, args=(hostname, DATA_DIR, queue)
    )
    p.start()
    try:
        port = queue.get(timeout=300)
        url = f"http://{hostname}:{port}"
        lgr.debug("HTTP: serving %s at %s", DATA_DIR, url)
        # The normal monkeypatch fixture cannot be used in a session fixture,
        # so we have to instantiate the class directly.
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


@pytest.fixture(params=["remote", "local"])
def url_dataset(local_server, request, tmp_path):
    ds = Dataset(str(tmp_path)).create()
    for p in DATA_DIR.iterdir():
        if p.is_file():
            if request.param == "remote":
                ds.repo.add_url_to_file(
                    p.name, f"{local_server}/{p.name}", options=["--relaxed"]
                )
            else:
                ds.download_url(urls=f"{local_server}/{p.name}", path=p.name)
            # Add an invalid URL in order to test bad-URL-fallback.
            # Add it after `download_url()` is called in order to not cause an
            #  error on filesystems without symlinks (in which case doing
            #  `add_url_to_file()` would cause the file to be created as a
            #  non-symlink, which `download_url()` would then see as the file
            #  already being present, leading to an error).
            # It appears that git annex returns files' URLs in lexicographic
            #  order, so in order for the bad URL to be tried first, we insert
            #  a '0'.
            ds.repo.add_url_to_file(
                p.name, f"{local_server}/0{p.name}", options=["--relaxed"]
            )
    return ds

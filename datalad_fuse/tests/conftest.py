from pathlib import Path
import subprocess
import sys
import time

from datalad.distribution.dataset import Dataset
import pytest
import requests

DATA_DIR = Path(__file__).with_name("data")

LOCAL_SERVER_PORT = 60069


@pytest.fixture(scope="session")
def local_server():
    # Based on <https://pawamoy.github.io/posts/local-http-server-fake-files
    # -testing-purposes/#pytest-fixture>
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "http.server",
            "--bind",
            "127.0.0.1",
            "--directory",
            str(DATA_DIR),
            str(LOCAL_SERVER_PORT),
        ]
    )
    url = f"http://127.0.0.1:{LOCAL_SERVER_PORT}"
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
    process.kill()
    process.wait()


@pytest.fixture(params=["remote", "local"])
def url_dataset(local_server, request, tmp_path):
    ds = Dataset(str(tmp_path)).create()
    for p in DATA_DIR.iterdir():
        if p.is_file():
            # Add an invalid URL in order to test bad-URL-fallback.
            # It appears that git annex returns files' URLs in lexicographic
            # order, so in order for the bad URL to be tried first, we insert a
            # '0'.
            ds.repo.add_url_to_file(
                p.name, f"{local_server}/0{p.name}", options=["--relaxed"]
            )
            if request.param == "remote":
                ds.repo.add_url_to_file(
                    p.name, f"{local_server}/{p.name}", options=["--relaxed"]
                )
            else:
                ds.download_url(urls=f"{local_server}/{p.name}", path=p.name)
    return ds

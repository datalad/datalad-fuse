from pathlib import Path
import subprocess
import sys
import time

from datalad.distribution.dataset import Dataset
import pytest
import requests

DATA_DIR = Path(__file__).with_name("data")

LOCAL_SERVER_PORT = 8000


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
    while True:
        try:
            requests.get(url)
        except requests.RequestException:
            time.sleep(0.1)
        else:
            break
    yield url
    process.kill()
    process.wait()


@pytest.fixture
def remoted_dataset(local_server, tmp_path):
    ds = Dataset(str(tmp_path)).create()
    for p in DATA_DIR.iterdir():
        if p.is_file():
            ds.repo.add_url_to_file(
                p.name, f"{local_server}/{p.name}", options=["--relaxed"]
            )
    return ds

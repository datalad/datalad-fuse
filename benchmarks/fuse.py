from pathlib import Path
import subprocess

TEST_REPO = "https://github.com/dandisets/000007"
TEST_TAG = "0.220126.1903"


class FuseBenchmarks:
    timeout = 3600

    def setup_cache(self):
        work_dir = Path("000007")
        subprocess.run(["datalad", "install", TEST_REPO], check=True)
        subprocess.run(["git", "checkout", TEST_TAG], cwd=str(work_dir), check=True)
        return work_dir

    def setup(self, work_dir):
        self.mount = Path("mount")
        self.mount.mkdir(exist_ok=True)
        self.p = subprocess.Popen(
            [
                "datalad",
                "fusefs",
                "-d",
                str(work_dir),
                "--foreground",
                str(self.mount),
            ]
        )
        try:
            self.p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
        else:
            raise RuntimeError("datalad fusefs died on startup!")

    def time_ls(self, _work_dir):
        subprocess.run(["ls", "-R"], cwd=self.mount, check=True)

    def time_ls_l(self, _work_dir):
        subprocess.run(["ls", "-lR"], cwd=self.mount, check=True)

    def time_ls_lL(self, _work_dir):
        subprocess.run(["ls", "-lLR"], cwd=self.mount, check=True)

    def teardown(self, _work_dir):
        self.p.terminate()
        self.p.wait(3)

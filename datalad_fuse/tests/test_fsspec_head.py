### TODO: Run each test on both a dataset with non-locally-available files and
### a dataset with locally-available files

from datalad.api import Dataset  # noqa
from datalad.tests.utils import assert_in_results


def test_get_default_lines_text(remoted_dataset):
    assert_in_results(
        remoted_dataset.fsspec_head("text.txt"),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=b"""\
Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh
euismod tincidunt ut laoreet dolore magna aliquam erat volutpat.  Ut wisi enim
ad minim veniam, quis nostrud exerci tation ullamcorper suscipit lobortis nisl
ut aliquip ex ea commodo consequat.  Duis autem vel eum iriure dolor in
hendrerit in vulputate velit esse molestie consequat, vel illum dolore eu
feugiat nulla facilisis at vero eros et accumsan et iusto odio dignissim qui
blandit praesent luptatum zzril delenit augue duis dolore te feugait nulla
facilisi.  Nam liber tempor cum soluta nobis eleifend option congue nihil
imperdiet doming id quod mazim placerat facer possim assum.  Typi non habent
claritatem insitam; est usus legentis in iis qui facit eorum claritatem.
""",
    )


def test_get_lines_text(remoted_dataset):
    assert_in_results(
        remoted_dataset.fsspec_head("text.txt", lines=4),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=b"""\
Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh
euismod tincidunt ut laoreet dolore magna aliquam erat volutpat.  Ut wisi enim
ad minim veniam, quis nostrud exerci tation ullamcorper suscipit lobortis nisl
ut aliquip ex ea commodo consequat.  Duis autem vel eum iriure dolor in
""",
    )


def test_get_bytes_text(remoted_dataset):
    assert_in_results(
        remoted_dataset.fsspec_head("text.txt", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=b"""\
Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh
euismod tincidunt ut""",
    )


def test_get_lines_binary(remoted_dataset):
    assert_in_results(
        remoted_dataset.fsspec_head("binary.png", lines=3),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=bytes.fromhex(
            "89 50 4e 47 0d 0a 1a 0a  00 00 00 0d 49 48 44 52"
            "00 00 00 20 00 00 00 20  08 06 00 00 00 73 7a 7a"
            "f4 00 00 00 19 74 45 58  74 53 6f 66 74 77 61 72"
            "65 00 41 64 6f 62 65 20  49 6d 61 67 65 52 65 61"
            "64 79 71 c9 65 3c 00 00  04 06 49 44 41 54 78 da"
            "c4 57 5b 48 14 61 14 3e  a6 ae d1 78 99 15 d9 29"
            "f3 b2 0a"
        ),
    )


def test_get_bytes_binary(remoted_dataset):
    assert_in_results(
        remoted_dataset.fsspec_head("binary.png", bytes=100),
        action="fsspec-head",
        type="dataset",
        status="ok",
        data=bytes.fromhex(
            "89 50 4e 47 0d 0a 1a 0a  00 00 00 0d 49 48 44 52"
            "00 00 00 20 00 00 00 20  08 06 00 00 00 73 7a 7a"
            "f4 00 00 00 19 74 45 58  74 53 6f 66 74 77 61 72"
            "65 00 41 64 6f 62 65 20  49 6d 61 67 65 52 65 61"
            "64 79 71 c9 65 3c 00 00  04 06 49 44 41 54 78 da"
            "c4 57 5b 48 14 61 14 3e  a6 ae d1 78 99 15 d9 29"
            "f3 b2 0a 5e"
        ),
    )

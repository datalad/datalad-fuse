"""Verify old datalad_fuse.fsspec import paths still work with warnings."""

from __future__ import annotations

import importlib
import warnings

import pytest

from datalad_fuse.fsspec import _COMPAT_MAP


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "old_name,module_path,canonical_name",
    [(name, mod, canon) for name, (mod, canon) in _COMPAT_MAP.items()],
    ids=list(_COMPAT_MAP),
)
def test_compat_import_warns(old_name, module_path, canonical_name):
    """Moved names imported from datalad_fuse.fsspec emit DeprecationWarning."""
    import datalad_fuse.fsspec as fsspec_mod

    with pytest.warns(DeprecationWarning, match=old_name):
        obj = getattr(fsspec_mod, old_name)

    canonical_mod = importlib.import_module(module_path)
    assert obj is getattr(canonical_mod, canonical_name)


@pytest.mark.ai_generated
def test_unknown_attr_raises():
    import datalad_fuse.fsspec as fsspec_mod

    # Direct attribute access raises AttributeError (module-level __getattr__).
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        with pytest.raises(AttributeError, match="no_such_name"):
            fsspec_mod.no_such_name  # noqa: B018


@pytest.mark.ai_generated
def test_unknown_attr_from_import_raises():
    # `from x import y` on a missing name converts AttributeError to ImportError.
    with pytest.raises(ImportError):
        from datalad_fuse.fsspec import (  # type: ignore[attr-defined]  # noqa: F401
            no_such_name,
        )


@pytest.mark.ai_generated
def test_fsspec_backend_no_warning():
    """FsspecBackend still lives in fsspec.py — no deprecation."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        from datalad_fuse.fsspec import FsspecBackend  # noqa: F401

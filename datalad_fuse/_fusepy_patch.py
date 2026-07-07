"""Runtime patch for a fusepy ``FUSE._wrapper`` bug.

Upstream ``fuse.FUSE._wrapper`` is a ``@staticmethod`` but its outer
``except BaseException`` handler references ``self.__critical_exception``
(fusepy master, ``fuse.py`` line ~756). Since there is no ``self`` in a
static method, whenever a ``BaseException`` propagates out of a FUSE
callback the handler itself raises ``NameError: name 'self' is not
defined``. That masks the original exception, causes the ctypes callback
to return garbage to the kernel, and typically surfaces on the user side
as ``OSError [Errno 34] Numerical result out of range`` or a truncated
``read()``.

This module replaces ``fuse.FUSE._wrapper`` with a corrected version that
stores the critical exception on the bound FUSE instance via
``func.__self__``. Import order matters: the patch must be installed
before any ``fuse.FUSE(...)`` is constructed. We import this module from
``datalad_fuse.fuse_`` (which is imported by ``datalad fusefs``).

TODO: version-gate this patch. Once a fusepy release ships the upstream
fix (see PR to fusepy/fusepy), skip patching for versions that already
contain it, e.g. via ``fuse.__version__`` or a source probe.
"""

from __future__ import annotations

import errno
from functools import wraps
import logging
from typing import Any, Callable

import fuse

log = logging.getLogger("fuse")

_PATCHED_ATTR = "_datalad_fuse_wrapper_patched"


def _patched_wrapper(func: Callable[..., Any], *args: Any, **kwargs: Any) -> int:
    try:
        if func.__name__ == "init":
            return func(*args, **kwargs) or 0
        try:
            return func(*args, **kwargs) or 0
        except OSError as e:
            if e.errno is not None and e.errno > 0:
                log.debug(
                    "FUSE operation %s raised a %s, returning errno %s.",
                    func.__name__,
                    type(e),
                    e.errno,
                    exc_info=True,
                )
                return -e.errno
            log.error(
                "FUSE operation %s raised an OSError with non-positive "
                "errno %s, returning errno.EINVAL.",
                func.__name__,
                e.errno,
                exc_info=True,
            )
            return -errno.EINVAL
        except Exception:
            log.error(
                "Uncaught exception from FUSE operation %s, " "returning errno.EINVAL.",
                func.__name__,
                exc_info=True,
            )
            return -errno.EINVAL
    except BaseException as e:  # noqa: B036 -- mirrors upstream fusepy behavior
        instance = getattr(func, "__self__", None)
        if instance is not None:
            try:
                instance._FUSE__critical_exception = e
            except Exception:
                log.exception("Failed to record critical exception on FUSE instance")
        log.critical(
            "Uncaught critical exception from FUSE operation %s, aborting.",
            func.__name__,
            exc_info=True,
        )
        fuse.fuse_exit()
        return -errno.EFAULT


def apply_patch() -> None:
    if getattr(fuse.FUSE._wrapper, _PATCHED_ATTR, False):
        return

    @wraps(fuse.FUSE._wrapper)
    def wrapper(func: Callable[..., Any], *args: Any, **kwargs: Any) -> int:
        return _patched_wrapper(func, *args, **kwargs)

    setattr(wrapper, _PATCHED_ATTR, True)
    fuse.FUSE._wrapper = staticmethod(wrapper)


apply_patch()

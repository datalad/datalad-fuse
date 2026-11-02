# Notes for AI coding agents

Conventions of this project that its tooling does not enforce.  See
[CONTRIBUTING.md](./CONTRIBUTING.md) for the development setup, tests, and
docs.

## Declare forwarded parameters explicitly

When a class or function passes options on to another one (e.g.,
`RemoteFilesystemAdapter` creates a `DatasetAdapter` for each dataset, passing
it `caching`, `mode_transparent`, `init_annex`, and `backends`):

- declare each forwarded option as an explicit parameter, with the same name,
  type, and default as in the callee;
- document it in the "Parameters" section of the docstring (e.g., "Passed to
  each :class:`DatasetAdapter`.");
- pass it on explicitly by name.

Do not collect such options in `**kwargs` to pass them on dynamically.  The
explicit parameters duplicate signatures and docstrings, but keep both, and
the API docs, complete and consistent.  When adding a parameter to the callee
that callers of a forwarding class or function may need, add it there as well.

# 0.3.1 (Thu Oct 20 2022)

#### 🏠 Internal

- Update GitHub Actions action versions [#78](https://github.com/datalad/datalad-fuse/pull/78) ([@jwodder](https://github.com/jwodder))

#### 📝 Documentation

- Fix a typo and overall replace comment with fresh from template [#81](https://github.com/datalad/datalad-fuse/pull/81) ([@yarikoptic](https://github.com/yarikoptic))

#### 🔩 Dependency Updates

- Don't use fsspec 2022.10.0 [#80](https://github.com/datalad/datalad-fuse/pull/80) ([@jwodder](https://github.com/jwodder))

#### Authors: 2

- John T. Wodder II ([@jwodder](https://github.com/jwodder))
- Yaroslav Halchenko ([@yarikoptic](https://github.com/yarikoptic))

---

# 0.3.0 (Tue Jul 05 2022)

#### 🚀 Enhancement

- Add `--allow-other` option [#77](https://github.com/datalad/datalad-fuse/pull/77) ([@jwodder](https://github.com/jwodder))

#### 🐛 Bug Fix

- DOC: Set language in Sphinx config to en [#72](https://github.com/datalad/datalad-fuse/pull/72) ([@adswa](https://github.com/adswa))

#### 🏠 Internal

- Include "data package" in project [#75](https://github.com/datalad/datalad-fuse/pull/75) ([@jwodder](https://github.com/jwodder))
- Use methodtools.lru_cache [#74](https://github.com/datalad/datalad-fuse/pull/74) ([@jwodder](https://github.com/jwodder))

#### 🧪 Tests

- Setup benchmarking with asv [#70](https://github.com/datalad/datalad-fuse/pull/70) ([@jwodder](https://github.com/jwodder))

#### Authors: 2

- Adina Wagner ([@adswa](https://github.com/adswa))
- John T. Wodder II ([@jwodder](https://github.com/jwodder))

---

# 0.2.0 (Fri Feb 18 2022)

#### 🚀 Enhancement

- RF/ENH: initial prototype for --mode-transparent (not hiding .git) [#46](https://github.com/datalad/datalad-fuse/pull/46) ([@yarikoptic](https://github.com/yarikoptic) [@jwodder](https://github.com/jwodder))

#### 🐛 Bug Fix

- Get `git status` to work under `--mode-transparent` [#57](https://github.com/datalad/datalad-fuse/pull/57) ([@jwodder](https://github.com/jwodder))
- RF: reuse is_annex_dir_or_key  at fsspec level to decide if file is annexed etc [#55](https://github.com/datalad/datalad-fuse/pull/55) ([@yarikoptic](https://github.com/yarikoptic))
- Don't pretend non-objects/ entries in .git/annex exist [#52](https://github.com/datalad/datalad-fuse/pull/52) ([@jwodder](https://github.com/jwodder) [@yarikoptic](https://github.com/yarikoptic))
- Delete cached file and try again on BlocksizeMismatchError [#43](https://github.com/datalad/datalad-fuse/pull/43) ([@jwodder](https://github.com/jwodder))

#### 🏠 Internal

- Move testing requirements to a "test" extra [#63](https://github.com/datalad/datalad-fuse/pull/63) ([@jwodder](https://github.com/jwodder))
- Remove tools/ [#64](https://github.com/datalad/datalad-fuse/pull/64) ([@jwodder](https://github.com/jwodder))
- Remove unused Appveyor configuration [#44](https://github.com/datalad/datalad-fuse/pull/44) ([@yarikoptic](https://github.com/yarikoptic))
- Set up auto [#42](https://github.com/datalad/datalad-fuse/pull/42) ([@jwodder](https://github.com/jwodder))

#### 🧪 Tests

- Assert that `datalad fusefs` test processes terminate quickly [#67](https://github.com/datalad/datalad-fuse/pull/67) ([@jwodder](https://github.com/jwodder))
- Use `coverage` directly instead of `pytest-cov` [#65](https://github.com/datalad/datalad-fuse/pull/65) ([@jwodder](https://github.com/jwodder))
- Factor out common test code for running `datalad fusefs` [#61](https://github.com/datalad/datalad-fuse/pull/61) ([@jwodder](https://github.com/jwodder))

#### Authors: 2

- John T. Wodder II ([@jwodder](https://github.com/jwodder))
- Yaroslav Halchenko ([@yarikoptic](https://github.com/yarikoptic))

---

# 0.1.0 (2022-01-04)

Initial release

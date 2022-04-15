Imported following directories from FDB release 7.1.3

1. `bindings/python/tests`

2. `bindings/bindingtester`

Following changes were applied.

1. Remove testing for `SET_VERSIONSTAMPED_VALUE` v1 format in
   `api.py`.

2. Set minimum API version to `630` for `python` and `python3` tester
   in `known_testers.py` and added `rust` tester.


"""Validate the generated V15 worker source without starting the scanner.

The production worker is a bootstrap that downloads the last clean worker,
injects V15.4/V15.6 code, and compiles the resulting source. This validator
imports the bootstrap exactly as CI does, which catches transformation errors
that py_compile(scanner/worker.py) alone cannot detect.
"""
import scanner.worker  # noqa: F401

print("V15 worker bootstrap: generated source compiled successfully")

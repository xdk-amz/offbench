"""Module entrypoint: ``python3 -m offbench ...`` dispatches to the CLI.

Kept trivial so the deploy's worker-start step
(``cd ~/offbench-src && python3 -m offbench --root ~/offbench run ...``) resolves
the moment the package is on the host. The prior run failed here: it deployed
before this file existed, so ``python3 -m offbench`` could not start and no
worker claimed the queued canaries.
"""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())

"""Project-root module entrypoint for ``python3 -m offbench``.

The canonical project nests two ``offbench`` directories: this project root
(holding ``bins.json``, ``data/``, ``vendor/`` and the inner
package) and the inner ``offbench/`` Python package (``cli.py``, ``runner.py``).
Which one ``python3 -m offbench`` resolves depends on the caller's cwd:

  * run from this project root, Python imports the inner package and executes
    ``offbench/offbench/__main__.py`` directly;
  * run from the parent directory, Python treats this project root as a
    namespace package and executes THIS file as ``offbench/__main__.py``.

In the second case ``offbench`` is already bound to this outer namespace
package, whose only submodule of interest is the inner package. This file
rebinds ``offbench`` to the inner package (which carries ``cli`` and ``runner``)
and dispatches into the same CLI, so ``python3 -m offbench run`` and
``offbench-worker`` land in ``offbench.cli.main`` -> ``offbench.runner.Runner``
regardless of cwd. Pure stdlib.
"""

import sys
from pathlib import Path

_INNER = Path(__file__).resolve().parent / "offbench"


def _load_main():
    # Fast path: cwd made the inner package the top-level ``offbench`` already.
    try:
        from offbench.cli import main
        return main
    except ImportError:
        pass
    # Otherwise ``offbench`` is this outer namespace package; rebind the name to
    # the inner package so its ``cli``/``runner`` relative imports resolve.
    if _INNER.is_dir():
        sys.path.insert(0, str(_INNER))
        for name in [n for n in sys.modules if n == "offbench" or n.startswith("offbench.")]:
            del sys.modules[name]
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "offbench", _INNER / "__init__.py",
            submodule_search_locations=[str(_INNER)])
        module = importlib.util.module_from_spec(spec)
        sys.modules["offbench"] = module
        spec.loader.exec_module(module)
        from offbench.cli import main  # now resolves to the inner package
        return main
    raise ModuleNotFoundError(f"inner offbench package not found at {_INNER}")


if __name__ == "__main__":
    sys.exit(_load_main()())

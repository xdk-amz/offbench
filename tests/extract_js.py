"""Extract the dashboard's inline <script> blocks to one file for `node --check`."""
import re
import sys
from pathlib import Path

html = Path(sys.argv[1]).read_text()
blocks = re.findall(r"<script>([\s\S]*?)</script>", html)
Path(sys.argv[2]).write_text("\n".join(blocks))
print(f"{len(blocks)} script blocks -> {sys.argv[2]}")

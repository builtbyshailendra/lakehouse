"""python -m lake <command>

    show        the cost table's metadata tree (metadata.json -> manifests -> files)
    snapshots   the snapshot history of main
"""

import sys

from . import meta, tables
from .config import catalog


def main(argv: list[str]) -> int:
    cmd = argv[0] if argv else "show"
    cost = catalog().load_table(tables.COST)
    if cmd == "show":
        print(meta.describe(meta.tree(cost.metadata_location)))
    elif cmd == "snapshots":
        for s in cost.snapshots():
            p = s.summary.additional_properties if s.summary else {}
            print(s.snapshot_id, s.summary.operation.value if s.summary else "-", p.get("lake.batch-id", ""),
                  p.get("total-records", ""))
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

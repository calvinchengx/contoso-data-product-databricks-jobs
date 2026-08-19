"""Land every vendor the product bronze reads.

FOUR VENDORS, FOUR TRANSPORTS, and this file runs them in order rather than
doing any of the work: paged delimited text and JSON Lines over HTTP, paged
JSON arrays over HTTP, binary Parquet over HTTP, and a Postgres change stream
carried by Kafka. Each step is its own module because each vendor is its own
failure -- a wrong key, a mangled binary body, a short change stream -- and a
single function would report all of them as "ingest failed".

WHAT THIS REPLACED. This file used to WRITE the data it then claimed to have
ingested: a handful of literal rows, three customers, two orders. That made
every downstream number this platform published true about a fixture it had
invented, and made comparing its gold against fabric-platform-notebook-pipelines's
meaningless -- the two runtimes were not building the same product, they were
building different data through similar code. Both now pull from the vendors
contoso-sources declares, so a disagreement in gold is a disagreement about the
ENGINE, which is the only thing worth measuring here.
"""

from __future__ import annotations

import ingest_erp_cdc
import ingest_pos
import ingest_reference
import ingest_web
import landing

# ORDER MATTERS ONLY FOR THE DATE. The first step to land decides the partition
# and writes it to state.json; the rest read that decision, and bronze reads it
# too. Nothing else here is sequential -- the vendors do not know about each
# other, which is the entire reason party resolution downstream is hard.
STEPS = [
    ("Contoso POS", ingest_pos),
    ("Contoso Web", ingest_web),
    ("Contoso Reference", ingest_reference),
    ("Contoso ERP", ingest_erp_cdc),
]


def main() -> int:
    day = landing.day()
    print(f"landing date partition: {day}")
    for name, step in STEPS:
        print(f"--- {name} ---")
        rc = step.main()
        if rc != 0:
            return rc
    print(f"all four vendors landed — date partition {day}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

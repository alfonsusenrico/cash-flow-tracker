#!/usr/bin/env bash
# Print "<table> <exact row count>" for every base table in the public schema of a database.
# Counts only, never row contents, so the output is safe to compare and keep as evidence.
#
# Usage: db-row-counts <database>
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: db-row-counts <database>" >&2
    exit 2
fi

psql --dbname="$1" --no-psqlrc --tuples-only --no-align --field-separator=' ' <<'SQL'
SELECT table_name,
       (xpath('/row/n/text()',
              query_to_xml(format('SELECT count(*) AS n FROM %I.%I', table_schema, table_name),
                           false, true, '')))[1]::text::bigint
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name;
SQL

#!/bin/sh
set -e

files='
test-data.zip
'

echo 'load normalized'
for file in $files; do
    python3 load_tweets.py --db "postgresql://postgres:pass@localhost:32561/postgres" --inputs "$file"
done

psql "postgresql://postgres:pass@localhost:32561/postgres" <<'SQL'
REFRESH MATERIALIZED VIEW tweet_tags_total;
REFRESH MATERIALIZED VIEW tweet_tags_cooccurrence;
SQL

echo 'load denormalized'
for file in $files; do
    python3 - "$file" <<'PY' | psql "postgresql://postgres:pass@localhost:32562/postgres" -c "COPY tweets_jsonb(data) FROM STDIN csv quote e'\x01' delimiter e'\x02';"
import io
import sys
import zipfile
from load_tweets import remove_nulls

filename = sys.argv[1]
with zipfile.ZipFile(filename, 'r') as archive:
    for subfilename in sorted(archive.namelist(), reverse=True):
        with io.TextIOWrapper(archive.open(subfilename)) as f:
            for line in f:
                print(remove_nulls(line.rstrip('\n')))
PY
done

#!/bin/bash

set -euo pipefail

mold -run cargo nextest run scratch

rg -v 'nsql-only' nsql/tests/nsql-test/scratch.sql | sed 's/nsql.csv/duckdb.csv/' | duckdb

delta /tmp/duckdb.csv /tmp/nsql.csv




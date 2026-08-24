#!/bin/sh
# Fetches the DuckDB SQLite extension that matches a given DuckDB version.
#
# The version is not written down here: the image build reads it from the duckdb-version.txt that
# MSBuild emits from the resolved DuckDB.NET package, so the extension cannot drift from the library
# that has to load it.
#
# The published file name is kept as-is on purpose - DuckDB derives the entry point it looks for from
# it, so a renamed extension fails to load with an error that reads like corruption.
#
# Usage: fetch-duckdb-sqlite-extension.sh <duckdb-version> <output-directory> [platform]

set -eu

version="${1:?usage: fetch-duckdb-sqlite-extension.sh <duckdb-version> <output-directory> [platform]}"
output_directory="${2:?usage: fetch-duckdb-sqlite-extension.sh <duckdb-version> <output-directory> [platform]}"
platform="${3:-linux_amd64}"

file_name="sqlite_scanner.duckdb_extension"
url="https://extensions.duckdb.org/v${version}/${platform}/${file_name}.gz"

echo "Fetching DuckDB SQLite extension v${version} (${platform})"

mkdir -p "${output_directory}"

# -f so an unknown version fails the build loudly rather than leaving an HTML error page behind and
# only failing at run time, in a task with no egress to recover from it.
curl -fSL --retry 3 --retry-all-errors --retry-delay 2 \
	"${url}" -o "${output_directory}/${file_name}.gz"
gunzip -f "${output_directory}/${file_name}.gz"

ls -l "${output_directory}/${file_name}"

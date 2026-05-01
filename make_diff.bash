#!/bin/bash

DIR2="modified_parquet_fdw"
DIR1="org_repo"
OUTDIR="diff_files"

mkdir -p "$OUTDIR"

# Traverse all files in DIR1
find "$DIR1" -type f | while read -r file1; do
    # Get relative path
    rel_path="${file1#$DIR1/}"
    file2="$DIR2/$rel_path"

    # Output file path
    out_file="$OUTDIR/$rel_path.diff"

    # Ensure directory exists
    mkdir -p "$(dirname "$out_file")"

    if [ -f "$file2" ]; then
        git diff --no-index -w --word-diff --ignore-blank-lines "$file1" "$file2" > "$out_file"
    else
        echo "File missing in DIR2: $rel_path" > "$out_file"
    fi
done
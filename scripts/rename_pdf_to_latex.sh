#!/bin/bash

set -e

# Adjust remote name according to you rclone config
PDF_LATEX_PATHS=(
    "digitization:cern-archives/raw/PDF_LATEX"
    "digitization:cern-archives/raw/CORRECTIONS/PDF_LATEX"
)

for BASE in "${PDF_LATEX_PATHS[@]}"; do
    rclone lsf -R --files-only "$BASE" | grep '\.pdf$' | while read -r FILE; do
        DIR=$(dirname "$FILE")
        BASENAME=$(basename "$FILE" .pdf)
        SRC="$BASE/$FILE"
        if [[ "$DIR" == "." ]]; then
            DST="$BASE/${BASENAME}_latex.pdf"
        else
            DST="$BASE/$DIR/${BASENAME}_latex.pdf"
        fi
        if [[ "$BASENAME" != *_latex ]]; then
            echo "Renaming $SRC -> $DST"
            rclone moveto "$SRC" "$DST"
        fi
    done
done

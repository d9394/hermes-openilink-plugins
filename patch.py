#!/usr/bin/env python3

import os
import re
import sys
import shutil
from pathlib import Path
from datetime import datetime

def backup_file(path):
    time_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = path.parent / f"{path.name}.{time_stamp}.bak"
    shutil.copy2(path, bak)
    print(f"[BACKUP] {bak}")

def parse_diff(diff_text):
    files = []

    pattern = re.compile(
        r'^diff --git a/(.*?) b/(.*?)\n(.*?)(?=^diff --git |\Z)',
        re.M | re.S
    )

    for old_file, new_file, body in pattern.findall(diff_text):
        files.append({
            "old": old_file,
            "new": new_file,
            "body": body
        })

    return files

def apply_new_file(root, relpath, body):

    lines = []

    for line in body.splitlines():

        if line.startswith('+++'):
            continue

        if line.startswith('@@'):
            continue

        if line.startswith('+'):
            lines.append(line[1:])

    target = root / relpath

    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        print(f"[SKIP] exists: {relpath}")
        return

    target.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8"
    )

    print(f"[ADD ] {relpath}")

def patch_existing_file(root, relpath, body):

    target = root / relpath

    if not target.exists():
        print(f"[MISS] {relpath}")
        return

    backup_file(target)

    original = target.read_text(
        encoding="utf-8",
        errors="ignore"
    )

    text = original

    hunks = re.findall(
        r'@@ .*? @@\n(.*?)(?=\n@@ |\Z)',
        body,
        re.S
    )

    changed = False

    for hunk in hunks:

        minus = []
        plus = []

        for line in hunk.splitlines():

            if line.startswith('-'):
                minus.append(line[1:])

            elif line.startswith('+'):
                plus.append(line[1:])

        if not minus:
            continue

        old_block = "\n".join(minus)
        new_block = "\n".join(plus)

        if old_block in text:
            text = text.replace(old_block, new_block, 1)
            changed = True

    if changed:
        target.write_text(text, encoding="utf-8")
        print(f"[PATCH] {relpath}")
    else:
        print(f"[SKIP ] no match: {relpath}")

def main():

    if len(sys.argv) != 3:

        print(
            "Usage:\n"
            "python patch.py /path/to/hermes-agent 9038.diff"
        )
        sys.exit(1)

    root = Path(sys.argv[1])
    diff_file = Path(sys.argv[2])

    diff_text = diff_file.read_text(
        encoding="utf-8",
        errors="ignore"
    )

    files = parse_diff(diff_text)

    for item in files:

        relpath = item["new"]

        body = item["body"]

        if "new file mode" in body:

            apply_new_file(
                root,
                relpath,
                body
            )

        else:

            patch_existing_file(
                root,
                relpath,
                body
            )

    print("\nDone.")

if __name__  == "main":
    main()

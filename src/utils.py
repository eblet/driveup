# -*- coding: utf-8 -*-

import re

# --- Helper Functions ---
def int_to_column_letter(n: int) -> str:
    """Converts a 1-based integer to an Excel-style column letter (A, B, ..., Z, AA, AB, ...)."""
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def sanitize_filename(name: str) -> str:
    """Removes or replaces characters that are invalid in filenames on common OS."""
    # Remove characters invalid in Windows/Linux/macOS filenames
    name = re.sub(r'[\\/*?"<>|]', "_", name)
    # Replace colons, often used in timestamps, with hyphens
    name = name.replace(":", "-")
    # Remove control characters
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)
    # Remove leading/trailing dots and spaces (problematic on Windows)
    name = name.strip(". ")
    # Handle empty names after sanitization
    if not name:
        name = "_unnamed_"
    return name 
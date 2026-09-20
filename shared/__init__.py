"""Knowledge both ingestion paths depend on.

Kept outside spark/ and streaming/ so neither owns it. A rule that lives in
one path and is copied into the other stops being one rule the first time
somebody edits a single copy.
"""
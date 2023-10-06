from dataclasses import dataclass


@dataclass
class ZipUtils:
    size_limit_in_bytes: int = 300_000_000
    page_limit: int = 1500
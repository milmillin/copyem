"""Utility functions for copyem."""


def parse_size_to_bytes(size_str: str) -> int:
    """Convert size string (e.g., '1G', '500M', '64K') to bytes.

    Supports suffixes: B, K/KB, M/MB, G/GB, T/TB (case-insensitive)
    """
    size_str = size_str.strip().upper()

    units = {
        "B": 1,
        "K": 1024,
        "KB": 1024,
        "M": 1024**2,
        "MB": 1024**2,
        "G": 1024**3,
        "GB": 1024**3,
        "T": 1024**4,
        "TB": 1024**4,
    }

    if not size_str:
        raise ValueError("Size string cannot be empty")

    # Check if last characters form a valid unit
    unit = None
    num_str = size_str

    for suffix in sorted(units.keys(), key=len, reverse=True):
        if size_str.endswith(suffix):
            unit = suffix
            num_str = size_str[: -len(suffix)]
            break

    if unit is None:
        # No unit specified, assume bytes
        unit = "B"

    try:
        number = float(num_str) if num_str else 1
        if number < 0:
            raise ValueError(f"Size cannot be negative: {size_str}")
        return int(number * units[unit])
    except ValueError as e:
        if "could not convert" in str(e):
            raise ValueError(f"Invalid size format: {size_str}")
        raise


def format_size(size_bytes: int) -> str:
    """Format bytes into human-readable units"""
    size_ = size_bytes
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_ < 1024.0:
            return f"{size_:.2f} {unit}"
        size_ /= 1024.0
    return f"{size_:.2f} PB"


def format_time(seconds: float) -> str:
    """Format ETA in appropriate units"""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds:02d}s"
    elif seconds < 3600:
        minutes = seconds // 60
        seconds = seconds % 60
        return f"{minutes:02d}m{seconds:02d}s"
    elif seconds < 86400:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d{hours}h"
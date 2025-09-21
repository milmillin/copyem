import argparse
import logging
from pathlib import Path
import subprocess
import sys
from typing import Optional, Generator
from blessed import Terminal
from time import time, sleep
import contextlib
import math
from blessed import Terminal

t = Terminal()


def log(message: str):
    print(message)


def _run_lines(cmds: list[str], cwd: Optional[Path] = None) -> list[str]:
    """Execute a command and return stdout, reporting line count during execution"""
    log(f"Running command: {cmds[0]}")

    process = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd)

    lines = []
    line_count = 0

    assert process.stdout is not None
    for line in process.stdout:
        lines.append(line.rstrip("\n"))
        line_count += 1
        if line_count % 10000 == 0:
            log(f"Processing {line_count:,} lines...")

    process.wait()

    if process.returncode != 0 and process.stderr is not None:
        stderr = process.stderr.read()
        log(f"Command failed with return code {process.returncode}: {stderr}")

    log(f"Command completed, processed {line_count:,} lines")
    return lines


def get_file_sizes(src_dir: Path, include_pattern: Optional[str]) -> list[tuple[str, int]]:
    """Get all files in src_dir that match the include pattern.

    Args:
        src_dir: Source directory to search
        include_pattern: Optional glob pattern to filter files (e.g., '*.txt', '**/*.py')

    Returns:
        List of Path objects for matching files
    """
    log(f"Scanning directory: {src_dir}")
    if include_pattern:
        log(f"Include pattern: {include_pattern}")

    args = ["find", "-type", "f"]
    if include_pattern is not None:
        args.extend(["-path", "./" + include_pattern])
    # args.append("-print0")
    files = _run_lines(args, cwd=src_dir)
    log(f"Found {len(files)} files")
    for f in files[:10]:
        log(f"  {f}")

    if len(files) == 0:
        return []

    # Calculate file sizes
    log(f"Querying file sizes")
    args = ["du", "--bytes", *files]
    sizes = _run_lines(args, cwd=src_dir)
    res: list[tuple[str, int]] = []
    for sizes in sizes:
        size, path = sizes.split("\t")
        res.append((path, int(size)))
    return res


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


# Format ETA in appropriate units
def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m ({seconds:.0f}s)"
    elif seconds < 86400:
        hours = seconds / 3600
        minutes = (seconds % 3600) / 60
        return f"{hours:.1f}h ({int(hours)}h {int(minutes)}m)"
    else:
        days = seconds / 86400
        hours = (seconds % 86400) / 3600
        return f"{days:.1f}d ({int(days)}d {int(hours)}h)"


def schedule_files(
    file_sizes: list[tuple[str, int]], tx_speed: int, buffer_size: int, latency: float
) -> tuple[list[str], float]:
    """
    Args:
        file_sizes: List of tuples of (path, size)
        tx_speed: Outgoing network speed in bytes per second
        buffer_size: Buffer size in bytes
        latency: Latency per file in seconds
    Returns:
        Ordered list of files to transfer
        Estimated time to transfer all files
    """
    file_sizes = list(file_sizes)
    file_sizes.sort(key=lambda x: x[1])

    # pointers are next to add
    small_ptr = 0
    big_ptr = len(file_sizes) - 1

    buffer_max_delay = buffer_size / tx_speed

    res: list[tuple[str, int]] = []
    eta = 0.0
    while small_ptr <= big_ptr:
        # add big file to buffer
        big_file, big_size = file_sizes[big_ptr]
        res.append((big_file, big_size))
        big_ptr -= 1
        big_time = big_size / tx_speed
        eta += big_time + latency

        # time where file will be in buffer
        buffer_time = min(big_time, buffer_max_delay)

        # during this time, add small files to buffer
        while small_ptr <= big_ptr and buffer_time > latency:
            small_file, small_size = file_sizes[small_ptr]
            res.append((small_file, small_size))
            small_ptr += 1
            small_time = small_size / tx_speed
            eta += small_time
            buffer_time += small_time - latency
            buffer_time = min(buffer_time, buffer_max_delay)

    total_size = sum(s[1] for s in file_sizes)
    estimated_speed = total_size / eta / 1024 / 1024

    return [r[0] for r in res], eta


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remote copy utility for efficient file transfers. Copyem uses tar to archive files and then transfers them over the network. It uses buffers and optimal file scheduling to maximize throughput."
    )
    parser.add_argument("src_dir", type=str, help="Directory to copy")
    parser.add_argument("--include", type=str, help="Include files matching this pattern")
    parser.add_argument(
        "-s",
        "--speed",
        type=str,
        default="20M",
        help="Assumption about network outgoing speed (e.g., '10M' for 10MB/s, '100K' for 100KB/s)",
    )

    parser.add_argument(
        "-l",
        "--latency",
        type=float,
        default=0.15,
        help="Assumption about the latency per loading a single file in a second",
    )

    parser.add_argument(
        "-b",
        "--buffer-size",
        type=str,
        default="1G",
        help="Buffer size for data transfers (e.g., '64K' for 64KB, '1M' for 1MB)",
    )

    parser.add_argument(
        "-p",
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel processes for file transfers (default: 1 for sequential)",
    )

    args = parser.parse_args()

    src_dir = Path(args.src_dir)
    if not src_dir.is_dir():
        parser.error(f"Source directory does not exist: {src_dir}")

    try:
        speed_bytes = parse_size_to_bytes(args.speed)
        buffer_bytes = parse_size_to_bytes(args.buffer_size)
    except ValueError as e:
        parser.error(str(e))

    # Get matching files
    log("Starting file discovery and size calculation")

    file_sizes = get_file_sizes(src_dir, args.include)

    # Calculate and format total size
    total_size = sum(size for _, size in file_sizes)

    log(f"Total size: {format_size(total_size)} ({total_size:,} bytes) across {len(file_sizes)} files")

    log("Scheduling Files")

    file_parts: list[list[tuple[str, int]]] = [[] for _ in range(args.parallel)]
    file_sizes.sort(key=lambda x: x[1])
    for i, f_ in enumerate(file_sizes):
        file_parts[i % args.parallel].append(f_)

    ordered_files: list[list[str]] = []
    overall_eta = 0.0
    for i, f in enumerate(file_parts):
        files, eta = schedule_files(f, speed_bytes, buffer_bytes, args.latency)
        ordered_files.append(files)
        part_size = sum(size for _, size in f)
        overall_eta = max(overall_eta, eta)
        print(f"Part {i + 1}: {len(files)} files, {format_size(part_size)}, eta: {format_time(eta)}")

    estimated_speed = total_size / overall_eta / 1024 / 1024
    log(
        f"Estimated time to transfer all files: {format_time(overall_eta)} @ avg. {estimated_speed:.2f}MB/s (max: {speed_bytes * args.parallel / 1024 / 1024:.2f}MB/s)"
    )

    # print("\n".join(map(str, file_sizes[:10])))

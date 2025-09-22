import argparse
import logging
from pathlib import Path
import subprocess
import sys
from typing import Optional, Generator, cast
from blessed import Terminal
from time import time, sleep
import selectors
import threading
import os
import tempfile
import io
import queue
import sys
from blessed import Terminal

t = Terminal()


class LogManager:
    """Manages terminal UI with scrolling messages and fixed status lines."""

    def __init__(self, term: Terminal, num_status_lines: int):
        self.term = term
        self.num_status_lines = num_status_lines
        self.mbuffer_status = {}  # suffix -> status text
        self.message_queue = queue.Queue()
        self.messages = []  # Keep track of scrolling messages
        self.lock = threading.Lock()
        self.setup_display()

    def setup_display(self):
        """Initialize the terminal display with scrolling region."""
        # Clear screen
        print(self.term.clear())

        # Set up scrolling region (leave bottom lines for status)
        # CSR sets scrolling from line 0 to height - num_status_lines - 1
        if self.num_status_lines > 0:
            scroll_bottom = self.term.height - self.num_status_lines - 1
            sys.stdout.write(self.term.csr(0, scroll_bottom))
            sys.stdout.flush()

    def update_mbuffer_status(self, suffix: str, text: str):
        """Update the mbuffer status line for a specific transfer."""
        with self.lock:
            self.mbuffer_status[suffix] = text.strip()
            self._redraw_status_lines()

    def add_message(self, text: str):
        """Add a message to the scrolling area."""
        with self.lock:
            # Save cursor position
            sys.stdout.write(self.term.save)

            # Move to end of scrolling region and print
            scroll_bottom = self.term.height - self.num_status_lines - 1
            sys.stdout.write(self.term.move(scroll_bottom, 0))
            print(text)

            # Restore cursor position
            sys.stdout.write(self.term.restore)
            sys.stdout.flush()

    def _redraw_status_lines(self):
        """Redraw all status lines at the bottom."""
        # Save cursor position
        sys.stdout.write(self.term.save)

        # Draw each mbuffer status line
        sorted_suffixes = sorted(self.mbuffer_status.keys())
        for i, suffix in enumerate(sorted_suffixes[:self.num_status_lines]):
            if suffix in self.mbuffer_status:
                line_pos = self.term.height - self.num_status_lines + i
                sys.stdout.write(self.term.move(line_pos, 0))
                sys.stdout.write(self.term.clear_eol)
                status = self.mbuffer_status[suffix]
                # Format status line with color
                sys.stdout.write(f"{self.term.cyan}[mbuffer-{suffix}]{self.term.normal} {status}")

        # Restore cursor position
        sys.stdout.write(self.term.restore)
        sys.stdout.flush()

    def cleanup(self):
        """Reset terminal to normal state."""
        # Reset scrolling region to full terminal
        sys.stdout.write(self.term.csr(0, self.term.height))
        # Clear screen
        print(self.term.clear())
        sys.stdout.flush()


# Global log manager (will be initialized in main)
log_manager = None


def log(message: str):
    """Log a message using the LogManager if available, otherwise print."""
    if log_manager:
        log_manager.add_message(message)
    else:
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
        if line_count % 100 == 0:
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


sel = selectors.DefaultSelector()


def monitor_stderr(stop_event: threading.Event) -> None:
    """Monitor stderr from processes using selector in a separate thread."""
    global log_manager

    while not stop_event.is_set():
        events = sel.select(timeout=0.1)
        for key, mask in events:
            fileobj = cast(io.BufferedReader, key.fileobj)
            data = str(key.data)
            try:
                line = fileobj.readline()
                if line:
                    line_str = line.decode()
                    line_stripped = line_str.rstrip()

                    # Check if this is mbuffer output
                    if data.startswith("mbuffer-") and log_manager:
                        # Extract suffix from data (e.g., "mbuffer-0" -> "0")
                        suffix = data.replace("mbuffer-", "")
                        log_manager.update_mbuffer_status(suffix, line_stripped)
                    else:
                        # Regular message - add to scrolling area
                        if log_manager:
                            log_manager.add_message(f"[{data}] {line_stripped}")
                        else:
                            print(f"[{data}] {line_stripped}", flush=True)
            except Exception as e:
                error_msg = f"Error reading from {data}: {e}"
                if log_manager:
                    log_manager.add_message(error_msg)
                else:
                    print(error_msg, flush=True)


def transfer_files(
    filelist: list[str],
    src_dir: Path,
    remote: str,
    dst_dir: str,
    buffer_size: int,
    suffix: str,
) -> tuple[list[subprocess.Popen], list, list[Path]]:
    """Transfer files using tar | mbuffer | ssh pipeline.

    Args:
        filelist: List of file paths to transfer
        src_dir: Source directory (for tar's working directory)
        remote: SSH remote (e.g., username@hostname.com)
        dst_dir: Destination directory on remote
        buffer_size: Buffer size in bytes for mbuffer
        suffix: Suffix for identifying this transfer

    Returns:
        Tuple of (processes, file handles to close, paths to unlink)
    """
    # Create temporary file with file list
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    filelist_path = Path(f.name)
    for file in filelist:
        f.write(file + "\n")
    f.close()

    pipe_name = tempfile.mkdtemp() + "/pipe"
    os.mkfifo(pipe_name)

    # Build the commands
    tar_cmd = ["tar", "-cf", "-", "-T", str(filelist_path)]
    mbuffer_cmd = ["mbuffer", "-m", f"{buffer_size}b", "-l", pipe_name, "-q"]
    ssh_cmd = ["ssh", remote, f"tar -xvf - -C {dst_dir}"]

    log(f"Starting transfer pipeline {suffix} to {remote}:{dst_dir}")
    log(f"Buffer size: {format_size(buffer_size)}")

    # Create the pipeline: tar | mbuffer | ssh
    tar_proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, cwd=src_dir)

    mbuffer_proc = subprocess.Popen(
        mbuffer_cmd,
        stdin=tar_proc.stdout,
        stdout=subprocess.PIPE,
    )

    # Close tar's stdout in parent to allow proper SIGPIPE handling
    assert tar_proc.stdout is not None
    tar_proc.stdout.close()

    ssh_proc = subprocess.Popen(ssh_cmd, stdin=mbuffer_proc.stdout, stdout=subprocess.PIPE)
    # Close mbuffer's stdout in parent
    assert mbuffer_proc.stdout is not None
    mbuffer_proc.stdout.close()

    pipe = open(pipe_name, "rb")
    sel.register(pipe, selectors.EVENT_READ, data=f"mbuffer-{suffix}")

    if ssh_proc.stdout is not None:
        sel.register(ssh_proc.stdout, selectors.EVENT_READ, data=f"ssh-{suffix}")

    # Return processes and cleanup info
    processes = [tar_proc, mbuffer_proc, ssh_proc]
    file_handles = [pipe]
    paths_to_unlink = [filelist_path, Path(pipe_name)]

    return processes, file_handles, paths_to_unlink


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
    parser.add_argument("src_dir", type=str, help="Source directory to copy")
    parser.add_argument("remote", type=str, help="SSH remote (e.g., username@hostname.com)")
    parser.add_argument("dst_dir", type=str, help="Target directory on remote")
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
        log(f"Part {i + 1}: {len(files)} files, {format_size(part_size)}, eta: {format_time(eta)}")

    estimated_speed = total_size / overall_eta / 1024 / 1024

    # Initialize the LogManager with number of parallel transfers for status lines
    global log_manager
    log_manager = LogManager(t, args.parallel)

    try:
        log(
            f"Estimated time to transfer all files: {format_time(overall_eta)} @ avg. {estimated_speed:.2f}MB/s (max: {speed_bytes * args.parallel / 1024 / 1024:.2f}MB/s)"
        )

        # Start the monitoring thread
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=monitor_stderr, args=(stop_event,))
        monitor_thread.start()

        # Start all transfers and collect processes/cleanup info
        all_processes: list[subprocess.Popen] = []
        all_file_handles: list[io.BufferedReader] = []
        all_paths_to_unlink: list[Path] = []

        for i in range(args.parallel):
            processes, file_handles, paths_to_unlink = transfer_files(
                ordered_files[i], src_dir, args.remote, args.dst_dir, buffer_bytes, f"{i}"
            )
            all_processes.extend(processes)
            all_file_handles.extend(file_handles)
            all_paths_to_unlink.extend(paths_to_unlink)

        # Wait for all processes to complete
        log("Waiting for all transfers to complete...")
        for proc in all_processes:
            returncode = proc.wait()
            if returncode != 0:
                assert isinstance(proc.args, list)
                log(f"Process {proc.args[0]} exited with code {returncode}")

        log("All transfers completed")

    finally:
        # Stop the monitoring thread
        stop_event.set()
        if 'monitor_thread' in locals():
            monitor_thread.join(timeout=1)

        # Clean up the LogManager
        if log_manager:
            log_manager.cleanup()
            log_manager = None

        # Clean up file handles
        for handle in all_file_handles:
            try:
                sel.unregister(handle)
                handle.close()
            except:
                pass

        # Clean up temporary files
        for path in all_paths_to_unlink:
            try:
                if path.exists():
                    path.unlink()
            except:
                pass

"""Core file processing functionality for copyem."""

import subprocess
import os
import tempfile
import io
import selectors
import threading
from pathlib import Path
from typing import Optional

from .logger import log
from .utils import format_size, format_time


def _run_lines(cmds: list[str], stdin: Optional[str] = None, cwd: Optional[Path] = None) -> list[str]:
    """Execute a command and return stdout, reporting line count during execution"""
    cmd_ = cmds[0]
    print(f"[{cmd_}] 0 lines", end="\r")

    process = subprocess.Popen(
        cmds,
        stdin=subprocess.PIPE if stdin is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=cwd,
    )

    # Write stdin in a separate thread to avoid deadlock
    stdin_thread = None
    if stdin is not None:
        def write_stdin():
            assert process.stdin is not None
            process.stdin.write(stdin)
            process.stdin.close()

        stdin_thread = threading.Thread(target=write_stdin)
        stdin_thread.start()

    lines = []
    line_count = 0

    assert process.stdout is not None
    for line in process.stdout:
        lines.append(line.rstrip("\n"))
        line_count += 1
        if line_count % 100 == 0:
            print(f"[{cmd_}] {line_count:,} lines", end="\r")

    process.wait()
    print(f"[{cmd_}] {line_count:,} lines. done.")

    # Wait for stdin thread to complete if it exists
    if stdin_thread is not None:
        stdin_thread.join()

    if process.returncode != 0 and process.stderr is not None:
        stderr = process.stderr.read()
        log(f"[{cmd_}] Command failed with return code {process.returncode}: {stderr}")

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
    # args = ["du", "--bytes", "--files0-from=-"]

    res: list[tuple[str, int]] = []

    args = ["xargs", "stat", "--format=%s\t%n"]
    batch_size = 100000
    for i in range(0, len(files), batch_size):
        cur_files = files[i : i + batch_size]
        sizes = _run_lines(args, stdin="\n".join(cur_files), cwd=src_dir)
        for sizes in sizes:
            size, path = sizes.split("\t")
            res.append((path, int(size)))

    return res


def get_remote_file_sizes(remote: str, cwd: str, files: list[str]) -> list[tuple[str, int]]:
    """Get sizes for a list of files on a remote server.

    Args:
        remote: SSH remote (e.g., username@hostname.com)
        files: List of file paths to query on the remote

    Returns:
        List of tuples of (path, size) for files that exist
    """
    if not files:
        return []

    log(f"Querying {len(files)} file sizes on remote: {remote}")

    res: list[tuple[str, int]] = []

    # Process files in batches to avoid command line length limits
    batch_size = 100000
    for i in range(0, len(files), batch_size):
        cur_files = files[i : i + batch_size]

        # Use xargs and stat to get file sizes efficiently
        stat_cmd = f"cd {cwd} && xargs stat '--format=%s\t%n' 2> /dev/null"
        ssh_stat_cmd = ["ssh", remote, stat_cmd]

        # Send file list via stdin
        stdin_data = "\n".join(cur_files)
        sizes = _run_lines(ssh_stat_cmd, stdin=stdin_data)

        for size_line in sizes:
            size, path = size_line.split('\t', 1)
            res.append((path, int(size)))

    log(f"Successfully queried {len(res)} file sizes from remote")
    return res


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


def transfer_files(
    filelist: list[str],
    src_dir: Path,
    remote: str,
    dst_dir: str,
    buffer_size: int,
    suffix: str,
    sel: selectors.BaseSelector,
) -> tuple[list[subprocess.Popen], list, list[Path]]:
    """Transfer files using tar | mbuffer | ssh pipeline.

    Args:
        filelist: List of file paths to transfer
        src_dir: Source directory (for tar's working directory)
        remote: SSH remote (e.g., username@hostname.com)
        dst_dir: Destination directory on remote
        buffer_size: Buffer size in bytes for mbuffer
        suffix: Suffix for identifying this transfer
        sel: Selector for monitoring

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

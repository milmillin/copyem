"""Core file processing functionality for copyem."""

import subprocess
import os
import tempfile
import io
import selectors
import threading
from pathlib import Path
from typing import Optional, IO
import shlex

from .logger import log
from .utils import format_size, format_time

def quote_args(args: list[str]) -> str:
    return ' '.join(shlex.quote(arg) for arg in args)

def parse_remote_path(path: str) -> tuple[Optional[str], str]:
    """Parse a path that may be in remote format (user@host:/path).

    Args:
        path: Path string, either local (/path/to/dir) or remote (user@host:/path/to/dir)

    Returns:
        Tuple of (remote, dir_path) where remote is None for local paths
    """
    if ':' in path:
        parts = path.split(':', 1)
        if len(parts) == 2 and parts[0]:
            # Check if it looks like a remote (has @ or is not a Windows drive letter)
            if '@' in parts[0] or (len(parts[0]) > 1):
                return parts[0], parts[1]
    return None, path


def _run_lines(cmds: list[str], stdin: Optional[str] = None, cwd: Optional[Path] = None) -> list[str]:
    """Execute a command and return stdout, reporting line count during execution"""
    cmd_ = cmds[0]
    print(cmds)
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


def get_file_sizes(dir_path: str, include_pattern: Optional[str], remote: Optional[str] = None) -> list[tuple[str, int]]:
    """Get all files in a directory that match the include pattern.

    Args:
        dir_path: Directory path to search (absolute path)
        include_pattern: Optional glob pattern to filter files (e.g., '*.txt', '**/*.py')
        remote: Optional SSH remote (e.g., username@hostname.com) for remote directories

    Returns:
        List of tuples of (path, size) for matching files
    """
    log(f"Scanning directory: {remote + ':' if remote else ''}{dir_path}")
    if include_pattern:
        log(f"Include pattern: {include_pattern}")

    # Build find commands
    find_args = ["find", "-type", "f"]
    find_link_args = ["find", "-type", "l"]
    if include_pattern is not None:
        find_args.extend(["-path", "./" + include_pattern])
        find_link_args.extend(["-path", "./" + include_pattern])

    # Execute find commands (locally or remotely)
    if remote is None:
        # Local execution
        files = _run_lines(find_args, cwd=Path(dir_path))
        files.extend(_run_lines(find_link_args, cwd=Path(dir_path)))
    else:
        # Remote execution via SSH
        find_cmd = f"cd {dir_path} && {quote_args(find_args)}"
        find_link_cmd = f"cd {dir_path} && {quote_args(find_link_args)}"
        files = _run_lines(["ssh", remote, find_cmd])
        files.extend(_run_lines(["ssh", remote, find_link_cmd]))

    log(f"Found {len(files):,} files")
    for f in files[:10]:
        log(f"  {f}")

    if len(files) == 0:
        return []

    # Calculate file sizes
    log(f"Querying file sizes")
    res: list[tuple[str, int]] = []

    batch_size = 100000
    for i in range(0, len(files), batch_size):
        cur_files = files[i : i + batch_size]
        stdin_data = "\n".join(cur_files)

        if remote is None:
            # Local stat
            stat_args = ["xargs", "stat", "--format=%s\t%n"]
            sizes = _run_lines(stat_args, stdin=stdin_data, cwd=Path(dir_path))
        else:
            # Remote stat via SSH
            stat_cmd = f"cd {dir_path} && xargs stat '--format=%s\t%n' 2> /dev/null"
            sizes = _run_lines(["ssh", remote, stat_cmd], stdin=stdin_data)

        for size_line in sizes:
            size, path = size_line.split('\t', 1)
            res.append((path, int(size)))

    log(f"Successfully queried {len(res)} file sizes")
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
    src_dir: str,
    dst_dir: str,
    buffer_size: int,
    suffix: str,
    sel: selectors.BaseSelector,
    src_remote: Optional[str] = None,
    dst_remote: Optional[str] = None,
) -> tuple[list[subprocess.Popen], list[IO], list[Path]]:
    """Transfer files using tar | mbuffer | ssh pipeline.

    Args:
        filelist: List of file paths to transfer
        src_dir: Source directory (for tar's working directory)
        dst_dir: Destination directory
        buffer_size: Buffer size in bytes for mbuffer
        suffix: Suffix for identifying this transfer
        sel: Selector for monitoring
        src_remote: Optional SSH remote for source (e.g., username@hostname.com)
        dst_remote: Optional SSH remote for destination

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

    # Determine transfer type and build commands
    src_label = f"{src_remote}:" if src_remote else ""
    dst_label = f"{dst_remote}:" if dst_remote else ""
    log(f"Starting transfer pipeline {suffix}: {src_label}{src_dir} -> {dst_label}{dst_dir}")
    log(f"Buffer size: {format_size(buffer_size)}")

    mbuffer_cmd = ["mbuffer", "-m", f"{buffer_size}b", "-l", pipe_name, "-q"]
    file_handles: list[IO] = []
    processes: list[subprocess.Popen] = []

    if src_remote is None and dst_remote is None:
        # Local to Local transfer
        tar_create_cmd = ["tar", "-cvf", "-", "-T", str(filelist_path)]
        tar_extract_cmd = ["tar", "-xvf", "-", "-C", dst_dir]

        tar_create_proc = subprocess.Popen(tar_create_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=Path(src_dir))
        mbuffer_proc = subprocess.Popen(mbuffer_cmd, stdin=tar_create_proc.stdout, stdout=subprocess.PIPE)
        assert tar_create_proc.stdout is not None
        tar_create_proc.stdout.close()

        tar_extract_proc = subprocess.Popen(tar_extract_cmd, stdin=mbuffer_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert mbuffer_proc.stdout is not None
        mbuffer_proc.stdout.close()

        processes = [tar_create_proc, mbuffer_proc, tar_extract_proc]

        if tar_extract_proc.stdout is not None:
            sel.register(tar_extract_proc.stdout, selectors.EVENT_READ, data=f"out-{suffix}")
            file_handles.append(tar_extract_proc.stdout)
        if tar_create_proc.stderr is not None:
            sel.register(tar_create_proc.stderr, selectors.EVENT_READ, data=f"in-{suffix}")
            file_handles.append(tar_create_proc.stderr)

    elif src_remote is None and dst_remote is not None:
        # Local to Remote transfer (original behavior)
        tar_cmd = ["tar", "-cvf", "-", "-T", str(filelist_path)]
        ssh_cmd = ["ssh", dst_remote, f"tar -xvf - -C {dst_dir}"]

        tar_proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=Path(src_dir))
        mbuffer_proc = subprocess.Popen(mbuffer_cmd, stdin=tar_proc.stdout, stdout=subprocess.PIPE)
        assert tar_proc.stdout is not None
        tar_proc.stdout.close()

        ssh_proc = subprocess.Popen(ssh_cmd, stdin=mbuffer_proc.stdout, stdout=subprocess.PIPE)
        assert mbuffer_proc.stdout is not None
        mbuffer_proc.stdout.close()

        processes = [tar_proc, mbuffer_proc, ssh_proc]

        if ssh_proc.stdout is not None:
            sel.register(ssh_proc.stdout, selectors.EVENT_READ, data=f"out-{suffix}")
            file_handles.append(ssh_proc.stdout)
        if tar_proc.stderr is not None:
            sel.register(tar_proc.stderr, selectors.EVENT_READ, data=f"in-{suffix}")
            file_handles.append(tar_proc.stderr)

    elif src_remote is not None and dst_remote is None:
        # Remote to Local transfer
        # Upload filelist to remote, then use it there
        ssh_tar_cmd = ["ssh", src_remote, f"cd {src_dir} && tar -cvf - -T -"]
        tar_extract_cmd = ["tar", "-xvf", "-", "-C", dst_dir]

        # Send filelist via stdin to the remote tar command
        ssh_tar_proc = subprocess.Popen(
            ssh_tar_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Write filelist to stdin in a thread
        def write_filelist():
            assert ssh_tar_proc.stdin is not None
            with open(filelist_path, 'r') as f:
                ssh_tar_proc.stdin.write(f.read().encode())
            ssh_tar_proc.stdin.close()

        filelist_thread = threading.Thread(target=write_filelist)
        filelist_thread.start()

        mbuffer_proc = subprocess.Popen(mbuffer_cmd, stdin=ssh_tar_proc.stdout, stdout=subprocess.PIPE)
        assert ssh_tar_proc.stdout is not None
        ssh_tar_proc.stdout.close()

        tar_extract_proc = subprocess.Popen(tar_extract_cmd, stdin=mbuffer_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert mbuffer_proc.stdout is not None
        mbuffer_proc.stdout.close()

        processes = [ssh_tar_proc, mbuffer_proc, tar_extract_proc]

        if tar_extract_proc.stdout is not None:
            sel.register(tar_extract_proc.stdout, selectors.EVENT_READ, data=f"out-{suffix}")
            file_handles.append(tar_extract_proc.stdout)
        if ssh_tar_proc.stderr is not None:
            sel.register(ssh_tar_proc.stderr, selectors.EVENT_READ, data=f"in-{suffix}")
            file_handles.append(ssh_tar_proc.stderr)

    else:
        # Remote to Remote transfer
        ssh_tar_create_cmd = ["ssh", src_remote, f"cd {src_dir} && tar -cvf - -T -"]
        ssh_tar_extract_cmd = ["ssh", dst_remote, f"tar -xvf - -C {dst_dir}"]

        # Send filelist via stdin to the remote tar command
        ssh_tar_create_proc = subprocess.Popen(
            ssh_tar_create_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Write filelist to stdin in a thread
        def write_filelist():
            assert ssh_tar_create_proc.stdin is not None
            with open(filelist_path, 'r') as f:
                ssh_tar_create_proc.stdin.write(f.read().encode())
            ssh_tar_create_proc.stdin.close()

        filelist_thread = threading.Thread(target=write_filelist)
        filelist_thread.start()

        mbuffer_proc = subprocess.Popen(mbuffer_cmd, stdin=ssh_tar_create_proc.stdout, stdout=subprocess.PIPE)
        assert ssh_tar_create_proc.stdout is not None
        ssh_tar_create_proc.stdout.close()

        ssh_tar_extract_proc = subprocess.Popen(ssh_tar_extract_cmd, stdin=mbuffer_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert mbuffer_proc.stdout is not None
        mbuffer_proc.stdout.close()

        processes = [ssh_tar_create_proc, mbuffer_proc, ssh_tar_extract_proc]

        if ssh_tar_extract_proc.stdout is not None:
            sel.register(ssh_tar_extract_proc.stdout, selectors.EVENT_READ, data=f"out-{suffix}")
            file_handles.append(ssh_tar_extract_proc.stdout)
        if ssh_tar_create_proc.stderr is not None:
            sel.register(ssh_tar_create_proc.stderr, selectors.EVENT_READ, data=f"in-{suffix}")
            file_handles.append(ssh_tar_create_proc.stderr)

    pipe = open(pipe_name, "rb")
    file_handles.insert(0, pipe)
    paths_to_unlink = [filelist_path, Path(pipe_name)]

    sel.register(pipe, selectors.EVENT_READ, data=f"mbuffer-{suffix}")

    return processes, file_handles, paths_to_unlink

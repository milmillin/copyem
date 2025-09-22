"""Remote copy utility for efficient file transfers."""

import argparse
import threading
import selectors
import io
from pathlib import Path
from blessed import Terminal
from typing import IO

from .utils import parse_size_to_bytes, format_size, format_time
from .logger import LogManager, log, monitor_stderr
from .core import get_file_sizes, get_remote_file_sizes, schedule_files, transfer_files

# Global terminal and selector
t = Terminal()
sel = selectors.DefaultSelector()

# Import log_manager from logger module for global access
import copyem.logger


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

    # Check remote files to identify what needs to be transferred
    log("Checking remote file sizes...")

    # Get sizes of files that exist on remote
    remote_file_info = get_remote_file_sizes(args.remote, args.dst_dir, [p[0] for p in file_sizes])

    # Create a dict for quick lookup of remote file sizes
    remote_sizes = {path: size for path, size in remote_file_info}

    # Filter out files that already exist with same size on remote
    files_to_transfer = []
    skipped_files = []
    for file_path, local_size in file_sizes:
        if file_path in remote_sizes:
            if remote_sizes[file_path] == local_size:
                skipped_files.append((file_path, local_size))
            else:
                # File exists but size differs - transfer it
                files_to_transfer.append((file_path, local_size))
        else:
            # File doesn't exist on remote - transfer it
            files_to_transfer.append((file_path, local_size))

    if skipped_files:
        log(f"Skipping {len(skipped_files)} files that already exist on remote with same size")

    if not files_to_transfer:
        log("All files already exist on remote with matching sizes. Nothing to transfer.")
        return

    # Update file_sizes to only include files that need transfer
    file_sizes = files_to_transfer
    total_size = sum(size for _, size in file_sizes)

    log(f"Will transfer: {format_size(total_size)} ({total_size:,} bytes) across {len(file_sizes)} files")

    log("Scheduling Files")

    # Adjust parallel count if we have fewer files than requested parallel transfers
    actual_parallel = min(args.parallel, len(file_sizes))
    if actual_parallel < args.parallel:
        log(f"Adjusting parallel transfers from {args.parallel} to {actual_parallel} (limited by file count)")

    file_parts: list[list[tuple[str, int]]] = [[] for _ in range(actual_parallel)]
    file_sizes.sort(key=lambda x: x[1])
    for i, f_ in enumerate(file_sizes):
        file_parts[i % actual_parallel].append(f_)

    ordered_files: list[list[str]] = []
    overall_eta = 0.0
    for i, f in enumerate(file_parts):
        if f:  # Only process non-empty file parts
            files, eta = schedule_files(f, speed_bytes, buffer_bytes, args.latency)
            ordered_files.append(files)
            part_size = sum(size for _, size in f)
            overall_eta = max(overall_eta, eta)
            log(f"Part {i + 1}: {len(files)} files, {format_size(part_size)}, eta: {format_time(eta)}")

    estimated_speed = total_size / overall_eta / 1024 / 1024 if overall_eta > 0 else 0

    # Get smallest and largest file sizes
    smallest_file = min(file_sizes, key=lambda x: x[1]) if file_sizes else (None, 0)
    largest_file = max(file_sizes, key=lambda x: x[1]) if file_sizes else (None, 0)

    # Display transfer summary and ask for confirmation
    print(f"\n{'='*60}")
    print("Transfer Summary:")
    print(f"  Source: {src_dir}")
    print(f"  Destination: {args.remote}:{args.dst_dir}")
    print(f"  Total files: {len(file_sizes)}")
    print(f"  Total size: {format_size(total_size)}")
    print(f"  Smallest file: {format_size(smallest_file[1])}")
    print(f"  Largest file: {format_size(largest_file[1])}")
    print(f"\nTransfer Settings:")
    print(f"  Parallel processes: {actual_parallel}")
    print(f"  Assumed speed: {args.speed} ({format_size(speed_bytes)}/s)")
    print(f"  Buffer size: {args.buffer_size} ({format_size(buffer_bytes)})")
    print(f"  File latency: {args.latency}s")
    print(f"\nEstimates:")
    print(f"  Transfer time: {format_time(overall_eta)}")
    print(f"  Average speed: {estimated_speed:.2f} MB/s")
    print(f"{'='*60}\n")

    # Ask for user confirmation
    response = input("Proceed with transfer? (y/N): ").strip().lower()
    if response != 'y':
        print("Transfer cancelled by user.")
        return

    # Initialize the LogManager with number of parallel transfers for status lines
    copyem.logger.log_manager = LogManager(t, actual_parallel, total_size)
    stop_event = threading.Event()
    monitor_thread = threading.Thread(target=monitor_stderr, args=(sel, stop_event))

    # Start all transfers and collect processes/cleanup info
    all_processes: list = []
    all_file_handles: list[IO] = []
    all_paths_to_unlink: list[Path] = []

    try:
        log(
            f"Estimated time to transfer all files: {format_time(overall_eta)} @ avg. {estimated_speed:.2f}MB/s (max: {speed_bytes * actual_parallel / 1024 / 1024:.2f}MB/s)"
        )

        monitor_thread.start()


        for i in range(len(ordered_files)):  # Use actual number of file parts
            processes, file_handles, paths_to_unlink = transfer_files(
                ordered_files[i], src_dir, args.remote, args.dst_dir, buffer_bytes, f"{i}", sel
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
        if "monitor_thread" in locals():
            monitor_thread.join(timeout=1)

        # Clean up the LogManager
        if copyem.logger.log_manager:
            copyem.logger.log_manager.cleanup()
            copyem.logger.log_manager = None

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
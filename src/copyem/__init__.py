"""Remote copy utility for efficient file transfers."""

import argparse
import threading
import selectors
import io
from pathlib import Path
from blessed import Terminal

from .utils import parse_size_to_bytes, format_size, format_time
from .logger import LogManager, log, monitor_stderr
from .core import get_file_sizes, schedule_files, transfer_files

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
    print(f"  Parallel processes: {args.parallel}")
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
    copyem.logger.log_manager = LogManager(t, args.parallel, total_size)

    try:
        log(
            f"Estimated time to transfer all files: {format_time(overall_eta)} @ avg. {estimated_speed:.2f}MB/s (max: {speed_bytes * args.parallel / 1024 / 1024:.2f}MB/s)"
        )

        # Start the monitoring thread
        stop_event = threading.Event()
        monitor_thread = threading.Thread(target=monitor_stderr, args=(sel, stop_event))
        monitor_thread.start()

        # Start all transfers and collect processes/cleanup info
        all_processes: list = []
        all_file_handles: list[io.BufferedReader] = []
        all_paths_to_unlink: list[Path] = []

        for i in range(args.parallel):
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
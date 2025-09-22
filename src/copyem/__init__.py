"""Remote copy utility for efficient file transfers."""

import argparse
import threading
import selectors
import io
import time
import subprocess
from pathlib import Path
from blessed import Terminal
from typing import IO, Dict, List, Tuple, Optional

from .utils import parse_size_to_bytes, format_size, format_time
from .logger import LogManager, log, monitor_stderr
from .core import get_file_sizes, get_remote_file_sizes, schedule_files, transfer_files

# Global terminal and selector
t = Terminal()
sel = selectors.DefaultSelector()

# Import log_manager from logger module for global access
import copyem.logger


# Transfer state tracking
class TransferState:
    def __init__(self, suffix: str, file_list: List[str]):
        self.suffix = suffix
        self.remaining_files = file_list.copy()
        self.completed_size = 0  # Total size of successfully transferred files
        self.processes: List[subprocess.Popen] = []
        self.file_handles: List[IO] = []
        self.paths_to_unlink: List[Path] = []
        self.retry_count = 0
        self.completed = False
        self.failed = False


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
        default=0.05,
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

    parser.add_argument(
        "-r",
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retry attempts for failed transfers (default: 3)",
    )

    parser.add_argument(
        "--retry-delay",
        type=float,
        default=2.0,
        help="Delay in seconds between retry attempts (default: 2.0)",
    )

    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.5,
        help="Polling interval in seconds for monitoring transfers (default: 0.5)",
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

    # HACK: add TAR header of 512 bytes and extra for long paths. I don't know exactly how TAR encoding works.
    file_sizes = [(f[0], f[1] + 512 + max(len(f[0]) - 100, 0)) for f in file_sizes]

    # Create file size mappings for each parallel transfer
    file_size_map: Dict[str, int] = {filepath: size for filepath, size in file_sizes}

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
    print(f"  Max retries: {args.max_retries}")
    print(f"  Retry delay: {args.retry_delay}s")
    print(f"\nEstimates:")
    print(f"  Transfer time: {format_time(overall_eta)}")
    print(f"  Average speed: {estimated_speed:.2f} MB/s")
    print(f"{'='*60}\n")

    # Ask for user confirmation
    response = input("Proceed with transfer? (y/N): ").strip().lower()
    if response != "y":
        print("Transfer cancelled by user.")
        return

    # Initialize the LogManager with number of parallel transfers for status lines
    copyem.logger.log_manager = LogManager(t, actual_parallel, total_size)
    stop_event = threading.Event()
    monitor_thread = threading.Thread(target=monitor_stderr, args=(sel, stop_event))

    # Configuration from arguments
    MAX_RETRIES = args.max_retries
    RETRY_DELAY = args.retry_delay  # seconds
    POLL_INTERVAL = args.poll_interval  # seconds

    # Initialize transfer states for each parallel transfer
    transfer_states: Dict[str, TransferState] = {}
    for i in range(len(ordered_files)):
        transfer_states[str(i)] = TransferState(str(i), ordered_files[i])

    try:
        log(
            f"Estimated time to transfer all files: {format_time(overall_eta)} @ avg. {estimated_speed:.2f}MB/s (max: {speed_bytes * actual_parallel / 1024 / 1024:.2f}MB/s)"
        )

        monitor_thread.start()

        # Start initial transfers
        for suffix, state in transfer_states.items():
            if state.remaining_files:
                processes, file_handles, paths_to_unlink = transfer_files(
                    state.remaining_files, src_dir, args.remote, args.dst_dir, buffer_bytes, suffix, sel
                )
                state.processes = processes
                state.file_handles = file_handles
                state.paths_to_unlink = paths_to_unlink
                log(f"Started transfer {suffix} with {len(state.remaining_files)} files")

        # Poll processes and handle retries
        log("Monitoring transfers...")
        while True:
            for suffix, state in transfer_states.items():
                if state.failed:
                    continue

                # Check if any process in the pipeline has finished
                all_proc_done = True
                failed_proc = None
                for proc in state.processes:
                    ret = proc.poll()
                    if ret is not None:
                        # process finished
                        if ret != 0:
                            failed_proc = proc
                    else:
                        all_proc_done = False

                # All processes finished or one failed
                if failed_proc:
                    # Transfer failed
                    log(f"Transfer {suffix} failed (process exited with code {failed_proc.returncode})")

                    # Get completed files from SSH messages
                    if copyem.logger.log_manager is not None:
                        ssh_messages = copyem.logger.log_manager.get_ssh_messages(f"ssh-{suffix}")[:-1]
                        # Remove the last message from the list
                        copyem.logger.log_manager.pop_ssh_messages(f"ssh-{suffix}")
                    else:
                        ssh_messages = []

                    # Files in SSH messages are successfully transferred
                    completed_files = set(ssh_messages)
                    if len(completed_files) > 0:
                        # Calculate size of completed files
                        state.completed_size = sum(file_size_map.get(f, 0) for f in completed_files)

                        # Update the logger with the completed size
                        if copyem.logger.log_manager:
                            copyem.logger.log_manager.update_completed_size(suffix, state.completed_size)

                        log(
                            f"Transfer {suffix}: {len(completed_files)} files confirmed transferred ({format_size(state.completed_size)})"
                        )

                    # Clean up current processes
                    for proc in state.processes:
                        try:
                            proc.terminate()
                            proc.wait(timeout=1)
                        except:
                            try:
                                proc.kill()
                            except:
                                pass

                    # Clean up file handles
                    for handle in state.file_handles:
                        try:
                            sel.unregister(handle)
                            handle.close()
                        except:
                            pass

                    # Clean up temp files
                    for path in state.paths_to_unlink:
                        try:
                            if path.exists():
                                path.unlink()
                        except:
                            pass

                    # Prepare for retry
                    state.remaining_files = [f for f in state.remaining_files if f not in completed_files]

                    if state.remaining_files and state.retry_count < MAX_RETRIES:
                        log(
                            f"Retrying transfer {suffix} (attempt {state.retry_count + 1}/{MAX_RETRIES + 1}) with {len(state.remaining_files)} remaining files"
                        )
                        state.retry_count += 1

                        # Wait before retry
                        time.sleep(RETRY_DELAY)

                        # Start new transfer with remaining files
                        processes, file_handles, paths_to_unlink = transfer_files(
                            state.remaining_files, src_dir, args.remote, args.dst_dir, buffer_bytes, suffix, sel
                        )
                        state.processes = processes
                        state.file_handles = file_handles
                        state.paths_to_unlink = paths_to_unlink
                    elif state.remaining_files:
                        log(
                            f"Transfer {suffix} failed after {MAX_RETRIES} retries. {len(state.remaining_files)} files remaining"
                        )
                        state.failed = True
                elif all_proc_done:
                    # Check if all processes completed successfully
                    assert all(proc.poll() == 0 for proc in state.processes)
                    log(f"Transfer {suffix} completed successfully")
                    state.completed = True

                    # Clean up
                    for handle in state.file_handles:
                        try:
                            sel.unregister(handle)
                            handle.close()
                        except:
                            pass
                    for path in state.paths_to_unlink:
                        try:
                            if path.exists():
                                path.unlink()
                        except:
                            pass

            all_terminated = all(state.completed or state.failed for state in transfer_states.values())

            if not all_terminated:
                time.sleep(POLL_INTERVAL)
            else:
                # All transfers either completed or failed
                break

        # Final summary
        successful = sum(1 for s in transfer_states.values() if s.completed)
        failed = sum(1 for s in transfer_states.values() if s.failed)
        if failed > 0:
            log(f"Transfers complete: {successful} successful, {failed} failed")
            for suffix, state in transfer_states.items():
                if state.failed:
                    log(f"  Transfer {suffix}: {len(state.remaining_files)} files failed")
        else:
            log("All transfers completed successfully")

    finally:
        # Stop the monitoring thread
        stop_event.set()
        if "monitor_thread" in locals():
            monitor_thread.join(timeout=1)

        # Clean up the LogManager (this will also close the log file)
        if copyem.logger.log_manager:
            try:
                copyem.logger.log_manager.cleanup()
            except Exception as e:
                print(f"Error during LogManager cleanup: {e}")
            finally:
                copyem.logger.log_manager = None

        # Clean up any remaining resources
        for state in transfer_states.values():
            for handle in state.file_handles:
                try:
                    sel.unregister(handle)
                    handle.close()
                except:
                    pass
            for path in state.paths_to_unlink:
                try:
                    if path.exists():
                        path.unlink()
                except:
                    pass

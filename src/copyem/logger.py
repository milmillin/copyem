"""Logging and terminal UI functionality for copyem."""

import sys
import threading
import queue
import re
from time import time
from typing import Dict, Optional, cast
import selectors
import io
from blessed import Terminal

from .utils import format_size, format_time


class LogManager:
    """Manages terminal UI with scrolling messages and fixed status lines."""

    def __init__(self, term: Terminal, num_status_lines: int, total_size: int = 0):
        self.term = term
        self.num_status_lines = num_status_lines
        self.total_size = total_size
        self.mbuffer_status = {}  # suffix -> status text
        self.transfer_metrics: Dict[str, dict] = {}  # suffix -> parsed metrics
        self.message_queue = queue.Queue()
        self.messages = []  # Keep track of scrolling messages
        self.lock = threading.Lock()
        self.progress_lines = 3  # Lines for stats + progress bar + separator
        self.start_time = time()

        # Check for terminal capabilities
        self.has_dim = False  # self._check_capability('dim')

        self.setup_display()

    def _check_capability(self, capability: str) -> bool:
        """Check if terminal supports a given capability."""
        try:
            # Try to access the capability
            getattr(self.term, capability)
            return True
        except:
            return False

    def setup_display(self):
        """Initialize the terminal display with scrolling region."""
        # Clear screen
        print(self.term.clear())

        # Set up scrolling region (leave bottom lines for status and progress)
        # CSR sets scrolling from line 0 to height - num_status_lines - progress_lines - 1
        if self.num_status_lines > 0:
            scroll_bottom = self.term.height - self.num_status_lines - self.progress_lines - 1
            sys.stdout.write(self.term.csr(0, scroll_bottom))
            sys.stdout.flush()

    def parse_mbuffer_status(self, text: str) -> Optional[dict]:
        """Parse mbuffer status line to extract metrics."""
        # Examples:
        # "in @ 14.0 MiB/s, out @ 24.0 MiB/s,  656 MiB total, buffer  99% full"
        # "in @  0.0 kiB/s, out @  0.0 kiB/s, 12.0 MiB total, buffer   0% full"
        # Note: units can be lowercase (kiB) or uppercase (MiB), and "iB" might be missing from total

        # More flexible pattern that handles various formats
        pattern = r"in\s+@\s+([\d.]+)\s+([kKmMgG]?)iB/s.*out\s+@\s+([\d.]+)\s+([kKmMgG]?)iB/s.*?([\d.]+)\s+([kKmMgG]?)iB\s+total.*buffer\s+(\d+)%"
        match = re.search(pattern, text, re.IGNORECASE)

        if match:
            in_rate, in_unit, out_rate, out_unit, total_val, total_unit, buffer_pct = match.groups()

            # Convert to bytes (handle both uppercase and lowercase units)
            units = {
                "": 1,
                "k": 1024, "K": 1024,
                "m": 1024**2, "M": 1024**2,
                "g": 1024**3, "G": 1024**3
            }

            in_unit_upper = in_unit.upper() if in_unit else ""
            out_unit_upper = out_unit.upper() if out_unit else ""
            total_unit_upper = total_unit.upper() if total_unit else ""

            in_rate_bytes = float(in_rate) * units.get(in_unit_upper, 1)
            out_rate_bytes = float(out_rate) * units.get(out_unit_upper, 1)
            total_bytes = float(total_val) * units.get(total_unit_upper, 1)

            return {
                "in_rate": in_rate_bytes,
                "out_rate": out_rate_bytes,
                "total_bytes": total_bytes,
                "buffer_pct": int(buffer_pct),
                "in_rate_str": f"{in_rate} {in_unit}iB/s" if in_unit else f"{in_rate} iB/s",
                "out_rate_str": f"{out_rate} {out_unit}iB/s" if out_unit else f"{out_rate} iB/s",
            }
        return None

    def update_mbuffer_status(self, suffix: str, text: str):
        """Update the mbuffer status line for a specific transfer."""
        with self.lock:
            self.mbuffer_status[suffix] = text.strip()

            # Parse and store metrics
            metrics = self.parse_mbuffer_status(text)
            if metrics:
                self.transfer_metrics[suffix] = metrics

            self._redraw_status_lines()

    def add_message(self, text: str):
        """Add a message to the scrolling area."""
        with self.lock:
            # Save cursor position
            sys.stdout.write(self.term.save)

            # Move to end of scrolling region and print
            scroll_bottom = self.term.height - self.num_status_lines - self.progress_lines - 1
            sys.stdout.write(self.term.move(scroll_bottom, 0))
            print(text)

            # Restore cursor position
            sys.stdout.write(self.term.restore)
            sys.stdout.flush()

    def _redraw_status_lines(self):
        """Redraw all status lines at the bottom."""
        # Save cursor position
        sys.stdout.write(self.term.save)

        # Draw separator line
        separator_pos = self.term.height - self.num_status_lines - self.progress_lines
        sys.stdout.write(self.term.move(separator_pos, 0))
        sys.stdout.write(self.term.clear_eol)
        # Use dim if available, otherwise just draw the line
        if self.has_dim:
            sys.stdout.write(self.term.dim + "─" * self.term.width + self.term.normal)
        else:
            sys.stdout.write("─" * self.term.width)

        # Draw each mbuffer status line
        sorted_suffixes = sorted(self.mbuffer_status.keys())
        for i, suffix in enumerate(sorted_suffixes[: self.num_status_lines]):
            if suffix in self.mbuffer_status:
                line_pos = self.term.height - self.num_status_lines - self.progress_lines + i + 1
                sys.stdout.write(self.term.move(line_pos, 0))
                sys.stdout.write(self.term.clear_eol)
                status = self.mbuffer_status[suffix]
                # Format status line with color
                sys.stdout.write(f"{self.term.cyan}[mbuffer-{suffix}]{self.term.normal} {status}")

        # Draw cumulative stats and progress bar
        self._draw_progress()

        # Restore cursor position
        sys.stdout.write(self.term.restore)
        sys.stdout.flush()

    def _draw_progress(self):
        """Draw the progress bar and cumulative statistics."""
        # Calculate cumulative metrics
        total_transferred = sum(m.get("total_bytes", 0) for m in self.transfer_metrics.values())
        total_in_rate = sum(m.get("in_rate", 0) for m in self.transfer_metrics.values())
        total_out_rate = sum(m.get("out_rate", 0) for m in self.transfer_metrics.values())

        # Calculate progress
        progress_pct = 0
        if self.total_size > 0:
            progress_pct = min(100, (total_transferred / self.total_size) * 100)

        # Calculate elapsed time
        elapsed = time() - self.start_time

        # Calculate average speed based on actual transfer
        avg_speed = total_transferred / elapsed if elapsed > 0 else 0

        # Calculate remaining time based on average speed
        remaining_bytes = max(0, self.total_size - total_transferred)
        eta = remaining_bytes / avg_speed if avg_speed > 0 else 0

        # Draw stats line
        stats_pos = self.term.height - 2
        sys.stdout.write(self.term.move(stats_pos, 0))
        sys.stdout.write(self.term.clear_eol)

        stats_str = (
            f"Curr: {format_size(int(total_out_rate))}/s | "
            f"Avg: {format_size(int(avg_speed))}/s | "
            f"Time: {format_time(elapsed)} | "
            f"ETA: {format_time(eta)}"
        )
        sys.stdout.write(self.term.bold + stats_str + self.term.normal)

        # Draw progress bar
        progress_pos = self.term.height - 1
        sys.stdout.write(self.term.move(progress_pos, 0))
        sys.stdout.write(self.term.clear_eol)

        # Calculate bar width
        bar_width = max(10, self.term.width - 35)  # Leave space for percentage and size info
        filled = int(bar_width * progress_pct / 100)
        empty = max(0, bar_width - filled)

        # Create the progress bar
        filled_bar = self.term.green("█" * filled) if filled > 0 else ""
        # Use dim if available for empty part, otherwise use regular
        if self.has_dim and empty > 0:
            empty_bar = self.term.dim("░" * empty)
        else:
            empty_bar = "░" * empty if empty > 0 else ""

        bar = filled_bar + empty_bar
        progress_info = f" {progress_pct:.1f}% ({format_size(int(total_transferred))} / {format_size(self.total_size)})"

        sys.stdout.write(f"[{bar}]{progress_info}")

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


def monitor_stderr(sel: selectors.BaseSelector, stop_event: threading.Event) -> None:
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
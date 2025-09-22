# copyem


A high-performance remote file transfer utility optimized for efficient bulk transfers over SSH. Copyem uses intelligent file scheduling, parallel transfers, and adaptive buffering to maximize throughput while minimizing transfer time.

## Features

- **Smart File Scheduling**: Optimizes file order to minimize latency and maximize throughput
- **Parallel Transfers**: Support for multiple concurrent transfer streams
- **Automatic Retry**: Failed transfers automatically retry with only remaining files
- **Progress Monitoring**: Real-time progress bars, transfer speeds, and ETA
- **Incremental Transfers**: Skips files that already exist with matching sizes on the remote
- **Adaptive Buffering**: Configurable buffer sizes with mbuffer for optimal performance
- **Comprehensive Logging**: Detailed transfer logs with timestamps for analysis

## Installation

### Using uv (recommended)

```bash
uv pip install copyem
```

### From source

```bash
git clone https://github.com/yourusername/copyem.git
cd copyem
uv pip install -e .
```

## Requirements

- Python 3.9+
- SSH access to remote host
- `tar`, `mbuffer`, and standard Unix utilities on both local and remote systems

## Usage

### Basic Usage

Transfer a directory to a remote host:

```bash
copyem /path/to/source user@remote.host /path/to/destination
```

### Advanced Options

```bash
copyem /path/to/source user@remote.host /path/to/destination \
  --parallel 4 \              # Use 4 parallel transfers
  --buffer-size 2G \          # 2GB buffer per transfer
  --speed 100M \              # Assume 100MB/s network speed
  --include "*.txt" \         # Only transfer .txt files
  --max-retries 5 \           # Retry failed transfers up to 5 times
  --retry-delay 5.0           # Wait 5 seconds between retries
```

### Command-line Arguments

#### Required Arguments
- `src_dir`: Source directory to copy
- `remote`: SSH remote (e.g., username@hostname.com)
- `dst_dir`: Target directory on remote

#### Optional Arguments
- `--include PATTERN`: Include files matching this pattern (e.g., '*.txt', '**/*.py')
- `-s, --speed SIZE`: Assumed network speed for scheduling (default: 20M)
- `-l, --latency SECONDS`: File loading latency assumption (default: 0.05)
- `-b, --buffer-size SIZE`: Buffer size for transfers (default: 1G)
- `-p, --parallel N`: Number of parallel transfers (default: 1)
- `-r, --max-retries N`: Maximum retry attempts for failed transfers (default: 3)
- `--retry-delay SECONDS`: Delay between retry attempts (default: 2.0)
- `--poll-interval SECONDS`: Polling interval for monitoring (default: 0.5)

### Size Format

Size arguments support the following units:
- `B`: Bytes
- `K/KB`: Kilobytes (1024 bytes)
- `M/MB`: Megabytes (1024^2 bytes)
- `G/GB`: Gigabytes (1024^3 bytes)
- `T/TB`: Terabytes (1024^4 bytes)

Examples: `100M`, `1.5G`, `512K`

## How It Works

### 1. File Discovery and Analysis
Copyem first scans the source directory to identify all files that need to be transferred. It then queries the remote system to check which files already exist and have matching sizes, avoiding unnecessary transfers.

### 2. Intelligent Scheduling
Files are distributed across parallel transfer streams using an optimized scheduling algorithm that:
- Minimizes overall transfer time
- Accounts for file size and network latency
- Balances load across parallel streams

### 3. Transfer Pipeline
Each transfer uses a pipeline architecture:
```
tar (create archive) | mbuffer (buffering) | ssh (network) | tar (extract)
```

This approach:
- Reduces per-file overhead
- Enables efficient buffering
- Maintains file permissions and metadata

### 4. Progress Monitoring
The terminal UI provides real-time feedback:
- Individual transfer speeds and buffer status
- Overall progress bar with percentage complete
- Current and average transfer speeds
- Estimated time remaining

### 5. Error Recovery
If a transfer fails:
- SSH output is analyzed to identify successfully transferred files
- Only remaining files are retried
- Progress is preserved across retries
- Failed transfers are reported in the final summary

## Terminal UI

During transfer, copyem displays:

```
[Starting transfer messages and file discovery...]

[mbuffer-1] in @ 38.0 MiB/s, out @ 38.0 MiB/s, 980 MiB total, buffer 82% full
[mbuffer-2] in @ 38.0 MiB/s, out @ 38.0 MiB/s, 980 MiB total, buffer 82% full
Curr: 83.0 MB/s | Avg: 78.5 MB/s | Time: 00m45s | ETA: 01m23s
[######------------] 45.2% (2.3GB/5.1GB)
```

## Transfer Summary

After completion, a detailed summary is displayed:

```
============================================================
TRANSFER SUMMARY
============================================================

Transfer Statistics:
  Total time: 02m15s
  Files transferred: 1523/1523
  Data transferred: 5.12 GB (5,497,558,528 bytes)
  Effective speed: 38.52 MB/s

Transfer Status:
  Successful transfers: 4/4
  Failed transfers: 0/4

============================================================
ALL TRANSFERS COMPLETED SUCCESSFULLY
============================================================
```

## Logging

Copyem creates detailed log files (`copyem_YYYYMMDD_HHMMSS.log`) containing:
- Timestamps for each file transfer
- Transfer stream identifiers
- File paths

These logs can be analyzed using the included visualization script in `metrics/visualize_latency.py`.

## Performance Tips

1. **Buffer Size**: Larger buffers (1-4GB) generally improve performance for fast networks
2. **Parallel Transfers**: Use 2-8 parallel transfers for optimal throughput
3. **Network Speed**: Set `--speed` to slightly below your actual network capacity
4. **File Patterns**: Use `--include` to filter files and reduce scanning time

## Architecture

### Core Components

- **`__init__.py`**: Main entry point, command-line interface, and transfer orchestration
- **`core.py`**: File discovery, scheduling algorithm, and transfer pipeline setup
- **`logger.py`**: Terminal UI management, progress tracking, and logging
- **`utils.py`**: Utility functions for size parsing and formatting

### Key Features

- **Retry Logic**: Intelligent retry mechanism that tracks completed files via SSH output
- **Progress Persistence**: Transfer progress is maintained even when retrying failed transfers
- **Resource Management**: Proper cleanup of processes, file handles, and temporary files
- **Thread Safety**: Concurrent operations are properly synchronized

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/copyem.git
cd copyem

# Install with development dependencies
uv pip install -e ".[dev]"
```

### Running Tests

```bash
# Run the transfer with verbose output
copyem /test/source user@host /test/dest --parallel 2
```

### Analyzing Transfer Metrics

The `metrics/visualize_latency.py` script can analyze log files to visualize:
- Inter-arrival times between files
- Transfer latencies between parallel streams
- Performance bottlenecks

## License

[Add your license information here]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Milin Kodnongbua <mil.millin@hotmail.com>

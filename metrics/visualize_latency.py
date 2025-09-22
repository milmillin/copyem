# %%
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# lines = Path("copyem_20250922_070637.log").read_text().splitlines()
lines = Path("copyem_20250922_073411.log").read_text().splitlines()

items: list[tuple[int, str, str]] = []
for l in lines:
    timestamp, suffix, filename = l.split("\t")
    items.append((int(timestamp), suffix, filename))

# Group by suffix for inter-arrival times
by_suffix = defaultdict(list)
for timestamp, suffix, filename in items:
    by_suffix[suffix].append((timestamp, filename))

# Group by filename to find matching suffixes
by_filename = defaultdict(list)
for timestamp, suffix, filename in items:
    by_filename[filename].append((timestamp, suffix))

# Calculate inter-arrival times for each suffix
inter_arrival_times = {}
for suffix, entries in by_suffix.items():
    # Sort by timestamp
    entries.sort(key=lambda x: x[0])
    timestamps = [t for t, _ in entries]

    # Calculate time differences between consecutive files
    if len(timestamps) > 1:
        diffs = np.diff(timestamps)
        inter_arrival_times[suffix] = diffs

# Plot histogram of inter-arrival times for each suffix
nrows = (len(inter_arrival_times) + 1) // 2
fig, axes = plt.subplots(nrows, 2, figsize=(6, 3 * nrows))
axes = axes.flatten()

for idx, (suffix, times) in enumerate(inter_arrival_times.items()):
    ax = axes[idx]
    ax.hist(times, bins=50, edgecolor='black', alpha=0.7)
    ax.set_xlabel('Inter-arrival time (ms)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Inter-arrival times for {suffix}')
    ax.grid(True, alpha=0.3)

    # Add statistics
    mean_time = np.mean(times)
    median_time = np.median(times)
    ax.axvline(mean_time, color='red', linestyle='--', label=f'Mean: {mean_time:.1f}ms')
    ax.axvline(median_time, color='green', linestyle='--', label=f'Median: {median_time:.1f}ms')
    ax.legend()

plt.suptitle('Distribution of Inter-arrival Times by Suffix')
plt.tight_layout()
plt.savefig('inter_arrival_times.png', dpi=150)
plt.show()

# Calculate suffix-to-suffix latencies for files with multiple suffixes
suffix_latencies = []
suffix_pairs = []

for filename, entries in by_filename.items():
    if len(entries) >= 2:
        # Sort by timestamp
        entries.sort(key=lambda x: x[0])

        # Calculate time difference between first and second suffix
        for i in range(len(entries) - 1):
            t1, s1 = entries[i]
            t2, s2 = entries[i + 1]
            latency = t2 - t1
            suffix_latencies.append(latency)
            suffix_pairs.append(f"{s1} → {s2}")

# Create histogram for suffix-to-suffix latencies
if suffix_latencies:
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot histogram
    ax.hist(suffix_latencies, bins=50, edgecolor='black', alpha=0.7, color='blue')
    ax.set_xlabel('Latency (ms)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Suffix-to-Suffix Latencies')
    ax.grid(True, alpha=0.3)

    # Add statistics
    mean_latency = np.mean(suffix_latencies)
    median_latency = np.median(suffix_latencies)
    min_latency = np.min(suffix_latencies)
    max_latency = np.max(suffix_latencies)

    ax.axvline(mean_latency, color='red', linestyle='--', label=f'Mean: {mean_latency:.1f}ms')
    ax.axvline(median_latency, color='green', linestyle='--', label=f'Median: {median_latency:.1f}ms')

    # Add text box with statistics
    stats_text = f'Count: {len(suffix_latencies)}\n'
    stats_text += f'Min: {min_latency:.1f}ms\n'
    stats_text += f'Max: {max_latency:.1f}ms\n'
    stats_text += f'Mean: {mean_latency:.1f}ms\n'
    stats_text += f'Median: {median_latency:.1f}ms'

    ax.text(0.98, 0.98, stats_text, transform=ax.transAxes,
            fontsize=10, verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    ax.legend()

    plt.tight_layout()
    plt.savefig('suffix_latencies.png', dpi=150)
    plt.show()

    # Analyze suffix pair patterns
    from collections import Counter
    pair_counts = Counter(suffix_pairs)
    print("\nSuffix pair counts:")
    for pair, count in pair_counts.most_common():
        print(f"  {pair}: {count} occurrences")
else:
    print("No files found with multiple suffixes")


# %%

import sqlite3
from pathlib import Path
import csv
import statistics
import matplotlib.pyplot as plt
import numpy as np

# File paths
DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "requests.db"
CSV_RAW = DATA_DIR / "requests_raw.csv"
CSV_AGG = DATA_DIR / "requests_agg.csv"
CSV_AGG_FILE = DATA_DIR / "requests_agg_file_request.csv"


# CSV Export Functions
def export_raw() -> None:
    """
    Export all request records from the database to a raw CSV file.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, algorithm, server_name, endpoint,
                   start_time, end_time, total_time,
                   success, client_ip, created_at
            FROM requests
            ORDER BY id
            """
        )
        rows = cursor.fetchall()

    headers = [
        "id", "algorithm", "server_name", "endpoint",
        "start_time", "end_time", "total_time",
        "success", "client_ip", "created_at"
    ]

    with open(CSV_RAW, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Saved raw data to {CSV_RAW}")


def export_aggregated(filter_clause: str, output_csv: Path) -> None:
    """
    Aggregate successful requests and save statistics to a CSV file.

    Parameters:
        filter_clause (str): SQL WHERE filter (e.g., "AND endpoint='/file-request'")
        output_csv (Path): CSV file path to write the results
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT algorithm, endpoint, total_time
            FROM requests
            WHERE success = 1
            {filter_clause}
            """
        )
        rows = cursor.fetchall()

    groups = {}
    for algo, endpoint, total_time in rows:
        groups.setdefault((algo, endpoint), []).append(total_time)

    headers = ["algorithm", "endpoint", "count", "avg_total_time",
               "min_total_time", "max_total_time", "stdev_total_time"]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for (algo, endpoint), times in groups.items():
            count = len(times)
            avg_time = sum(times) / count if count else 0.0
            min_time = min(times) if times else 0.0
            max_time = max(times) if times else 0.0
            stdev_time = statistics.stdev(times) if count >= 2 else 0.0
            writer.writerow([algo, endpoint, count, avg_time, min_time, max_time, stdev_time])

    print(f"Saved aggregated stats to {output_csv}")


def export_aggregated_all() -> None:
    """Aggregate all successful requests (all endpoints)."""
    export_aggregated("", CSV_AGG)


def export_aggregated_file_only() -> None:
    """Aggregate only /file-request endpoint."""
    export_aggregated("AND endpoint='/file-request'", CSV_AGG_FILE)


# Plotting Functions
def plot_avg_time(csv_path: Path, title: str, output_file: Path, error_bars: bool = False) -> None:
    """
    Create a bar chart for average request times.

    Parameters:
        csv_path (Path): CSV file with aggregated stats
        title (str): Chart title
        output_file (Path): Output PNG path
        error_bars (bool): Whether to plot standard deviation error bars
    """
    if not csv_path.exists():
        print(f"No CSV found at {csv_path}, skipping plot.")
        return

    algorithms, endpoints, avg_times, stdevs = [], [], [], []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            algorithms.append(row["algorithm"])
            endpoints.append(row["endpoint"])
            avg_times.append(float(row["avg_total_time"]))
            if error_bars:
                stdevs.append(float(row["stdev_total_time"]))

    unique_algos = sorted(set(algorithms))
    endpoint_types = sorted(set(endpoints))
    x = np.arange(len(unique_algos))
    width = 0.35 if len(endpoint_types) == 2 else 0.6

    fig, ax = plt.subplots(figsize=(10, 6) if not error_bars else (8, 5))

    if error_bars:
        ax.bar(x, avg_times, width, yerr=stdevs, capsize=5)
    else:
        for i, ep in enumerate(endpoint_types):
            ys = [next((t for a, e, t in zip(algorithms, endpoints, avg_times) if a == algo and e == ep), 0.0)
                  for algo in unique_algos]
            ax.bar(x + i * width, ys, width, label=ep)
        ax.set_xticks(x + width * (len(endpoint_types) - 1) / 2)
        ax.set_xticklabels(unique_algos)
        ax.legend()

    ax.set_ylabel("Average total_time (s)")
    ax.set_title(title)
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    plt.savefig(output_file, dpi=200)
    plt.close(fig)
    print(f"Saved plot {output_file}")


if __name__ == "__main__":
    # Export CSVs
    export_raw()
    export_aggregated_all()
    export_aggregated_file_only()

    # Generate plots
    plot_avg_time(CSV_AGG, "Average response time by algorithm and endpoint", DATA_DIR / "chart_avg_time_all.png")
    plot_avg_time(CSV_AGG_FILE, "Average conversion time for /file-request by algorithm",
                  DATA_DIR / "chart_avg_time_file_request.png", error_bars=True)

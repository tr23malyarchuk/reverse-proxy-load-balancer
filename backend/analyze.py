import sqlite3
from pathlib import Path
import csv
import statistics

import matplotlib.pyplot as plt
import numpy as np

DB_PATH = Path(__file__).parent / "requests.db"
CSV_RAW_PATH = Path(__file__).parent / "requests_raw.csv"
CSV_AGG_PATH = Path(__file__).parent / "requests_agg.csv"
CSV_AGG_FILE_ONLY_PATH = Path(__file__).parent / "requests_agg_file_request.csv"
CSV_ERRORS_PATH = Path(__file__).parent / "requests_errors.csv"


def export_raw():
    """
    Выгружает все записи из таблицы requests в CSV (сырой лог).
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            algorithm,
            server_name,
            endpoint,
            start_time,
            end_time,
            total_time,
            success,
            client_ip,
            created_at
        FROM requests
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    conn.close()

    headers = [
        "id",
        "algorithm",
        "server_name",
        "endpoint",
        "start_time",
        "end_time",
        "total_time",
        "success",
        "client_ip",
        "created_at",
    ]

    with open(CSV_RAW_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Saved raw data to {CSV_RAW_PATH}")


def _export_aggregated(where_clause: str, csv_path: Path):
    """
    Агрегирует по (algorithm, endpoint) с фильтром WHERE
    и сохраняет статистику по времени в указанный CSV.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    query = f"""
        SELECT
            algorithm,
            endpoint,
            total_time
        FROM requests
        WHERE success = 1
        {where_clause}
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    groups = {}  # (algorithm, endpoint) -> [total_time, ...]
    for algo, endpoint, total_time in rows:
        key = (algo, endpoint)
        groups.setdefault(key, []).append(total_time)

    headers = [
        "algorithm",
        "endpoint",
        "count",
        "avg_total_time",
        "min_total_time",
        "max_total_time",
        "stdev_total_time",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for (algo, endpoint), times in groups.items():
            count = len(times)
            avg_t = sum(times) / count if count > 0 else 0.0
            min_t = min(times) if times else 0.0
            max_t = max(times) if times else 0.0
            stdev_t = statistics.stdev(times) if count >= 2 else 0.0

            writer.writerow(
                [
                    algo,
                    endpoint,
                    count,
                    avg_t,
                    min_t,
                    max_t,
                    stdev_t,
                ]
            )

    print(f"Saved aggregated stats to {csv_path}")


def export_aggregated_all():
    """
    Агрегация по всем успешным запросам (и /request, и /file-request).
    """
    _export_aggregated(where_clause="", csv_path=CSV_AGG_PATH)


def export_aggregated_file_only():
    """
    Агрегация только для файловых запросов (/file-request).
    """
    _export_aggregated(
        where_clause="AND endpoint = '/file-request'",
        csv_path=CSV_AGG_FILE_ONLY_PATH,
    )


def export_errors():
    """
    Считает количество успешных и неуспешных запросов по (algorithm, endpoint),
    сохраняет в CSV для анализа надёжности.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            algorithm,
            endpoint,
            success,
            COUNT(*) as cnt
        FROM requests
        GROUP BY algorithm, endpoint, success
        """
    )
    rows = cur.fetchall()
    conn.close()

    # (algorithm, endpoint) -> {"success": n, "error": m}
    stats = {}
    for algo, endpoint, success, cnt in rows:
        key = (algo, endpoint)
        d = stats.setdefault(key, {"success": 0, "error": 0})
        if success == 1:
            d["success"] += cnt
        else:
            d["error"] += cnt

    headers = [
        "algorithm",
        "endpoint",
        "success_count",
        "error_count",
        "total",
        "error_rate",
    ]

    with open(CSV_ERRORS_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for (algo, endpoint), d in stats.items():
            success_count = d["success"]
            error_count = d["error"]
            total = success_count + error_count
            error_rate = error_count / total if total > 0 else 0.0
            writer.writerow(
                [
                    algo,
                    endpoint,
                    success_count,
                    error_count,
                    total,
                    error_rate,
                ]
            )

    print(f"Saved error stats to {CSV_ERRORS_PATH}")


# ---------- Построение графиков ----------


def plot_avg_time_all():
    """
    Строит столбиковый график среднего времени обработки
    по алгоритмам и endpoint`ам на основе requests_agg.csv.
    """
    if not CSV_AGG_PATH.exists():
        print("No aggregated CSV for all endpoints, skip plot_avg_time_all")
        return

    algorithms = []
    endpoints = []
    avg_times = []

    with open(CSV_AGG_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            algorithms.append(row["algorithm"])
            endpoints.append(row["endpoint"])
            avg_times.append(float(row["avg_total_time"]))

    # Удобно разделить на две группы по endpoint
    unique_algos = sorted(set(algorithms))
    endpoints_types = sorted(set(endpoints))

    # строим по одному бару на комбинацию (algo, endpoint)
    x = np.arange(len(unique_algos))
    width = 0.35 if len(endpoints_types) == 2 else 0.2

    fig, ax = plt.subplots(figsize=(10, 6))

    for i, ep in enumerate(endpoints_types):
        ys = []
        for algo in unique_algos:
            val = 0.0
            for a, e, t in zip(algorithms, endpoints, avg_times):
                if a == algo and e == ep:
                    val = t
                    break
            ys.append(val)

        ax.bar(
            x + i * width,
            ys,
            width,
            label=ep,
        )

    ax.set_xticks(x + width * (len(endpoints_types) - 1) / 2)
    ax.set_xticklabels(unique_algos)
    ax.set_ylabel("Average total_time, s")
    ax.set_title("Average response time by algorithm and endpoint")
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    out_path = Path(__file__).parent / "chart_avg_time_all.png"
    plt.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Saved plot {out_path}")


def plot_avg_time_file_request():
    """
    Строит столбиковый график среднего времени конвертации
    по алгоритмам только для /file-request.
    """
    if not CSV_AGG_FILE_ONLY_PATH.exists():
        print("No aggregated CSV for file-request, skip plot_avg_time_file_request")
        return

    algorithms = []
    avg_times = []
    stdevs = []

    with open(CSV_AGG_FILE_ONLY_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            algorithms.append(row["algorithm"])
            avg_times.append(float(row["avg_total_time"]))
            stdevs.append(float(row["stdev_total_time"]))

    x = np.arange(len(algorithms))
    width = 0.6

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x, avg_times, width, yerr=stdevs, capsize=5)

    ax.set_xticks(x)
    ax.set_xticklabels(algorithms)
    ax.set_ylabel("Average total_time, s")
    ax.set_title("Average conversion time for /file-request by algorithm")
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    out_path = Path(__file__).parent / "chart_avg_time_file_request.png"
    plt.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Saved plot {out_path}")


def plot_error_rate():
    """
    Строит столбиковый график доли ошибок по алгоритмам
    (агрегация по всем endpoint`ам).
    """
    if not CSV_ERRORS_PATH.exists():
        print("No errors CSV, skip plot_error_rate")
        return

    # Сначала агрегируем по алгоритму (суммируем по endpoint`ам)
    agg = {}  # algo -> {"success": n, "error": m}
    with open(CSV_ERRORS_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            algo = row["algorithm"]
            success_count = int(row["success_count"])
            error_count = int(row["error_count"])
            d = agg.setdefault(algo, {"success": 0, "error": 0})
            d["success"] += success_count
            d["error"] += error_count

    algorithms = sorted(agg.keys())
    error_rates = []
    for algo in algorithms:
        d = agg[algo]
        total = d["success"] + d["error"]
        rate = d["error"] / total if total > 0 else 0.0
        error_rates.append(rate)

    x = np.arange(len(algorithms))
    width = 0.6

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x, error_rates, width, color="tab:red")

    ax.set_xticks(x)
    ax.set_xticklabels(algorithms)
    ax.set_ylabel("Error rate")
    ax.set_title("Error rate by algorithm (all endpoints)")
    ax.set_ylim(0, 1)
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    plt.tight_layout()
    out_path = Path(__file__).parent / "chart_error_rate.png"
    plt.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Saved plot {out_path}")


if __name__ == "__main__":
    # Экспорт CSV
    export_raw()
    export_aggregated_all()
    export_aggregated_file_only()
    export_errors()

    # Построение графиков
    plot_avg_time_all()
    plot_avg_time_file_request()
    plot_error_rate()


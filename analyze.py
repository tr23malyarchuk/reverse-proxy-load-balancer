import sqlite3
from pathlib import Path
import csv
import statistics

DB_PATH = Path(__file__).parent / "requests.db"
CSV_RAW_PATH = Path(__file__).parent / "requests_raw.csv"
CSV_AGG_PATH = Path(__file__).parent / "requests_agg.csv"
CSV_AGG_FILE_ONLY_PATH = Path(__file__).parent / "requests_agg_file_request.csv"


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
    Внутренняя функция: агрегирует по (algorithm, endpoint) с фильтром WHERE
    и сохраняет в указанный CSV.
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
    Агрегация только для файловых запросов (/file-request),
    удобно для графиков "латентність конвертації" по алгоритмам.
    """
    _export_aggregated(
        where_clause="AND endpoint = '/file-request'",
        csv_path=CSV_AGG_FILE_ONLY_PATH,
    )


if __name__ == "__main__":
    export_raw()
    export_aggregated_all()
    export_aggregated_file_only()


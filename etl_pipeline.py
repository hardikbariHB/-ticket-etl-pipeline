"""Daily ETL: extract one day's ticket CSV -> validate -> load into SQLite -> write a data-quality report.
Runs standalone (python etl_pipeline.py 2025-01-01) or from the Airflow DAG in dag.py."""
import csv, os, sqlite3, sys, datetime as dt

BASE = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR, REJECT_DIR = os.path.join(BASE, "data", "raw"), os.path.join(BASE, "data", "rejects")
DB = os.path.join(os.path.dirname(__file__), "warehouse.db")
FIELDS = ["ticket_id", "opened_at", "resolved_at", "priority", "category", "assignment_group", "reopen_count"]
VALID_PRIORITY = {"P1", "P2", "P3", "P4"}

def extract(run_date):
    path = os.path.join(RAW_DIR, f"tickets_{run_date}.csv")
    with open(path, newline="") as f:
        return list(csv.DictReader(f))

def validate(rows):
    """Returns (good_rows, rejects). Each reject carries the reason it failed."""
    good, rejects, seen = [], [], set()
    for r in rows:
        reason = None
        if not r["ticket_id"]: reason = "missing_ticket_id"
        elif r["ticket_id"] in seen: reason = "duplicate_ticket_id"
        elif r["priority"] not in VALID_PRIORITY: reason = "invalid_priority"
        elif not r["category"]: reason = "missing_category"
        elif r["resolved_at"] and r["resolved_at"] < r["opened_at"]: reason = "resolved_before_opened"
        if reason: rejects.append({**r, "reject_reason": reason})
        else: seen.add(r["ticket_id"]); good.append(r)
    return good, rejects

def init_db(db):
    db.execute("""CREATE TABLE IF NOT EXISTS tickets(ticket_id TEXT PRIMARY KEY, opened_at TEXT, resolved_at TEXT,
                  priority TEXT, category TEXT, assignment_group TEXT, reopen_count INTEGER, load_date TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS quality_report(run_date TEXT PRIMARY KEY, rows_in INTEGER, rows_loaded INTEGER,
                  rows_rejected INTEGER, reject_pct REAL, top_reject_reason TEXT, status TEXT)""")

def load(good, run_date):
    db = sqlite3.connect(DB); init_db(db)
    # idempotent: re-running a day replaces that day's rows instead of duplicating them
    db.executemany("INSERT OR REPLACE INTO tickets VALUES (?,?,?,?,?,?,?,?)",
                   [tuple(r[c] for c in FIELDS) + (run_date,) for r in good])
    db.commit(); db.close()

def report(rows_in, good, rejects, run_date, max_reject_pct=10.0):
    os.makedirs(REJECT_DIR, exist_ok=True)
    if rejects:
        with open(os.path.join(REJECT_DIR, f"rejects_{run_date}.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS + ["reject_reason"]); w.writeheader(); w.writerows(rejects)
    pct = round(100 * len(rejects) / rows_in, 2) if rows_in else 0
    reasons = {}
    for r in rejects: reasons[r["reject_reason"]] = reasons.get(r["reject_reason"], 0) + 1
    top = max(reasons, key=reasons.get) if reasons else ""
    status = "FAIL" if pct > max_reject_pct else "OK"
    db = sqlite3.connect(DB); init_db(db)
    db.execute("INSERT OR REPLACE INTO quality_report VALUES (?,?,?,?,?,?,?)",
               (run_date, rows_in, len(good), len(rejects), pct, top, status))
    db.commit(); db.close()
    return status, pct

def run(run_date):
    rows = extract(run_date); good, rejects = validate(rows); load(good, run_date)
    status, pct = report(len(rows), good, rejects, run_date)
    print(f"{run_date}: in={len(rows)} loaded={len(good)} rejected={len(rejects)} ({pct}%) status={status}")
    if status == "FAIL": raise ValueError(f"Reject rate {pct}% above threshold on {run_date}")

if __name__ == "__main__":
    if len(sys.argv) > 1: run(sys.argv[1])
    else:  # backfill every available day
        for fn in sorted(os.listdir(RAW_DIR)):
            try: run(fn[8:18])
            except ValueError as e: print("WARNING:", e)

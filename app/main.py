"""
Lab 2 — MongoDB Replica Set (Student Edition)
----------------------------------------------
This tiny client does 4 things:
1) Seed one simple document (user_profiles).
2) Measure write/read latency under different concerns.
3) Show "strong consistency" (majority write + majority read on primary).
4) Show "eventual consistency" (w=1 write + read from secondary that becomes fresh later).

TIP: Run this after `docker compose up -d` and verifying PRIMARY/SECONDARY.
"""

import time
import csv
from pathlib import Path
from datetime import datetime, timezone
from pymongo import MongoClient, ReadPreference
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern


# Connect to ALL three hosts with the replicaSet name.
URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
OUT_DIR = Path("results")
OUT_DIR.mkdir(exist_ok=True)

def get_coll(wc="majority", rc="majority", rp="primary"):
    """
    Return a collection handle with the requested write concern (wc),
    read concern (rc), and read preference (rp).
    - wc: 1 | "majority" | "w3" (we'll treat "w3" as all members here)
    - rc: "local" | "majority"
    - rp: "primary" | "secondary" | "nearest"
    """
    client = MongoClient(URI, serverSelectionTimeoutMS=5000)
    db = client["lab2"]
    rp_map = {
        "primary": ReadPreference.PRIMARY,
        "secondary": ReadPreference.SECONDARY,
        "nearest": ReadPreference.NEAREST
    }
    # Map wc "w3" to integer 3 (all members)
    wc_value = 3 if wc == "w3" else wc
    return db.get_collection(
        "user_profiles",
        write_concern=WriteConcern(w=wc_value),
        read_concern=ReadConcern(rc),
        read_preference=rp_map.get(rp, ReadPreference.PRIMARY)
    )

def seed_once():
    """Create one sample user profile so we always have a target document."""
    c = get_coll()
    c.delete_many({})
    c.insert_one({
        "user_id": "u123",
        "username": "saniya",
        "email": "saniya@example.com",
        "last_login_time": datetime.now(timezone.utc).isoformat()
    })
    print("✅ Seeded 1 user_profile.")

def time_write(wc, repeats=5):
    """
    Time the update write under a given write concern.
    Returns a list of measured latencies (ms).
    """
    c = get_coll(wc=wc, rc="local", rp="primary")
    latencies = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        c.update_one(
            {"user_id": "u123"},
            {"$set": {"last_login_time": datetime.now(timezone.utc).isoformat()}},
            upsert=True
        )
        dt_ms = (time.perf_counter() - t0) * 1000
        latencies.append(dt_ms)
    print(f"📝 writeConcern={wc}: {sum(latencies)/len(latencies):.2f} ms (avg over {repeats})")
    return latencies

def time_read(rc, rp, repeats=5):
    """
    Time the read under a given read concern + read preference.
    Returns (latencies_ms, last_value) so we can also see the value observed.
    """
    c = get_coll(wc="majority", rc=rc, rp=rp)
    latencies = []
    last_val = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        doc = c.find_one({"user_id": "u123"})
        dt_ms = (time.perf_counter() - t0) * 1000
        latencies.append(dt_ms)
        last_val = doc["last_login_time"] if doc else None
    print(f"🔎 readConcern={rc} readPref={rp}: {sum(latencies)/len(latencies):.2f} ms (avg over {repeats}) | sample={last_val}")
    return latencies, last_val

def strong_consistency_demo():
    """
    "Strong" in Mongo terms: write with majority and read with majority from primary.
    Expectation: immediately read back exactly what we wrote.
    """
    print("\n== Strong Consistency Demo ==")
    ts = datetime.now(timezone.utc).isoformat()
    get_coll(wc="majority", rc="majority", rp="primary").update_one(
        {"user_id": "u123"}, {"$set": {"last_login_time": ts}}, upsert=True
    )
    seen = get_coll(wc="majority", rc="majority", rp="primary") \
        .find_one({"user_id": "u123"})["last_login_time"]
    print(f"✅ Wrote {ts} and immediately read {seen} (should match).")

def eventual_consistency_demo(max_polls=25, sleep_s=0.2):
    """
    Eventual consistency: write with w=1, then read from secondary immediately -> may be stale.
    Poll until the secondary converges (replication catches up).
    """
    print("\n== Eventual Consistency Demo ==")
    ts = datetime.now(timezone.utc).isoformat()
    get_coll(wc=1, rc="local", rp="primary").update_one(
        {"user_id": "u123"}, {"$set": {"last_login_time": ts}}, upsert=True
    )
    print(f"✍️  Wrote {ts} with w=1 (fast; secondaries might not have it yet).")
    r = get_coll(wc="majority", rc="local", rp="secondary")
    first = r.find_one({"user_id": "u123"})["last_login_time"]
    print(f"👀 Immediate secondary read -> {first} (may be old)")

    print("⏳ Polling until secondary catches up...")
    for i in range(max_polls):
        cur = r.find_one({"user_id": "u123"})["last_login_time"]
        if cur == ts:
            print(f"✅ Secondary converged after ~{i} polls.")
            return
        time.sleep(sleep_s)
    print("⚠️ Secondary did not converge within the polling window (increase max_polls?)")

def save_csv(row_dict, csv_path):
    """
    Append a single result row to CSV (creates header on first write).
    """
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row_dict.keys()))
            writer.writeheader()
            writer.writerow(row_dict)
    else:
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row_dict.keys()))
            writer.writerow(row_dict)

def run_all_and_log():
    """
    Run the full experiment suite and store summarized results in CSVs for the report.
    """
    seed_once()

    # 1) Write concerns
    for wc in (1, "majority", "w3"):
        lats = time_write(wc)
        save_csv(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "metric": "write_avg_ms",
                "wc": wc,
                "value": round(sum(lats)/len(lats), 2),
                "n": len(lats)
            },
            OUT_DIR / "writes.csv"
        )

    # 2) Read concerns + preferences
    read_cases = [
        ("local", "primary"),
        ("majority", "primary"),
        ("local", "secondary"),
    ]
    for rc, rp in read_cases:
        lats, sample = time_read(rc, rp)
        save_csv(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "metric": "read_avg_ms",
                "rc": rc,
                "rp": rp,
                "value": round(sum(lats)/len(lats), 2),
                "n": len(lats),
                "sample_value": sample
            },
            OUT_DIR / "reads.csv"
        )

    # 3) Consistency demos
    strong_consistency_demo()
    eventual_consistency_demo()

if __name__ == "__main__":
    run_all_and_log()

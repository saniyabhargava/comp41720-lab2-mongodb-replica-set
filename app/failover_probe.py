import time
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import PyMongoError

URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
c = MongoClient(URI).lab2.user_profiles

print("Starting write probe (Ctrl+C to stop). Now stop the primary in another terminal: docker stop mongo1")
while True:
    try:
        t = datetime.now(timezone.utc).isoformat()
        c.update_one({"user_id":"u123"}, {"$set":{"probe_ts": t}}, upsert=True)
        print("write OK", t)
    except PyMongoError as e:
        print("write FAILED:", e)
    time.sleep(0.2)

"""Extend Jito MEV extraction to cover epochs 396-552 (incremental)."""
import csv, json, os, sys, time, urllib.request, urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DATA_RAW, CSV_DELIMITER, CSV_ENCODING

LAMPORTS = 1_000_000_000
LOG = "_extend_jito.log"

def log(msg):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

with open(LOG, "w", encoding="utf-8") as f:
    f.write("")

# Load existing
JITO_COLUMNS = ["epoch", "jito_total_mev_lamports", "jito_total_mev_sol",
                "jito_stake_weight_lamports", "jito_mev_reward_per_lamport"]
jito_path = DATA_RAW / "jito_mev_rewards.csv"
existing = []
existing_epochs = set()
if jito_path.exists():
    with open(jito_path, "r", encoding=CSV_ENCODING) as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        existing = list(reader)
    existing_epochs = {int(r["epoch"]) for r in existing}
log(f"Existing: {len(existing)} rows, epochs {min(existing_epochs)}-{max(existing_epochs)}")

# Epochs to fetch: 396-552 not already in file
to_fetch = sorted([e for e in range(396, 553) if e not in existing_epochs])
log(f"To fetch: {len(to_fetch)} epochs (396-552)")

t0 = time.time()
new_rows = []
errors = []

for i, epoch in enumerate(to_fetch):
    try:
        body = json.dumps({"epoch": epoch}).encode("utf-8")
        req = urllib.request.Request(
            "https://kobe.mainnet.jito.network/api/v1/mev_rewards",
            data=body, headers={"Accept":"application/json","Content-Type":"application/json","User-Agent":"Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        if data and data.get("epoch") == epoch:
            total_mev = int(data.get("total_network_mev_lamports", 0))
            new_rows.append({
                "epoch": epoch,
                "jito_total_mev_lamports": total_mev,
                "jito_total_mev_sol": round(total_mev / LAMPORTS, 6),
                "jito_stake_weight_lamports": data.get("jito_stake_weight_lamports", ""),
                "jito_mev_reward_per_lamport": data.get("mev_reward_per_lamport", ""),
            })
        else:
            errors.append(epoch)
    except Exception:
        errors.append(epoch)

    if (i+1) % 25 == 0 or (i+1) == len(to_fetch):
        log(f"  [{i+1}/{len(to_fetch)}] +{len(new_rows)} new, {len(errors)} errors ({time.time()-t0:.0f}s)")
    time.sleep(0.25)

# Merge and save
all_rows = existing + new_rows
all_rows.sort(key=lambda r: int(r["epoch"]))

with open(jito_path, "w", encoding=CSV_ENCODING, newline="") as f:
    writer = csv.DictWriter(f, fieldnames=JITO_COLUMNS, delimiter=CSV_DELIMITER)
    writer.writeheader()
    for row in all_rows:
        writer.writerow(row)

log(f"\nTotal: {len(all_rows)} rows (was {len(existing)}, +{len(new_rows)} new)")
log(f"Errors: {len(errors)} — {errors[:10] if errors else 'none'}")
log(f"Time: {time.time()-t0:.0f}s")
log("FINISHED")

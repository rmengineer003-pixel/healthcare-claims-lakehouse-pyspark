"""Generate synthetic healthcare data. Every value is random; no real member, provider, or claim data."""
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

PLANS = ["MEDICAID", "MEDICARE_ADV", "MARKETPLACE"]
STATES = ["TX", "CA", "FL", "OH", "WA"]
SPECIALTIES = ["FAMILY_MED", "CARDIOLOGY", "ORTHOPEDICS", "PEDIATRICS", "RADIOLOGY"]
CLAIM_STATUS = ["PAID", "DENIED", "PENDED"]


def generate(out_dir: str | Path, members: int = 200, providers: int = 30, claims: int = 2000,
             seed: int = 42, dirty_rate: float = 0.03) -> dict[str, Path]:
    """Write members.csv, providers.csv, claims.csv. A small share of claims is deliberately dirty
    (duplicates, negative amounts, unknown members) so the silver layer has something to quarantine."""
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {k: out / f"{k}.csv" for k in ("members", "providers", "claims")}

    with paths["members"].open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["member_id", "plan_code", "state", "birth_year", "enroll_date"])
        for i in range(1, members + 1):
            w.writerow([f"M{i:06d}", rng.choice(PLANS), rng.choice(STATES), rng.randint(1940, 2020),
                        (date(2023, 1, 1) + timedelta(days=rng.randint(0, 500))).isoformat()])

    with paths["providers"].open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["provider_id", "specialty", "state"])
        for i in range(1, providers + 1):
            w.writerow([f"P{i:05d}", rng.choice(SPECIALTIES), rng.choice(STATES)])

    with paths["claims"].open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["claim_id", "member_id", "provider_id", "service_date", "received_date",
                    "billed_amount", "paid_amount", "status"])
        rows = []
        for i in range(1, claims + 1):
            svc = date(2024, 1, 1) + timedelta(days=rng.randint(0, 364))
            billed = round(rng.uniform(50, 5000), 2)
            status = rng.choices(CLAIM_STATUS, weights=[0.8, 0.12, 0.08])[0]
            paid = round(billed * rng.uniform(0.4, 0.95), 2) if status == "PAID" else 0.0
            row = [f"C{i:08d}", f"M{rng.randint(1, members):06d}", f"P{rng.randint(1, providers):05d}",
                   svc.isoformat(), (svc + timedelta(days=rng.randint(1, 45))).isoformat(), billed, paid, status]
            if rng.random() < dirty_rate:
                kind = rng.choice(["neg", "orphan", "dup"])
                if kind == "neg":
                    row[5] = -abs(billed)
                elif kind == "orphan":
                    row[1] = "M999999"
                else:
                    rows.append(list(row))
            rows.append(row)
        w.writerows(rows)
    return paths


if __name__ == "__main__":
    import sys
    print(generate(sys.argv[1] if len(sys.argv) > 1 else "data/landing"))

import json

from claims_lakehouse.pipeline import bronze, run, silver_claims
from claims_lakehouse.synthetic import generate


def test_generate_is_deterministic(tmp_path):
    a = generate(tmp_path / "a", claims=100, seed=7)
    b = generate(tmp_path / "b", claims=100, seed=7)
    assert a["claims"].read_text() == b["claims"].read_text()


def test_silver_quarantines_bad_rows(spark, tmp_path):
    land = tmp_path / "landing"
    land.mkdir()
    (land / "members.csv").write_text("member_id,plan_code,state,birth_year,enroll_date\nM1,MEDICAID,TX,1980,2023-01-01\n")
    (land / "providers.csv").write_text("provider_id,specialty,state\nP1,CARDIOLOGY,TX\n")
    (land / "claims.csv").write_text(
        "claim_id,member_id,provider_id,service_date,received_date,billed_amount,paid_amount,status\n"
        "C1,M1,P1,2024-01-01,2024-01-05,100,80,PAID\n"
        "C1,M1,P1,2024-01-01,2024-01-05,100,80,PAID\n"      # duplicate -> collapsed
        "C2,M1,P1,2024-01-01,2024-01-05,-5,0,DENIED\n"      # negative billed
        "C3,M9,P1,2024-01-01,2024-01-05,100,10,PAID\n"      # unknown member
        "C4,M1,P1,2024-02-01,2024-01-05,100,10,PAID\n"      # received before service
    )
    b = bronze(spark, str(land))
    valid, bad = silver_claims(b["claims"], b["members"])
    assert [r.claim_id for r in valid.collect()] == ["C1"]
    failures = {r.claim_id: set(r.dq_failures) for r in bad.collect()}
    assert failures == {"C2": {"billed_non_negative", "paid_le_billed"}, "C3": {"member_exists"}, "C4": {"received_after_service"}}


def test_end_to_end(spark, tmp_path):
    generate(tmp_path / "landing", members=50, providers=10, claims=500, seed=1)
    report = run(spark, str(tmp_path / "landing"), str(tmp_path / "out"))
    assert report["silver_claims"] + report["quarantined"] <= report["bronze_claims"]
    assert report["quarantined"] > 0
    mart = spark.read.parquet(str(tmp_path / "out" / "mart_monthly_plan_kpis"))
    assert mart.filter("denial_rate < 0 OR denial_rate > 1").count() == 0
    assert json.loads((tmp_path / "out" / "run_report.json").read_text())["silver_claims"] == report["silver_claims"]

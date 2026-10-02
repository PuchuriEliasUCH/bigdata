from src.metrics import measure_batches


def test_measure_only_parquet_data_files_and_keep_modes(tmp_path):
    silver = tmp_path / "silver"
    bronze = tmp_path / "bronze"
    batch = "incremental-1"
    work = silver / "works" / batch
    state = silver / "_processing"
    metadata = bronze / "batch_metadata"
    for path in (work, state, metadata):
        path.mkdir(parents=True)
    (work / "part-00000.parquet").write_bytes(b"123")
    (work / "_SUCCESS").write_bytes(b"x")
    (state / f"{batch}.json").write_text('{"valid_rows": 2, "rejected_rows": 1, "write_seconds": 1.5}')
    (metadata / f"{batch}.json").write_text('{"source": "openalex", "mode": "incremental"}')
    assert measure_batches(bronze, silver) == [{
        "batch_id": batch, "source": "openalex", "mode": "incremental",
        "valid_rows": 2, "rejected_rows": 1, "parquet_files": 1,
        "parquet_bytes": 3, "write_seconds": 1.5,
    }]

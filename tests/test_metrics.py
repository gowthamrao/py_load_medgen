import gzip
import sys
import uuid
from pathlib import Path

import pytest
from py_load_medgen.cli import ETL_CONFIG
from py_load_medgen.cli import main as cli_main
from py_load_medgen.loader.postgres import PostgresNativeLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
def medgen_data_dir(tmp_path: Path) -> Path:
    """Creates a temporary directory with mock MedGen data files for testing."""
    # --- Version 1 Data ---
    v1_dir = tmp_path / "v1"
    v1_dir.mkdir()
    # Mock MRCONSO.RRF with the correct number of columns (18).
    # Added a final pipe to each line to create the 18th empty column (CVF).
    mrconso_v1_content = [
        "C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||||OMIM|PT|100650|OMIM:100650|0|N||",
        "C0000002|ENG|P|L0000002|PF|S0000002|Y|A0000002||||OMIM|PT|100700|OMIM:100700|0|N||",
        "C0000003|ENG|P|L0000003|PF|S0000003|Y|A0000003||||OMIM|PT|100800|OMIM:100800|0|N||",
    ]
    with open(v1_dir / "MRCONSO.RRF", "w", encoding="utf-8") as f:
        f.write("\n".join(mrconso_v1_content) + "\n")

    # --- Version 2 Data (for Delta Load) ---
    v2_dir = tmp_path / "v2"
    v2_dir.mkdir()
    # A0000001: Unchanged, A0000002: Deleted, A0000003: Updated, A0000004: Inserted
    mrconso_v2_content = [
        "C0000001|ENG|P|L0000001|PF|S0000001|Y|A0000001||||OMIM|PT|100650|OMIM:100650|0|N||",
        "C0000003|ENG|P|L0000003|PF|S0000003|Y|A0000003||||OMIM|PT|100800-updated|OMIM:100800|0|N||",
        "C0000004|ENG|P|L0000004|PF|S0000004|Y|A0000004||||OMIM|PT|100900|OMIM:100900|0|N||",
    ]
    with open(v2_dir / "MRCONSO.RRF", "w", encoding="utf-8") as f:
        f.write("\n".join(mrconso_v2_content) + "\n")

    return tmp_path


@pytest.fixture
def mock_downloader_and_config(monkeypatch):
    """
    Mocks the Downloader and isolates the ETL_CONFIG to only run the
    MRCONSO.RRF -> medgen_concepts ETL step.
    """
    # Isolate the config to only the part we are testing
    mrconso_config = next(c for c in ETL_CONFIG if c["file"] == "MRCONSO.RRF")
    monkeypatch.setattr("py_load_medgen.cli.ETL_CONFIG", [mrconso_config])

    # Mock the downloader methods
    monkeypatch.setattr("py_load_medgen.cli.Downloader.__enter__", lambda self: self)
    monkeypatch.setattr("py_load_medgen.cli.Downloader.__exit__", lambda self, exc_type, exc_val, exc_tb: None)
    monkeypatch.setattr("py_load_medgen.cli.Downloader.get_release_version", lambda self: "test_release_version")
    monkeypatch.setattr("py_load_medgen.cli.Downloader.list_files", lambda self: ["MRCONSO.RRF"])
    monkeypatch.setattr("py_load_medgen.cli.Downloader.get_checksums", lambda self, filename: {})
    monkeypatch.setattr("py_load_medgen.cli.Downloader.download_file", lambda self, remote, local, checksums: None)


def run_cli(monkeypatch, args: list[str]):
    """Helper to run the CLI main function with mocked sys.argv and sys.exit."""
    monkeypatch.setattr(sys, "argv", ["py_load_medgen"] + args)
    try:
        cli_main()
    except SystemExit as e:
        assert e.code == 0, f"CLI exited with non-zero code: {e.code}"


def test_full_load_metrics(
    postgres_db_dsn: str, medgen_data_dir: Path, monkeypatch, mock_downloader_and_config
):
    """
    Tests that a full load correctly populates the metrics in the audit tables.
    """
    run_cli(
        monkeypatch,
        [
            "--db-dsn",
            postgres_db_dsn,
            "--mode",
            "full",
            "--download-dir",
            str(medgen_data_dir / "v1"),
        ],
    )

    # --- Verification ---
    with PostgresNativeLoader(db_dsn=postgres_db_dsn) as loader:
        with loader.conn.cursor() as cur:
            cur.execute(
                "SELECT log_id, status, records_extracted, records_loaded "
                "FROM etl_audit_log ORDER BY start_time DESC LIMIT 1"
            )
            result = cur.fetchone()
            assert result is not None
            log_id, status, total_extracted, total_loaded = result

            assert status == "Succeeded"
            assert total_extracted == 3
            assert total_loaded == 3

            cur.execute(
                "SELECT records_extracted, records_inserted, records_updated, records_deleted "
                "FROM etl_run_details WHERE log_id = %s AND table_name = 'medgen_concepts'",
                (log_id,),
            )
            detail_result = cur.fetchone()
            assert detail_result is not None
            extracted, inserted, updated, deleted = detail_result

            assert extracted == 3
            assert inserted == 3
            assert updated == 0
            assert deleted == 0


def test_delta_load_metrics(
    postgres_db_dsn: str, medgen_data_dir: Path, monkeypatch, mock_downloader_and_config
):
    """
    Tests that a delta load correctly identifies and logs inserts, updates,
    and deletes.
    """
    # --- Phase 1: Run initial FULL load ---
    run_cli(
        monkeypatch,
        [
            "--db-dsn",
            postgres_db_dsn,
            "--mode",
            "full",
            "--download-dir",
            str(medgen_data_dir / "v1"),
        ],
    )

    # --- Phase 2: Run DELTA load with updated data ---
    run_cli(
        monkeypatch,
        [
            "--db-dsn",
            postgres_db_dsn,
            "--mode",
            "delta",
            "--download-dir",
            str(medgen_data_dir / "v2"),
        ],
    )

    # --- Verification ---
    with PostgresNativeLoader(db_dsn=postgres_db_dsn) as loader:
        with loader.conn.cursor() as cur:
            cur.execute(
                "SELECT log_id, status, records_extracted, records_loaded "
                "FROM etl_audit_log ORDER BY start_time DESC LIMIT 1"
            )
            result = cur.fetchone()
            assert result is not None
            log_id, status, total_extracted, total_loaded = result

            assert status == "Succeeded"
            assert total_extracted == 3
            assert total_loaded == 2  # 1 insert + 1 update

            cur.execute(
                "SELECT records_extracted, records_inserted, records_updated, records_deleted "
                "FROM etl_run_details WHERE log_id = %s AND table_name = 'medgen_concepts'",
                (log_id,),
            )
            detail_result = cur.fetchone()
            assert detail_result is not None
            extracted, inserted, updated, deleted = detail_result

            assert extracted == 3
            assert inserted == 1
            assert updated == 1
            assert deleted == 1

import subprocess
from pathlib import Path

expected_lines = [
    "Total airflow dags load time",
    "Airflow defs import time",
    "Mark as dagster migrating time",
    "Airflow setup time",
    "Airflow standup time",
    "Peered defs import time",
    "Peered defs initial load time",
    "Peered sensor tick with no runs time",
    "Peered sensor tick with single run time",
    "Peered sensor tick with 1 runs time",
    "Observed defs import time",
    "Observed defs initial load time",
    "Observed sensor tick with no runs time",
    "Observed sensor tick with single run time",
    "Observed sensor tick with 1 runs time",
    "Migrated defs import time",
    "Migrated defs initial load time",
    "Migrated sensor tick with no runs time",
    "Migrated sensor tick with single run time",
    "Migrated sensor tick with 1 runs time",
]

makefile_dir = Path(__file__).parent.parent
expected_file = makefile_dir / "perf_harness" / "shared" / "1_dags_1_tasks_perf_output.txt"


def test_cli() -> None:
    """Test that the CLI can be run, and produces expected output."""
    subprocess.call(
        ["make", "run_perf_scenarios_test"], cwd=makefile_dir, stdout=subprocess.DEVNULL
    )
    assert expected_file.exists()
    for i, line in enumerate(expected_file.read_text().split("\n")[:-1]):  # last line is empty
        assert expected_lines[i] in line

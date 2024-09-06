# ruff: noqa: T201
import argparse
import os
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import dagster._check as check
from dagster import Definitions, build_sensor_context
from dagster._core.test_utils import environ
from dagster_airlift.core.airflow_instance import AirflowInstance
from dagster_airlift.test.shared_fixtures import stand_up_airflow

from perf_harness.shared.constants import CONSTANTS_FILE, get_perf_output_file
from perf_harness.shared.utils import scaffold_migration_state

MAKEFILE_DIR = Path(__file__).parent.parent
DAGSTER_HOME = MAKEFILE_DIR / ".dagster_home"


@contextmanager
def modify_constants(num_dags, num_tasks) -> Generator[None, None, None]:
    # Read the original content
    with open(CONSTANTS_FILE, "r") as f:
        original_content = f.read()

    # Write new constants
    modified_content = f"NUM_DAGS {num_dags}\nNUM_TASKS {num_tasks}\n"

    # Write the modified content
    with open(CONSTANTS_FILE, "w") as f:
        f.write(modified_content)

    try:
        # Yield control back to the caller
        yield
    finally:
        # Restore the original content
        with open(CONSTANTS_FILE, "w") as f:
            f.write(original_content)


def main() -> None:
    lines = []
    parser = argparse.ArgumentParser(description="Performance scenario testing for airlift")
    parser.add_argument("num_dags", type=int, help="Number of DAGs to generate")
    parser.add_argument("num_tasks", type=int, help="Number of tasks per DAG")

    args = parser.parse_args()

    num_dags = check.int_param(args.num_dags, "num_dags")
    num_tasks = check.int_param(args.num_tasks, "num_tasks")

    with modify_constants(num_dags, num_tasks), environ({"DAGSTER_HOME": str(DAGSTER_HOME)}):
        print("Scaffolding migration state...")
        scaffold_migration_state(num_dags=num_dags, num_tasks=num_tasks, migration_state=True)

        print("Importing airflow defs...")
        from perf_harness.airflow_dags.dags import (
            import_time,
            mark_as_dagster_time,
            total_time as total_load_time,
        )

        lines.append(f"Total airflow dags load time: {total_load_time:.4f} seconds\n")
        lines.append(f"Airflow defs import time: {import_time:.4f} seconds\n")
        lines.append(f"Mark as dagster migrating time: {mark_as_dagster_time:.4f} seconds\n")

        print("Initializing airflow...")
        # Take in as an argument the number of tasks and the number of dags.
        # Create an ephemeral file where the results will be written, and assign it in shared.constants.py
        # Stand up airflow.
        # Stand up dagster at each stage (peer, observe, migrate), wiping .dagster_home in between.
        # At each stage, time how long initial load takes
        # Then, time how long subsequent load takes
        # Run the test sensor graphql mutation and time how long it takes.
        # Write all results to the ephemeral file.

        airflow_setup_time = time.time()
        subprocess.run(
            ["make", "setup_local_env"], check=True, cwd=MAKEFILE_DIR, stdout=subprocess.DEVNULL
        )
        airflow_setup_completion_time = time.time()
        lines.append(
            f"Airflow setup time: {airflow_setup_completion_time - airflow_setup_time:.4f} seconds\n"
        )

        print("Standing up airflow...")
        start_airflow_standup_time = time.time()
        with stand_up_airflow(
            env=os.environ,
            airflow_cmd=["make", "run_airflow"],
            cwd=MAKEFILE_DIR,
            stdout_channel=subprocess.DEVNULL,
        ):
            finished_airflow_standup_time = time.time()
            lines.append(
                f"Airflow standup time: {finished_airflow_standup_time - start_airflow_standup_time:.4f} seconds\n"
            )

            print("Running peering suite...")
            peered_defs_import_start_time = time.time()
            from perf_harness.dagster_defs.peer import defs

            peered_defs_import_end_time = time.time()
            peered_defs_import_time = peered_defs_import_end_time - peered_defs_import_start_time
            lines.append(f"Peered defs import time: {peered_defs_import_time:.4f} seconds\n")
            from perf_harness.dagster_defs.peer import airflow_instance

            run_suite_for_defs("Peered", num_dags, num_tasks, defs, lines, airflow_instance)

            print("Running observing suite...")
            observe_defs_import_start_time = time.time()
            from perf_harness.dagster_defs.observe import defs

            observe_defs_import_end_time = time.time()
            observe_defs_import_time = observe_defs_import_end_time - observe_defs_import_start_time
            lines.append(f"Observed defs import time: {observe_defs_import_time:.4f} seconds\n")
            from perf_harness.dagster_defs.observe import airflow_instance

            run_suite_for_defs("Observed", num_dags, num_tasks, defs, lines, airflow_instance)

            print("Running migrating suite...")
            migrate_defs_import_start_time = time.time()
            from perf_harness.dagster_defs.migrate import defs

            migrate_defs_import_end_time = time.time()
            migrate_defs_import_time = migrate_defs_import_end_time - migrate_defs_import_start_time
            lines.append(f"Migrated defs import time: {migrate_defs_import_time:.4f} seconds\n")
            from perf_harness.dagster_defs.migrate import airflow_instance

            run_suite_for_defs("Migrated", num_dags, num_tasks, defs, lines, airflow_instance)

        with open(get_perf_output_file(), "w") as f:
            f.writelines(lines)
    print("Performance harness completed.")


def run_suite_for_defs(
    module_name: str,
    num_dags: int,
    num_tasks: int,
    defs: Definitions,
    lines: list,
    af_instance: AirflowInstance,
) -> None:
    defs_initial_load_time = time.time()
    repo = defs.get_repository_def()
    defs_initial_load_completion_time = time.time()
    lines.append(
        f"{module_name} defs initial load time: {defs_initial_load_completion_time - defs_initial_load_time:.4f} seconds\n"
    )

    sensor_def = defs.get_sensor_def("airflow_dag_status_sensor")
    sensor_tick_no_runs_start_time = time.time()
    sensor_def(build_sensor_context(repository_def=repo))
    sensor_tick_no_runs_end_time = time.time()
    lines.append(
        f"{module_name} sensor tick with no runs time: {sensor_tick_no_runs_end_time - sensor_tick_no_runs_start_time:.4f} seconds\n"
    )

    run_id = af_instance.trigger_dag("dag_0")
    af_instance.wait_for_run_completion("dag_0", run_id)
    sensor_tick_with_single_run_start_time = time.time()
    sensor_def(build_sensor_context(repository_def=repo))
    sensor_tick_with_single_run_end_time = time.time()
    lines.append(
        f"{module_name} sensor tick with single run time: {sensor_tick_with_single_run_end_time - sensor_tick_with_single_run_start_time:.4f} seconds\n"
    )
    # Delete that run. Then add a run to every dag.
    af_instance.delete_run("dag_0", run_id)
    newly_added_runs = []
    for i in range(num_dags):
        run_id = af_instance.trigger_dag(f"dag_{i}")
        af_instance.wait_for_run_completion(f"dag_{i}", run_id)
    sensor_tick_with_all_runs_start_time = time.time()
    sensor_def(build_sensor_context(repository_def=repo))
    sensor_tick_with_all_runs_end_time = time.time()
    lines.append(
        f"{module_name} sensor tick with {num_dags} runs time: {sensor_tick_with_all_runs_end_time - sensor_tick_with_all_runs_start_time:.4f} seconds\n"
    )
    # Delete all runs.
    for run_id in newly_added_runs:
        af_instance.delete_run("dag_0", run_id)


if __name__ == "__main__":
    main()

from pathlib import Path
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtProfile, DbtProject

dbt_flow = dbt_flow(
    project=DbtProject(
        name="dbt workflow",
        project_dir=Path() / "./dbt_transformation",
        profiles_dir=Path.home() / ".dbt",
    ),
    profile=DbtProfile(
        target="dev",
    ),
    flow_kwargs={
        "task_runner": SequentialTaskRunner(),
    },
)

if __name__ == "__main__":
    dbt_flow()
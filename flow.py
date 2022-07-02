import prefect
from prefect import task, Flow
from prefect.run_configs import DockerRun
from prefect.storage import Docker, GitHub
import pandas as pd
import os


@task(name="hello_task")
def hello_task():

    logger = prefect.context.get("logger")
    logger.info("Hello world!")
    logger.info(f"I am at {os.getcwd()}")
    logger.info(f"Listing files:")
    for s in os.listdir("/tmp"):
        logger.info(s)
    df = pd.read_csv("/tmp/risk_hourly.csv")
    logger.info(df.size)


with Flow("hello-flow", storage=Docker(python_dependencies=["pandas"])) as flow:
    hello_task()

flow.run_config = DockerRun(labels=["agent1"])
flow.register(project_name="tutorial_docker",
              add_default_labels=False)

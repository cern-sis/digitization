import sys
from pathlib import Path

# DAG modules import each other from the dags folder root (as Airflow does).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

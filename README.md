Requirements:
pip install pandas
pip install heapq_max


How to run the project:
1) Download all files

2) Create virtual environment (for example with PyCharm)

3) Insert all downloaded files in the current project (of PyCharm) [DO NOT remove packages]->[files in data are needed to avoid creating directories, they will all be refreshed, except for the tweets.csv]

4) install all requirements
<!-- Change your directory -->
5)

	export AIRFLOW_HOME=~/PycharmProjects/airflow

	AIRFLOW_VERSION=2.2.1

	PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

	CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

	pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

6) airflow standalone

7) Log in on localhost:8080 by given login and password

8) From DAGs select MapReduceDag and unpause it

9) Trigger the DAG if it did not start by itself

10) The results of the processing are in data/result_unsorted.csv of the project

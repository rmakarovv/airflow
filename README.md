Requirements:
pip install pandas
pip install apache-airflow['cncf.kubernetes']


How to run the project:
1) Download all files
2) Create virtual environment (for example with PyCharm)
3) Insert all downloaded files in the current project (of PyCharm) [DO NOT remove packages]
4) install airflow in the current venv (pip install airflow)
4') install heapq (heapq_max), pandas [pip install heapq; pip install pandas]
5) $ export export AIRFLOW_HOME=~/PycharmProjects/airflow
				path should be the current path to your project
6) run 'airflow standalone'
7) Log in on localhost:8080
8) From Dags select MapReduceDag and run it
9) The results of the processing are in data/result_unsorted.csv of the project

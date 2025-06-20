### Installation
To run Airflow, simply use the following instructions:

First, make sure you have Docker installed on your machine.
Second, change path to dags in docker-compose.yaml
Last, run the following commands:

```bash
docker build -t airflow .
docker compose up
```
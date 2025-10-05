# This section describes how to setup docker compose for development

##### Environment Setup
For local development first create `.env` file in root directory,
basically need to add these values:
```dotenv
AIRFLOW_PROJ_DIR=./src
AIRFLOW__CORE__LOAD_EXAMPLES=false
```
Then run `echo "AIRFLOW_UID=$(id -u)" >> .env` (linux) for add root access to files which
will be created by docker.

##### Services
After docker ups, these services will be available on local ports:
 - `8080` - airflow api server
 - `5555` - flower (for see celery tasks results)

##### Databases
- `./postgres-data` - path for local postgres data (used by airflow)
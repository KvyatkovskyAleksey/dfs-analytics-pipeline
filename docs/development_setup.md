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

##### Wasabi S3 Storage Setup
To use Wasabi S3-compatible storage with Spark:

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Get your Wasabi credentials from [Wasabi Console](https://console.wasabisys.com)

3. Update the `.env` file with your credentials:
   ```dotenv
   WASABI_ENDPOINT=s3.wasabisys.com
   WASABI_ACCESS_KEY=your-actual-access-key
   WASABI_SECRET_KEY=your-actual-secret-key
   ```

4. In your Spark code, use S3A protocol to access Wasabi buckets:
   ```python
   # Read from Wasabi
   df = spark.read.parquet("s3a://your-bucket-name/path/to/data.parquet")

   # Write to Wasabi
   df.write.parquet("s3a://your-bucket-name/output/path")
   ```

See `notebooks/wasabi_s3_example.ipynb` for a complete example.

##### Services
After docker ups, these services will be available on local ports:
 - `8080` - airflow api server
 - `5555` - flower (for see celery tasks results)
 - `8888` - jupyter notebook
 - `9090` - spark master UI

##### Databases
- `./postgres-data` - path for local postgres data (used by airflow)
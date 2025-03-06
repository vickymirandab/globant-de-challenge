from fastapi import FastAPI, HTTPException
from google.cloud import storage, bigquery
from google.auth import exceptions
import logging

app = FastAPI()
bq_client = bigquery.Client()
gcs_client = storage.Client()

DATASET_ID = "globant_challenge"
GCS_BUCKET_NAME = "globant-de-challenge-bucket"

# Define Tables
SCHEMAS = {
    "departments": [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("department", "STRING")
    ],
    "jobs": [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("job", "STRING")
    ],
    "hired_employees": [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("datetime", "STRING"),
        bigquery.SchemaField("department_id", "INTEGER"),
        bigquery.SchemaField("job_id", "INTEGER")
    ]
}

##### SECTION 1 #####

@app.post("/load_csv")
async def load_csv_from_gcs():
    """Trigger BigQuery to load a CSV from GCS"""

    bucket = gcs_client.get_bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs()

    try:
        # Get CSV files in GCS
        for blob in blobs:
            if blob.name.endswith(".csv"):
                file_name = blob.name
                table_name = file_name.replace(".csv", "")  # Remove .csv from filename to get table name
                table_id = f"{bq_client.project}.{DATASET_ID}.{table_name}"

                # Set job configuration
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=0,
                    schema=SCHEMAS[table_name],
                    write_disposition="WRITE_TRUNCATE"
                )

                # Load the CSV file into BigQuery
                uri = f"gs://{GCS_BUCKET_NAME}/{file_name}"
                job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
                job.result()

                logging.info(f"Loaded {file_name} into {table_name} in BigQuery.")

        return {"message": "All CSV files loaded into BigQuery."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


##### SECTION 2 #####

# Endpoint to get the number of employees hired for each job and department by quarter in 2021
@app.get("/metrics/quarterly_hires_2021")
async def get_quarterly_hires_2021():
    quarterly_hires_query = """
            SELECT
            d.department,
            j.job,
            EXTRACT(QUARTER FROM CAST(e.datetime AS TIMESTAMP)) AS quarter,
            COUNT(*) AS total_employees
        FROM globant_challenge.hired_employees AS e
        JOIN globant_challenge.departments AS d ON e.department_id = d.id
        JOIN globant_challenge.jobs AS j ON e.job_id = j.id
        WHERE EXTRACT(YEAR FROM CAST(e.datetime AS TIMESTAMP)) = 2021
        GROUP BY d.department, j.job, quarter
        ORDER BY d.department, j.job;
    """

    try:
        query_job = bq_client.query(quarterly_hires_query)
        results = query_job.result()

        return [
            {
                "department": row.department,
                "job": row.job,
                "quarter":
                row.quarter,
                "num_hired": row.num_hired}
            for row in results
        ]
    except exceptions.GoogleAuthError as auth_error:
        raise HTTPException(status_code=500, detail=f"Authentication error: {auth_error}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# Endpoint to get departments that hired more than the average number of employees in 2021
@app.get("/metrics/departments_above_avg_2021")
async def get_departments_above_avg_2021():
    departments_above_abg_query = """
        WITH department_hires AS (
            -- Get the number of hires per department
            SELECT department_id, COUNT(*) AS hired_count
            FROM hired_employees
            WHERE datetime BETWEEN '2021-01-01' AND '2021-12-31'
            GROUP BY department_id
        ),
        avg_hires AS (
            -- Get average number of hires across all departments
            SELECT AVG(hired_count) AS avg_hires_all_departments
            FROM department_hires
        )
        -- Get departments that hired more than the average
        SELECT d.id, d.department, dh.hired_count AS hired
        FROM department_hires dh
        JOIN departments d ON dh.department_id = d.id
        CROSS JOIN avg_hires
        WHERE dh.hired_count > avg_hires.avg_hires_all_departments
        ORDER BY hired DESC
    """

    try:
        query_job = bq_client.query(departments_above_abg_query)
        results = query_job.result()

        return [
            {"department_id": row["id"], "department": row["department"], "hired": row["hired"]}
            for row in results
        ]

    except exceptions.GoogleAuthError as auth_error:
        print(f"Authentication error: {auth_error}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

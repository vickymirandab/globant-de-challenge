# globant-de-challenge
Globant's Data Engineering Challenge

## Section 1: API

In the context of a DB migration with 3 different tables (departments, jobs, employees), create a local REST API that must:

1. Receive historical data from CSV files
2. Upload these files to the new DB
3. Be able to insert batch transactions (1 up to 1000 rows) with one request

You need to publish your code in GitHub. It will be taken into account if frequent updates are made to the repository that allow analyzing the development process. Ideally, create a markdown file for the Readme.md

**Clarifications**

- You decide the origin where the CSV files are located.
- You decide the destination database type, but it must be a SQL database.
- The CSV file is comma separated.

## Section 2: SQL

You need to explore the data that was inserted in the previous section. The stakeholders ask for some specific metrics they need. You should create an end-point for each requirement.

Requirements

- Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.

**Output example**

| Department     | Job             | Q1 | Q2 | Q3 | Q4 |
|----------------|-----------------|----|----|----|----|
| Staff          | Recruiter       | 3  | 0  | 7  | 11 |
| Staff          | Manager         | 2  | 1  | 0  | 2  |
| Supply Chain   | Manager         | 0  | 1  | 3  | 0  |


- List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

**Output example**

| id | department   | hired |
|----|--------------|-------|
| 7  | Staff        | 45    |
| 9  | Supply Chain | 12    |

## Test local API

Go to folder where code is located and run

```
python main.py
```

Then you can go to http://localhost:8000/docs#/ (FastAPI Swagger UI) and easily call the endpoints. For upload_csv endpoint you would need to enter the table name (departments, jobs or hired_employees) and load the corresponding CSV.

## Deployment in GCP

To deploy the API run:

```
gcloud run deploy <service-name> --source <code-path>   --region <gcp-region>   --port 8080
```

To test the endpoints:

```
curl -X 'POST' 'https://<cloud-run-service-name>.<gcp-region>.run.app/load_csv'

curl -X 'GET' 'https://<cloud-run-service-name>.<gcp-region>.run.app/metrics/quarterly_hires_2021'

curl -X 'GET' 'https://<cloud-run-service-name>.<gcp-region>.run.app/metrics/departments_above_avg_2021'
```

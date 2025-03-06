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

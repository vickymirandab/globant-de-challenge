from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, text
from sqlalchemy.orm import sessionmaker, Session
from collections import defaultdict
import pandas as pd
import io


app = FastAPI()


DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

# Define Tables
departments = Table(
    "departments", metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("department", String, index=True)
)

jobs = Table(
    "jobs", metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("job", String, index=True)
)

hired_employees = Table(
    "hired_employees", metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("name", String, index=True),
    Column("datetime", String, index=True),
    Column("department_id", Integer),
    Column("job_id", Integer)
)

metadata.create_all(bind=engine)

file_headers = {
    "departments": ["id", "department"],
    "jobs": ["id", "job"],
    "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/upload_csv/{table_name}")
async def upload_csv(table_name: str, file: UploadFile = File(...), db=Depends(get_db)):
    """ Upload a CSV file and insert data into the specified table. """
    
    if file_headers.get(table_name):
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")), names=file_headers[table_name], header=None)

        for column in df.columns:
            if df[column].dtype == 'float64':
                df[column] = df[column].where(df[column].notnull(), -1)
        
        # Insert Data in Batches
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].to_dict(orient="records")
            db.execute(metadata.tables[table_name].insert().values(batch))
            db.commit()
        
        return {"message": f"Inserted {len(df)} rows into {table_name}"}
    
    raise HTTPException(status_code=400, detail="Invalid table name")


##### SECTION 2 #####

# Endpoint to get the number of employees hired for each job and department by quarter in 2021
@app.get("/metrics/quarterly_hires_2021")
async def get_quarterly_hires_2021(db: Session = Depends(get_db)):
    sql = text("""
            SELECT
                d.department,
                j.job,
            CASE
                WHEN strftime('%m', e.datetime) BETWEEN '01' AND '03' THEN 1
                WHEN strftime('%m', e.datetime) BETWEEN '04' AND '06' THEN 2
                WHEN strftime('%m', e.datetime) BETWEEN '07' AND '09' THEN 3
                WHEN strftime('%m', e.datetime) BETWEEN '10' AND '12' THEN 4
                ELSE null
            END as quarter,
            count(*) as total_employees
            FROM hired_employees AS e
            JOIN departments AS d ON e.department_id = d.id
            JOIN jobs AS j ON e.job_id = j.id
            WHERE strftime('%Y', e.datetime) = '2021'
            GROUP BY 1, 2, 3
            ORDER BY d.department, j.job
    """)

    result = db.execute(sql).fetchall()
    print(result)

    data = defaultdict(lambda: {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0})

    for dept, job, quarter, count in result:
        data[(dept, job)]["department"], data[(dept, job)]["job"] = dept, job
        data[(dept, job)][f"Q{quarter}"] = count

    return list(data.values())

# Endpoint to get departments that hired more than the average number of employees in 2021
@app.get("/metrics/departments_above_avg_2021")
async def get_departments_above_avg_2021(db: Session = Depends(get_db)):
    sql = text("""
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
    """)

    result = db.execute(sql).fetchall()

    return [
        {
            "id": row[0],
            "department": row[1],
            "hired": row[2]
        }
        for row in result
    ]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

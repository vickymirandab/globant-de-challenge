from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import io


app = FastAPI()


DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

# Define All Tables
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

# Tables' columns
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
        
        # Insert the data in batches of size 1000 as required
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].to_dict(orient="records")
            db.execute(metadata.tables[table_name].insert().values(batch))
            db.commit()
        
        return {"message": f"Inserted {len(df)} rows into {table_name}"}
    
    raise HTTPException(status_code=400, detail="Invalid table name")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
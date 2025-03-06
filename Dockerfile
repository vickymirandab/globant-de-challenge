# Use the official Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /globant-de-challenge

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Check if uvicorn is installed (optional debugging step)
RUN which uvicorn 

# Copy the rest of the application code
COPY . .

# Ensure the container listens on the correct port
EXPOSE 8080

# Set the default command to run your FastAPI app with Uvicorn
CMD ["uvicorn", "bonus.main:app", "--host", "0.0.0.0", "--port", "8080"]


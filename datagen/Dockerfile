FROM python:3.12-slim

WORKDIR /app

# Install build dependencies if needed (psycopg2-binary doesn't usually need gcc, but good to have)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

EXPOSE 8000

# Run FastAPI app with Uvicorn
CMD ["uvicorn", "stream_app:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.12-slim

WORKDIR /app

# psycopg2-binary is pre-compiled — no gcc or build tools needed.
# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

EXPOSE 8000

# Run FastAPI app with Uvicorn
CMD ["uvicorn", "stream_app:app", "--host", "0.0.0.0", "--port", "8000"]

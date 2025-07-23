FROM python:3.11-slim
WORKDIR /app

# Copy and install dependencies first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire source code into the container
COPY src/ .

# The entrypoint runs the main script, which executes the full pipeline.
ENTRYPOINT ["python", "main.py"]
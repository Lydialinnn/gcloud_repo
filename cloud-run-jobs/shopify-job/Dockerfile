# Use the official lightweight Python image.
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy local code to the container image.
COPY . .

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Cloud Run Jobs use standard python execution, not a web server.
# The entrypoint will read arguments (e.g., --offset_days 0) passed by Cloud Scheduler, just a test
ENTRYPOINT ["python", "shopify_etl.py"]
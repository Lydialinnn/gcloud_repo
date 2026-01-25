# Use a lightweight Python image
FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Set working directory
WORKDIR /app

# Copy local code to the container image
COPY . ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the web service on container startup using gunicorn
# timeout 0 means no timeout for worker startup
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
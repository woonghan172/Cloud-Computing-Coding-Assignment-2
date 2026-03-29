# Use a lightweight Python base image (Alpine variant is smaller)
FROM python:3.12-alpine

# Set working directory inside the container
WORKDIR /app

# Install system dependencies needed for some Python packages + security updates
RUN apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    && pip install --no-cache-dir --upgrade pip

# Copy and install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py storage.py ./

# The application saves to data.json in the current directory
# We create it as empty if needed (but normally volume should be used)
RUN touch data.json logger.log

# Expose the port your app listens on
EXPOSE 8080

# Run the application
# Using --host 0.0.0.0 is very important in Docker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
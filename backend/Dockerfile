# Use a slim python image to keep things fast
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the code
COPY . .

# We don't define CMD here because we will override it in docker-compose
# to run either producer or consumer
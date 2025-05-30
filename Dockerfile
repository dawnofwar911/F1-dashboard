# Dockerfile
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install git
# This is necessary for pip to install packages from git repositories
RUN apt-get update && \
    apt-get install -y git --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
COPY . .

# Ensure the 'replays' directory exists
RUN mkdir -p /app/replays 

# Make port 8050 available to the world outside this container
EXPOSE 8050

# Define environment variable
ENV PYTHONUNBUFFERED 1

# Run main.py when the container launches
CMD ["waitress-serve", "--host", "0.0.0.0", "--port", "8050", "main:server"]

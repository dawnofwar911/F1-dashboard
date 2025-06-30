# Dockerfile
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Install git, needed for some pip installations from git repos
RUN apt-get update && \
    apt-get install -y git --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file first to leverage Docker layer caching.
# This way, dependencies are only re-installed when requirements.txt changes.
COPY app/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the application code and config
COPY gunicorn.conf.py .
COPY app/ ./app/

# Make port 8050 available to the world outside this container
EXPOSE 8050

# Define environment variable for unbuffered python output
ENV PYTHONUNBUFFERED=1

# Run main.py with the Gunicorn server using our config file
CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.main:server"]
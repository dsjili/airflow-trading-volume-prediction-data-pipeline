FROM python:3.9

# Install additional dependencies
USER root

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    unzip \
    libpq-dev

# Copy project files to the container
COPY . /usr/local/airflow

# Set the working directory
WORKDIR /usr/local/airflow

# Install project dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Unzip the file during the build process
RUN unzip /usr/local/airflow/archive.zip -d /usr/local/airflow/data

# Use Python 3.12 Version as latest
FROM python:3.12

# Make relations requirements.txt
COPY jsons/requirements.txt /jsons/requirements.txt
RUN pip install -r /jsons/requirements.txt

# Make relations for PostgreSQL
RUN apt-get update && apt-get install -y postgresql-client

# Copy all files from jsons in directory /jsons of container
COPY jsons /jsons

# Make of working directory
WORKDIR /jsons

# Launch script main.py
ENTRYPOINT ["python", "main.py"]

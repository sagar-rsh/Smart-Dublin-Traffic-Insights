FROM quay.io/astronomer/astro-runtime:12.8.0

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy the 'scripts' folder into the container
COPY scripts /opt/airflow/scripts

# Add scripts to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"
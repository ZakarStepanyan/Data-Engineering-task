FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libpq-dev \
    git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

# Now that the file is in the root, this will work:
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt
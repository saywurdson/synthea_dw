FROM jupyter/all-spark-notebook:python-3.11.6

# Set working directory
WORKDIR /container

# Install lzop and other dependencies
USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER $NB_UID

# Upgrade pip and install packages using pip
RUN python -m pip install --upgrade pip && \
    pip install duckdb duckdb-engine dbt-postgres dbt-duckdb

# Install packages using conda
RUN conda install -y -c conda-forge xgboost python-dotenv=0.21.1 xlwings openai findspark pyspark polars && \
    conda install pip boto3 && \
    conda update pandas -y && \
    conda clean --all
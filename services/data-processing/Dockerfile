FROM das-financial-analysis-base:latest

# Additional dependencies specific to data processing
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SERVICE_NAME=data-processing
ENV PORT=5002
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# Expose specific port for this service
EXPOSE 5002

# Override the command to use the specific port
CMD ["gunicorn", "--bind", "0.0.0.0:5002", "main:app"] 
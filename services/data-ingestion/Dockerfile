FROM das-financial-analysis-base:latest

# Additional dependencies specific to data ingestion
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SERVICE_NAME=data-ingestion
ENV PORT=5001

# Expose specific port for this service
EXPOSE 5001

# Override the command to use the specific port
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "main:app"] 
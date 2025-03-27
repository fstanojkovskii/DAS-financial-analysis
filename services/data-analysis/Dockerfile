FROM das-financial-analysis-base:latest

# Additional dependencies specific to data analysis
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SERVICE_NAME=data-analysis
ENV PORT=5003
ENV PYTHONPATH=/app/src

# Create src directory
WORKDIR /app/src

# Copy source files
COPY src/ .

# Expose specific port for this service
EXPOSE 5003

# Override the command to use the specific port
CMD ["gunicorn", "--bind", "0.0.0.0:5003", "app:app"] 
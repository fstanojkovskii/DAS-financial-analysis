FROM das-financial-analysis-base:latest

# Additional dependencies specific to data visualization
RUN apt-get update && apt-get install -y \
    libpq-dev \
    python3-dev \
    build-essential \
    libev-dev \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SERVICE_NAME=data-visualization
ENV PORT=5004
ENV PYTHONPATH=/app/src

# Create src directory
WORKDIR /app/src

# Copy source files
COPY src/ .

# Expose specific port for this service
EXPOSE 5004

# Override the command to use the specific port with gevent worker
CMD ["gunicorn", "--bind", "0.0.0.0:5004", "--worker-class", "gevent", "app:app"] 
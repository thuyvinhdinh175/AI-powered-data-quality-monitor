FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port for Streamlit
EXPOSE 8501

# Set environment variables
ENV PYTHONPATH=/app

# Create directories
RUN mkdir -p /app/data/raw /app/data/validation_results /app/data/insights /app/data/fixes

# Set up Great Expectations
RUN great_expectations init

# Set entrypoint - can be overridden at runtime
ENTRYPOINT ["python", "-m", "app.dashboard"]

# Default command for Streamlit
CMD ["streamlit", "run", "app/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]

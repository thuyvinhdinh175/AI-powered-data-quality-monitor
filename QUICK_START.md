# Quick Start Guide

This guide will help you get started with the AI-Powered Data Quality Monitor.

## Prerequisites

- Python 3.10+
- Docker (optional, for containerized deployment)
- OpenAI API key (for LLM functionality)

## Installation

### Option 1: Local Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ai-data-quality-monitor.git
   cd ai-data-quality-monitor
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Initialize Great Expectations:
   ```bash
   great_expectations init
   ```

4. Set up your API key:
   ```bash
   export OPENAI_API_KEY=your-api-key
   ```

### Option 2: Docker Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ai-data-quality-monitor.git
   cd ai-data-quality-monitor
   ```

2. Build and run the Docker containers:
   ```bash
   cd docker
   docker-compose up -d
   ```

## Running the System

### Validate a Dataset

```bash
python run.py validate --dataset data/transactions.csv --suite transactions_suite
```

### Generate Insights for Failed Checks

```bash
python run.py insights --results data/validation_results/YYYY-MM-DD/transactions/results.json
```

### Generate Fix Suggestions

```bash
python run.py fixes --results data/validation_results/YYYY-MM-DD/transactions/results.json
```

### Run the Dashboard

```bash
python run.py dashboard
```
Then visit http://localhost:8501 in your browser.

### Run the Complete Pipeline

```bash
python run.py pipeline --source data/transactions.csv --suite transactions_suite
```

## Example Workflow

1. Place your dataset in the `data/` directory
2. Generate a test suite based on the dataset:
   ```bash
   python run.py generate --dataset data/your_data.csv
   ```
3. Run validation:
   ```bash
   python run.py validate --dataset data/your_data.csv --suite your_data_suite
   ```
4. View results in the dashboard:
   ```bash
   python run.py dashboard
   ```

## Project Structure

- `app/`: Core application code
  - `data_ingestion/`: Data ingestion modules
  - `validator/`: Data validation logic
  - `dashboard.py`: Streamlit dashboard
  - `alert_manager.py`: Alert system
- `dags/`: Airflow DAGs for scheduling
- `data/`: Data storage
- `expectations/`: Great Expectations configuration
- `llm_agent/`: LLM-powered components
- `docker/`: Docker configuration files
- `notebooks/`: Jupyter notebooks for examples and exploration
- `tests/`: Unit tests
- `run.py`: Command-line interface

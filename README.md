# AI-Powered Data Quality Monitor

An intelligent data quality system that continuously monitors datasets for anomalies, schema drift, missing values, and suspicious patterns. It uses LLMs to provide explanations, auto-generated test cases, and remediation suggestions.

## 🏗️ System Architecture

```mermaid
flowchart TB
    subgraph Data_Sources["Data Sources"]
        CSV["CSV Files"]
        API["API Data"]
        DB["Databases"]
    end

    subgraph Data_Ingestion["Data Ingestion"]
        Ingest["app/data_ingestion/ingest.py"]
    end

    subgraph Validation_Layer["Validation Layer"]
        GE["Great Expectations"]
        Custom["Custom Checks"]
        Validator["app/validator/run_checks.py"]
    end

    subgraph LLM_Engine["LLM Intelligence Engine"]
        ExpGen["llm_agent/expectation_generator.py"]
        InsightGen["llm_agent/insight_generator.py"]
        FixGen["llm_agent/fix_suggestor.py"]
    end

    subgraph Storage["Storage"]
        Raw["data/raw"]
        Results["data/validation_results"]
        Insights["data/insights"]
        Fixes["data/fixes"]
        Memory["data/memory"]
    end

    subgraph Alerts["Alerting System"]
        AlertMgr["app/alert_manager.py"]
        Email["Email Notifications"]
        Slack["Slack Alerts"]
    end

    subgraph Visualization["Visualization"]
        Dashboard["app/dashboard.py (Streamlit)"]
        Reports["Summary Reports"]
    end

    subgraph Orchestration["Orchestration"]
        Airflow["dags/data_quality_dag.py"]
    end

    %% Connections between components
    Data_Sources --> Data_Ingestion
    Data_Ingestion --> Raw
    Raw --> Validation_Layer
    Validation_Layer --> Results
    Results --> LLM_Engine
    LLM_Engine --> Insights
    LLM_Engine --> Fixes
    LLM_Engine --> Memory
    Results --> Alerts
    Insights --> Alerts
    Fixes --> Alerts
    Results --> Visualization
    Insights --> Visualization
    Fixes --> Visualization
    Airflow --> Data_Ingestion
    Airflow --> Validation_Layer
    Airflow --> LLM_Engine
    Airflow --> Alerts

    %% Styles
    classDef sourceStyle fill:#f5f5f5,stroke:#333,stroke-width:1px;
    classDef ingestStyle fill:#d4f1f9,stroke:#333,stroke-width:1px;
    classDef validationStyle fill:#ffe6cc,stroke:#333,stroke-width:1px;
    classDef llmStyle fill:#e1d5e7,stroke:#333,stroke-width:1px;
    classDef storageStyle fill:#d5e8d4,stroke:#333,stroke-width:1px;
    classDef alertStyle fill:#fff2cc,stroke:#333,stroke-width:1px;
    classDef vizStyle fill:#dae8fc,stroke:#333,stroke-width:1px;
    classDef orchestrationStyle fill:#f8cecc,stroke:#333,stroke-width:1px;

    class Data_Sources,CSV,API,DB sourceStyle;
    class Data_Ingestion,Ingest ingestStyle;
    class Validation_Layer,GE,Custom,Validator validationStyle;
    class LLM_Engine,ExpGen,InsightGen,FixGen llmStyle;
    class Storage,Raw,Results,Insights,Fixes,Memory storageStyle;
    class Alerts,AlertMgr,Email,Slack alertStyle;
    class Visualization,Dashboard,Reports vizStyle;
    class Orchestration,Airflow orchestrationStyle;
```

## 🛠️ Core Stack

| Layer | Tools |
|-------|-------|
| Ingestion | Airflow / Pandas / Spark |
| Validation | Great Expectations + custom rules |
| LLM Insight | LangChain + DeepSeek / OpenAI / Ollama |
| Storage | S3 / Parquet / Delta |
| Visualization | Streamlit / Superset |
| Alerting | Email, Slack, or webhook |
| CI/CD | Docker + GitHub Actions |

## 🔍 Key Features

### ✅ Data Validation Rules
* Missing values, null rate
* Schema drift / column type change
* Value range checks, duplicates

### ✅ LLM-Powered Insights
* "Why is this failing?"
* "What does this anomaly mean?"
* "Suggest a better threshold or check"

### ✅ Auto-Test Suggestion
* LLM generates Great Expectations YAML config based on data

### ✅ Alerting System
* Slack/email alerts on validation failures
* Summary logs + human-readable diagnosis

### ✅ Dashboard
* Streamlit UI to explore:
   * Daily DQ report
   * Failing checks
   * Fix suggestions
   * Anomaly charts

## 🚀 Advanced Ideas

| Feature | Description |
|---------|-------------|
| 🧠 Memory | Store historical DQ trends to detect drift over time |
| 🔁 Active Learning | Flagged rows get reviewed by users, improving LLM fix recommendations |
| 📥 REST API | Expose validation pipeline as an API for plug-and-play use |

## 🗂️ Project Folder Structure

```
ai-data-quality-monitor/
│
├── dags/                  # Airflow DAGs (optional)
├── data/                  # Sample data files
├── expectations/          # Great Expectations config and suites
├── llm_agent/             # LLM logic: LangChain chains, fix suggestor
├── app/                   # Streamlit app code
├── tests/                 # Unit tests for checks
├── docker/                # Dockerfiles, entrypoints
├── requirements.txt
├── README.md
└── .github/workflows/     # CI/CD pipelines
```

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ai-data-quality-monitor.git
cd ai-data-quality-monitor

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up Great Expectations
great_expectations init

# Run the Streamlit app
cd app
streamlit run dashboard.py
```

## 🚀 Usage

1. Place your datasets in the `data/` directory
2. Configure validation rules in the `expectations/` directory
3. Run the data quality checks:
   ```
   python run_validator.py --dataset your_data.csv
   ```
4. View results in the Streamlit dashboard:
   ```
   cd app
   streamlit run dashboard.py
   ```

## 🔧 Configuration

- LLM settings can be configured in `config.yaml`
- Alert thresholds and rules can be modified in `expectations/rules.yaml`
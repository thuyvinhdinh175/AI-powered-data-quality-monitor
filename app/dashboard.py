"""
Streamlit dashboard for data quality monitoring

This dashboard displays data quality validation results,
LLM-generated insights, and suggested fixes.
"""
import os
import json
import logging
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set page configuration
st.set_page_config(
    page_title="AI-Powered Data Quality Monitor",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Global variables
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
VALIDATION_RESULTS_DIR = os.path.join(DATA_DIR, "validation_results")
INSIGHTS_DIR = os.path.join(DATA_DIR, "insights")
FIXES_DIR = os.path.join(DATA_DIR, "fixes")

# Function to load available dates
def load_available_dates():
    """Load available dates from validation results directory."""
    if not os.path.exists(VALIDATION_RESULTS_DIR):
        return []
    
    dates = []
    for date_dir in os.listdir(VALIDATION_RESULTS_DIR):
        if os.path.isdir(os.path.join(VALIDATION_RESULTS_DIR, date_dir)) and date_dir.startswith("20"):
            try:
                datetime.strptime(date_dir, "%Y-%m-%d")
                dates.append(date_dir)
            except ValueError:
                continue
    
    return sorted(dates, reverse=True)

# Function to load available datasets for a given date
def load_available_datasets(date):
    """Load available datasets for a given date."""
    date_dir = os.path.join(VALIDATION_RESULTS_DIR, date)
    if not os.path.exists(date_dir):
        return []
    
    datasets = []
    for dataset_dir in os.listdir(date_dir):
        if os.path.isdir(os.path.join(date_dir, dataset_dir)):
            datasets.append(dataset_dir)
    
    return sorted(datasets)

# Function to load validation results
def load_validation_results(date, dataset):
    """Load validation results for a given date and dataset."""
    results_path = os.path.join(VALIDATION_RESULTS_DIR, date, dataset, "results.json")
    if not os.path.exists(results_path):
        return None
    
    with open(results_path, 'r') as f:
        return json.load(f)

# Function to load insights
def load_insights(date, dataset):
    """Load insights for a given date and dataset."""
    insights_path = os.path.join(INSIGHTS_DIR, date, dataset, "insights.json")
    if not os.path.exists(insights_path):
        return {}
    
    with open(insights_path, 'r') as f:
        return json.load(f)

# Function to load fix suggestions
def load_fixes(date, dataset):
    """Load fix suggestions for a given date and dataset."""
    fixes_path = os.path.join(FIXES_DIR, date, dataset, "fixes.json")
    if not os.path.exists(fixes_path):
        return {}
    
    with open(fixes_path, 'r') as f:
        return json.load(f)

# Function to create success rate chart
def create_success_rate_chart(dates, datasets):
    """Create a chart showing success rate over time."""
    data = []
    
    for date in dates:
        for dataset in datasets:
            results = load_validation_results(date, dataset)
            if results:
                success_percent = results.get('statistics', {}).get('success_percent', 0)
                data.append({
                    'Date': date,
                    'Dataset': dataset,
                    'Success Rate': success_percent
                })
    
    if not data:
        return None
    
    df = pd.DataFrame(data)
    fig = px.line(
        df, 
        x='Date', 
        y='Success Rate', 
        color='Dataset',
        title='Data Quality Success Rate Over Time',
        labels={'Success Rate': 'Success Rate (%)', 'Date': 'Date'}
    )
    
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Success Rate (%)",
        legend_title="Dataset",
        yaxis=dict(range=[0, 100])
    )
    
    return fig

# Function to create failed checks chart
def create_failed_checks_chart(results):
    """Create a chart showing failed checks."""
    if not results or not results.get('failed_checks'):
        return None
    
    data = []
    for check in results.get('failed_checks', []):
        data.append({
            'Check': check.get('check_name', 'Unknown'),
            'Failure Percentage': check.get('failure_percentage', 0)
        })
    
    df = pd.DataFrame(data)
    fig = px.bar(
        df, 
        x='Check', 
        y='Failure Percentage',
        title='Failed Checks',
        labels={'Failure Percentage': 'Failure Percentage (%)', 'Check': 'Check'}
    )
    
    fig.update_layout(
        xaxis_title="Check",
        yaxis_title="Failure Percentage (%)",
        xaxis={'categoryorder':'total descending'}
    )
    
    return fig

# Sidebar
st.sidebar.title("🧠 AI-Powered Data Quality Monitor")

# Date selector
available_dates = load_available_dates()
if not available_dates:
    st.sidebar.warning("No validation results found. Please run validation first.")
    selected_date = None
else:
    selected_date = st.sidebar.selectbox("Select Date", available_dates)

# Dataset selector
if selected_date:
    available_datasets = load_available_datasets(selected_date)
    if not available_datasets:
        st.sidebar.warning(f"No datasets found for {selected_date}")
        selected_dataset = None
    else:
        selected_dataset = st.sidebar.selectbox("Select Dataset", available_datasets)

# Load data if date and dataset are selected
if selected_date and selected_dataset:
    # Load validation results
    results = load_validation_results(selected_date, selected_dataset)
    if not results:
        st.error(f"No validation results found for {selected_dataset} on {selected_date}")
    else:
        # Load insights and fixes
        insights = load_insights(selected_date, selected_dataset)
        fixes = load_fixes(selected_date, selected_dataset)
        
        # Main content
        st.title(f"Data Quality Report: {selected_dataset}")
        st.write(f"Date: {selected_date}")
        
        # Summary metrics
        st.header("Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        stats = results.get('statistics', {})
        with col1:
            st.metric("Total Checks", stats.get('evaluated_expectations', 0))
        with col2:
            st.metric("Passed Checks", stats.get('successful_expectations', 0))
        with col3:
            st.metric("Failed Checks", stats.get('unsuccessful_expectations', 0))
        with col4:
            st.metric("Success Rate", f"{stats.get('success_percent', 0)}%")
        
        # Success rate over time chart
        st.header("Trends")
        recent_dates = sorted(available_dates)[-7:]  # Last 7 days
        success_chart = create_success_rate_chart(recent_dates, [selected_dataset])
        if success_chart:
            st.plotly_chart(success_chart, use_container_width=True)
        else:
            st.info("Not enough data to show trends")
        
        # Failed checks chart
        failed_checks_chart = create_failed_checks_chart(results)
        if failed_checks_chart:
            st.plotly_chart(failed_checks_chart, use_container_width=True)
        
        # Failed checks details
        st.header("Failed Checks")
        if results.get('failed_checks'):
            for i, check in enumerate(results.get('failed_checks', [])):
                with st.expander(f"{check.get('check_name', 'Unknown check')} ({check.get('failure_percentage', 0)}% failure)"):
                    # Check details
                    st.subheader("Check Details")
                    st.write(f"**Check Type:** {check.get('check_type', 'Unknown')}")
                    st.write(f"**Failed Rows:** {check.get('failed_rows', 0)}")
                    st.write(f"**Failure Percentage:** {check.get('failure_percentage', 0)}%")
                    
                    # Expected value
                    st.subheader("Expected Value/Condition")
                    st.json(check.get('expected_value', {}))
                    
                    # Actual value
                    st.subheader("Actual Value/Finding")
                    st.json(check.get('actual_value', {}))
                    
                    # LLM Insight
                    if insights and check['check_name'] in insights:
                        st.subheader("🧠 AI Insight")
                        insight = insights[check['check_name']]
                        st.write(f"**Description:** {insight.get('issue_description', 'No insight available')}")
                        st.write(f"**Impact Level:** {insight.get('impact_level', 'Unknown')}")
                        st.write(f"**Business Impact:** {insight.get('business_impact', 'Unknown')}")
                        
                        st.write("**Possible Causes:**")
                        for cause in insight.get('possible_causes', []):
                            st.write(f"- {cause}")
                        
                        st.write("**Recommended Actions:**")
                        for action in insight.get('recommended_actions', []):
                            st.write(f"- {action}")
                    
                    # LLM Fix Suggestion
                    if fixes and check['check_name'] in fixes:
                        st.subheader("🛠️ AI Fix Suggestion")
                        fix = fixes[check['check_name']]
                        st.write(f"**Approach:** {fix.get('fix_approach', 'No suggestion available')}")
                        st.write(f"**Rationale:** {fix.get('rationale', 'No rationale available')}")
                        st.write(f"**Confidence:** {fix.get('confidence', 'Unknown')}")
                        
                        st.write("**Implementation:**")
                        st.code(fix.get('implementation', 'No implementation available'))
                        
                        st.write("**Alternative Approaches:**")
                        for alt in fix.get('alternative_approaches', []):
                            st.write(f"- {alt}")
        else:
            st.success("All checks passed! No issues detected.")

# Add sidebar sections
with st.sidebar:
    st.header("Actions")
    
    # Run validation button
    if st.button("Run Validation"):
        st.info("Running validation... This feature is not implemented in the demo.")
    
    # Generate test suites button
    if st.button("Generate Test Suite"):
        st.info("Generating test suite... This feature is not implemented in the demo.")

    st.header("Resources")
    st.markdown("[📚 Documentation](https://github.com/yourusername/ai-data-quality-monitor)")
    st.markdown("[🐙 GitHub Repository](https://github.com/yourusername/ai-data-quality-monitor)")
    st.markdown("[📊 Great Expectations Docs](https://docs.greatexpectations.io/)")

    st.header("About")
    st.markdown("""
    **AI-Powered Data Quality Monitor** is an intelligent system that 
    continuously monitors datasets for anomalies, schema drift, missing values, 
    and suspicious patterns. It uses LLMs to provide explanations, auto-generated 
    test cases, and remediation suggestions.
    """)

if not selected_date:
    # Welcome message when no data is selected
    st.title("🧠 AI-Powered Data Quality Monitor")
    
    st.markdown("""
    Welcome to the AI-Powered Data Quality Monitor dashboard!
    
    This tool helps you monitor the quality of your data by:
    
    1. Running validation checks against your datasets
    2. Using AI to explain data quality issues
    3. Suggesting fixes for common problems
    4. Alerting you when issues are detected
    
    To get started, select a date and dataset from the sidebar.
    """)
    
    # Display sample insights
    st.header("Sample AI Insights")
    with st.expander("✨ Example: Missing Values in Customer ID"):
        st.markdown("""
        **Issue Description:** The customer_id field is missing values in 2.5% of records, which exceeds our threshold of 1%.
        
        **Impact Level:** Medium
        
        **Business Impact:** Missing customer IDs prevent proper attribution of transactions and can lead to inaccurate customer analytics and personalization efforts.
        
        **Possible Causes:**
        - Data entry errors in the source system
        - Integration issues between customer database and transaction system
        - New transactions being processed before customer records are created
        
        **Recommended Actions:**
        - Investigate the 17 records with missing IDs to identify patterns
        - Check if these transactions can be matched to customers using other attributes
        - Implement validation at the source to prevent missing customer IDs
        - Consider raising the threshold to 3% if some level of missing IDs is acceptable
        """)
    
    with st.expander("✨ Example: Schema Drift in Transaction Amount"):
        st.markdown("""
        **Issue Description:** The transaction_amount field contains non-numeric values in 0.8% of records, which should be numeric according to the schema.
        
        **Impact Level:** High
        
        **Business Impact:** Non-numeric transaction amounts will cause errors in financial reporting, skew analytics, and potentially result in incorrect revenue calculations.
        
        **Possible Causes:**
        - Special characters (like currency symbols) included in the amount field
        - Text status codes mistakenly entered in amount fields
        - Source system allowing free-form text entry for amounts
        
        **Recommended Actions:**
        - Clean the data by removing non-numeric characters and converting to numbers
        - Implement input validation in the source system
        - Create a separate field for transaction notes/comments
        - Consider adding a data transformation step to handle common formatting issues
        """)

if __name__ == "__main__":
    # This is needed for Streamlit to run
    pass

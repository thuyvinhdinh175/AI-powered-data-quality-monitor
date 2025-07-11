"""
Tests for LLM agent components
"""
import os
import sys
import json
import pytest
from unittest.mock import patch, MagicMock

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the modules to test
from llm_agent.insight_generator import generate_llm_insights, DataQualityInsight
from llm_agent.fix_suggestor import suggest_fixes, FixSuggestion
from llm_agent.expectation_generator import generate_expectations_config, analyze_dataset

# Sample validation result for testing
SAMPLE_VALIDATION_RESULT = {
    "dataset_path": "/path/to/data.csv",
    "dataset_name": "data.csv",
    "suite_name": "test_suite",
    "timestamp": "2025-07-10T12:34:56",
    "success": False,
    "statistics": {
        "evaluated_expectations": 3,
        "successful_expectations": 2,
        "unsuccessful_expectations": 1,
        "success_percent": 66.67
    },
    "failed_checks": [
        {
            "check_name": "expect_column_values_to_be_between",
            "check_type": "column_values_to_be_between",
            "dataset_name": "data.csv",
            "failed_rows": 2,
            "failure_percentage": 40.0,
            "timestamp": "2025-07-10T12:34:56",
            "expected_value": {
                "column": "amount",
                "min_value": 0,
                "max_value": 1000
            },
            "actual_value": {
                "unexpected_count": 2,
                "unexpected_percent": 40.0,
                "unexpected_values": [1200.0, -10.25]
            },
            "check_implementation": "{\"expectation_type\": \"expect_column_values_to_be_between\", \"kwargs\": {\"column\": \"amount\", \"min_value\": 0, \"max_value\": 1000}}",
            "dataset_path": "/path/to/data.csv"
        }
    ]
}

# Sample LLM response for insight
SAMPLE_INSIGHT_RESPONSE = """
{
  "check_name": "expect_column_values_to_be_between",
  "issue_description": "The transaction amount field contains values outside the expected range of 0 to 1000.",
  "possible_causes": [
    "Data entry errors in the source system",
    "Special transactions with unusually high or negative amounts",
    "Missing validation in the source system"
  ],
  "impact_level": "Medium",
  "business_impact": "Out-of-range transaction amounts can skew financial reporting and analytics, potentially leading to incorrect business decisions.",
  "recommended_actions": [
    "Investigate the transactions with amounts of 1200.0 and -10.25",
    "Implement input validation in the source system",
    "Consider adjusting the allowed range if these values are legitimate"
  ]
}
"""

# Sample LLM response for fix
SAMPLE_FIX_RESPONSE = """
{
  "check_name": "expect_column_values_to_be_between",
  "fix_approach": "Data Transformation + Threshold Adjustment",
  "rationale": "The current data contains both a negative value (-10.25) which might be a refund and a high value (1200.0) which could be a legitimate large purchase. Both scenarios should be handled appropriately.",
  "implementation": "# Option 1: Transform data during ingestion\ndef transform_amounts(df):\n    # Convert negative values to positive but mark as refunds\n    refunds = df['amount'] < 0\n    df.loc[refunds, 'status'] = 'Refunded'\n    df.loc[refunds, 'amount'] = df.loc[refunds, 'amount'].abs()\n    return df\n\n# Option 2: Adjust validation threshold\n# Update Great Expectations suite:\nexpectation_config = {\n    'expectation_type': 'expect_column_values_to_be_between',\n    'kwargs': {\n        'column': 'amount',\n        'min_value': -1000,  # Allow negative values for refunds\n        'max_value': 2000  # Increase upper limit for large purchases\n    }\n}",
  "confidence": "High",
  "alternative_approaches": [
    "Create separate validation rules for different transaction types",
    "Add a pre-processing step to flag outlier transactions for review",
    "Implement conditional validation based on transaction category"
  ]
}
"""

@pytest.fixture
def sample_validation_results_file(tmp_path):
    """Create a sample validation results file."""
    results_dir = tmp_path / "validation_results" / "2025-07-10" / "data"
    results_dir.mkdir(parents=True)
    file_path = results_dir / "results.json"
    with open(file_path, 'w') as f:
        json.dump(SAMPLE_VALIDATION_RESULT, f)
    return str(file_path)

@patch('langchain.chains.LLMChain.run')
def test_generate_llm_insights(mock_llm_run, sample_validation_results_file, tmp_path):
    """Test generating LLM insights."""
    # Mock the LLM response
    mock_llm_run.return_value = SAMPLE_INSIGHT_RESPONSE
    
    # Create output directory
    insights_dir = tmp_path / "insights" / "2025-07-10" / "data"
    insights_dir.mkdir(parents=True)
    
    # Run the function
    with patch('os.path.dirname') as mock_dirname:
        mock_dirname.return_value = str(tmp_path / "validation_results" / "2025-07-10" / "data")
        insights = generate_llm_insights(sample_validation_results_file)
    
    # Check results
    assert insights is not None
    assert isinstance(insights, dict)
    assert "expect_column_values_to_be_between" in insights
    
    check_insight = insights["expect_column_values_to_be_between"]
    assert check_insight["issue_description"] == "The transaction amount field contains values outside the expected range of 0 to 1000."
    assert len(check_insight["possible_causes"]) == 3
    assert check_insight["impact_level"] == "Medium"

@patch('langchain.chains.LLMChain.run')
def test_suggest_fixes(mock_llm_run, sample_validation_results_file, tmp_path):
    """Test suggesting fixes."""
    # Mock the LLM response
    mock_llm_run.return_value = SAMPLE_FIX_RESPONSE
    
    # Create output directory
    fixes_dir = tmp_path / "fixes" / "2025-07-10" / "data"
    fixes_dir.mkdir(parents=True)
    
    # Run the function
    with patch('os.path.dirname') as mock_dirname:
        mock_dirname.return_value = str(tmp_path / "validation_results" / "2025-07-10" / "data")
        fixes = suggest_fixes(sample_validation_results_file)
    
    # Check results
    assert fixes is not None
    assert isinstance(fixes, dict)
    assert "expect_column_values_to_be_between" in fixes
    
    check_fix = fixes["expect_column_values_to_be_between"]
    assert check_fix["fix_approach"] == "Data Transformation + Threshold Adjustment"
    assert "implementation" in check_fix
    assert len(check_fix["alternative_approaches"]) == 3
    assert check_fix["confidence"] == "High"

@patch('langchain.chains.LLMChain.run')
@patch('llm_agent.expectation_generator.analyze_dataset')
def test_generate_expectations_config(mock_analyze, mock_llm_run, tmp_path):
    """Test generating expectations configuration."""
    # Mock dataset analysis
    mock_analyze.return_value = {
        'dataset_name': 'test_data.csv',
        'row_count': 5,
        'columns': ['transaction_id', 'customer_id', 'transaction_date', 'amount', 'category', 'status', 'location'],
        'data_types': {
            'transaction_id': 'int64',
            'customer_id': 'object',
            'transaction_date': 'object',
            'amount': 'float64',
            'category': 'object',
            'status': 'object',
            'location': 'object'
        },
        'data_stats': {
            'transaction_id': {
                'min': 1001,
                'max': 1005,
                'mean': 1003,
                'null_count': 0
            },
            'amount': {
                'min': 12.99,
                'max': 800.0,
                'mean': 210.146,
                'null_count': 0
            }
        }
    }
    
    # Mock LLM response
    mock_llm_run.return_value = """
name: test_data_suite
config_version: 1.0
expectations:
  - expectation_type: expect_table_columns_to_match_ordered_list
    kwargs:
      column_list:
        - transaction_id
        - customer_id
        - transaction_date
        - amount
        - category
        - status
        - location
  - expectation_type: expect_column_values_to_be_between
    kwargs:
      column: amount
      min_value: 0
      max_value: 1000
"""
    
    # Create output path
    output_path = tmp_path / "expectations" / "test_data_suite.yml"
    
    # Run the function
    config = generate_expectations_config("test_data.csv", str(output_path))
    
    # Check results
    assert config is not None
    assert "expect_table_columns_to_match_ordered_list" in config
    assert "expect_column_values_to_be_between" in config
    assert "amount" in config

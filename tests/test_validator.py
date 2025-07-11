"""
Tests for data quality validator
"""
import os
import sys
import json
import pytest
import pandas as pd
import great_expectations as ge

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the modules to test
from app.validator.run_checks import DataQualityValidator

# Test data
TEST_DATA = {
    'transaction_id': [1001, 1002, 1003, 1004, 1005],
    'customer_id': ['C5432', 'C1234', 'C7890', 'C3456', 'C5432'],
    'transaction_date': ['2025-07-01', '2025-07-01', '2025-07-01', '2025-07-02', '2025-07-02'],
    'amount': [125.99, 45.50, 800.00, 65.25, 12.99],
    'category': ['Electronics', 'Groceries', 'Furniture', 'Dining', 'Books'],
    'status': ['Completed', 'Completed', 'Pending', 'Completed', 'Completed'],
    'location': ['New York', 'San Francisco', 'Chicago', 'Boston', 'New York']
}

# Test expectation suite
TEST_EXPECTATION_SUITE = {
    'name': 'test_suite',
    'expectations': [
        {
            'expectation_type': 'expect_table_columns_to_match_ordered_list',
            'kwargs': {
                'column_list': ['transaction_id', 'customer_id', 'transaction_date', 'amount', 'category', 'status', 'location']
            }
        },
        {
            'expectation_type': 'expect_column_values_to_not_be_null',
            'kwargs': {
                'column': 'transaction_id'
            }
        },
        {
            'expectation_type': 'expect_column_values_to_be_between',
            'kwargs': {
                'column': 'amount',
                'min_value': 0,
                'max_value': 1000
            }
        }
    ]
}

@pytest.fixture
def test_data_file(tmp_path):
    """Create a test data file."""
    df = pd.DataFrame(TEST_DATA)
    file_path = tmp_path / 'test_data.csv'
    df.to_csv(file_path, index=False)
    return file_path

@pytest.fixture
def test_suite_file(tmp_path):
    """Create a test expectation suite file."""
    suite_dir = tmp_path / 'expectations'
    suite_dir.mkdir()
    file_path = suite_dir / 'test_suite.yml'
    with open(file_path, 'w') as f:
        json.dump(TEST_EXPECTATION_SUITE, f)
    return suite_dir, 'test_suite'

@pytest.fixture
def validator(tmp_path, test_suite_file):
    """Create a DataQualityValidator instance."""
    suite_dir, _ = test_suite_file
    results_dir = tmp_path / 'results'
    results_dir.mkdir()
    return DataQualityValidator(expectations_dir=str(suite_dir), results_dir=str(results_dir))

def test_validator_initialization(validator):
    """Test validator initialization."""
    assert validator is not None
    assert os.path.isdir(validator.results_dir)

def test_load_dataset(validator, test_data_file):
    """Test loading a dataset."""
    dataset = validator._load_dataset(str(test_data_file))
    assert dataset is not None
    assert len(dataset.columns) == 7
    assert len(dataset) == 5

def test_load_expectation_suite(validator, test_suite_file):
    """Test loading an expectation suite."""
    suite_dir, suite_name = test_suite_file
    suite = validator._load_expectation_suite(suite_name)
    assert suite is not None
    assert suite.expectation_suite_name == 'test_suite'
    assert len(suite.expectations) == 3

def test_validate_passing_dataset(validator, test_data_file, test_suite_file):
    """Test validating a dataset that passes all checks."""
    suite_dir, suite_name = test_suite_file
    results = validator.validate(str(test_data_file), suite_name, save_results=False)
    assert results is not None
    assert results['success'] is True
    assert results['statistics']['unsuccessful_expectations'] == 0

def test_validate_failing_dataset(validator, tmp_path, test_suite_file):
    """Test validating a dataset that fails some checks."""
    # Create a dataset with invalid data
    failing_data = TEST_DATA.copy()
    failing_data['amount'] = [125.99, 45.50, 1200.00, -10.25, 12.99]  # Values outside range
    df = pd.DataFrame(failing_data)
    file_path = tmp_path / 'failing_data.csv'
    df.to_csv(file_path, index=False)
    
    suite_dir, suite_name = test_suite_file
    results = validator.validate(str(file_path), suite_name, save_results=False)
    assert results is not None
    assert results['success'] is False
    assert results['statistics']['unsuccessful_expectations'] > 0
    assert len(results['failed_checks']) > 0
    
    # Check that failed checks contain the right information
    for check in results['failed_checks']:
        if check['check_name'] == 'expect_column_values_to_be_between':
            assert check['failed_rows'] == 2  # Two rows fail the range check
            assert check['check_type'] == 'column_values_to_be_between'
            assert 'amount' in str(check['expected_value'])

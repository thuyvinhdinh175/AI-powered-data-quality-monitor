"""
Main validator module for running data quality checks

This module uses Great Expectations to validate datasets against
defined expectations and saves validation results.
"""
import os
import sys
import json
import yaml
import logging
import datetime
import pandas as pd
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Validator class for running data quality checks."""
    
    def __init__(self, 
                expectations_dir: str = "../expectations",
                results_dir: str = "../data/validation_results"):
        """
        Initialize the validator.
        
        Args:
            expectations_dir (str): Directory containing expectation suite YAML files
            results_dir (str): Directory to save validation results
        """
        self.expectations_dir = expectations_dir
        self.results_dir = results_dir
        
        # Ensure results directory exists
        os.makedirs(self.results_dir, exist_ok=True)
    
    def _load_expectation_suite(self, suite_name: str) -> ExpectationSuite:
        """
        Load an expectation suite from a YAML file.
        
        Args:
            suite_name (str): Name of the expectation suite (without .yml extension)
            
        Returns:
            ExpectationSuite: Loaded expectation suite
        """
        suite_path = os.path.join(self.expectations_dir, f"{suite_name}.yml")
        
        if not os.path.exists(suite_path):
            raise FileNotFoundError(f"Expectation suite not found: {suite_path}")
        
        # Load from YAML
        with open(suite_path, 'r') as f:
            suite_config = json.load(f) if suite_path.endswith('.json') else yaml.safe_load(f)
        
        # Create expectation suite
        suite = ExpectationSuite(
            expectation_suite_name=suite_config.get('name', suite_name),
            expectations=suite_config.get('expectations', []),
            meta=suite_config.get('meta', {})
        )
        
        return suite
    
    def _load_dataset(self, dataset_path: str) -> PandasDataset:
        """
        Load a dataset from a file.
        
        Args:
            dataset_path (str): Path to the dataset file
            
        Returns:
            PandasDataset: Loaded dataset as a Great Expectations PandasDataset
        """
        # Determine file type and load accordingly
        if dataset_path.endswith('.csv'):
            df = pd.read_csv(dataset_path)
        elif dataset_path.endswith('.parquet'):
            df = pd.read_parquet(dataset_path)
        elif dataset_path.endswith('.json'):
            df = pd.read_json(dataset_path)
        else:
            raise ValueError(f"Unsupported file format: {dataset_path}")
        
        # Convert to Great Expectations dataset
        ge_dataset = ge.from_pandas(df)
        
        return ge_dataset
    
    def validate(self, 
                dataset_path: str, 
                suite_name: str,
                save_results: bool = True) -> Dict[str, Any]:
        """
        Validate a dataset against an expectation suite.
        
        Args:
            dataset_path (str): Path to the dataset file
            suite_name (str): Name of the expectation suite
            save_results (bool): Whether to save validation results
            
        Returns:
            Dict[str, Any]: Validation results
        """
        logger.info(f"Validating dataset: {dataset_path} with suite: {suite_name}")
        
        try:
            # Load dataset and expectation suite
            dataset = self._load_dataset(dataset_path)
            suite = self._load_expectation_suite(suite_name)
            
            # Validate dataset against expectation suite
            validation_result = dataset.validate(expectation_suite=suite)
            
            # Process validation results
            results = self._process_validation_results(
                validation_result, 
                dataset_path, 
                suite_name
            )
            
            # Save results if requested
            if save_results:
                self._save_results(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    def _process_validation_results(self, 
                                   validation_result: Dict[str, Any], 
                                   dataset_path: str, 
                                   suite_name: str) -> Dict[str, Any]:
        """
        Process validation results into a more usable format.
        
        Args:
            validation_result (Dict[str, Any]): Raw validation results
            dataset_path (str): Path to the dataset file
            suite_name (str): Name of the expectation suite
            
        Returns:
            Dict[str, Any]: Processed validation results
        """
        # Extract basic information
        results = {
            "dataset_path": dataset_path,
            "dataset_name": os.path.basename(dataset_path),
            "suite_name": suite_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "success": validation_result.get('success', False),
            "statistics": {
                "evaluated_expectations": validation_result.get('statistics', {}).get('evaluated_expectations', 0),
                "successful_expectations": validation_result.get('statistics', {}).get('successful_expectations', 0),
                "unsuccessful_expectations": validation_result.get('statistics', {}).get('unsuccessful_expectations', 0),
                "success_percent": validation_result.get('statistics', {}).get('success_percent', 0),
            },
            "failed_checks": []
        }
        
        # Process failed expectations
        for result in validation_result.get('results', []):
            if not result.get('success', False):
                # Extract relevant information for failed check
                failed_check = {
                    "check_name": result.get('expectation_config', {}).get('expectation_type', 'unknown'),
                    "check_type": result.get('expectation_config', {}).get('expectation_type', 'unknown').split('expect_')[1] if 'expect_' in result.get('expectation_config', {}).get('expectation_type', '') else 'custom',
                    "dataset_name": os.path.basename(dataset_path),
                    "failed_rows": result.get('result', {}).get('unexpected_count', 0),
                    "failure_percentage": result.get('result', {}).get('unexpected_percent', 0),
                    "timestamp": datetime.datetime.now().isoformat(),
                    "expected_value": result.get('expectation_config', {}).get('kwargs', {}),
                    "actual_value": {
                        "unexpected_count": result.get('result', {}).get('unexpected_count', 0),
                        "unexpected_percent": result.get('result', {}).get('unexpected_percent', 0),
                        "unexpected_values": result.get('result', {}).get('partial_unexpected_list', [])
                    },
                    "check_implementation": json.dumps(result.get('expectation_config', {}), indent=2),
                    "dataset_path": dataset_path
                }
                
                results['failed_checks'].append(failed_check)
        
        return results
    
    def _save_results(self, results: Dict[str, Any]) -> str:
        """
        Save validation results to a file.
        
        Args:
            results (Dict[str, Any]): Validation results
            
        Returns:
            str: Path to the saved results file
        """
        # Create directory for results
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        dataset_name = os.path.splitext(results['dataset_name'])[0]
        output_dir = os.path.join(self.results_dir, date_str, dataset_name)
        os.makedirs(output_dir, exist_ok=True)
        
        # Save results to file
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        output_path = os.path.join(output_dir, f"results_{timestamp}.json")
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Also save a copy as 'latest.json'
        latest_path = os.path.join(output_dir, "results.json")
        with open(latest_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Validation results saved to: {output_path}")
        return output_path


def run_validation(dataset_path: str, suite_name: str) -> Dict[str, Any]:
    """
    Run validation on a dataset.
    
    Args:
        dataset_path (str): Path to the dataset file
        suite_name (str): Name of the expectation suite
        
    Returns:
        Dict[str, Any]: Validation results
    """
    validator = DataQualityValidator()
    return validator.validate(dataset_path, suite_name)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run data quality validation")
    parser.add_argument("--dataset", required=True, help="Path to the dataset file")
    parser.add_argument("--suite", required=True, help="Name of the expectation suite")
    
    args = parser.parse_args()
    
    run_validation(args.dataset, args.suite)

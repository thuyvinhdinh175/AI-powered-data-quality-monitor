"""
LLM-based expectation generator

This module uses LLM to analyze a dataset and automatically generate
appropriate Great Expectations test suite configurations.
"""
import os
import json
import yaml
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional

from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_openai import OpenAI, ChatOpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables (should be set in .env file or environment)
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-3.5-turbo-0613")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your-api-key")

# Prompt template for expectation generation
EXPECTATION_PROMPT = """
You are a data quality expert creating Great Expectations test suite configuration.

Here is a summary of a dataset:
Dataset Name: {dataset_name}
Number of Rows: {row_count}
Columns: {columns}

Data Types:
{data_types}

Data Statistics:
{data_stats}

Based on this information, generate a Great Expectations configuration YAML that includes appropriate tests for:
1. Column presence and order
2. Data types
3. Missing values and nulls
4. Value ranges and distributions
5. Category sets for categorical variables
6. Any other appropriate checks

Provide the output in valid YAML format compatible with Great Expectations.
IMPORTANT: Ensure your response contains ONLY the YAML configuration, no other text.
"""

def get_llm():
    """Initialize and return the LLM."""
    try:
        # Try to use ChatOpenAI first
        return ChatOpenAI(
            model=LLM_MODEL,
            temperature=0,
            openai_api_key=OPENAI_API_KEY,
        )
    except Exception as e:
        logger.warning(f"Failed to initialize ChatOpenAI: {e}. Falling back to OpenAI.")
        # Fallback to OpenAI
        return OpenAI(
            model_name=LLM_MODEL,
            temperature=0,
            openai_api_key=OPENAI_API_KEY,
        )

def analyze_dataset(dataset_path: str) -> Dict[str, Any]:
    """
    Analyze a dataset to provide statistical information for the LLM.
    
    Args:
        dataset_path (str): Path to the dataset file
        
    Returns:
        Dict[str, Any]: Dictionary containing dataset statistics and information
    """
    logger.info(f"Analyzing dataset: {dataset_path}")
    
    try:
        # Load the dataset
        df = pd.read_csv(dataset_path)
        
        # Basic info
        dataset_info = {
            'dataset_name': os.path.basename(dataset_path),
            'row_count': len(df),
            'columns': df.columns.tolist(),
            'data_types': {}
        }
        
        # Data types for each column
        for col in df.columns:
            dataset_info['data_types'][col] = str(df[col].dtype)
        
        # Statistics for each column
        dataset_info['data_stats'] = {}
        for col in df.columns:
            col_stats = {}
            
            # For numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_stats['min'] = df[col].min()
                col_stats['max'] = df[col].max()
                col_stats['mean'] = df[col].mean()
                col_stats['median'] = df[col].median()
                col_stats['std'] = df[col].std()
                col_stats['null_count'] = df[col].isnull().sum()
                col_stats['null_percentage'] = round(df[col].isnull().mean() * 100, 2)
            
            # For categorical columns
            elif pd.api.types.is_object_dtype(df[col]):
                value_counts = df[col].value_counts().head(10).to_dict()  # Top 10 values
                col_stats['unique_count'] = df[col].nunique()
                col_stats['top_values'] = value_counts
                col_stats['null_count'] = df[col].isnull().sum()
                col_stats['null_percentage'] = round(df[col].isnull().mean() * 100, 2)
            
            # For datetime columns (try to infer)
            elif pd.api.types.is_datetime64_dtype(df[col]) or ('date' in col.lower() and pd.to_datetime(df[col], errors='coerce').notnull().any()):
                try:
                    datetime_col = pd.to_datetime(df[col], errors='coerce')
                    col_stats['min_date'] = datetime_col.min()
                    col_stats['max_date'] = datetime_col.max()
                    col_stats['null_count'] = datetime_col.isnull().sum()
                    col_stats['null_percentage'] = round(datetime_col.isnull().mean() * 100, 2)
                except:
                    # Fallback if datetime conversion fails
                    col_stats['unique_count'] = df[col].nunique()
                    col_stats['null_count'] = df[col].isnull().sum()
                    col_stats['null_percentage'] = round(df[col].isnull().mean() * 100, 2)
            
            dataset_info['data_stats'][col] = col_stats
        
        return dataset_info
        
    except Exception as e:
        logger.error(f"Failed to analyze dataset: {e}")
        raise

def generate_expectations_config(dataset_path: str, output_path: Optional[str] = None) -> str:
    """
    Generate Great Expectations configuration for a dataset using LLM.
    
    Args:
        dataset_path (str): Path to the dataset file
        output_path (Optional[str]): Path to save the generated configuration
        
    Returns:
        str: Generated YAML configuration
    """
    logger.info(f"Generating expectations for dataset: {dataset_path}")
    
    try:
        # Analyze dataset
        dataset_info = analyze_dataset(dataset_path)
        
        # Prepare input for LLM
        llm_input = {
            "dataset_name": dataset_info['dataset_name'],
            "row_count": dataset_info['row_count'],
            "columns": ", ".join(dataset_info['columns']),
            "data_types": "\n".join([f"- {col}: {dtype}" for col, dtype in dataset_info['data_types'].items()]),
            "data_stats": json.dumps(dataset_info['data_stats'], indent=2, default=str)
        }
        
        # Initialize LLM
        llm = get_llm()
        
        # Create prompt template
        prompt = PromptTemplate(
            template=EXPECTATION_PROMPT,
            input_variables=["dataset_name", "row_count", "columns", "data_types", "data_stats"],
        )
        
        # Create LLMChain
        chain = LLMChain(llm=llm, prompt=prompt)
        
        # Generate expectations configuration
        yaml_config = chain.run(**llm_input)
        
        # Clean up the output to ensure it's valid YAML
        yaml_config = yaml_config.strip()
        if yaml_config.startswith("```yaml"):
            yaml_config = yaml_config[7:]
        if yaml_config.endswith("```"):
            yaml_config = yaml_config[:-3]
        yaml_config = yaml_config.strip()
        
        # Validate that the output is valid YAML
        try:
            yaml.safe_load(yaml_config)
        except yaml.YAMLError as e:
            logger.error(f"Generated YAML is invalid: {e}")
            raise ValueError(f"Generated YAML is invalid: {e}")
        
        # Save to file if output path provided
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(yaml_config)
            logger.info(f"Expectations configuration saved to: {output_path}")
        
        return yaml_config
        
    except Exception as e:
        logger.error(f"Failed to generate expectations configuration: {e}")
        raise

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate Great Expectations configuration for a dataset")
    parser.add_argument("--dataset", required=True, help="Path to the dataset file")
    parser.add_argument("--output", help="Path to save the generated configuration")
    
    args = parser.parse_args()
    
    output_path = args.output
    if not output_path:
        base_name = os.path.splitext(os.path.basename(args.dataset))[0]
        output_path = f"expectations/{base_name}_suite.yml"
    
    generate_expectations_config(args.dataset, output_path)

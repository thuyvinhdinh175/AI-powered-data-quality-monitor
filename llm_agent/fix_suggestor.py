"""
LLM-based fix suggestor for data quality issues

This module uses LangChain and LLMs to suggest fixes for data quality issues,
including schema changes, validation thresholds, and data transformations.
"""
import os
import json
import logging
from typing import Dict, List, Any, Optional

from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_openai import OpenAI, ChatOpenAI
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables (should be set in .env file or environment)
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-3.5-turbo-0613")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your-api-key")

# Output schema for structured fix suggestions
class FixSuggestion(BaseModel):
    """Suggested fix for a data quality issue."""
    check_name: str = Field(description="Name of the failing data quality check")
    fix_approach: str = Field(description="Approach to fix the issue (Schema Change, Data Transformation, Threshold Adjustment, etc.)")
    rationale: str = Field(description="Explanation of why this fix is appropriate")
    implementation: str = Field(description="Implementation details or code snippet")
    confidence: str = Field(description="Confidence level in this solution (Low, Medium, High)")
    alternative_approaches: List[str] = Field(description="Alternative approaches to consider")


# Prompt template for fix suggestions
FIX_PROMPT = """
You are a data quality expert tasked with suggesting fixes for data quality issues.

Here is a summary of a failed data quality check:
Check Name: {check_name}
Check Type: {check_type}
Dataset: {dataset_name}
Failed Rows: {failed_rows}
Failure Percentage: {failure_percentage}%
Timestamp: {timestamp}
Expected Value/Condition: {expected_value}
Actual Value/Finding: {actual_value}

Current Check Implementation:
```
{check_implementation}
```

Sample of problematic data:
```
{sample_data}
```

Based on this information, suggest a fix that would address the data quality issue.
Your answer should include:
1. The approach to fix (schema change, data transformation, threshold adjustment, etc.)
2. A clear explanation of why this fix is appropriate
3. Implementation details or code snippet
4. Your confidence level in this solution
5. Alternative approaches to consider

{format_instructions}
"""

def get_llm():
    """Initialize and return the LLM."""
    try:
        # Try to use ChatOpenAI first
        return ChatOpenAI(
            model=LLM_MODEL,
            temperature=0.2,  # Slightly more creative for suggesting fixes
            openai_api_key=OPENAI_API_KEY,
        )
    except Exception as e:
        logger.warning(f"Failed to initialize ChatOpenAI: {e}. Falling back to OpenAI.")
        # Fallback to OpenAI
        return OpenAI(
            model_name=LLM_MODEL,
            temperature=0.2,
            openai_api_key=OPENAI_API_KEY,
        )

def get_sample_problematic_data(dataset_path: str, check_info: Dict[str, Any]) -> str:
    """
    Extract sample rows that failed validation.
    
    Args:
        dataset_path (str): Path to the dataset
        check_info (Dict[str, Any]): Information about the failing check
        
    Returns:
        str: String representation of sample problematic data
    """
    # This is a placeholder. In a real implementation, you would:
    # 1. Load the dataset
    # 2. Apply filtering based on the check type and conditions
    # 3. Return a sample of rows that failed the check
    
    # For now, just return some mock data
    return """
    transaction_id,customer_id,transaction_date,amount,category,status,location
    1007,C7890,null,null,Electronics,Completed,Chicago
    1011,C9999,,35.40,Books,Completed,Seattle
    1012,C8765,2025-07-05,abc,Electronics,Completed,Denver
    1017,,2025-07-07,75.50,Groceries,Completed,Dallas
    """

def suggest_fixes(validation_results_path: str) -> Dict[str, Any]:
    """
    Suggest fixes for data quality issues based on validation results.
    
    Args:
        validation_results_path (str): Path to the validation results JSON file
        
    Returns:
        Dict[str, Any]: Dictionary containing fix suggestions for each failing check
    """
    logger.info(f"Generating fix suggestions for validation results at: {validation_results_path}")
    
    try:
        # Load validation results
        with open(validation_results_path, 'r') as f:
            validation_results = json.load(f)
        
        # Initialize LLM and output parser
        llm = get_llm()
        parser = PydanticOutputParser(pydantic_object=FixSuggestion)
        
        # Create prompt template with output parser instructions
        prompt = PromptTemplate(
            template=FIX_PROMPT,
            input_variables=[
                "check_name", "check_type", "dataset_name", "failed_rows", 
                "failure_percentage", "timestamp", "expected_value", "actual_value",
                "check_implementation", "sample_data"
            ],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )
        
        # Create LLMChain
        chain = LLMChain(llm=llm, prompt=prompt)
        
        # Generate fix suggestions for each failed check
        fixes = {}
        for check in validation_results.get('failed_checks', []):
            logger.info(f"Generating fix suggestion for check: {check['check_name']}")
            
            # Get sample data that failed the check
            dataset_path = check.get('dataset_path', '')
            sample_data = get_sample_problematic_data(dataset_path, check)
            
            # Extract check information
            check_info = {
                "check_name": check.get('check_name'),
                "check_type": check.get('check_type'),
                "dataset_name": check.get('dataset_name'),
                "failed_rows": check.get('failed_rows', 0),
                "failure_percentage": check.get('failure_percentage', 0),
                "timestamp": check.get('timestamp'),
                "expected_value": str(check.get('expected_value', 'Not specified')),
                "actual_value": str(check.get('actual_value', 'Not specified')),
                "check_implementation": check.get('check_implementation', 'Not available'),
                "sample_data": sample_data
            }
            
            # Run LLM chain
            raw_fix = chain.run(**check_info)
            
            try:
                # Parse the output
                parsed_fix = parser.parse(raw_fix)
                fixes[check['check_name']] = parsed_fix.dict()
            except Exception as e:
                logger.error(f"Failed to parse LLM output: {e}")
                # Store raw output if parsing fails
                fixes[check['check_name']] = {"raw_fix": raw_fix}
        
        # Save fixes to file
        output_dir = os.path.dirname(validation_results_path).replace('validation_results', 'fixes')
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = os.path.join(output_dir, 'fixes.json')
        with open(output_path, 'w') as f:
            json.dump(fixes, f, indent=2)
        
        logger.info(f"Fix suggestions saved to: {output_path}")
        return fixes
        
    except Exception as e:
        logger.error(f"Failed to generate fix suggestions: {e}")
        raise
    
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate fix suggestions for data quality issues")
    parser.add_argument("--results", required=True, help="Path to validation results JSON")
    
    args = parser.parse_args()
    suggest_fixes(args.results)

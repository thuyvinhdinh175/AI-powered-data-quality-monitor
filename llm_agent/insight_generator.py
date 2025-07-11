"""
LLM-based insight generator for data quality issues

This module uses LangChain and LLMs to analyze data quality check results
and generate human-readable insights about what might be causing issues.
"""
import os
import json
from typing import Dict, List, Any, Optional
import logging

from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_openai import OpenAI, ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables (should be set in .env file or environment)
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-3.5-turbo-0613")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your-api-key")

# Output schema for structured LLM responses
class DataQualityInsight(BaseModel):
    """Insight about a data quality issue."""
    check_name: str = Field(description="Name of the failing data quality check")
    issue_description: str = Field(description="Human-readable description of the issue")
    possible_causes: List[str] = Field(description="Potential causes of the data quality issue")
    impact_level: str = Field(description="Impact level (Low, Medium, High, Critical)")
    business_impact: str = Field(description="How this issue might impact business decisions")
    recommended_actions: List[str] = Field(description="Suggested actions to investigate or fix")


# Prompt template for data quality insights
INSIGHT_PROMPT = """
You are a data quality expert analyzing validation results.

Here is a summary of a failed data quality check:
Check Name: {check_name}
Check Type: {check_type}
Dataset: {dataset_name}
Failed Rows: {failed_rows}
Failure Percentage: {failure_percentage}%
Failure Timestamp: {timestamp}
Expected Value/Condition: {expected_value}
Actual Value/Finding: {actual_value}

Based on this information:
1. Explain the issue in a way that a business user would understand
2. List possible causes of this data quality problem
3. Rate the impact level (Low/Medium/High/Critical)
4. Describe how this issue might impact business decisions
5. Recommend specific actions to investigate or fix this issue

{format_instructions}
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

def generate_llm_insights(validation_results_path: str) -> Dict[str, Any]:
    """
    Generate LLM insights for data quality validation results.
    
    Args:
        validation_results_path (str): Path to the validation results JSON file
        
    Returns:
        Dict[str, Any]: Dictionary containing insights for each failing check
    """
    logger.info(f"Generating insights for validation results at: {validation_results_path}")
    
    try:
        # Load validation results
        with open(validation_results_path, 'r') as f:
            validation_results = json.load(f)
        
        # Initialize LLM and output parser
        llm = get_llm()
        parser = PydanticOutputParser(pydantic_object=DataQualityInsight)
        
        # Create prompt template with output parser instructions
        prompt = PromptTemplate(
            template=INSIGHT_PROMPT,
            input_variables=["check_name", "check_type", "dataset_name", "failed_rows", 
                            "failure_percentage", "timestamp", "expected_value", "actual_value"],
            partial_variables={"format_instructions": parser.get_format_instructions()},
        )
        
        # Create LLMChain
        chain = LLMChain(llm=llm, prompt=prompt)
        
        # Generate insights for each failed check
        insights = {}
        for check in validation_results.get('failed_checks', []):
            logger.info(f"Generating insight for check: {check['check_name']}")
            
            # Extract check information
            check_info = {
                "check_name": check.get('check_name'),
                "check_type": check.get('check_type'),
                "dataset_name": check.get('dataset_name'),
                "failed_rows": check.get('failed_rows', 0),
                "failure_percentage": check.get('failure_percentage', 0),
                "timestamp": check.get('timestamp'),
                "expected_value": str(check.get('expected_value', 'Not specified')),
                "actual_value": str(check.get('actual_value', 'Not specified'))
            }
            
            # Run LLM chain
            raw_insight = chain.run(**check_info)
            
            try:
                # Parse the output
                parsed_insight = parser.parse(raw_insight)
                insights[check['check_name']] = parsed_insight.dict()
            except Exception as e:
                logger.error(f"Failed to parse LLM output: {e}")
                # Store raw output if parsing fails
                insights[check['check_name']] = {"raw_insight": raw_insight}
        
        # Save insights to file
        output_dir = os.path.dirname(validation_results_path).replace('validation_results', 'insights')
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = os.path.join(output_dir, 'insights.json')
        with open(output_path, 'w') as f:
            json.dump(insights, f, indent=2)
        
        logger.info(f"Insights saved to: {output_path}")
        return insights
        
    except Exception as e:
        logger.error(f"Failed to generate insights: {e}")
        raise
    
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate LLM insights for data quality results")
    parser.add_argument("--results", required=True, help="Path to validation results JSON")
    
    args = parser.parse_args()
    generate_llm_insights(args.results)

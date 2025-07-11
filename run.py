#!/usr/bin/env python
"""
Main entry point for AI-powered data quality monitor

This script provides a command-line interface for running
data quality checks, generating insights, and starting the dashboard.
"""
import os
import sys
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# Add the project directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_ingestion(args):
    """Run data ingestion."""
    from app.data_ingestion.ingest import ingest
    
    logger.info(f"Running data ingestion from source: {args.source}")
    
    target = args.target
    if not target:
        date_dir = datetime.now().strftime("%Y-%m-%d")
        target = f"data/raw/{date_dir}/"
        os.makedirs(target, exist_ok=True)
    
    result = ingest(args.source, target, args.config)
    
    logger.info(f"Data ingestion complete. Data saved to: {result}")
    return result

def run_validation(args):
    """Run data quality validation."""
    from app.validator.run_checks import run_validation
    
    logger.info(f"Running validation on dataset: {args.dataset} with suite: {args.suite}")
    
    result = run_validation(args.dataset, args.suite)
    
    if result['success']:
        logger.info("Validation passed! All checks successful.")
    else:
        logger.warning(f"Validation failed! {result['statistics']['unsuccessful_expectations']} checks failed.")
    
    return result

def run_insights(args):
    """Generate LLM insights for validation results."""
    from llm_agent.insight_generator import generate_llm_insights
    
    logger.info(f"Generating insights for validation results: {args.results}")
    
    insights = generate_llm_insights(args.results)
    
    logger.info(f"Generated insights for {len(insights)} failed checks.")
    return insights

def run_fixes(args):
    """Generate LLM fix suggestions for validation results."""
    from llm_agent.fix_suggestor import suggest_fixes
    
    logger.info(f"Generating fix suggestions for validation results: {args.results}")
    
    fixes = suggest_fixes(args.results)
    
    logger.info(f"Generated fix suggestions for {len(fixes)} failed checks.")
    return fixes

def run_alerts(args):
    """Send alerts for validation results."""
    from app.alert_manager import send_alerts
    
    logger.info(f"Sending alerts for validation results: {args.results}")
    
    success = send_alerts(args.results, args.insights, args.fixes, args.config)
    
    if success:
        logger.info("Alerts sent successfully.")
    else:
        logger.warning("Failed to send alerts.")
    
    return success

def generate_test_suite(args):
    """Generate Great Expectations test suite from data."""
    from llm_agent.expectation_generator import generate_expectations_config
    
    logger.info(f"Generating test suite for dataset: {args.dataset}")
    
    output_path = args.output
    if not output_path:
        dataset_name = os.path.splitext(os.path.basename(args.dataset))[0]
        output_path = f"expectations/{dataset_name}_suite.yml"
    
    config = generate_expectations_config(args.dataset, output_path)
    
    logger.info(f"Generated test suite saved to: {output_path}")
    return config

def run_dashboard(args):
    """Start the Streamlit dashboard."""
    import streamlit.cli
    
    logger.info("Starting Streamlit dashboard")
    
    sys.argv = ["streamlit", "run", "app/dashboard.py"]
    if args.port:
        sys.argv.extend(["--server.port", str(args.port)])
    
    streamlit.cli.main()
    return True

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="AI-powered data quality monitor")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Ingestion command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest data")
    ingest_parser.add_argument("--source", required=True, help="Source specification (file path, 'api', 'database')")
    ingest_parser.add_argument("--target", help="Target path or name")
    ingest_parser.add_argument("--config", help="Path to configuration file")
    
    # Validation command
    validate_parser = subparsers.add_parser("validate", help="Validate data quality")
    validate_parser.add_argument("--dataset", required=True, help="Path to the dataset file")
    validate_parser.add_argument("--suite", required=True, help="Name of the expectation suite")
    
    # Insights command
    insights_parser = subparsers.add_parser("insights", help="Generate LLM insights")
    insights_parser.add_argument("--results", required=True, help="Path to validation results JSON")
    
    # Fixes command
    fixes_parser = subparsers.add_parser("fixes", help="Generate LLM fix suggestions")
    fixes_parser.add_argument("--results", required=True, help="Path to validation results JSON")
    
    # Alerts command
    alerts_parser = subparsers.add_parser("alerts", help="Send alerts")
    alerts_parser.add_argument("--results", required=True, help="Path to validation results JSON")
    alerts_parser.add_argument("--insights", help="Path to LLM insights JSON")
    alerts_parser.add_argument("--fixes", help="Path to LLM fix suggestions JSON")
    alerts_parser.add_argument("--config", help="Path to configuration file")
    
    # Generate test suite command
    generate_parser = subparsers.add_parser("generate", help="Generate test suite")
    generate_parser.add_argument("--dataset", required=True, help="Path to the dataset file")
    generate_parser.add_argument("--output", help="Path to save the generated configuration")
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser("dashboard", help="Start the Streamlit dashboard")
    dashboard_parser.add_argument("--port", type=int, default=8501, help="Port for the dashboard")
    
    # Pipeline command (runs all steps)
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the full pipeline")
    pipeline_parser.add_argument("--source", required=True, help="Source specification (file path, 'api', 'database')")
    pipeline_parser.add_argument("--suite", required=True, help="Name of the expectation suite")
    pipeline_parser.add_argument("--config", help="Path to configuration file")
    
    args = parser.parse_args()
    
    if args.command == "ingest":
        run_ingestion(args)
    elif args.command == "validate":
        run_validation(args)
    elif args.command == "insights":
        run_insights(args)
    elif args.command == "fixes":
        run_fixes(args)
    elif args.command == "alerts":
        run_alerts(args)
    elif args.command == "generate":
        generate_test_suite(args)
    elif args.command == "dashboard":
        run_dashboard(args)
    elif args.command == "pipeline":
        # Run full pipeline
        logger.info("Running full pipeline")
        
        # Ingest data
        target = f"data/raw/{datetime.now().strftime('%Y-%m-%d')}/data.csv"
        ingest_args = argparse.Namespace(
            source=args.source,
            target=target,
            config=args.config
        )
        dataset_path = run_ingestion(ingest_args)
        
        # Validate data
        validate_args = argparse.Namespace(
            dataset=dataset_path,
            suite=args.suite
        )
        validation_results = run_validation(validate_args)
        results_path = os.path.dirname(dataset_path).replace('raw', 'validation_results')
        results_path = os.path.join(results_path, 'results.json')
        
        # Generate insights
        insights_args = argparse.Namespace(
            results=results_path
        )
        insights = run_insights(insights_args)
        
        # Generate fix suggestions
        fixes_args = argparse.Namespace(
            results=results_path
        )
        fixes = run_fixes(fixes_args)
        
        # Send alerts
        alerts_args = argparse.Namespace(
            results=results_path,
            insights=None,
            fixes=None,
            config=args.config
        )
        run_alerts(alerts_args)
        
        logger.info("Pipeline completed successfully")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

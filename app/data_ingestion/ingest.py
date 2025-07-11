"""
Data ingestion module

This module handles data ingestion from various sources:
- CSV files
- API endpoints
- Database connections
"""
import os
import json
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngester:
    """Class for ingesting data from various sources."""
    
    def __init__(self, 
                target_dir: str = "../data/raw",
                config_path: Optional[str] = "../config.json"):
        """
        Initialize the data ingester.
        
        Args:
            target_dir (str): Directory to save ingested data
            config_path (Optional[str]): Path to configuration file
        """
        self.target_dir = target_dir
        
        # Ensure target directory exists
        os.makedirs(self.target_dir, exist_ok=True)
        
        # Load configuration if provided
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
    
    def ingest_from_file(self, 
                       source_path: str, 
                       target_name: Optional[str] = None) -> str:
        """
        Ingest data from a file.
        
        Args:
            source_path (str): Path to the source file
            target_name (Optional[str]): Name for the target file
            
        Returns:
            str: Path to the ingested data file
        """
        logger.info(f"Ingesting data from file: {source_path}")
        
        try:
            # Determine target file name
            if not target_name:
                target_name = os.path.basename(source_path)
            
            # Create date-based directory
            date_dir = datetime.now().strftime("%Y-%m-%d")
            output_dir = os.path.join(self.target_dir, date_dir)
            os.makedirs(output_dir, exist_ok=True)
            
            # Copy file to target directory
            target_path = os.path.join(output_dir, target_name)
            
            # Handle different file formats
            if source_path.endswith('.csv'):
                df = pd.read_csv(source_path)
                df.to_csv(target_path, index=False)
            elif source_path.endswith('.parquet'):
                df = pd.read_parquet(source_path)
                df.to_parquet(target_path, index=False)
            elif source_path.endswith('.json'):
                df = pd.read_json(source_path)
                df.to_json(target_path, orient='records')
            else:
                # For other file types, just copy the file
                import shutil
                shutil.copyfile(source_path, target_path)
            
            logger.info(f"Data ingested to: {target_path}")
            return target_path
            
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise
    
    def ingest_from_api(self, 
                      api_config: Dict[str, Any],
                      target_name: Optional[str] = None) -> str:
        """
        Ingest data from an API.
        
        Args:
            api_config (Dict[str, Any]): API configuration
            target_name (Optional[str]): Name for the target file
            
        Returns:
            str: Path to the ingested data file
        """
        logger.info(f"Ingesting data from API: {api_config.get('url')}")
        
        try:
            # Extract API configuration
            url = api_config.get('url')
            method = api_config.get('method', 'GET')
            headers = api_config.get('headers', {})
            params = api_config.get('params', {})
            data = api_config.get('data', {})
            auth = None
            
            if api_config.get('auth'):
                auth_config = api_config.get('auth')
                if auth_config.get('type') == 'basic':
                    auth = requests.auth.HTTPBasicAuth(
                        auth_config.get('username'),
                        auth_config.get('password')
                    )
                elif auth_config.get('type') == 'token':
                    headers['Authorization'] = f"Bearer {auth_config.get('token')}"
            
            # Make API request
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data if method in ['POST', 'PUT', 'PATCH'] else None,
                auth=auth
            )
            
            # Check response
            response.raise_for_status()
            
            # Parse response
            if 'application/json' in response.headers.get('Content-Type', ''):
                data = response.json()
            else:
                data = response.text
            
            # Create date-based directory
            date_dir = datetime.now().strftime("%Y-%m-%d")
            output_dir = os.path.join(self.target_dir, date_dir)
            os.makedirs(output_dir, exist_ok=True)
            
            # Determine target file name
            if not target_name:
                target_name = f"api_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Save data to file
            target_path = os.path.join(output_dir, target_name)
            
            # Convert to DataFrame and save
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict) and 'results' in data and isinstance(data['results'], list):
                df = pd.DataFrame(data['results'])
            elif isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                df = pd.DataFrame(data['data'])
            elif isinstance(data, dict):
                # Flatten dictionary for single-row dataframe
                df = pd.DataFrame([data])
            else:
                # Save raw data if can't convert to DataFrame
                with open(target_path, 'w') as f:
                    f.write(str(data))
                logger.info(f"Raw data ingested to: {target_path}")
                return target_path
            
            # Save DataFrame
            if target_name.endswith('.csv'):
                df.to_csv(target_path, index=False)
            elif target_name.endswith('.parquet'):
                df.to_parquet(target_path, index=False)
            else:
                df.to_json(target_path, orient='records')
            
            logger.info(f"Data ingested to: {target_path}")
            return target_path
            
        except Exception as e:
            logger.error(f"API data ingestion failed: {e}")
            raise
    
    def ingest_from_database(self, 
                           db_config: Dict[str, Any],
                           query: str,
                           target_name: Optional[str] = None) -> str:
        """
        Ingest data from a database.
        
        Args:
            db_config (Dict[str, Any]): Database configuration
            query (str): SQL query to execute
            target_name (Optional[str]): Name for the target file
            
        Returns:
            str: Path to the ingested data file
        """
        logger.info(f"Ingesting data from database: {db_config.get('name')}")
        
        try:
            # Extract database configuration
            db_type = db_config.get('type', 'postgresql')
            host = db_config.get('host', 'localhost')
            port = db_config.get('port')
            database = db_config.get('database')
            username = db_config.get('username')
            password = db_config.get('password')
            
            # Build connection string
            if db_type == 'postgresql':
                connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            elif db_type == 'mysql':
                connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
            elif db_type == 'sqlite':
                connection_string = f"sqlite:///{db_config.get('path')}"
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            # Connect to database and execute query
            import sqlalchemy
            engine = sqlalchemy.create_engine(connection_string)
            df = pd.read_sql(query, engine)
            
            # Create date-based directory
            date_dir = datetime.now().strftime("%Y-%m-%d")
            output_dir = os.path.join(self.target_dir, date_dir)
            os.makedirs(output_dir, exist_ok=True)
            
            # Determine target file name
            if not target_name:
                target_name = f"db_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Save data to file
            target_path = os.path.join(output_dir, target_name)
            
            # Save DataFrame
            if target_name.endswith('.csv'):
                df.to_csv(target_path, index=False)
            elif target_name.endswith('.parquet'):
                df.to_parquet(target_path, index=False)
            else:
                df.to_json(target_path, orient='records')
            
            logger.info(f"Data ingested to: {target_path}")
            return target_path
            
        except Exception as e:
            logger.error(f"Database data ingestion failed: {e}")
            raise
    
    def ingest_from_source(self, 
                         source_type: str, 
                         source_config: Dict[str, Any],
                         target_name: Optional[str] = None) -> str:
        """
        Ingest data from a specified source.
        
        Args:
            source_type (str): Type of source ('file', 'api', 'database')
            source_config (Dict[str, Any]): Source configuration
            target_name (Optional[str]): Name for the target file
            
        Returns:
            str: Path to the ingested data file
        """
        if source_type == 'file':
            return self.ingest_from_file(source_config.get('path'), target_name)
        elif source_type == 'api':
            return self.ingest_from_api(source_config, target_name)
        elif source_type == 'database':
            return self.ingest_from_database(
                source_config, 
                source_config.get('query'),
                target_name
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")


def ingest(source: str, target: Optional[str] = None, config_path: Optional[str] = None) -> str:
    """
    Ingest data from a specified source.
    
    Args:
        source (str): Source specification (file path, 'api', 'database')
        target (Optional[str]): Target path or name
        config_path (Optional[str]): Path to configuration file
        
    Returns:
        str: Path to the ingested data file
    """
    ingester = DataIngester(config_path=config_path)
    
    if os.path.exists(source):
        # Source is a file path
        return ingester.ingest_from_file(source, target)
    elif source == 'api':
        # Load API configuration from config
        config = ingester.config.get('api', {})
        return ingester.ingest_from_api(config, target)
    elif source == 'database':
        # Load database configuration from config
        config = ingester.config.get('database', {})
        return ingester.ingest_from_database(config, config.get('query'), target)
    else:
        raise ValueError(f"Invalid source: {source}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest data from various sources")
    parser.add_argument("--source", required=True, help="Source specification (file path, 'api', 'database')")
    parser.add_argument("--target", help="Target path or name")
    parser.add_argument("--config", help="Path to configuration file")
    
    args = parser.parse_args()
    
    ingest(args.source, args.target, args.config)

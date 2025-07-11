"""
Alert manager for data quality issues

This module handles sending alerts for data quality issues
via email, Slack, or webhooks.
"""
import os
import json
import logging
import smtplib
import requests
from email.message import EmailMessage
from typing import Dict, List, Any, Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertManager:
    """Class for sending data quality alerts."""
    
    def __init__(self, config_path: Optional[str] = "../config.json"):
        """
        Initialize the alert manager.
        
        Args:
            config_path (Optional[str]): Path to configuration file
        """
        # Load configuration if provided
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        
        # Extract alert configurations
        self.email_config = self.config.get('alerts', {}).get('email', {})
        self.slack_config = self.config.get('alerts', {}).get('slack', {})
        self.webhook_config = self.config.get('alerts', {}).get('webhook', {})
    
    def send_email_alert(self, 
                       subject: str, 
                       message: str,
                       recipients: Optional[List[str]] = None) -> bool:
        """
        Send an email alert.
        
        Args:
            subject (str): Email subject
            message (str): Email message
            recipients (Optional[List[str]]): List of email recipients
            
        Returns:
            bool: Whether the email was sent successfully
        """
        if not self.email_config or not self.email_config.get('enabled', False):
            logger.warning("Email alerts are not enabled")
            return False
        
        try:
            # Get email configuration
            smtp_server = self.email_config.get('smtp_server', 'smtp.gmail.com')
            smtp_port = self.email_config.get('smtp_port', 587)
            sender_email = self.email_config.get('sender_email')
            sender_password = self.email_config.get('sender_password')
            
            if not recipients:
                recipients = self.email_config.get('recipients', [])
            
            if not sender_email or not sender_password or not recipients:
                logger.error("Incomplete email configuration")
                return False
            
            # Create email message
            msg = EmailMessage()
            msg['Subject'] = subject
            msg['From'] = sender_email
            msg['To'] = ', '.join(recipients)
            msg.set_content(message)
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            logger.info(f"Email alert sent to: {recipients}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def send_slack_alert(self, 
                       message: str,
                       channel: Optional[str] = None) -> bool:
        """
        Send a Slack alert.
        
        Args:
            message (str): Slack message
            channel (Optional[str]): Slack channel
            
        Returns:
            bool: Whether the Slack message was sent successfully
        """
        if not self.slack_config or not self.slack_config.get('enabled', False):
            logger.warning("Slack alerts are not enabled")
            return False
        
        try:
            # Get Slack configuration
            webhook_url = self.slack_config.get('webhook_url')
            token = self.slack_config.get('token')
            
            if not channel:
                channel = self.slack_config.get('channel')
            
            if webhook_url:
                # Use webhook
                payload = {
                    "text": message,
                    "channel": channel
                }
                
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                response.raise_for_status()
                
            elif token:
                # Use Slack API
                from slack import WebClient
                from slack.errors import SlackApiError
                
                client = WebClient(token=token)
                client.chat_postMessage(channel=channel, text=message)
                
            else:
                logger.error("Incomplete Slack configuration")
                return False
            
            logger.info(f"Slack alert sent to: {channel}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_webhook_alert(self, 
                         payload: Dict[str, Any]) -> bool:
        """
        Send a webhook alert.
        
        Args:
            payload (Dict[str, Any]): Webhook payload
            
        Returns:
            bool: Whether the webhook was sent successfully
        """
        if not self.webhook_config or not self.webhook_config.get('enabled', False):
            logger.warning("Webhook alerts are not enabled")
            return False
        
        try:
            # Get webhook configuration
            webhook_url = self.webhook_config.get('url')
            headers = self.webhook_config.get('headers', {})
            
            if not webhook_url:
                logger.error("Incomplete webhook configuration")
                return False
            
            # Send webhook request
            response = requests.post(
                webhook_url,
                json=payload,
                headers=headers
            )
            
            response.raise_for_status()
            
            logger.info(f"Webhook alert sent to: {webhook_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
            return False

def format_alert_message(validation_results: Dict[str, Any], 
                        insights: Dict[str, Any] = None,
                        fixes: Dict[str, Any] = None) -> str:
    """
    Format an alert message from validation results.
    
    Args:
        validation_results (Dict[str, Any]): Validation results
        insights (Dict[str, Any], optional): LLM insights
        fixes (Dict[str, Any], optional): LLM fix suggestions
        
    Returns:
        str: Formatted alert message
    """
    # Extract basic information
    dataset_name = validation_results.get('dataset_name', 'Unknown dataset')
    timestamp = validation_results.get('timestamp', 'Unknown time')
    success = validation_results.get('success', False)
    stats = validation_results.get('statistics', {})
    failed_checks = validation_results.get('failed_checks', [])
    
    # Format message
    message = f"""
    *Data Quality Alert: {dataset_name}*
    Time: {timestamp}
    Status: {'✅ PASSED' if success else '❌ FAILED'}
    
    *Summary:*
    - Total checks: {stats.get('evaluated_expectations', 0)}
    - Passed checks: {stats.get('successful_expectations', 0)}
    - Failed checks: {stats.get('unsuccessful_expectations', 0)}
    - Success rate: {stats.get('success_percent', 0)}%
    
    *Failed Checks:*
    """
    
    # Add failed checks
    for i, check in enumerate(failed_checks):
        message += f"""
    {i+1}. {check.get('check_name', 'Unknown check')}
       - Failed rows: {check.get('failed_rows', 0)} ({check.get('failure_percentage', 0)}%)
       """
        
        # Add insights if available
        if insights and check['check_name'] in insights:
            insight = insights[check['check_name']]
            message += f"""
       - *Insight:* {insight.get('issue_description', 'No insight available')}
       - *Impact:* {insight.get('impact_level', 'Unknown')} - {insight.get('business_impact', 'Unknown')}
       """
        
        # Add fix suggestions if available
        if fixes and check['check_name'] in fixes:
            fix = fixes[check['check_name']]
            message += f"""
       - *Suggested Fix:* {fix.get('fix_approach', 'No suggestion available')}
       - *Rationale:* {fix.get('rationale', 'No rationale available')}
       """
    
    return message

def send_alerts(validation_results_path: str, 
               insights_path: Optional[str] = None,
               fixes_path: Optional[str] = None,
               config_path: Optional[str] = None) -> bool:
    """
    Send alerts for data quality issues.
    
    Args:
        validation_results_path (str): Path to validation results JSON
        insights_path (Optional[str]): Path to LLM insights JSON
        fixes_path (Optional[str]): Path to LLM fix suggestions JSON
        config_path (Optional[str]): Path to configuration file
        
    Returns:
        bool: Whether alerts were sent successfully
    """
    try:
        # Load validation results
        with open(validation_results_path, 'r') as f:
            validation_results = json.load(f)
        
        # Load insights if available
        insights = None
        if insights_path and os.path.exists(insights_path):
            with open(insights_path, 'r') as f:
                insights = json.load(f)
        
        # Load fixes if available
        fixes = None
        if fixes_path and os.path.exists(fixes_path):
            with open(fixes_path, 'r') as f:
                fixes = json.load(f)
        
        # Only send alerts if validation failed
        if validation_results.get('success', True):
            logger.info("Validation passed, no alerts needed")
            return True
        
        # Format alert message
        message = format_alert_message(validation_results, insights, fixes)
        
        # Initialize alert manager
        alert_manager = AlertManager(config_path)
        
        # Send alerts via all configured channels
        email_sent = alert_manager.send_email_alert(
            subject=f"Data Quality Alert: {validation_results.get('dataset_name', 'Unknown dataset')}",
            message=message
        )
        
        slack_sent = alert_manager.send_slack_alert(message=message)
        
        webhook_sent = alert_manager.send_webhook_alert(
            payload={
                "validation_results": validation_results,
                "insights": insights,
                "fixes": fixes,
                "message": message
            }
        )
        
        return email_sent or slack_sent or webhook_sent
        
    except Exception as e:
        logger.error(f"Failed to send alerts: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Send alerts for data quality issues")
    parser.add_argument("--results", required=True, help="Path to validation results JSON")
    parser.add_argument("--insights", help="Path to LLM insights JSON")
    parser.add_argument("--fixes", help="Path to LLM fix suggestions JSON")
    parser.add_argument("--config", help="Path to configuration file")
    
    args = parser.parse_args()
    
    send_alerts(args.results, args.insights, args.fixes, args.config)

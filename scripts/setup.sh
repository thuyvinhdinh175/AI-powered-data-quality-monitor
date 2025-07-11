#!/bin/bash
# Make run.py executable
chmod +x ../run.py

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install dependencies
pip install -r ../requirements.txt

# Initialize Great Expectations
great_expectations init

echo "Setup complete!"
echo "To run the dashboard, use: python ../run.py dashboard"

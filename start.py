#!/usr/bin/env python3
"""
Production startup script for the Email Processor
This ensures proper initialization and automatic startup
"""

import os
import sys
import logging
import signal
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('startup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_environment():
    """Setup environment variables and check requirements"""
    # Ensure required environment variables exist
    required_vars = {
        'WEBHOOK_URL': 'https://your-webhook-url.com',
        'AUTO_START': 'true',
        'CHECK_INTERVAL': '20',
        'MAX_EMAILS': '10'
    }
    
    for var, default in required_vars.items():
        if not os.getenv(var):
            os.environ[var] = default
            logger.info(f"Set default {var}={default}")
    
    # Create necessary directories
    for directory in ['logs', 'data']:
        Path(directory).mkdir(exist_ok=True)
    
    # Check if credentials file exists
    if not os.path.exists('credentials.json'):
        logger.error("credentials.json not found!")
        logger.error("Please upload your Google API credentials file")
        return False
    
    return True

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

def main():
    """Main startup function"""
    # Setup signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Starting Email Processor System...")
    
    # Setup environment
    if not setup_environment():
        sys.exit(1)
    
    # Import and start the app
    try:
        from app import run_app
        logger.info("Starting web application...")
        run_app()
    except ImportError as e:
        logger.error(f"Import error: {e}")
        logger.error("Please ensure all dependencies are installed")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Startup error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
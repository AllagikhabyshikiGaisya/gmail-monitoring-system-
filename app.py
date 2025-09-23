#!/usr/bin/env python3
"""
Main Flask Application for Email Processor
Production-ready version with environment configuration and auto-start
"""

import os
import sys
import json
import threading
import time
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

# Import the updated email processor
from email_processor import GmailAPIProcessor

# Configure logging
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper())
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EmailProcessorService:
    """Service to manage the email processor with proper state management"""
    
    def __init__(self):
        self.processor = None
        self.is_running = False
        self.is_paused = False
        self.processing_thread = None
        self.should_stop = False
        self.config = self.load_config()
        
        # Initialize processor on startup if auto_start is enabled
        if self.config.get('auto_start', False):
            logger.info("Auto-start enabled, initializing processor...")
            self.initialize_processor()
        
    def load_config(self) -> Dict:
        """Load configuration from environment variables with fallbacks"""
        return {
            'check_interval': int(os.getenv('CHECK_INTERVAL_SECONDS', '60')),
            'max_emails': int(os.getenv('MAX_EMAILS_PER_CHECK', '10')),
            'webhook_url': os.getenv('WEBHOOK_URL', ''),
            'credentials_path': os.getenv('GMAIL_CREDENTIALS_PATH', 'credentials.json'),
            'token_path': os.getenv('GMAIL_TOKEN_PATH', 'token.json'),
            'auto_start': os.getenv('AUTO_START', 'false').lower() == 'true',
            'archive_processed': os.getenv('ARCHIVE_PROCESSED_EMAILS', 'true').lower() == 'true'
        }
    
    def initialize_processor(self) -> bool:
        """Initialize the email processor with current config"""
        try:
            # Check required files
            if not os.path.exists(self.config['credentials_path']):
                logger.error(f"Gmail credentials file not found: {self.config['credentials_path']}")
                return False
            
            if not self.config['webhook_url']:
                logger.warning("No webhook URL configured - webhooks will be skipped")
            
            self.processor = GmailAPIProcessor(webhook_url=self.config['webhook_url'])
            
            # Pre-authenticate to avoid repeated auth prompts
            if not self.processor.authenticate():
                logger.error("Failed to authenticate with Gmail API")
                return False
            
            logger.info("Email processor initialized successfully")
            
            # Auto-start if configured
            if self.config.get('auto_start', False):
                return self.start_processing()
            
            return True
            
        except Exception as e:
            logger.error(f"Error initializing processor: {e}")
            return False
    
    def start_processing(self) -> bool:
        """Start the email processing in a separate thread"""
        if self.is_running:
            if self.is_paused:
                return self.resume_processing()
            else:
                logger.warning("Processor is already running")
                return False
        
        if not self.processor and not self.initialize_processor():
            return False
        
        self.is_running = True
        self.is_paused = False
        self.should_stop = False
        
        # Start processing thread
        self.processing_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self.processing_thread.start()
        
        logger.info("Email processing started")
        return True
    
    def pause_processing(self) -> bool:
        """Pause the email processing"""
        if not self.is_running:
            return False
        
        self.is_paused = True
        logger.info("Email processing paused")
        return True
    
    def resume_processing(self) -> bool:
        """Resume the email processing"""
        if not self.is_running or not self.is_paused:
            return False
        
        self.is_paused = False
        logger.info("Email processing resumed")
        return True
    
    def stop_processing(self) -> bool:
        """Stop the email processing"""
        if not self.is_running:
            return False
        
        logger.info("Stopping email processing...")
        self.should_stop = True
        self.is_running = False
        self.is_paused = False
        
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=10)
        
        logger.info("Email processing stopped")
        return True
    
    def run_once(self) -> Dict:
        """Run email processing once and return results"""
        if not self.processor and not self.initialize_processor():
            return {'success': False, 'message': 'Failed to initialize processor', 'processed': 0}
        
        try:
            results = self.processor.process_emails()
            
            return {
                'success': True,
                'message': f"Processed {results['processed']} emails successfully",
                'processed': results['processed'],
                'successful_webhooks': results['successful_webhooks'],
                'failed_webhooks': results['failed_webhooks'],
                'archived': results['archived']
            }
            
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            return {'success': False, 'message': f'Error: {str(e)}', 'processed': 0}
    
    def _processing_loop(self):
        """Main processing loop that runs in a separate thread"""
        logger.info(f"Starting processing loop with {self.config['check_interval']} second intervals")
        
        while self.is_running and not self.should_stop:
            try:
                if not self.is_paused:
                    logger.info("Processing emails...")
                    results = self.processor.process_emails()
                    
                    if results['processed'] > 0:
                        logger.info(f"Processed {results['processed']} emails, "
                                  f"{results['successful_webhooks']} webhooks successful, "
                                  f"{results['archived']} archived")
                
                # Wait for the configured interval (with early exit capability)
                for _ in range(self.config['check_interval']):
                    if self.should_stop or not self.is_running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                # Wait a minute before retrying on error (with early exit)
                for _ in range(60):
                    if self.should_stop or not self.is_running:
                        break
                    time.sleep(1)
        
        logger.info("Processing loop ended")
    
    def get_status(self) -> Dict:
        """Get current processor status"""
        return {
            'running': self.is_running,
            'paused': self.is_paused,
            'initialized': self.processor is not None,
            'config': self.config
        }
    
    def get_recent_emails(self) -> List[Dict]:
        """Get recently processed emails"""
        if self.processor:
            return self.processor.get_recent_emails()
        return []
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        if self.processor:
            return self.processor.get_stats()
        return {
            'total_processed': 0,
            'successful_webhooks': 0,
            'failed_webhooks': 0,
            'today_processed': 0,
            'today_successful': 0,
            'today_failed': 0
        }
    
    def clear_processed_data(self) -> bool:
        """Clear processed email data"""
        try:
            if self.processor:
                return self.processor.clear_processed_data()
            return False
        except Exception as e:
            logger.error(f"Error clearing processed data: {e}")
            return False
    
    def test_connection(self) -> Dict:
        """Test Gmail API and webhook connections"""
        results = {
            'gmail_connection': False,
            'webhook_connection': False,
            'message': ''
        }
        
        try:
            # Test Gmail connection
            if not self.processor:
                if not self.initialize_processor():
                    results['message'] = 'Failed to initialize Gmail processor'
                    return results
            
            # Try to get email list (minimal call)
            test_result = self.processor.service.users().messages().list(
                userId='me', maxResults=1
            ).execute()
            
            if 'messages' in test_result or test_result.get('resultSizeEstimate', 0) >= 0:
                results['gmail_connection'] = True
            
            # Test webhook connection if URL is configured
            if self.config['webhook_url']:
                import requests
                test_data = {'test': True, 'timestamp': datetime.now().isoformat()}
                response = requests.post(
                    self.config['webhook_url'],
                    json=test_data,
                    timeout=10,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code in [200, 201, 202, 204]:
                    results['webhook_connection'] = True
                    results['message'] = 'All connections successful'
                else:
                    results['message'] = f'Webhook returned status {response.status_code}'
            else:
                results['webhook_connection'] = True  # No webhook configured is OK
                results['message'] = 'Gmail connection successful (no webhook configured)'
            
        except Exception as e:
            results['message'] = f'Connection test failed: {str(e)}'
        
        return results


# Initialize the email processor service
processor_service = EmailProcessorService()

# Create Flask app
app = Flask(__name__)
CORS(app)

# Flask Routes
@app.route('/')
def dashboard():
    """Serve the dashboard"""
    return render_template('dashboard.html')

@app.route('/api/status')
def get_status():
    """Get processor status and configuration"""
    try:
        return jsonify(processor_service.get_status())
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/start', methods=['POST'])
def start_processor():
    """Start the email processor"""
    try:
        if processor_service.is_paused:
            success = processor_service.resume_processing()
            message = "プロセッサを再開しました" if success else "プロセッサの再開に失敗しました"
        else:
            success = processor_service.start_processing()
            message = "プロセッサを開始しました" if success else "プロセッサの開始に失敗しました"
        
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error starting processor: {e}")
        return jsonify({'success': False, 'message': f'エラー: {str(e)}'}), 500

@app.route('/api/pause', methods=['POST'])
def pause_processor():
    """Pause the email processor"""
    try:
        success = processor_service.pause_processing()
        message = "プロセッサを一時停止しました" if success else "プロセッサの一時停止に失敗しました"
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error pausing processor: {e}")
        return jsonify({'success': False, 'message': f'エラー: {str(e)}'}), 500

@app.route('/api/stop', methods=['POST'])
def stop_processor():
    """Stop the email processor"""
    try:
        success = processor_service.stop_processing()
        message = "プロセッサを停止しました" if success else "プロセッサの停止に失敗しました"
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error stopping processor: {e}")
        return jsonify({'success': False, 'message': f'エラー: {str(e)}'}), 500

@app.route('/api/run-once', methods=['POST'])
def run_once():
    """Run email processing once"""
    try:
        result = processor_service.run_once()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in run-once: {e}")
        return jsonify({'success': False, 'message': f'エラー: {str(e)}'}), 500

@app.route('/api/stats')
def get_stats():
    """Get processing statistics"""
    try:
        return jsonify(processor_service.get_stats())
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/recent-emails')
def get_recent_emails():
    """Get recently processed emails"""
    try:
        emails = processor_service.get_recent_emails()
        return jsonify(emails)
    except Exception as e:
        logger.error(f"Error getting recent emails: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs')
def get_logs():
    """Get system logs"""
    try:
        logs = []
        log_files = ['app.log', 'email_processor.log']
        
        for log_file in log_files:
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        # Get last 100 lines
                        recent_lines = lines[-100:] if len(lines) > 100 else lines
                        logs.extend([line.strip() for line in recent_lines if line.strip()])
                except Exception as e:
                    logs.append(f"Error reading {log_file}: {e}")
        
        if not logs:
            logs = ["No log data available"]
        
        return jsonify({'logs': logs[-200:]})  # Return last 200 log entries
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """Test Gmail and webhook connections"""
    try:
        result = processor_service.test_connection()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        return jsonify({
            'gmail_connection': False,
            'webhook_connection': False,
            'message': f'接続テストエラー: {str(e)}'
        }), 500

@app.route('/api/clear-processed', methods=['POST'])
def clear_processed():
    """Clear processed email data"""
    try:
        success = processor_service.clear_processed_data()
        message = "処理済みデータをクリアしました" if success else "データのクリアに失敗しました"
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error clearing processed data: {e}")
        return jsonify({'success': False, 'message': f'エラー: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint for deployment platforms"""
    try:
        status = processor_service.get_status()
        return jsonify({
            'status': 'healthy',
            'initialized': status['initialized'],
            'running': status['running'],
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    processor_service.stop_processing()
    sys.exit(0)

def run_app():
    """Run the Flask application with proper configuration"""
    try:
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        logger.info("Starting Email Processor Dashboard...")
        
        # Log configuration
        config = processor_service.config
        logger.info("Configuration:")
        logger.info(f"  Check interval: {config['check_interval']} seconds")
        logger.info(f"  Max emails: {config['max_emails']}")
        logger.info(f"  Webhook URL: {'Configured' if config['webhook_url'] else 'Not configured'}")
        logger.info(f"  Auto start: {config['auto_start']}")
        logger.info(f"  Archive processed: {config['archive_processed']}")
        
        # Determine host and port
        host = os.getenv('HOST', '0.0.0.0')
        port = int(os.getenv('PORT', '5000'))
        debug = os.getenv('DEBUG', 'false').lower() == 'true'
        
        logger.info(f"Dashboard starting on http://{host}:{port}")
        logger.info("Access the dashboard in your web browser to monitor and control email processing")
        
        # Start the Flask app
        app.run(
            host=host, 
            port=port, 
            debug=debug,
            threaded=True,
            use_reloader=False  # Important: prevents double initialization
        )
        
    except KeyboardInterrupt:
        logger.info("Dashboard stopped by user")
        processor_service.stop_processing()
    except Exception as e:
        logger.error(f"Error running app: {e}")
        processor_service.stop_processing()
        raise

if __name__ == '__main__':
    run_app()
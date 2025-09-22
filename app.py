#!/usr/bin/env python3
"""
Enhanced Email Processor with Dashboard
Main application file that integrates Gmail API processing with web dashboard
"""

import os
import sys
import json
import threading
import time
from datetime import datetime
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_cors import CORS
import logging
import traceback

# Import our processor classes
try:
    from email_processor import GmailAPIProcessor, EmailProcessorStats
except ImportError as e:
    print(f"Error importing email_processor: {e}")
    print("Please ensure email_processor.py is in the current directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)
app.secret_key = 'email-processor-secret-key-change-this-in-production'
CORS(app, origins="*")

# Global variables
email_processor = None
stats_manager = None
processor_thread = None
processor_running = False
processor_paused = False
processor_lock = threading.Lock()

class ConfigManager:
    """Enhanced configuration manager with validation"""
    
    def __init__(self):
        self.config_file = 'config.json'
        self.default_config = {
            'credentials_path': 'credentials.json',
            'token_path': 'token.json',
            'webhook_url': 'https://y8xp2r4oy7i.jp.larksuite.com/base/automation/webhook/event/DuuGaDaKVw5FCFhFKogjybwepic',
            'check_interval': 20,
            'max_emails': 10,
            'log_level': 'INFO',
            'filter_keywords': ['lark', 'larksuite', 'イベント', '申込', '問い合わせ', 'フォーム', '不動産'],
            'auto_start': False,
            'webhook_timeout': 30,
            'max_retries': 3
        }
        self.config = self.load_config()
    
    def load_config(self):
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # Merge with defaults for any missing keys
                    for key, value in self.default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            else:
                logger.info("Config file not found, creating default configuration")
                self.config = self.default_config.copy()
                self.save_config()
                return self.config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self.default_config.copy()
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            logger.info("Configuration saved successfully")
            return True
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return False
    
    def get(self, key, default=None):
        """Get configuration value"""
        return self.config.get(key, default)
    
    def set(self, key, value):
        """Set configuration value"""
        self.config[key] = value
        return self.save_config()
    
    def update(self, updates):
        """Update multiple configuration values"""
        self.config.update(updates)
        return self.save_config()
    
    def validate_config(self):
        """Validate current configuration"""
        errors = []
        
        # Check credentials file
        credentials_path = self.get('credentials_path')
        if not os.path.exists(credentials_path):
            errors.append(f"Credentials file not found: {credentials_path}")
        
        # Check webhook URL
        webhook_url = self.get('webhook_url')
        if not webhook_url or not webhook_url.startswith(('http://', 'https://')):
            errors.append("Invalid webhook URL")
        
        # Check intervals
        check_interval = self.get('check_interval', 20)
        if not isinstance(check_interval, int) or check_interval < 5:
            errors.append("Check interval must be at least 5 seconds")
        
        max_emails = self.get('max_emails', 10)
        if not isinstance(max_emails, int) or max_emails < 1:
            errors.append("Max emails must be at least 1")
        
        return errors

# Global configuration manager
config_manager = ConfigManager()

def initialize_processor():
    """Initialize the email processor with error handling"""
    global email_processor, stats_manager
    
    try:
        # Validate configuration first
        config_errors = config_manager.validate_config()
        if config_errors:
            for error in config_errors:
                logger.error(f"Config validation error: {error}")
            return False
        
        # Initialize processor
        email_processor = GmailAPIProcessor(
            credentials_path=config_manager.get('credentials_path'),
            token_path=config_manager.get('token_path'),
            webhook_url=config_manager.get('webhook_url')
        )
        
        # Initialize stats manager
        stats_manager = EmailProcessorStats()
        
        logger.info("Email processor initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing processor: {e}")
        logger.error(traceback.format_exc())
        return False

def processor_worker():
    """Enhanced worker function that runs in background thread"""
    global processor_running, processor_paused
    
    logger.info("Email processor worker started")
    
    while processor_running:
        try:
            if not processor_paused and email_processor:
                with processor_lock:
                    logger.info("Processing emails...")
                    processed = email_processor.process_emails()
                    
                    if processed > 0:
                        stats_manager.update_stats(processed, processed)  # Assume all successful for now
                        logger.info(f"Processed {processed} emails successfully")
                    else:
                        logger.info("No new emails to process")
                
                # Wait for next check
                interval = config_manager.get('check_interval', 20)
                for _ in range(interval):
                    if not processor_running or processor_paused:
                        break
                    time.sleep(1)
            else:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in processor worker: {e}")
            logger.error(traceback.format_exc())
            time.sleep(5)  # Wait before retry
    
    logger.info("Email processor worker stopped")

# Flask routes
@app.route('/')
def index():
    """Main dashboard page"""
    try:
        # Check if dashboard.html exists
        if os.path.exists('dashboard.html'):
            return send_from_directory('.', 'dashboard.html')
        else:
            # Create basic HTML if file doesn't exist
            return """
            <html>
            <head><title>Email Processor</title></head>
            <body>
                <h1>Email Processor Dashboard</h1>
                <p>Dashboard file not found. Please ensure dashboard.html is in the project directory.</p>
                <p><a href="/api/status">API Status</a></p>
            </body>
            </html>
            """
    except Exception as e:
        logger.error(f"Error serving dashboard: {e}")
        return f"Error loading dashboard: {str(e)}", 500

@app.route('/api/status')
def get_status():
    """Get current processor status"""
    try:
        return jsonify({
            'running': processor_running,
            'paused': processor_paused,
            'last_update': datetime.now().isoformat(),
            'initialized': email_processor is not None,
            'config_valid': len(config_manager.validate_config()) == 0
        })
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get processing statistics"""
    try:
        if stats_manager:
            return jsonify(stats_manager.get_stats())
        return jsonify({
            'total_processed': 0,
            'successful_webhooks': 0,
            'failed_webhooks': 0,
            'daily_stats': {}
        })
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/config')
def get_config():
    """Get current configuration"""
    try:
        return jsonify(config_manager.config)
    except Exception as e:
        logger.error(f"Error getting config: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/config', methods=['POST'])
def update_config():
    """Update configuration"""
    try:
        updates = request.json
        if not updates:
            return jsonify({'success': False, 'message': '設定データが見つかりません'}), 400
        
        # Validate updates
        if 'check_interval' in updates:
            if not isinstance(updates['check_interval'], int) or updates['check_interval'] < 5:
                return jsonify({'success': False, 'message': 'チェック間隔は5秒以上である必要があります'}), 400
        
        if 'max_emails' in updates:
            if not isinstance(updates['max_emails'], int) or updates['max_emails'] < 1:
                return jsonify({'success': False, 'message': '最大メール数は1以上である必要があります'}), 400
        
        if 'webhook_url' in updates:
            if not updates['webhook_url'] or not updates['webhook_url'].startswith(('http://', 'https://')):
                return jsonify({'success': False, 'message': '有効なWebhook URLを入力してください'}), 400
        
        if config_manager.update(updates):
            return jsonify({'success': True, 'message': '設定が正常に保存されました'})
        else:
            return jsonify({'success': False, 'message': '設定の保存に失敗しました'}), 500
    except Exception as e:
        logger.error(f"Error updating config: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/start', methods=['POST'])
def start_processor():
    """Start the email processor"""
    global processor_running, processor_paused, processor_thread
    
    try:
        with processor_lock:
            if not email_processor:
                if not initialize_processor():
                    return jsonify({'success': False, 'message': 'プロセッサの初期化に失敗しました'}), 500
            
            if not processor_running:
                processor_running = True
                processor_paused = False
                processor_thread = threading.Thread(target=processor_worker, daemon=True, name="EmailProcessor")
                processor_thread.start()
                logger.info("Email processor started")
                return jsonify({'success': True, 'message': 'プロセッサを開始しました'})
            elif processor_paused:
                processor_paused = False
                logger.info("Email processor resumed")
                return jsonify({'success': True, 'message': 'プロセッサを再開しました'})
            else:
                return jsonify({'success': True, 'message': 'プロセッサは既に実行中です'})
        
    except Exception as e:
        logger.error(f"Error starting processor: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/pause', methods=['POST'])
def pause_processor():
    """Pause the email processor"""
    global processor_paused
    
    try:
        if processor_running and not processor_paused:
            processor_paused = True
            logger.info("Email processor paused")
            return jsonify({'success': True, 'message': 'プロセッサを一時停止しました'})
        elif processor_paused:
            return jsonify({'success': True, 'message': 'プロセッサは既に一時停止中です'})
        else:
            return jsonify({'success': False, 'message': 'プロセッサが実行されていません'})
    except Exception as e:
        logger.error(f"Error pausing processor: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/stop', methods=['POST'])
def stop_processor():
    """Stop the email processor"""
    global processor_running, processor_paused
    
    try:
        if processor_running:
            processor_running = False
            processor_paused = False
            logger.info("Email processor stopped")
            return jsonify({'success': True, 'message': 'プロセッサを停止しました'})
        else:
            return jsonify({'success': True, 'message': 'プロセッサは既に停止しています'})
    except Exception as e:
        logger.error(f"Error stopping processor: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/run-once', methods=['POST'])
def run_once():
    """Run email processing once"""
    try:
        if not email_processor:
            if not initialize_processor():
                return jsonify({'success': False, 'message': 'プロセッサの初期化に失敗しました'}), 500
        
        with processor_lock:
            processed = email_processor.run_once()
        
        if stats_manager:
            stats_manager.update_stats(processed, processed)
        
        return jsonify({
            'success': True, 
            'message': f'{processed}件のメールを処理しました',
            'processed': processed
        })
        
    except Exception as e:
        logger.error(f"Error in run_once: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """Test Gmail and webhook connections"""
    try:
        if not email_processor:
            if not initialize_processor():
                return jsonify({'success': False, 'message': 'プロセッサの初期化に失敗しました'}), 500
        
        # Test Gmail authentication
        gmail_ok = email_processor.authenticate()
        if not gmail_ok:
            return jsonify({'success': False, 'message': 'Gmail API認証に失敗しました'})
        
        # Test webhook connection
        import requests
        webhook_url = config_manager.get('webhook_url')
        try:
            # Send a test ping to webhook
            test_data = {
                "test": True,
                "message": "Connection test from Email Processor",
                "timestamp": datetime.now().isoformat()
            }
            response = requests.post(webhook_url, json=test_data, timeout=10)
            webhook_ok = response.status_code in [200, 201, 204]
            if not webhook_ok:
                logger.warning(f"Webhook test returned status: {response.status_code}")
        except requests.exceptions.Timeout:
            return jsonify({'success': False, 'message': 'Webhook接続がタイムアウトしました'})
        except requests.exceptions.ConnectionError:
            return jsonify({'success': False, 'message': 'Webhookに接続できませんでした'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'Webhookテストエラー: {str(e)}'})
        
        if gmail_ok and webhook_ok:
            return jsonify({'success': True, 'message': 'Gmail API および Webhook の接続テストが成功しました'})
        elif gmail_ok:
            return jsonify({'success': False, 'message': 'Gmail APIは正常ですがWebhookに問題があります'})
        else:
            return jsonify({'success': False, 'message': 'Gmail API接続に問題があります'})
            
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/logs')
def get_logs():
    """Get recent log entries"""
    try:
        logs = []
        log_files = ['app.log', 'email_processor.log']
        
        for log_file in log_files:
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        # Get last 50 lines from each file
                        recent_lines = lines[-50:] if len(lines) > 50 else lines
                        
                        for line in recent_lines:
                            if line.strip():
                                logs.append(f"[{log_file}] {line.strip()}")
                except Exception as e:
                    logs.append(f"[ERROR] Could not read {log_file}: {str(e)}")
        
        # Sort logs by timestamp if possible
        logs.sort()
        
        return jsonify({'logs': logs[-100:]})  # Return last 100 log entries
        
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        return jsonify({'logs': [f"Error loading logs: {str(e)}"]})

@app.route('/api/export-data')
def export_data():
    """Export processing data"""
    try:
        export_data = {
            'config': config_manager.config,
            'stats': stats_manager.get_stats() if stats_manager else {},
            'export_timestamp': datetime.now().isoformat(),
            'system_info': {
                'processor_running': processor_running,
                'processor_paused': processor_paused,
                'processor_initialized': email_processor is not None
            }
        }
        
        return jsonify(export_data)
        
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/clear-processed', methods=['POST'])
def clear_processed():
    """Clear processed emails record"""
    try:
        if email_processor:
            with processor_lock:
                email_processor.processed_emails.clear()
                email_processor.save_processed_emails()
        
        # Also clear processed emails file
        files_to_clear = ['processed_emails.json']
        
        # Clear processed data files
        import glob
        data_files = glob.glob('processed_data_*.json')
        files_to_clear.extend(data_files)
        
        cleared_files = []
        for file in files_to_clear:
            if os.path.exists(file):
                try:
                    os.remove(file)
                    cleared_files.append(file)
                except Exception as e:
                    logger.warning(f"Could not remove {file}: {e}")
        
        return jsonify({
            'success': True, 
            'message': f'処理済みメールの記録をクリアしました ({len(cleared_files)}個のファイル)',
            'cleared_files': cleared_files
        })
        
    except Exception as e:
        logger.error(f"Error clearing processed emails: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/recent-emails')
def get_recent_emails():
    """Get recently processed emails"""
    try:
        recent_emails = []
        
        # Look for recent processed data files
        import glob
        files = glob.glob('processed_data_*.json')
        files.sort(key=os.path.getctime, reverse=True)  # Most recent first
        
        for file in files[:20]:  # Get last 20 processed emails
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    email_meta = data.get('email_metadata', {})
                    recent_emails.append({
                        'subject': email_meta.get('subject', 'No Subject'),
                        'sender': email_meta.get('sender', 'Unknown'),
                        'date': email_meta.get('date', ''),
                        'processed_at': data.get('processed_at', ''),
                        'status': 'processed',
                        'body_length': email_meta.get('body_length', 0),
                        'file': file
                    })
            except Exception as e:
                logger.warning(f"Could not read processed data file {file}: {e}")
                continue
        
        return jsonify({'emails': recent_emails})
        
    except Exception as e:
        logger.error(f"Error getting recent emails: {e}")
        return jsonify({'emails': []})

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'processor_status': {
                'running': processor_running,
                'paused': processor_paused,
                'initialized': email_processor is not None
            },
            'config_status': {
                'loaded': config_manager.config is not None,
                'valid': len(config_manager.validate_config()) == 0
            }
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def create_dashboard_html():
    """Create the dashboard HTML file if it doesn't exist"""
    dashboard_path = 'dashboard.html'
    
    if not os.path.exists(dashboard_path):
        logger.warning(f"Dashboard HTML file not found: {dashboard_path}")
        logger.info("Please copy the dashboard.html content to create the file")
        return False
    return True

def check_dependencies():
    """Check if all required files and dependencies exist"""
    required_files = {
        'credentials.json': 'Gmail API credentials file (download from Google Cloud Console)',
        'email_processor.py': 'Email processor module'
    }
    
    missing_files = []
    for file, description in required_files.items():
        if not os.path.exists(file):
            missing_files.append(f"{file} ({description})")
    
    if missing_files:
        logger.error("Missing required files:")
        for file in missing_files:
            logger.error(f"  - {file}")
        return False
    
    # Check Python dependencies
    try:
        import flask, requests, google.oauth2.credentials
        logger.info("Required Python dependencies are available")
    except ImportError as e:
        logger.error(f"Missing Python dependency: {e}")
        logger.error("Please run: pip install -r requirements.txt")
        return False
    
    return True

def run_app():
    """Function to run the Flask application with comprehensive setup"""
    print("=" * 70)
    print("🚀 Enhanced Email Processor with Dashboard")
    print("=" * 70)
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ Please ensure all required files and dependencies are present.")
        print("Required files:")
        print("  - credentials.json (Gmail API credentials)")
        print("  - email_processor.py (email processing logic)")
        print("  - requirements.txt (Python dependencies)")
        print("\nSetup instructions:")
        print("  1. pip install -r requirements.txt")
        print("  2. Place credentials.json in current directory")
        print("  3. Run this script again")
        return False
    
    # Create dashboard HTML file reference
    create_dashboard_html()
    
    # Validate configuration
    config_errors = config_manager.validate_config()
    if config_errors:
        print("⚠️  Configuration warnings:")
        for error in config_errors:
            print(f"  - {error}")
        print()
    
    # Initialize processor if auto_start is enabled
    if config_manager.get('auto_start', False):
        logger.info("Auto-start enabled, initializing processor...")
        if initialize_processor():
            logger.info("Auto-initialization successful")
        else:
            logger.warning("Auto-initialization failed")
    
    # Display startup information
    print(f"\n📊 Dashboard URL: http://localhost:5000")
    print(f"📧 Gmail credentials: {config_manager.get('credentials_path')}")
    print(f"🔗 Webhook URL: {config_manager.get('webhook_url')}")
    print(f"⏰ Check interval: {config_manager.get('check_interval')} seconds")
    print(f"📨 Max emails per check: {config_manager.get('max_emails')}")
    print("\n🎮 Use the web dashboard to control the email processor")
    print("📋 Monitor logs and statistics through the dashboard")
    print("🔧 Configuration can be updated through the API")
    print("\nPress Ctrl+C to stop the application")
    print("=" * 70)
    
    try:
        # Set Flask configuration
        app.config.update(
            DEBUG=False,  # Set to True for development
            THREADED=True,
            JSON_AS_ASCII=False
        )
        
        # Start the Flask application
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False,
            threaded=True,
            use_reloader=False  # Disable reloader to avoid issues with threading
        )
        return True
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        print("\n👋 Stopping application...")
        
        # Stop processor if running
        global processor_running
        if processor_running:
            processor_running = False
            print("📧 Email processor stopped")
        
        print("✅ Application stopped successfully")
        return True
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        logger.error(traceback.format_exc())
        print(f"\n❌ Application error: {e}")
        return False
        
    finally:
        # Cleanup
        if processor_running:
            processor_running = False
        print("🧹 Cleanup completed")

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {e}")
    logger.error(traceback.format_exc())
    return jsonify({'error': 'An unexpected error occurred'}), 500

if __name__ == '__main__':
    try:
        run_app()
    except KeyboardInterrupt:
        print("\n\n👋 Application interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Critical application error: {e}")
        logger.error(f"Critical error: {e}")
        logger.error(traceback.format_exc())
    finally:
        print("🏁 Application shutdown complete")
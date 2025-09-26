#!/usr/bin/env python3
"""
Main Flask Application for Email Processor
Production-ready version with environment configuration and auto-start
Updated for Render deployment with proper OAuth flow support
"""

import os
import sys
import json
import threading
import time
import signal
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, render_template_string, jsonify, request
from flask_cors import CORS

# Import the updated email processor
from email_processor import EnhancedGmailProcessor 

# Configure logging for Render (no file logging)
log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper())
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only console logging for Render
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
        self.system_logs = []
        self.authentication_status = None
        
        # Initialize processor on startup if auto_start is enabled
        if self.config.get('auto_start', False):
            logger.info("Auto-start enabled, initializing processor...")
            self.log_message("Auto-start enabled, initializing processor...")
            self.initialize_processor()
    
    def log_message(self, message: str):
        """Add message to system logs with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}"
        self.system_logs.append(log_entry)
        
        # Keep only last 200 log entries
        if len(self.system_logs) > 200:
            self.system_logs.pop(0)
        
        logger.info(message)
        
    def load_config(self) -> Dict:
        """Load configuration from environment variables with fallbacks"""
        return {
            'check_interval': int(os.getenv('CHECK_INTERVAL_SECONDS', '60')),
            'max_emails': int(os.getenv('MAX_EMAILS_PER_CHECK', '50')),
            'webhook_url': os.getenv('WEBHOOK_URL', ''),
            'auto_start': os.getenv('AUTO_START', 'false').lower() == 'true',
            'archive_processed': os.getenv('ARCHIVE_PROCESSED_EMAILS', 'true').lower() == 'true'
        }
    
    def initialize_processor(self) -> bool:
        """Initialize the email processor with current config"""
        try:
            # Check for credentials availability
            has_env_creds = bool(os.getenv('GMAIL_CREDENTIALS_JSON') and os.getenv('GMAIL_REFRESH_TOKEN'))
            has_local_creds = os.path.exists(os.getenv('GMAIL_CREDENTIALS_PATH', 'credentials.json'))
            
            if not has_env_creds and not has_local_creds:
                error_msg = "Gmail credentials not found. For production: set GMAIL_CREDENTIALS_JSON and GMAIL_REFRESH_TOKEN. For local: ensure credentials.json exists."
                self.log_message(error_msg)
                logger.error(error_msg)
                self.authentication_status = "missing_credentials"
                return False
            
            if not self.config['webhook_url']:
                self.log_message("No webhook URL configured - webhooks will be skipped")
                logger.warning("No webhook URL configured - webhooks will be skipped")
            
            # Initialize processor
            self.processor = EnhancedGmailProcessor(webhook_url=self.config['webhook_url'])
            
            # Attempt authentication
            auth_success = self.processor.authenticate()
            
            if auth_success:
                self.log_message("Email processor initialized and authenticated successfully")
                logger.info("Email processor initialized and authenticated successfully")
                self.authentication_status = "authenticated"
                
                # Auto-start if configured
                if self.config.get('auto_start', False):
                    return self.start_processing()
                
                return True
            else:
                self.log_message("Failed to authenticate with Gmail API")
                logger.error("Failed to authenticate with Gmail API")
                self.authentication_status = "auth_failed"
                return False
            
        except Exception as e:
            error_msg = f"Error initializing processor: {e}"
            self.log_message(error_msg)
            logger.error(error_msg)
            self.authentication_status = "initialization_error"
            return False
    
    def start_processing(self) -> bool:
        """Start the email processing in a separate thread"""
        if self.is_running:
            if self.is_paused:
                return self.resume_processing()
            else:
                self.log_message("Processor is already running")
                logger.warning("Processor is already running")
                return False
        
        if not self.processor:
            if not self.initialize_processor():
                return False
        elif self.authentication_status != "authenticated":
            if not self.processor.authenticate():
                self.log_message("Authentication required before starting")
                return False
        
        self.is_running = True
        self.is_paused = False
        self.should_stop = False
        
        # Start processing thread
        self.processing_thread = threading.Thread(target=self._processing_loop, daemon=True)
        self.processing_thread.start()
        
        self.log_message("Email processing started")
        logger.info("Email processing started")
        return True
    
    def pause_processing(self) -> bool:
        """Pause the email processing"""
        if not self.is_running:
            return False
        
        self.is_paused = True
        self.log_message("Email processing paused")
        logger.info("Email processing paused")
        return True
    
    def resume_processing(self) -> bool:
        """Resume the email processing"""
        if not self.is_running or not self.is_paused:
            return False
        
        self.is_paused = False
        self.log_message("Email processing resumed")
        logger.info("Email processing resumed")
        return True
    
    def stop_processing(self) -> bool:
        """Stop the email processing"""
        if not self.is_running:
            return False
        
        self.log_message("Stopping email processing...")
        logger.info("Stopping email processing...")
        self.should_stop = True
        self.is_running = False
        self.is_paused = False
        
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=10)
        
        self.log_message("Email processing stopped")
        logger.info("Email processing stopped")
        return True
    
    def run_once(self) -> Dict:
        """Run email processing once and return results"""
        if not self.processor:
            if not self.initialize_processor():
                return {'success': False, 'message': 'Failed to initialize processor', 'processed': 0}
        elif self.authentication_status != "authenticated":
            if not self.processor.authenticate():
                return {'success': False, 'message': 'Authentication failed', 'processed': 0}
        
        try:
            self.log_message("Running one-time email processing...")
            results = self.processor.process_emails()
            
            message = f"Processed {results['processed']} emails, {results['successful_webhooks']} webhooks successful"
            self.log_message(message)
            
            return {
                'success': True,
                'message': message,
                'processed': results['processed'],
                'successful_webhooks': results['successful_webhooks'],
                'failed_webhooks': results['failed_webhooks'],
                'archived': results['archived']
            }
            
        except Exception as e:
            error_msg = f"Error in run_once: {e}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return {'success': False, 'message': error_msg, 'processed': 0}
    
    def _processing_loop(self):
        """Main processing loop that runs in a separate thread"""
        self.log_message(f"Starting processing loop with {self.config['check_interval']} second intervals")
        logger.info(f"Starting processing loop with {self.config['check_interval']} second intervals")
        
        while self.is_running and not self.should_stop:
            try:
                if not self.is_paused:
                    self.log_message("Processing emails...")
                    results = self.processor.process_emails()
                    
                    if results['processed'] > 0:
                        message = f"Processed {results['processed']} emails, {results['successful_webhooks']} webhooks successful, {results['archived']} archived"
                        self.log_message(message)
                        logger.info(message)
                
                # Wait for the configured interval (with early exit capability)
                for _ in range(self.config['check_interval']):
                    if self.should_stop or not self.is_running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                error_msg = f"Error in processing loop: {e}"
                self.log_message(error_msg)
                logger.error(error_msg)
                # Wait a minute before retrying on error (with early exit)
                for _ in range(60):
                    if self.should_stop or not self.is_running:
                        break
                    time.sleep(1)
        
        self.log_message("Processing loop ended")
        logger.info("Processing loop ended")
    
    def get_status(self) -> Dict:
        """Get current processor status"""
        return {
            'running': self.is_running,
            'paused': self.is_paused,
            'initialized': self.processor is not None,
            'authenticated': self.authentication_status == "authenticated",
            'authentication_status': self.authentication_status,
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
                success = self.processor.clear_processed_data()
                if success:
                    self.log_message("Cleared all processed email data")
                return success
            return False
        except Exception as e:
            error_msg = f"Error clearing processed data: {e}"
            self.log_message(error_msg)
            logger.error(error_msg)
            return False
    
    def test_connection(self) -> Dict:
        """Test Gmail API and webhook connections"""
        results = {
            'gmail_connection': False,
            'webhook_connection': False,
            'message': ''
        }
        
        try:
            # Initialize processor if needed
            if not self.processor:
                if not self.initialize_processor():
                    results['message'] = 'Failed to initialize Gmail processor'
                    self.log_message('Connection test failed: Unable to initialize processor')
                    return results
            
            # Test Gmail connection
            if self.authentication_status != "authenticated":
                if not self.processor.authenticate():
                    results['message'] = 'Gmail authentication failed'
                    self.log_message('Connection test failed: Gmail authentication failed')
                    return results
            
            # Try to get email list (minimal call)
            test_result = self.processor.service.users().messages().list(
                userId='me', maxResults=1
            ).execute()
            
            if 'messages' in test_result or test_result.get('resultSizeEstimate', 0) >= 0:
                results['gmail_connection'] = True
                self.log_message('Gmail connection test: SUCCESS')
            
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
                    self.log_message('Webhook connection test: SUCCESS')
                else:
                    results['message'] = f'Webhook returned status {response.status_code}'
                    self.log_message(f'Webhook connection test: FAILED - Status {response.status_code}')
            else:
                results['webhook_connection'] = True  # No webhook configured is OK
                results['message'] = 'Gmail connection successful (no webhook configured)'
                self.log_message('Gmail connection successful, no webhook configured')
            
        except Exception as e:
            results['message'] = f'Connection test failed: {str(e)}'
            self.log_message(f'Connection test failed: {str(e)}')
        
        return results
    
    def setup_oauth(self) -> Dict:
        """Set up OAuth credentials manually"""
        try:
            if not self.processor:
                self.processor = EnhancedGmailProcessor(webhook_url=self.config['webhook_url'])
            
            self.log_message("Starting OAuth setup...")
            
            # Try to authenticate (this will trigger OAuth flow if needed)
            success = self.processor.authenticate()
            
            if success:
                self.authentication_status = "authenticated"
                message = "OAuth setup completed successfully"
                self.log_message(message)
                return {'success': True, 'message': message}
            else:
                self.authentication_status = "auth_failed"
                message = "OAuth setup failed"
                self.log_message(message)
                return {'success': False, 'message': message}
                
        except Exception as e:
            error_msg = f"Error during OAuth setup: {e}"
            self.log_message(error_msg)
            return {'success': False, 'message': error_msg}
    
    def get_system_logs(self) -> List[str]:
        """Get system logs"""
        return self.system_logs[-100:] if self.system_logs else ["No logs available"]


# Initialize the email processor service
processor_service = EmailProcessorService()

# Create Flask app
app = Flask(__name__)
CORS(app)

# Load the dashboard HTML content as a string for Render deployment
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email Processor Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            color: white;
        }
        
        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }
        
        .header p {
            font-size: 1.1rem;
            opacity: 0.9;
        }
        
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.2);
        }
        
        .card h3 {
            color: #4a5568;
            margin-bottom: 15px;
            font-size: 1.3rem;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .status-card {
            text-align: center;
        }
        
        .status-indicator {
            width: 80px;
            height: 80px;
            border-radius: 50%;
            margin: 0 auto 15px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.2rem;
            font-weight: bold;
            color: white;
            transition: all 0.3s ease;
        }
        
        .status-running {
            background: linear-gradient(45deg, #4CAF50, #45a049);
            animation: pulse 2s infinite;
        }
        
        .status-stopped {
            background: linear-gradient(45deg, #f44336, #d32f2f);
        }
        
        .status-paused {
            background: linear-gradient(45deg, #ff9800, #f57c00);
        }
        
        .status-auth-failed {
            background: linear-gradient(45deg, #e91e63, #c2185b);
        }
        
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.05); }
            100% { transform: scale(1); }
        }
        
        .controls {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            justify-content: center;
            margin-top: 20px;
        }
        
        .btn {
            padding: 12px 24px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .btn:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none;
        }
        
        .btn-primary {
            background: linear-gradient(45deg, #2196F3, #1976D2);
            color: white;
        }
        
        .btn-success {
            background: linear-gradient(45deg, #4CAF50, #45a049);
            color: white;
        }
        
        .btn-warning {
            background: linear-gradient(45deg, #ff9800, #f57c00);
            color: white;
        }
        
        .btn-danger {
            background: linear-gradient(45deg, #f44336, #d32f2f);
            color: white;
        }
        
        .btn-secondary {
            background: linear-gradient(45deg, #607D8B, #546E7A);
            color: white;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 15px;
        }
        
        .stat-item {
            text-align: center;
            padding: 15px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 10px;
        }
        
        .stat-number {
            font-size: 1.8rem;
            font-weight: bold;
            color: #2196F3;
        }
        
        .stat-label {
            font-size: 0.85rem;
            color: #6c757d;
            margin-top: 5px;
        }
        
        .config-section {
            margin-bottom: 20px;
        }
        
        .form-group {
            margin-bottom: 15px;
        }
        
        .form-group label {
            display: block;
            margin-bottom: 5px;
            font-weight: 600;
            color: #4a5568;
        }
        
        .form-control {
            width: 100%;
            padding: 10px;
            border: 2px solid #e2e8f0;
            border-radius: 8px;
            font-size: 14px;
            transition: border-color 0.3s ease;
        }
        
        .form-control:focus {
            outline: none;
            border-color: #2196F3;
            box-shadow: 0 0 0 3px rgba(33, 150, 243, 0.1);
        }
        
        .recent-emails {
            max-height: 300px;
            overflow-y: auto;
        }
        
        .email-item {
            padding: 12px;
            border-bottom: 1px solid #eee;
            transition: background-color 0.2s ease;
        }
        
        .email-item:hover {
            background-color: #f8f9fa;
        }
        
        .email-subject {
            font-weight: 600;
            color: #2196F3;
            margin-bottom: 5px;
            word-break: break-word;
        }
        
        .email-meta {
            font-size: 0.85rem;
            color: #6c757d;
        }
        
        .logs-container {
            background: #1a1a1a;
            color: #00ff00;
            padding: 15px;
            border-radius: 8px;
            font-family: 'Courier New', monospace;
            max-height: 250px;
            overflow-y: auto;
            font-size: 11px;
        }
        
        .log-entry {
            margin-bottom: 3px;
            word-break: break-all;
        }
        
        .notification {
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 20px;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            z-index: 1000;
            transform: translateX(100%);
            transition: transform 0.3s ease;
            max-width: 400px;
        }
        
        .notification.show {
            transform: translateX(0);
        }
        
        .notification.success {
            background: linear-gradient(45deg, #4CAF50, #45a049);
        }
        
        .notification.error {
            background: linear-gradient(45deg, #f44336, #d32f2f);
        }
        
        .notification.warning {
            background: linear-gradient(45deg, #ff9800, #f57c00);
        }
        
        .last-update {
            text-align: center;
            color: rgba(255,255,255,0.8);
            font-size: 0.9rem;
            margin-top: 20px;
        }
        
        .loading {
            opacity: 0.6;
            pointer-events: none;
        }
        
        .config-note {
            font-size: 0.8rem;
            color: #6c757d;
            margin-top: 3px;
            font-style: italic;
        }
        
        .auth-status {
            padding: 10px;
            margin-bottom: 15px;
            border-radius: 8px;
            font-weight: 600;
        }
        
        .auth-status.authenticated {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        
        .auth-status.failed {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        
        .auth-status.missing {
            background: #fff3cd;
            color: #856404;
            border: 1px solid #ffeaa7;
        }
        
        @media (max-width: 768px) {
            .container {
                padding: 10px;
            }
            
            .header h1 {
                font-size: 2rem;
            }
            
            .dashboard-grid {
                grid-template-columns: 1fr;
            }
            
            .btn {
                flex: 1;
                min-width: 100px;
            }
            
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📧 Email Processor Dashboard</h1>
            <p>Gmail監視・データ抽出・Webhook送信システム v2.1</p>
        </div>
        
        <div class="dashboard-grid">
            <!-- Status Card -->
            <div class="card status-card">
                <h3>システム状態</h3>
                <div id="authStatus" class="auth-status"></div>
                <div id="statusIndicator" class="status-indicator status-stopped">
                    停止中
                </div>
                <p id="statusText">プロセッサは停止中です</p>
                <div class="controls">
                    <button id="startBtn" class="btn btn-success">開始</button>
                    <button id="pauseBtn" class="btn btn-warning" disabled>一時停止</button>
                    <button id="stopBtn" class="btn btn-danger" disabled>停止</button>
                    <button id="runOnceBtn" class="btn btn-primary">一回実行</button>
                    <button id="setupOauthBtn" class="btn btn-secondary">OAuth設定</button>
                </div>
            </div>
            
            <!-- Statistics Card -->
            <div class="card">
                <h3>📊 処理統計</h3>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div id="totalProcessed" class="stat-number">0</div>
                        <div class="stat-label">総処理数</div>
                    </div>
                    <div class="stat-item">
                        <div id="successfulWebhooks" class="stat-number">0</div>
                        <div class="stat-label">成功</div>
                    </div>
                    <div class="stat-item">
                        <div id="todayProcessed" class="stat-number">0</div>
                        <div class="stat-label">今日</div>
                    </div>
                    <div class="stat-item">
                        <div id="failedWebhooks" class="stat-number">0</div>
                        <div class="stat-label">失敗</div>
                    </div>
                </div>
                <div class="controls">
                    <button id="refreshStatsBtn" class="btn btn-secondary">更新</button>
                    <button id="clearDataBtn" class="btn btn-danger">データクリア</button>
                </div>
            </div>
            
            <!-- Configuration Card -->
            <div class="card">
                <h3>⚙️ 設定</h3>
                <div class="config-section">
                    <div class="form-group">
                        <label>チェック間隔 (秒)</label>
                        <input type="number" id="checkInterval" class="form-control" value="60" min="30" max="3600" readonly>
                        <div class="config-note">環境変数 CHECK_INTERVAL_SECONDS で設定</div>
                    </div>
                    <div class="form-group">
                        <label>最大メール数</label>
                        <input type="number" id="maxEmails" class="form-control" value="50" min="1" max="100" readonly>
                        <div class="config-note">環境変数 MAX_EMAILS_PER_CHECK で設定</div>
                    </div>
                    <div class="form-group">
                        <label>Webhook URL</label>
                        <input type="text" id="webhookUrl" class="form-control" placeholder="環境変数から取得..." readonly>
                        <div class="config-note">環境変数 WEBHOOK_URL で設定</div>
                    </div>
                </div>
                <div class="controls">
                    <button id="testConnectionBtn" class="btn btn-primary">接続テスト</button>
                </div>
            </div>
        </div>
        
        <div class="dashboard-grid">
            <!-- Recent Emails Card -->
            <div class="card">
                <h3>📋 最近の処理済みメール</h3>
                <div id="recentEmails" class="recent-emails">
                    <p style="text-align: center; color: #6c757d; padding: 20px;">
                        処理済みメールはここに表示されます
                    </p>
                </div>
                <div class="controls">
                    <button id="refreshEmailsBtn" class="btn btn-secondary">更新</button>
                </div>
            </div>
            
            <!-- System Logs Card -->
            <div class="card">
                <h3>📝 システムログ</h3>
                <div id="systemLogs" class="logs-container">
                    <div class="log-entry">システムログを読み込み中...</div>
                </div>
                <div class="controls">
                    <button id="refreshLogsBtn" class="btn btn-secondary">ログ更新</button>
                </div>
            </div>
        </div>
        
        <div class="last-update">
            最終更新: <span id="lastUpdate">--</span>
        </div>
    </div>
    
    <div id="notification" class="notification"></div>
    
    <script>
        // Global variables
        let updateInterval;
        let isProcessorRunning = false;
        let isProcessorPaused = false;
        let authenticationStatus = null;
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            loadConfiguration();
            updateStatus();
            updateStats();
            updateRecentEmails();
            updateLogs();
            
            // Start periodic updates every 5 seconds
            updateInterval = setInterval(() => {
                updateStatus();
                updateStats();
                updateRecentEmails();
                updateLogs();
            }, 5000);
            
            // Set up event handlers
            setupEventHandlers();
        });
        
        function setupEventHandlers() {
            document.getElementById('startBtn').addEventListener('click', handleStart);
            document.getElementById('pauseBtn').addEventListener('click', handlePause);
            document.getElementById('stopBtn').addEventListener('click', handleStop);
            document.getElementById('runOnceBtn').addEventListener('click', handleRunOnce);
            document.getElementById('testConnectionBtn').addEventListener('click', handleTestConnection);
            document.getElementById('setupOauthBtn').addEventListener('click', handleSetupOauth);
            document.getElementById('refreshStatsBtn').addEventListener('click', updateStats);
            document.getElementById('refreshEmailsBtn').addEventListener('click', updateRecentEmails);
            document.getElementById('refreshLogsBtn').addEventListener('click', updateLogs);
            document.getElementById('clearDataBtn').addEventListener('click', handleClearData);
        }
        
        // API functions
        async function apiCall(endpoint, method = 'GET', data = null) {
            try {
                const options = {
                    method: method,
                    headers: {
                        'Content-Type': 'application/json'
                    }
                };
                
                if (data) {
                    options.body = JSON.stringify(data);
                }
                
                const response = await fetch(`/api/${endpoint}`, options);
                const result = await response.json();
                
                if (!response.ok) {
                    throw new Error(result.message || `HTTP ${response.status}`);
                }
                
                return result;
            } catch (error) {
                console.error(`API call failed: ${endpoint}`, error);
                showNotification(`API エラー: ${error.message}`, 'error');
                return null;
            }
        }
        
        // Status updates
        async function updateStatus() {
            const status = await apiCall('status');
            if (!status) return;
            
            isProcessorRunning = status.running;
            isProcessorPaused = status.paused;
            authenticationStatus = status.authentication_status;
            
            // Update authentication status display
            const authStatusElement = document.getElementById('authStatus');
            if (status.authenticated) {
                authStatusElement.className = 'auth-status authenticated';
                authStatusElement.textContent = '✓ Gmail認証済み';
            } else if (authenticationStatus === 'missing_credentials') {
                authStatusElement.className = 'auth-status missing';
                authStatusElement.textContent = '⚠️ Gmail認証情報が不足しています';
            } else if (authenticationStatus === 'auth_failed') {
                authStatusElement.className = 'auth-status failed';
                authStatusElement.textContent = '✗ Gmail認証に失敗しました';
            } else {
                authStatusElement.className = 'auth-status missing';
                authStatusElement.textContent = '⚠️ Gmail認証が必要です';
            }
            
            // Update UI
            const indicator = document.getElementById('statusIndicator');
            const text = document.getElementById('statusText');
            const startBtn = document.getElementById('startBtn');
            const pauseBtn = document.getElementById('pauseBtn');
            const stopBtn = document.getElementById('stopBtn');
            const setupOauthBtn = document.getElementById('setupOauthBtn');
            
            if (isProcessorRunning && !isProcessorPaused) {
                indicator.className = 'status-indicator status-running';
                indicator.textContent = '実行中';
                text.textContent = 'プロセッサは実行中です';
                startBtn.disabled = true;
                pauseBtn.disabled = false;
                stopBtn.disabled = false;
            } else if (isProcessorRunning && isProcessorPaused) {
                indicator.className = 'status-indicator status-paused';
                indicator.textContent = '一時停止';
                text.textContent = 'プロセッサは一時停止中です';
                startBtn.disabled = false;
                pauseBtn.disabled = true;
                stopBtn.disabled = false;
            } else if (!status.authenticated) {
                indicator.className = 'status-indicator status-auth-failed';
                indicator.textContent = '認証必要';
                text.textContent = 'Gmail認証が必要です';
                startBtn.disabled = true;
                pauseBtn.disabled = true;
                stopBtn.disabled = true;
            } else {
                indicator.className = 'status-indicator status-stopped';
                indicator.textContent = '停止中';
                text.textContent = 'プロセッサは停止中です';
                startBtn.disabled = false;
                pauseBtn.disabled = true;
                stopBtn.disabled = true;
            }
            
            // Enable OAuth setup button if authentication is needed
            setupOauthBtn.disabled = status.authenticated;
            
            document.getElementById('lastUpdate').textContent = new Date().toLocaleString('ja-JP');
        }
        
        async function updateStats() {
            const stats = await apiCall('stats');
            if (!stats) return;
            
            document.getElementById('totalProcessed').textContent = stats.total_processed || 0;
            document.getElementById('successfulWebhooks').textContent = stats.successful_webhooks || 0;
            document.getElementById('failedWebhooks').textContent = stats.failed_webhooks || 0;
            document.getElementById('todayProcessed').textContent = stats.today_processed || 0;
        }
        
        async function updateRecentEmails() {
            const emails = await apiCall('recent-emails');
            if (!emails) return;
            
            const container = document.getElementById('recentEmails');
            
            if (emails.length > 0) {
                container.innerHTML = emails.map(email => `
                    <div class="email-item">
                        <div class="email-subject">${escapeHtml(email.subject || 'No Subject')}</div>
                        <div class="email-meta">
                            送信者: ${escapeHtml(email.sender || 'Unknown')} | 
                            処理日時: ${email.processed_date || 'Unknown'}
                            ${email.webhook_sent ? ' | ✓ Webhook送信済み' : ' | ✗ Webhook失敗'}
                        </div>
                    </div>
                `).join('');
            } else {
                container.innerHTML = '<p style="text-align: center; color: #6c757d; padding: 20px;">処理済みメールはありません</p>';
            }
        }
        
        async function updateLogs() {
            const result = await apiCall('logs');
            if (!result) return;
            
            const container = document.getElementById('systemLogs');
            
            if (result.logs && result.logs.length > 0) {
                container.innerHTML = result.logs.slice(-50).map(log => 
                    `<div class="log-entry">${escapeHtml(log)}</div>`
                ).join('');
                container.scrollTop = container.scrollHeight;
            } else {
                container.innerHTML = '<div class="log-entry">ログデータが見つかりません</div>';
            }
        }
        
        async function loadConfiguration() {
            const status = await apiCall('status');
            if (!status || !status.config) return;
            
            const config = status.config;
            document.getElementById('checkInterval').value = config.check_interval || 60;
            document.getElementById('maxEmails').value = config.max_emails || 50;
            document.getElementById('webhookUrl').value = config.webhook_url ? 
                config.webhook_url.substring(0, 50) + '...' : '未設定';
        }
        
        // Event handlers
        async function handleStart() {
            const result = await apiCall('start', 'POST');
            if (result) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
        }
        
        async function handlePause() {
            const result = await apiCall('pause', 'POST');
            if (result) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
        }
        
        async function handleStop() {
            const result = await apiCall('stop', 'POST');
            if (result) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
        }
        
        async function handleRunOnce() {
            const btn = document.getElementById('runOnceBtn');
            btn.disabled = true;
            btn.textContent = '実行中...';
            
            const result = await apiCall('run-once', 'POST');
            if (result) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStats();
                updateRecentEmails();
            }
            
            btn.disabled = false;
            btn.textContent = '一回実行';
        }
        
        async function handleSetupOauth() {
            const btn = document.getElementById('setupOauthBtn');
            btn.disabled = true;
            btn.textContent = '設定中...';
            
            showNotification('OAuth設定を開始しています...', 'warning');
            
            const result = await apiCall('setup-oauth', 'POST');
            if (result) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
            
            btn.disabled = false;
            btn.textContent = 'OAuth設定';
        }
        
        async function handleTestConnection() {
            const btn = document.getElementById('testConnectionBtn');
            btn.disabled = true;
            btn.textContent = 'テスト中...';
            
            const result = await apiCall('test-connection', 'POST');
            if (result) {
                const success = result.gmail_connection && result.webhook_connection;
                showNotification(result.message, success ? 'success' : 'warning');
            }
            
            btn.disabled = false;
            btn.textContent = '接続テスト';
        }
        
        async function handleClearData() {
            if (confirm('すべての処理済みデータを削除しますか？この操作は取り消せません。')) {
                const result = await apiCall('clear-processed', 'POST');
                if (result) {
                    showNotification(result.message, result.success ? 'success' : 'error');
                    updateStats();
                    updateRecentEmails();
                }
            }
        }
        
        // Utility functions
        function showNotification(message, type) {
            const notification = document.getElementById('notification');
            notification.textContent = message;
            notification.className = `notification ${type}`;
            notification.classList.add('show');
            
            setTimeout(() => {
                notification.classList.remove('show');
            }, 4000);
        }
        
        function escapeHtml(text) {
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        // Cleanup
        window.addEventListener('beforeunload', () => {
            if (updateInterval) {
                clearInterval(updateInterval);
            }
        });
    </script>
</body>
</html>'''

# Flask Routes
@app.route('/')
def dashboard():
    """Serve the dashboard"""
    return DASHBOARD_HTML

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

@app.route('/api/setup-oauth', methods=['POST'])
def setup_oauth():
    """Set up OAuth credentials"""
    try:
        result = processor_service.setup_oauth()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in setup-oauth: {e}")
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
        logs = processor_service.get_system_logs()
        return jsonify({'logs': logs})
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
            'authenticated': status.get('authenticated', False),
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
        
        # Determine host and port for Render
        host = os.getenv('HOST', '0.0.0.0')
        port = int(os.getenv('PORT', '10000'))  # Render uses PORT environment variable
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
#!/usr/bin/env python3
"""
Flask Web Application for Email Processor Dashboard
Production-ready version with proper authentication handling and automatic startup
"""

import os
import sys
import json
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

from flask import Flask, render_template_string, jsonify, request, send_from_directory
from flask_cors import CORS

# Import the email processor
from email_processor import GmailAPIProcessor, EmailProcessorStats

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

class EmailProcessorService:
    """Service to manage the email processor with proper state management"""
    
    def __init__(self):
        self.processor = None
        self.stats = EmailProcessorStats()
        self.is_running = False
        self.is_paused = False
        self.processing_thread = None
        self.config = self.load_config()
        self.last_processed_emails = []
        
    def load_config(self) -> Dict:
        """Load configuration from file or environment variables"""
        config_file = 'config.json'
        default_config = {
            'check_interval': int(os.getenv('CHECK_INTERVAL', 20)),
            'max_emails': int(os.getenv('MAX_EMAILS', 10)),
            'webhook_url': os.getenv('WEBHOOK_URL', ''),
            'credentials_path': os.getenv('CREDENTIALS_PATH', 'credentials.json'),
            'token_path': os.getenv('TOKEN_PATH', 'token.json'),
            'auto_start': os.getenv('AUTO_START', 'false').lower() == 'true'
        }
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                # Merge with defaults
                for key, value in default_config.items():
                    if key not in config:
                        config[key] = value
                return config
        except Exception as e:
            logger.warning(f"Could not load config file: {e}")
        
        return default_config
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open('config.json', 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Could not save config: {e}")
    
    def initialize_processor(self) -> bool:
        """Initialize the email processor with current config"""
        try:
            # Check required files
            if not os.path.exists(self.config['credentials_path']):
                logger.error(f"Credentials file not found: {self.config['credentials_path']}")
                return False
            
            if not self.config['webhook_url']:
                logger.error("Webhook URL is required")
                return False
            
            self.processor = GmailAPIProcessor(
                credentials_path=self.config['credentials_path'],
                token_path=self.config['token_path'],
                webhook_url=self.config['webhook_url']
            )
            
            # Pre-authenticate to avoid repeated auth prompts
            if not self.processor.authenticate():
                logger.error("Failed to authenticate with Gmail API")
                return False
            
            logger.info("Email processor initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing processor: {e}")
            return False
    
    def start_processing(self) -> bool:
        """Start the email processing in a separate thread"""
        if self.is_running:
            logger.warning("Processor is already running")
            return False
        
        if not self.processor and not self.initialize_processor():
            return False
        
        self.is_running = True
        self.is_paused = False
        
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
        
        self.is_running = False
        self.is_paused = False
        
        if self.processing_thread:
            self.processing_thread.join(timeout=5)
        
        logger.info("Email processing stopped")
        return True
    
    def run_once(self) -> Dict:
        """Run email processing once and return results"""
        if not self.processor and not self.initialize_processor():
            return {'success': False, 'message': 'Failed to initialize processor', 'processed': 0}
        
        try:
            processed = self.processor.process_emails()
            self.stats.update_stats(processed, processed)  # Assume all successful for simplicity
            
            # Update recent emails list
            self._update_recent_emails()
            
            return {
                'success': True, 
                'message': f'Processed {processed} emails successfully',
                'processed': processed
            }
            
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            return {'success': False, 'message': f'Error: {str(e)}', 'processed': 0}
    
    def _processing_loop(self):
        """Main processing loop that runs in a separate thread"""
        logger.info(f"Starting processing loop with {self.config['check_interval']} second intervals")
        
        while self.is_running:
            try:
                if not self.is_paused:
                    logger.info("Processing emails...")
                    processed = self.processor.process_emails()
                    
                    if processed > 0:
                        self.stats.update_stats(processed, processed)
                        self._update_recent_emails()
                        logger.info(f"Processed {processed} emails in this cycle")
                    
                # Wait for the configured interval
                for _ in range(self.config['check_interval']):
                    if not self.is_running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                time.sleep(60)  # Wait a minute before retrying on error
    
    def _update_recent_emails(self):
        """Update the list of recently processed emails"""
        try:
            # Read processed emails from the processor's stored data
            if hasattr(self.processor, 'processed_emails') and self.processor.processed_emails:
                # Get recent processed files
                processed_files = []
                for filename in os.listdir('.'):
                    if filename.startswith('processed_data_') and filename.endswith('.json'):
                        try:
                            with open(filename, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                processed_files.append({
                                    'filename': filename,
                                    'data': data,
                                    'processed_at': data.get('processed_at', ''),
                                    'subject': data.get('email_metadata', {}).get('subject', 'No Subject'),
                                    'sender': data.get('email_metadata', {}).get('sender', 'Unknown'),
                                    'date': data.get('email_metadata', {}).get('date', '')
                                })
                        except Exception as e:
                            logger.warning(f"Could not read processed file {filename}: {e}")
                
                # Sort by processed_at and keep only recent ones
                processed_files.sort(key=lambda x: x.get('processed_at', ''), reverse=True)
                self.last_processed_emails = processed_files[:20]  # Keep last 20
                
        except Exception as e:
            logger.error(f"Error updating recent emails: {e}")
    
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
        self._update_recent_emails()
        return self.last_processed_emails
    
    def clear_processed_data(self) -> bool:
        """Clear processed email data"""
        try:
            # Clear processed emails set
            if self.processor:
                self.processor.processed_emails.clear()
                self.processor.save_processed_emails()
            
            # Remove processed data files
            for filename in os.listdir('.'):
                if filename.startswith('processed_data_') and filename.endswith('.json'):
                    os.remove(filename)
            
            # Reset stats
            self.stats.stats = {
                'total_processed': 0,
                'successful_webhooks': 0,
                'failed_webhooks': 0,
                'last_run': None,
                'daily_stats': {},
                'created_at': datetime.now().isoformat()
            }
            self.stats.save_stats()
            
            self.last_processed_emails = []
            logger.info("Processed data cleared")
            return True
            
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
            
            # Test webhook connection
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
            
        except Exception as e:
            results['message'] = f'Connection test failed: {str(e)}'
        
        return results


# Initialize the email processor service
processor_service = EmailProcessorService()

# Create Flask app
app = Flask(__name__)
CORS(app)

# Dashboard HTML template
DASHBOARD_HTML = '''
<!DOCTYPE html>
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
            font-size: 1.5rem;
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
        
        .btn:hover {
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
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
        }
        
        .stat-item {
            text-align: center;
            padding: 15px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 10px;
        }
        
        .stat-number {
            font-size: 2rem;
            font-weight: bold;
            color: #2196F3;
        }
        
        .stat-label {
            font-size: 0.9rem;
            color: #6c757d;
            margin-top: 5px;
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
        
        .logs-container {
            background: #1a1a1a;
            color: #00ff00;
            padding: 20px;
            border-radius: 10px;
            font-family: 'Courier New', monospace;
            max-height: 400px;
            overflow-y: auto;
            margin-top: 15px;
        }
        
        .log-entry {
            margin-bottom: 5px;
            font-size: 12px;
        }
        
        .recent-emails {
            max-height: 300px;
            overflow-y: auto;
        }
        
        .email-item {
            padding: 10px;
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
        }
        
        .email-meta {
            font-size: 0.9rem;
            color: #6c757d;
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
        
        .last-update {
            text-align: center;
            color: #6c757d;
            font-size: 0.9rem;
            margin-top: 20px;
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
            
            .controls {
                justify-content: center;
            }
            
            .btn {
                flex: 1;
                min-width: 120px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Email Processor Dashboard</h1>
            <p>Gmail監視・データ抽出・Webhook送信システム</p>
        </div>
        
        <div class="dashboard-grid">
            <!-- Status Card -->
            <div class="card status-card">
                <h3>システム状態</h3>
                <div id="statusIndicator" class="status-indicator status-stopped">
                    停止
                </div>
                <p id="statusText">プロセッサは停止中です</p>
                <div class="controls">
                    <button id="startBtn" class="btn btn-success">開始</button>
                    <button id="pauseBtn" class="btn btn-warning" disabled>一時停止</button>
                    <button id="stopBtn" class="btn btn-danger" disabled>停止</button>
                    <button id="runOnceBtn" class="btn btn-primary">一回実行</button>
                </div>
            </div>
            
            <!-- Statistics Card -->
            <div class="card">
                <h3>処理統計</h3>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div id="totalProcessed" class="stat-number">0</div>
                        <div class="stat-label">処理済み</div>
                    </div>
                    <div class="stat-item">
                        <div id="successfulWebhooks" class="stat-number">0</div>
                        <div class="stat-label">成功</div>
                    </div>
                    <div class="stat-item">
                        <div id="failedWebhooks" class="stat-number">0</div>
                        <div class="stat-label">失敗</div>
                    </div>
                </div>
            </div>
            
            <!-- Configuration Card -->
            <div class="card">
                <h3>設定</h3>
                <div class="form-group">
                    <label>チェック間隔 (秒)</label>
                    <input type="number" id="checkInterval" class="form-control" value="20" min="5" max="3600">
                </div>
                <div class="form-group">
                    <label>最大メール数</label>
                    <input type="number" id="maxEmails" class="form-control" value="10" min="1" max="100">
                </div>
                <div class="form-group">
                    <label>Webhook URL</label>
                    <input type="url" id="webhookUrl" class="form-control" placeholder="https://...">
                </div>
                <div class="controls">
                    <button id="saveConfigBtn" class="btn btn-primary">設定保存</button>
                    <button id="testConnectionBtn" class="btn btn-secondary">接続テスト</button>
                </div>
            </div>
        </div>
        
        <div class="dashboard-grid">
            <!-- Recent Emails Card -->
            <div class="card">
                <h3>最近の処理済みメール</h3>
                <div id="recentEmails" class="recent-emails">
                    <p style="text-align: center; color: #6c757d; padding: 20px;">
                        処理済みメールはここに表示されます
                    </p>
                </div>
                <div class="controls">
                    <button id="refreshEmailsBtn" class="btn btn-secondary">更新</button>
                    <button id="clearProcessedBtn" class="btn btn-danger">履歴クリア</button>
                    <button id="exportDataBtn" class="btn btn-primary">データ出力</button>
                </div>
            </div>
            
            <!-- System Logs Card -->
            <div class="card">
                <h3>システムログ</h3>
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
        
        // DOM elements
        const statusIndicator = document.getElementById('statusIndicator');
        const statusText = document.getElementById('statusText');
        const startBtn = document.getElementById('startBtn');
        const pauseBtn = document.getElementById('pauseBtn');
        const stopBtn = document.getElementById('stopBtn');
        const runOnceBtn = document.getElementById('runOnceBtn');
        const lastUpdate = document.getElementById('lastUpdate');
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            loadConfiguration();
            updateStatus();
            updateStats();
            updateRecentEmails();
            updateLogs();
            
            // Start periodic updates
            updateInterval = setInterval(() => {
                updateStatus();
                updateStats();
                updateRecentEmails();
                updateLogs();
            }, 5000);
        });
        
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
            
            // Update UI
            if (isProcessorRunning && !isProcessorPaused) {
                statusIndicator.className = 'status-indicator status-running';
                statusIndicator.textContent = '実行中';
                statusText.textContent = 'プロセッサは実行中です';
                startBtn.disabled = true;
                pauseBtn.disabled = false;
                stopBtn.disabled = false;
            } else if (isProcessorRunning && isProcessorPaused) {
                statusIndicator.className = 'status-indicator status-paused';
                statusIndicator.textContent = '一時停止';
                statusText.textContent = 'プロセッサは一時停止中です';
                startBtn.disabled = false;
                pauseBtn.disabled = true;
                stopBtn.disabled = false;
            } else {
                statusIndicator.className = 'status-indicator status-stopped';
                statusIndicator.textContent = '停止';
                statusText.textContent = 'プロセッサは停止中です';
                startBtn.disabled = false;
                pauseBtn.disabled = true;
                stopBtn.disabled = true;
            }
            
            lastUpdate.textContent = new Date().toLocaleString('ja-JP');
        }
        
        async function updateStats() {
            const stats = await apiCall('stats');
            if (!stats) return;
            
            document.getElementById('totalProcessed').textContent = stats.total_processed || 0;
            document.getElementById('successfulWebhooks').textContent = stats.successful_webhooks || 0;
            document.getElementById('failedWebhooks').textContent = stats.failed_webhooks || 0;
        }
        
        async function updateRecentEmails() {
            const result = await apiCall('recent-emails');
            if (!result) return;
            
            const container = document.getElementById('recentEmails');
            
            if (result.emails && result.emails.length > 0) {
                container.innerHTML = result.emails.map(email => `
                    <div class="email-item">
                        <div class="email-subject">${escapeHtml(email.subject || 'No Subject')}</div>
                        <div class="email-meta">
                            送信者: ${escapeHtml(email.sender || 'Unknown')} | 
                            日時: ${email.date || 'Unknown'} | 
                            処理日時: ${email.processed_at || 'Unknown'}
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
                container.innerHTML = result.logs.map(log => 
                    `<div class="log-entry">${escapeHtml(log)}</div>`
                ).join('');
                container.scrollTop = container.scrollHeight;
            } else {
                container.innerHTML = '<div class="log-entry">ログデータが見つかりません</div>';
            }
        }
        
        async function loadConfiguration() {
            const config = await apiCall('config');
            if (!config) return;
            
            document.getElementById('checkInterval').value = config.check_interval || 20;
            document.getElementById('maxEmails').value = config.max_emails || 10;
            document.getElementById('webhookUrl').value = config.webhook_url || '';
        }
        
        // Event handlers
        startBtn.addEventListener('click', async () => {
            const result = await apiCall('start', 'POST');
            if (result && result.success !== undefined) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
        });
        
        pauseBtn.addEventListener('click', async () => {
            const result = await apiCall('pause', 'POST');
            if (result && result.success !== undefined) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
        });
        
        stopBtn.addEventListener('click', async () => {
            const result = await apiCall('stop', 'POST');
            if (result && result.success !== undefined) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStatus();
            }
        });
        
        runOnceBtn.addEventListener('click', async () => {
            runOnceBtn.disabled = true;
            runOnceBtn.textContent = '実行中...';
            
            const result = await apiCall('run-once', 'POST');
            if (result && result.success !== undefined) {
                showNotification(result.message, result.success ? 'success' : 'error');
                updateStats();
                updateRecentEmails();
            }
            
            runOnceBtn.disabled = false;
            runOnceBtn.textContent = '一回実行';
        });
        
        document.getElementById('saveConfigBtn').addEventListener('click', async () => {
            const config = {
                check_interval: parseInt(document.getElementById('checkInterval').value),
                max_emails: parseInt(document.getElementById('maxEmails').value),
                webhook_url: document.getElementById('webhookUrl').value
            };
            
            const result = await apiCall('config', 'POST', config);
            if (result && result.success !== undefined) {
                showNotification(result.message, result.success ? 'success' : 'error');
            }
        });
        
        document.getElementById('testConnectionBtn').addEventListener('click', async () => {
            const btn = document.getElementById('testConnectionBtn');
            btn.disabled = true;
            btn.textContent = 'テスト中...';
            
            const result = await apiCall('test-connection', 'POST');
            if (result) {
                const success = result.gmail_connection && result.webhook_connection;
                showNotification(result.message, success ? 'success' : 'error');
            }
            
            btn.disabled = false;
            btn.textContent = '接続テスト';
        });
        
        document.getElementById('refreshEmailsBtn').addEventListener('click', updateRecentEmails);
        document.getElementById('refreshLogsBtn').addEventListener('click', updateLogs);
        
        document.getElementById('clearProcessedBtn').addEventListener('click', async () => {
            if (confirm('処理済みメールの履歴をクリアしますか？')) {
                const result = await apiCall('clear-processed', 'POST');
                if (result && result.success !== undefined) {
                    showNotification(result.message, result.success ? 'success' : 'error');
                    updateRecentEmails();
                    updateStats();
                }
            }
        });
        
        document.getElementById('exportDataBtn').addEventListener('click', async () => {
            const result = await apiCall('export-data');
            if (result) {
                const dataStr = JSON.stringify(result, null, 2);
                const dataBlob = new Blob([dataStr], {type: 'application/json'});
                const url = URL.createObjectURL(dataBlob);
                const link = document.createElement('a');
                link.href = url;
                link.download = `email_processor_data_${new Date().toISOString().split('T')[0]}.json`;
                link.click();
                URL.revokeObjectURL(url);
                showNotification('データをエクスポートしました', 'success');
            }
        });
        
        // Utility functions
        function showNotification(message, type) {
            const notification = document.getElementById('notification');
            notification.textContent = message;
            notification.className = `notification ${type}`;
            notification.classList.add('show');
            
            setTimeout(() => {
                notification.classList.remove('show');
            }, 3000);
        }
        
        function escapeHtml(text) {
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', () => {
            if (updateInterval) {
                clearInterval(updateInterval);
            }
        });
    </script>
</body>
</html>
'''

# Routes
@app.route('/')
def dashboard():
    """Serve the dashboard"""
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/status')
def get_status():
    """Get processor status"""
    try:
        status = processor_service.get_status()
        return jsonify(status)
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
        stats = processor_service.stats.get_stats()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/recent-emails')
def get_recent_emails():
    """Get recently processed emails"""
    try:
        emails = processor_service.get_recent_emails()
        return jsonify({'emails': emails})
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
                        # Get last 50 lines
                        recent_lines = lines[-50:] if len(lines) > 50 else lines
                        logs.extend([line.strip() for line in recent_lines if line.strip()])
                except Exception as e:
                    logs.append(f"Error reading {log_file}: {e}")
        
        if not logs:
            logs = ["No log data available"]
        
        return jsonify({'logs': logs[-100:]})  # Return last 100 log entries
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/config', methods=['GET', 'POST'])
def handle_config():
    """Get or update configuration"""
    try:
        if request.method == 'GET':
            return jsonify(processor_service.config)
        else:
            # Update configuration
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'message': 'No data provided'}), 400
            
            # Validate data
            if 'check_interval' in data:
                processor_service.config['check_interval'] = max(5, min(3600, int(data['check_interval'])))
            if 'max_emails' in data:
                processor_service.config['max_emails'] = max(1, min(100, int(data['max_emails'])))
            if 'webhook_url' in data:
                processor_service.config['webhook_url'] = str(data['webhook_url']).strip()
            
            processor_service.save_config()
            return jsonify({'success': True, 'message': '設定を保存しました'})
    
    except Exception as e:
        logger.error(f"Error handling config: {e}")
        return jsonify({'success': False, 'message': f'エラー: {str(e)}'}), 500

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

@app.route('/api/export-data')
def export_data():
    """Export all processed data"""
    try:
        # Collect all data for export
        export_data = {
            'stats': processor_service.stats.get_stats(),
            'config': processor_service.config,
            'recent_emails': processor_service.get_recent_emails(),
            'export_timestamp': datetime.now().isoformat()
        }
        
        # Add processed files data
        processed_files = []
        for filename in os.listdir('.'):
            if filename.startswith('processed_data_') and filename.endswith('.json'):
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        processed_files.append(data)
                except Exception as e:
                    logger.warning(f"Could not read {filename}: {e}")
        
        export_data['processed_files'] = processed_files
        return jsonify(export_data)
        
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        return jsonify({'error': str(e)}), 500

def run_app():
    """Run the Flask application with auto-start capability"""
    try:
        logger.info("Starting Email Processor Dashboard...")
        
        # Auto-start processor if configured
        if processor_service.config.get('auto_start', False):
            logger.info("Auto-start is enabled, starting processor...")
            if processor_service.start_processing():
                logger.info("Processor auto-started successfully")
            else:
                logger.warning("Failed to auto-start processor")
        
        # Determine host and port
        host = os.getenv('HOST', '0.0.0.0')
        port = int(os.getenv('PORT', 5000))
        debug = os.getenv('DEBUG', 'false').lower() == 'true'
        
        logger.info(f"Dashboard starting on http://{host}:{port}")
        app.run(host=host, port=port, debug=debug)
        
    except KeyboardInterrupt:
        logger.info("Dashboard stopped by user")
        processor_service.stop_processing()
    except Exception as e:
        logger.error(f"Error running app: {e}")
        processor_service.stop_processing()
        raise

if __name__ == '__main__':
    run_app()
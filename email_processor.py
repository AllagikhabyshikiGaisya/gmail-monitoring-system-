#!/usr/bin/env python3

import os
import json
import re
import requests
import base64
import time
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import html
import unicodedata
import threading
from pathlib import Path
import tempfile

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional

# Configure logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only console logging for Render
    ]
)
logger = logging.getLogger(__name__)

class EmailDatabase:
    """SQLite database manager for tracking processed emails"""
    
    def __init__(self, db_path: str = None):
        # Use /tmp directory for Render (writable filesystem)
        if db_path is None:
            self.db_path = os.path.join(tempfile.gettempdir(), "processed_emails.db")
        else:
            self.db_path = db_path
        self.init_database()
        
    def init_database(self):
        """Initialize the SQLite database with required tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create processed emails table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS processed_emails (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        email_id TEXT UNIQUE NOT NULL,
                        thread_id TEXT,
                        subject TEXT,
                        sender TEXT,
                        received_date TEXT,
                        processed_date TEXT DEFAULT CURRENT_TIMESTAMP,
                        webhook_sent BOOLEAN DEFAULT FALSE,
                        archived BOOLEAN DEFAULT FALSE,
                        json_data TEXT
                    )
                ''')
                
                # Create processing stats table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS processing_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        emails_processed INTEGER DEFAULT 0,
                        webhooks_successful INTEGER DEFAULT 0,
                        webhooks_failed INTEGER DEFAULT 0,
                        UNIQUE(date)
                    )
                ''')
                
                conn.commit()
                logger.info(f"Database initialized: {self.db_path}")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def is_email_processed(self, email_id: str) -> bool:
        """Check if an email has already been processed"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM processed_emails WHERE email_id = ?", (email_id,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if email is processed: {e}")
            return False
    
    def mark_email_processed(self, email_data: Dict, json_data: Dict = None, webhook_sent: bool = False):
        """Mark an email as processed in the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO processed_emails 
                    (email_id, thread_id, subject, sender, received_date, webhook_sent, json_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    email_data.get('id'),
                    email_data.get('thread_id'),
                    email_data.get('subject', ''),
                    email_data.get('sender', ''),
                    email_data.get('formatted_date', ''),
                    webhook_sent,
                    json.dumps(json_data) if json_data else None
                ))
                conn.commit()
                logger.info(f"Marked email {email_data.get('id', 'unknown')} as processed")
        except Exception as e:
            logger.error(f"Error marking email as processed: {e}")
    
    def get_processed_count(self) -> int:
        """Get total number of processed emails"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM processed_emails")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting processed count: {e}")
            return 0
    
    def get_recent_processed_emails(self, limit: int = 20) -> List[Dict]:
        """Get recently processed emails"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT email_id, subject, sender, received_date, processed_date, webhook_sent
                    FROM processed_emails 
                    ORDER BY processed_date DESC 
                    LIMIT ?
                ''', (limit,))
                
                emails = []
                for row in cursor.fetchall():
                    emails.append({
                        'email_id': row[0],
                        'subject': row[1],
                        'sender': row[2],
                        'received_date': row[3],
                        'processed_date': row[4],
                        'webhook_sent': row[5]
                    })
                return emails
        except Exception as e:
            logger.error(f"Error getting recent emails: {e}")
            return []
    
    def update_daily_stats(self, processed: int, successful: int, failed: int):
        """Update daily processing statistics"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO processing_stats 
                    (date, emails_processed, webhooks_successful, webhooks_failed)
                    VALUES (?, 
                        COALESCE((SELECT emails_processed FROM processing_stats WHERE date = ?), 0) + ?,
                        COALESCE((SELECT webhooks_successful FROM processing_stats WHERE date = ?), 0) + ?,
                        COALESCE((SELECT webhooks_failed FROM processing_stats WHERE date = ?), 0) + ?
                    )
                ''', (today, today, processed, today, successful, today, failed))
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total processed
                cursor.execute("SELECT COUNT(*) FROM processed_emails")
                total_processed = cursor.fetchone()[0]
                
                # Successful webhooks
                cursor.execute("SELECT COUNT(*) FROM processed_emails WHERE webhook_sent = TRUE")
                successful_webhooks = cursor.fetchone()[0]
                
                # Failed webhooks
                cursor.execute("SELECT COUNT(*) FROM processed_emails WHERE webhook_sent = FALSE")
                failed_webhooks = cursor.fetchone()[0]
                
                # Today's stats
                today = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("SELECT * FROM processing_stats WHERE date = ?", (today,))
                today_stats = cursor.fetchone()
                
                return {
                    'total_processed': total_processed,
                    'successful_webhooks': successful_webhooks,
                    'failed_webhooks': failed_webhooks,
                    'today_processed': today_stats[2] if today_stats else 0,
                    'today_successful': today_stats[3] if today_stats else 0,
                    'today_failed': today_stats[4] if today_stats else 0
                }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {
                'total_processed': 0,
                'successful_webhooks': 0,
                'failed_webhooks': 0,
                'today_processed': 0,
                'today_successful': 0,
                'today_failed': 0
            }
    
    def clear_all_data(self):
        """Clear all processed email data - use with caution!"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM processed_emails")
                cursor.execute("DELETE FROM processing_stats")
                conn.commit()
                logger.info("Cleared all processed email data")
        except Exception as e:
            logger.error(f"Error clearing data: {e}")


class UniversalJSONProcessor:
    """Enhanced processor with better Japanese text handling"""
    
    @staticmethod
    def get_universal_template() -> Dict[str, Any]:
        """Returns the universal JSON template structure"""
        return {
            "sender_email(送信元メールアドレス)": "",
            "timestamp(タイムスタンプ)": "",
            "subject(件名)": "",
            "company_info(会社情報)": {
                "company_name(会社名)": "",
                "branch_name(支店名)": "",
                "received_datetime(受信日時)": "",
                "id(ＩＤ)": "",
                "serial_number(連番)": "",
                "contact_datetime(お問合せ日時)": "",
                "contact_plan(お問合せ企画)": "",
                "delivery_type(反響送付先区分)": "",
                "delivery_code(反響送付先コード)": "",
                "url(URL)": ""
            },
            "staff_info(担当者情報)": {
                "staff_in_charge(担当)": "",
                "status(ステータス)": "",
                "occurrence_type(発生区分)": ""
            },
            "event_info(イベント情報)": {
                "event_name(イベント名)": "",
                "event_date(開催日)": "",
                "event_time(時間)": "",
                "event_place(会場)": "",
                "event_url(URL)": ""
            },
            "reservation_info(ご予約情報)": [{
                "preferred_date(ご希望日)": "",
                "preferred_time(ご希望時間)": "",
                "reservation_status(予約状況)": "",
                "meeting_place(集合場所)": "",
                "reservation_id(予約ID)": "",
                "property_type(物件種別)": "",
                "property_code(物件コード)": "",
                "property_name(物件名)": "",
                "company_property_code(貴社物件コード)": "",
                "location(所在地)": "",
                "price(価格)": "",
                "property_url(物件詳細画面)": ""
            }],
            "document_request_info(資料請求情報)": {
                "requested_booklets(ご希望の冊子)": "",
                "requested_properties(資料請求物件情報)": []
            },
            "inquiry_info(お問い合わせ内容)": {
                "inquiry_text(お問い合わせ内容)": "",
                "inquiry_source(お問い合わせのきっかけ)": ""
            },
            "survey_info(アンケート情報)": {
                "preferred_area(ご希望エリア)": "",
                "railway_line(沿線)": "",
                "other_requests(その他ご要望)": "",
                "school_district(学校区)": "",
                "parking_spaces(駐車場台数)": "",
                "floors(階数)": "",
                "budget_total(総予算)": "",
                "budget_monthly(希望返済額)": ""
            },
            "property_info(物件情報)": {
                "company_name(会社名)": "",
                "branch_name(支店名)": "",
                "issue(掲載号)": "",
                "property_type(物件種別)": "",
                "property_code(物件コード)": "",
                "property_name(物件名)": "",
                "company_property_code(貴社物件コード)": "",
                "nearest_station(最寄り駅)": "",
                "bus_walk(バス／歩)": "",
                "location(所在地)": "",
                "price(価格)": "",
                "land_area(土地面積)": "",
                "building_area(建物面積)": "",
                "property_url(物件詳細画面)": "",
                "floor_plan(間取り)": "",
                "age(築年数)": "",
                "other_pr_points(その他PRポイント)": ""
            },
            "customer_info(お客様情報)": [{
                "name(お名前)": "",
                "furigana(フリガナ)": "",
                "email(メールアドレス)": "",
                "phone_number(電話番号)": "",
                "phone_number2(電話番号2)": "",
                "fax_number(FAX番号)": "",
                "age(年齢)": "",
                "postal_code(郵便番号)": "",
                "address(ご住所)": "",
                "monthly_rent(月々の家賃)": "",
                "monthly_payment(月々の返済額)": "",
                "preferred_area(希望エリア)": "",
                "registration_reason(会員登録のきっかけ)": "",
                "preferred_contact_method(希望連絡方法)": "",
                "newsletter_opt_in(お知らせメール希望)": "",
                "comments(ご意見・ご質問等)": ""
            }],
            "housing_preferences(希望条件情報)": {
                "mansion_preferences(マンション希望条件情報)": {
                    "preferred_area(希望エリア)": "",
                    "school_district(希望校区)": "",
                    "price_range(希望価格)": "",
                    "floor_plan(希望間取り)": "",
                    "exclusive_area(希望専有面積)": "",
                    "pet_allowed(ペット可物件希望)": "",
                    "other_conditions(その他希望条件)": ""
                },
                "house_preferences(一戸建て希望条件情報)": {
                    "preferred_area(希望エリア)": "",
                    "school_district(希望校区)": "",
                    "price_range(希望価格)": "",
                    "floor_plan(希望間取り)": "",
                    "land_area(希望土地面積)": "",
                    "building_area(希望建物面積)": "",
                    "other_conditions(その他希望条件)": ""
                }
            }
        }

    @staticmethod
    def normalize_japanese_text(text: str) -> str:
        """Normalize Japanese text for better pattern matching"""
        if not text:
            return ""
        
        # Normalize Unicode (NFD -> NFC)
        text = unicodedata.normalize('NFC', text)
        
        # Convert full-width numbers and letters to half-width
        text = text.translate(str.maketrans(
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        ))
        
        # Normalize punctuation
        text = text.replace('：', ':').replace('；', ';').replace('，', ',')
        
        return text.strip()

    @staticmethod
    def extract_patterns() -> Dict[str, List[str]]:
        """Returns enhanced extraction patterns for Japanese content"""
        return {
            # Customer Information Patterns (Enhanced Japanese)
            'name': [
                r'(?:お?名前|氏名|名前|Name)[：:\s]*([^\n\r]+?)(?:\s*(?:\n|$|フリガナ|ふりがな|カナ))',
                r'お客様?名[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'申込者?名[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'ご依頼者[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'([^\s\n]{2,10})\s*(?:様|さん|氏|殿)(?:\s|$)',
            ],
            'furigana': [
                r'(?:フリガナ|ふりがな|カナ|Furigana)[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'(?:よみがな|読み仮名)[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'([ァ-ヾ\s]{3,20})(?:\s|$)'
            ],
            'email': [
                r'(?:メールアドレス|E-?mail|e-?mail)[：:\s]*([^\s\n]+@[^\s\n]+)(?:\s|$)',
                r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
                r'連絡先.*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            ],
            'phone': [
                r'(?:電話番号|TEL|Tel|Phone|携帯)[：:\s]*([0-9\-\(\)\s]+?)(?:\n|$)',
                r'(?:連絡先|でんわ)[：:\s]*([0-9\-\(\)\s]+?)(?:\n|$)',
                r'(\d{2,4}[-\s]?\d{2,4}[-\s]?\d{4})',
                r'(\(\d{2,4}\)\s?\d{2,4}[-\s]?\d{4})',
                r'(0\d{1,4}-\d{2,4}-\d{4})'
            ],
            'age': [
                r'(?:年齢|Age)[：:\s]*(\d+)(?:歳|才|$)',
                r'(\d+)(?:歳|才)(?:\s|$)',
                r'年齢[：:\s]*([^\n]+?)(?:\n|$)'
            ],
            'postal_code': [
                r'(?:郵便番号|〒|Postal)[：:\s]*(\d{3}-?\d{4})(?:\s|$)',
                r'〒\s*(\d{3}-?\d{4})',
                r'(\d{3}-\d{4})(?:\s|$)'
            ],
            'address': [
                r'(?:住所|ご住所|所在地|Address)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:都道府県|市区町村).*?([^\n]+?)(?:\n|$)',
                r'([都道府県市区町村][^\n]+?)(?:\n|$)'
            ],
            
            # Company Information Patterns
            'company_name': [
                r'(?:会社名|企業名|法人名|Company)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:勤務先|お勤め先)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\s]+(?:株式会社|有限会社|合同会社|LLC|Inc|Corp|Ltd))',
                r'(株式会社[^\s\n]+)',
                r'([^\s]+会社)(?:\s|$)'
            ],
            
            # Event Information Patterns
            'event_name': [
                r'(?:イベント名|セミナー名|講座名|Event)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:説明会|相談会|見学会)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\n]*(?:セミナー|講座|説明会|相談会|見学会|イベント)[^\n]*)(?:\n|$)'
            ],
            
            # Inquiry Information Patterns
            'inquiry_text': [
                r'(?:お問い?合わせ内容|ご質問|相談内容|Inquiry)[：:\s]*([^\n]*?)(?:\n\n|$)',
                r'(?:メッセージ|内容|詳細)[：:\s]*([^\n]*?)(?:\n\n|$)',
                r'(?:ご要望|要望|希望)[：:\s]*([^\n]*?)(?:\n\n|$)',
                r'その他.*?[：:\s]*([^\n]*?)(?:\n\n|$)'
            ],
            'inquiry_source': [
                r'(?:お問い?合わせのきっかけ|きっかけ|ご質問のきっかけ|お申し込みのきっかけ)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:どちらでお知りになりましたか|どちらで知り|どこで知り)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:ご紹介|紹介|媒体|メディア)[：:\s]*([^\n]+?)(?:\n|$)',
                r'きっかけ[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:予約のきっかけ|お問い?合わせのきっかけ|きっかけ|ご質問のきっかけ|お申し込みのきっかけ)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:どちらでお知りになりましたか|どちらで知り|どこで知り)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:ご紹介|紹介|媒体|メディア)[：:\s]*([^\n]+?)(?:\n|$)',
                r'きっかけ[：:\s]*([^\n]+?)(?:\n|$)',
                r'([インスタグラム|Instagram|IG|facebook|Facebook|FB|Twitter|YouTube|Google|検索|チラシ|広告|紹介|口コミ])(?:\s|$)',
            ],
            
            # Property Information Patterns
            'property_name': [
                r'(?:物件名|建物名|Property)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:マンション名|アパート名)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\n]*(?:マンション|アパート|ハウス|レジデンス|パーク)[^\n]*)(?:\n|$)'
            ],
            'price': [
                r'(?:価格|金額|Price|販売価格)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:坪単価|㎡単価)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(\d+(?:万|億|千万)円?)(?:\s|$)',
                r'(\d+,\d{3}(?:,\d{3})*円?)(?:\s|$)'
            ],
            
            # URLs
            'url': [
                r'(https?://[^\s\n]+)',
                r'(?:URL|Link)[：:\s]*(https?://[^\s\n]+)',
                r'(?:詳細|詳しく).*?(https?://[^\s\n]+)',
                r'(www\.[^\s\n]+)',
                r'([a-zA-Z0-9.-]+\.(?:com|co\.jp|jp|net|org)[^\s]*)'
            ]
        }


class GmailAPIProcessor:
    """Enhanced Gmail API processor with proper OAuth flow"""
    
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
    
    def __init__(self, webhook_url: str = None):
        """Initialize Gmail API processor"""
        # Get configuration from environment variables
        self.webhook_url = webhook_url or os.getenv('WEBHOOK_URL')
        self.max_emails = int(os.getenv('MAX_EMAILS_PER_CHECK', '50'))
        self.archive_processed = os.getenv('ARCHIVE_PROCESSED_EMAILS', 'true').lower() == 'true'
        
        # Initialize components
        self.service = None
        self.db = EmailDatabase()
        self.universal_json = UniversalJSONProcessor()
        self._auth_lock = threading.Lock()
        
        logger.info(f"Email processor initialized with webhook: {bool(self.webhook_url)}")
        
    def create_credentials_from_env(self) -> Optional[Credentials]:
        """Create credentials from environment variables for production deployment"""
        try:
            # Get credentials JSON and refresh token from environment
            creds_json = os.getenv('GMAIL_CREDENTIALS_JSON')
            refresh_token = os.getenv('GMAIL_REFRESH_TOKEN')
            
            if not creds_json or not refresh_token:
                logger.info("Missing environment credentials, will try local files")
                return None
            
            # Parse the credentials JSON
            creds_info = json.loads(creds_json)
            client_info = creds_info.get('installed', creds_info)
            
            # Validate that refresh_token is actually a token string, not JSON
            if refresh_token.strip().startswith('{'):
                logger.error("GMAIL_REFRESH_TOKEN appears to be JSON, but should be just the token string")
                return None
            
            # Create credentials object
            creds = Credentials(
                token=None,  # Will be refreshed
                refresh_token=refresh_token.strip(),
                token_uri=client_info['token_uri'],
                client_id=client_info['client_id'],
                client_secret=client_info['client_secret'],
                scopes=self.SCOPES
            )
            
            # Refresh the token
            creds.refresh(Request())
            
            logger.info("Successfully created credentials from environment variables")
            return creds
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in credentials: {e}")
            return None
        except Exception as e:
            logger.error(f"Error creating credentials from environment: {e}")
            return None
    
    def setup_local_oauth(self) -> Optional[Credentials]:
        """Set up OAuth flow for local development"""
        credentials_path = os.getenv('GMAIL_CREDENTIALS_PATH', 'credentials.json')
        token_path = os.getenv('GMAIL_TOKEN_PATH', 'token.json')
        
        # Check if credentials.json exists
        if not os.path.exists(credentials_path):
            logger.error(f"Credentials file not found: {credentials_path}")
            logger.error("Please download credentials.json from Google Cloud Console")
            logger.error("Visit: https://console.cloud.google.com/apis/credentials")
            return None
        
        creds = None
        
        # Load existing token if available
        if os.path.exists(token_path):
            try:
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)
                logger.info("Loaded existing token")
            except Exception as e:
                logger.warning(f"Could not load existing token: {e}")
                # Remove corrupted token file
                try:
                    os.remove(token_path)
                except:
                    pass
        
        # If we have valid credentials, use them
        if creds and creds.valid:
            logger.info("Using existing valid credentials")
            return creds
        
        # If credentials are expired, try to refresh
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("Refreshing expired credentials")
                creds.refresh(Request())
                
                # Save refreshed credentials
                with open(token_path, 'wb') as token:
                    pickle.dump(creds, token)
                logger.info("Credentials refreshed and saved")
                return creds
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}")
                creds = None
        
        # Run OAuth flow for new credentials
        try:
            logger.info("Starting OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, self.SCOPES)
            
            # Try local server first, fallback to console
            try:
                creds = flow.run_local_server(port=0, open_browser=True)
                logger.info("OAuth completed via local server")
            except Exception as e:
                logger.warning(f"Local server failed: {e}")
                logger.info("Please complete OAuth manually:")
                creds = flow.run_console()
                logger.info("OAuth completed via console")
            
            # Save the credentials
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
            logger.info(f"Credentials saved to {token_path}")
            
            # Also log the refresh token for production use
            if creds.refresh_token:
                logger.info("=" * 50)
                logger.info("IMPORTANT: Save this refresh token for production:")
                logger.info(f"GMAIL_REFRESH_TOKEN={creds.refresh_token}")
                logger.info("=" * 50)
            
            return creds
            
        except Exception as e:
            logger.error(f"OAuth flow failed: {e}")
            return None

    def extract_email_address(self, email_string: str) -> str:
        """Extract clean email address from email string"""
        if not email_string:
            return ""
    
    # Remove quotes from the beginning and end
        email_string = email_string.strip('"')
    
    # Extract email from "Display Name" <email@domain.com> format
        email_match = re.search(r'<([^>]+)>', email_string)
        if email_match:
            return email_match.group(1).strip()
    
    # Check if it's already just an email address
        email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', email_string)
        if email_match:
            return email_match.group(1).strip()
    
    # If no email found, return empty string
        return ""  
    
    def authenticate(self) -> bool:
        """Authenticate and build Gmail service"""
        with self._auth_lock:
            try:
                creds = None
                
                # Try environment variables first (for production)
                logger.info("Attempting authentication...")
                
                # For production (Render), use environment variables
                if os.getenv('GMAIL_CREDENTIALS_JSON') and os.getenv('GMAIL_REFRESH_TOKEN'):
                    logger.info("Trying environment variable authentication")
                    creds = self.create_credentials_from_env()
                
                # For local development, use files and OAuth flow
                if not creds:
                    logger.info("Trying local file authentication")
                    creds = self.setup_local_oauth()
                
                if not creds:
                    logger.error("Failed to obtain valid credentials")
                    return False
                
                # Build the Gmail service
                self.service = build('gmail', 'v1', credentials=creds)
                
                # Test the service
                try:
                    profile = self.service.users().getProfile(userId='me').execute()
                    logger.info(f"Gmail API authentication successful for {profile.get('emailAddress', 'unknown')}")
                    return True
                except Exception as e:
                    logger.error(f"Gmail API test call failed: {e}")
                    return False
                
            except Exception as e:
                logger.error(f"Authentication failed: {e}")
                return False
    
    def get_latest_emails(self) -> List[Dict]:
        """Get latest unprocessed emails from inbox"""
        try:
            if not self.service:
                if not self.authenticate():
                    return []
            
            # Get list of emails
            try:
                results = self.service.users().messages().list(
                    userId='me',
                    labelIds=['INBOX'],
                    maxResults=self.max_emails * 2  # Get more to account for already processed
                ).execute()
            except HttpError as e:
                if e.resp.status == 401:
                    logger.warning("Authentication expired, re-authenticating...")
                    if self.authenticate():
                        results = self.service.users().messages().list(
                            userId='me',
                            labelIds=['INBOX'],
                            maxResults=self.max_emails * 2
                        ).execute()
                    else:
                        return []
                else:
                    raise
            
            messages = results.get('messages', [])
            
            if not messages:
                logger.info("No messages found in inbox")
                return []
            
            # Filter out already processed emails and get email data
            emails = []
            processed_count = 0
            
            for message in messages:
                email_id = message['id']
                
                # Skip if already processed
                if self.db.is_email_processed(email_id):
                    processed_count += 1
                    continue
                
                try:
                    # Get full message
                    msg = self.service.users().messages().get(
                        userId='me', 
                        id=email_id,
                        format='full'
                    ).execute()
                    
                    # Extract email data
                    email_data = self.extract_email_data(msg)
                    if email_data:
                        emails.append(email_data)
                        
                        # Stop if we have enough new emails
                        if len(emails) >= self.max_emails:
                            break
                            
                except Exception as e:
                    logger.error(f"Error processing email {email_id}: {e}")
                    continue
            
            logger.info(f"Retrieved {len(emails)} new emails ({processed_count} already processed)")
            return emails
            
        except Exception as e:
            logger.error(f"Error getting emails: {e}")
            return []
    
    def extract_email_data(self, message: Dict) -> Optional[Dict]:
        """Extract structured data from Gmail API message"""
        try:
            payload = message.get('payload', {})
            headers = payload.get('headers', [])
            
            # Extract headers
            email_data = {
                'id': message['id'],
                'thread_id': message['threadId'],
                'label_ids': message.get('labelIds', []),
                'snippet': message.get('snippet', ''),
                'internal_date': message.get('internalDate', ''),
                'size_estimate': message.get('sizeEstimate', 0)
            }
            
            # Process headers
            for header in headers:
                name = header['name'].lower()
                value = header['value']
                
                if name == 'from':
    # FIXED: Extract clean email address only
                    email_data['sender'] = self.extract_email_address(value)
                elif name == 'to':
                    email_data['recipient'] = value
                elif name == 'subject':
                    email_data['subject'] = value
                elif name == 'date':
                    email_data['date'] = value
                elif name == 'message-id':
                    email_data['message_id'] = value
            
            # Extract body
            body = self.extract_email_body(payload)
            email_data['body'] = self.universal_json.normalize_japanese_text(body)
            
            # Convert internal date to readable format
            if email_data.get('internal_date'):
                timestamp = int(email_data['internal_date']) / 1000
                email_data['formatted_date'] = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).strftime('%Y/%m/%d %H:%M:%S')
            
            return email_data
            
        except Exception as e:
            logger.error(f"Error extracting email data: {e}")
            return None
    
    def extract_email_body(self, payload: Dict) -> str:
        """Extract text body from email payload with HTML handling"""
        try:
            body_parts = []
            
            def extract_part_body(part):
                """Recursively extract body from email parts"""
                mime_type = part.get('mimeType', '')
                
                if 'parts' in part:
                    for subpart in part['parts']:
                        extract_part_body(subpart)
                elif mime_type == 'text/plain':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        try:
                            decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                            body_parts.append(decoded)
                        except Exception as e:
                            logger.warning(f"Error decoding plain text: {e}")
                elif mime_type == 'text/html':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        try:
                            decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                            # Remove HTML tags and decode HTML entities
                            text = html.unescape(re.sub(r'<[^>]+>', '', decoded))
                            body_parts.append(text)
                        except Exception as e:
                            logger.warning(f"Error decoding HTML: {e}")
            
            extract_part_body(payload)
            
            # Combine all body parts
            full_body = '\n'.join(body_parts)
            
            # Clean up the text
            full_body = re.sub(r'\n\s*\n', '\n\n', full_body)  # Normalize line breaks
            full_body = re.sub(r'[ \t]+', ' ', full_body)      # Normalize spaces
            
            return full_body.strip()
            
        except Exception as e:
            logger.error(f"Error extracting email body: {e}")
            return ""
    
    def check_data_relevance(self, email_data: Dict) -> bool:
        """Enhanced relevance check for Japanese content"""
        subject = email_data.get('subject', '').lower()
        body = email_data.get('body', '').lower()
        email_text = f"{subject} {body}"
        
        # Enhanced relevance keywords (Japanese and English)
        relevance_keywords = [
            # Customer info keywords
            '名前', 'なまえ', 'name', 'お名前', '氏名', '申込者',
            'メール', 'email', 'e-mail', 'アドレス', 'メルアド',
            '電話', 'tel', 'phone', '番号', 'でんわ', '携帯',
            '住所', 'address', '所在地', 'じゅうしょ',
            
            # Form and inquiry keywords
            'フォーム', 'form', 'お問い合わせ', '問い合わせ', '問合せ', 'inquiry',
            '申込', '申し込み', 'application', '登録', 'registration',
            '予約', 'reservation', 'booking', 'よやく',
            '相談', 'consultation', '見学', 'けんがく',
            
            # Real estate keywords
            '物件', 'property', '不動産', 'real estate', 'ぶっけん',
            '住宅', 'house', 'housing', 'じゅうたく',
            'マンション', 'mansion', 'アパート', 'apartment',
            '戸建', '一戸建て', 'house', 'こだて',
            
            # Lark specific
            'lark', 'larksuite', '飛書', 'feishu', 'webhook', 'api'
        ]
        
        # Check if any relevance keywords are present
        keyword_found = any(keyword in email_text for keyword in relevance_keywords)
        
        # Enhanced pattern checking for structured data
        patterns_found = 0
        structure_patterns = [
            r'[：:]\s*[^\n]',  # Colon patterns
            r'お客様情報',
            r'ご質問.*[：:]',
            r'申し込み.*[：:]',
            r'フォーム',
            r'\d+-\d+-\d+',  # Phone/postal patterns
            r'@[a-zA-Z0-9.-]+\.',  # Email pattern
            r'[都道府県市区町村]',  # Japanese address
            r'\d+(?:万|千|億)円',  # Price patterns
        ]
        
        for pattern in structure_patterns:
            if re.search(pattern, email_text, re.IGNORECASE):
                patterns_found += 1
        
        # Accept if keyword found OR multiple patterns found
        is_relevant = keyword_found or patterns_found >= 2
        
        if is_relevant:
            logger.info(f"Email marked as relevant (keywords: {keyword_found}, patterns: {patterns_found})")
        
        return is_relevant
    
    def extract_universal_json_data(self, email_data: Dict) -> Dict:
        """Extract and map data to universal JSON format"""
        # Get universal template
        universal_data = self.universal_json.get_universal_template()
        patterns = self.universal_json.extract_patterns()
        
        # Basic email info
        universal_data["sender_email(送信元メールアドレス)"] = email_data.get('sender', '')
        universal_data["timestamp(タイムスタンプ)"] = email_data.get('formatted_date', '')
        universal_data["subject(件名)"] = email_data.get('subject', '')
        
        # Company info
        universal_data["company_info(会社情報)"]["received_datetime(受信日時)"] = email_data.get('formatted_date', '')
        universal_data["company_info(会社情報)"]["id(ＩＤ)"] = email_data.get('id', '')
        
        # Extract information using patterns
        email_body = email_data.get('body', '')
        email_subject = email_data.get('subject', '')
        full_text = f"{email_subject}\n{email_body}"
        
        # Track extracted data for logging
        extracted_data = {}
        
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                try:
                    match = re.search(pattern, full_text, re.MULTILINE | re.DOTALL | re.IGNORECASE)
                    if match:
                        value = match.group(1).strip() if match.groups() else match.group(0).strip()
                        
                        # Clean and validate the extracted value
                        value = self.clean_extracted_value(value, field)
                        
                        if value and len(value) > 0:
                            # Map to universal JSON structure
                            self.map_field_to_json(universal_data, field, value)
                            extracted_data[field] = value
                            logger.info(f"Extracted {field}: {value}")
                            break
                except Exception as e:
                    logger.warning(f"Error processing pattern for field {field}: {e}")
                    continue
        
        # Log extraction summary
        if extracted_data:
            logger.info(f"Successfully extracted {len(extracted_data)} fields: {list(extracted_data.keys())}")
        else:
            logger.warning("No data extracted from email")
        
        return universal_data
    
    def clean_extracted_value(self, value: str, field: str) -> str:
        """Clean and validate extracted values"""
        if not value:
            return ""
        
        # Remove extra whitespace and normalize
        value = value.strip()
        value = re.sub(r'\s+', ' ', value)
        
        # Field-specific cleaning
        if field == 'email':
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', value)
            if email_match:
                value = email_match.group(1)
        
        elif field == 'phone':
            value = re.sub(r'[^\d\-\(\)\s]', '', value)
            value = re.sub(r'\s+', ' ', value).strip()
        
        elif field == 'postal_code':
            digits = re.findall(r'\d', value)
            if len(digits) == 7:
                value = f"{digits[0]}{digits[1]}{digits[2]}-{digits[3]}{digits[4]}{digits[5]}{digits[6]}"
            elif not re.match(r'\d{3}-\d{4}', value):
                return ""
        
        elif field in ['age']:
            numbers = re.findall(r'\d+', value)
            if numbers:
                age = int(numbers[0])
                if 0 <= age <= 120:
                    value = str(age)
                else:
                    return ""
            else:
                return ""
        
        elif field == 'url':
            if not value.startswith(('http://', 'https://')):
                if value.startswith('www.'):
                    value = 'https://' + value
                elif '.' in value and not value.startswith('//'):
                    value = 'https://' + value
        
        # Remove unwanted patterns
        unwanted_patterns = [
            r'^\s*[：:]\s*',  # Leading colon
            r'\s*[：:]\s*',
            r'^.*?[：:]\s*',    # Trailing colon
            r'\s*様\s*',     # Honorific suffix
            r'\s*さん\s*',
            r'\s*殿\s*',   # Honorific suffix
        ]
        
        for pattern in unwanted_patterns:
            value = re.sub(pattern, '', value).strip()
        
        # Return empty if too short or only special characters
        if len(value) < 1 or re.match(r'^[^\w\  u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]+$'
                    , value):
            return ""
        
        return value
    
    def map_field_to_json(self, universal_data: Dict, field: str, value: str):
        """Map extracted field to correct position in universal JSON"""
        try:
            if not value or len(value.strip()) == 0:
                return
            
            # Customer info mappings
            if field == 'name':
                universal_data["customer_info(お客様情報)"][0]["name(お名前)"] = value
            elif field == 'furigana':
                universal_data["customer_info(お客様情報)"][0]["furigana(フリガナ)"] = value
            elif field == 'email':
                universal_data["customer_info(お客様情報)"][0]["email(メールアドレス)"] = value
            elif field == 'phone':
                universal_data["customer_info(お客様情報)"][0]["phone_number(電話番号)"] = value
            elif field == 'age':
                if not value.endswith("歳"):
                    value = value + "歳"
                universal_data["customer_info(お客様情報)"][0]["age(年齢)"] = value
            elif field == 'postal_code':
                if not value.startswith("〒"):
                    value = "〒" + value
                universal_data["customer_info(お客様情報)"][0]["postal_code(郵便番号)"] = value
            elif field == 'address':
                universal_data["customer_info(お客様情報)"][0]["address(ご住所)"] = value
            elif field == 'inquiry_text':
                universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_text(お問い合わせ内容)"] = value
                universal_data["customer_info(お客様情報)"][0]["comments(ご意見・ご質問等)"] = value
            elif field == 'inquiry_source':
                universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_source(お問い合わせのきっかけ)"] = value
            elif field == 'company_name':
                universal_data["company_info(会社情報)"]["company_name(会社名)"] = value
                universal_data["property_info(物件情報)"]["company_name(会社名)"] = value
            elif field == 'property_name':
                universal_data["property_info(物件情報)"]["property_name(物件名)"] = value
                universal_data["reservation_info(ご予約情報)"][0]["property_name(物件名)"] = value
            elif field == 'price':
                universal_data["property_info(物件情報)"]["price(価格)"] = value
                universal_data["reservation_info(ご予約情報)"][0]["price(価格)"] = value
            elif field == 'url':
                universal_data["company_info(会社情報)"]["url(URL)"] = value
                universal_data["event_info(イベント情報)"]["event_url(URL)"] = value
                universal_data["property_info(物件情報)"]["property_url(物件詳細画面)"] = value
                    
        except Exception as e:
            logger.error(f"Error mapping field {field} with value '{value}': {e}")
    
    def send_to_webhook(self, data: Dict) -> bool:
        """Send processed data to webhook with retry logic"""
        if not self.webhook_url:
            logger.warning("No webhook URL configured, skipping webhook send")
            return False
            
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                headers = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Email-Processor/2.0'
                }
                
                response = requests.post(
                    self.webhook_url, 
                    json=data, 
                    headers=headers, 
                    timeout=30
                )
                
                if response.status_code in [200, 201, 202, 204]:
                    logger.info(f"Data successfully sent to webhook (attempt {attempt + 1})")
                    return True
                else:
                    logger.warning(f"Webhook returned status {response.status_code} (attempt {attempt + 1}): {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Webhook timeout (attempt {attempt + 1})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Webhook connection error (attempt {attempt + 1})")
            except Exception as e:
                logger.error(f"Error sending to webhook (attempt {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
        
        logger.error("Failed to send to webhook after all retry attempts")
        return False
    
    def archive_email(self, email_id: str) -> bool:
        """Archive processed email (remove from inbox)"""
        if not self.archive_processed:
            return True  # Skip archiving if disabled
            
        try:
            # Remove inbox label to archive the email
            self.service.users().messages().modify(
                userId='me',
                id=email_id,
                body={
                    'removeLabelIds': ['INBOX']
                }
            ).execute()
            
            logger.info(f"Archived email {email_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to archive email {email_id}: {e}")
            return False
    
    def process_emails(self) -> Dict:
        """Main processing function - returns processing results"""
        try:
            # Get latest unprocessed emails
            emails = self.get_latest_emails()
            
            if not emails:
                logger.info("No new emails to process")
                return {
                    'processed': 0,
                    'successful_webhooks': 0,
                    'failed_webhooks': 0,
                    'archived': 0
                }
            
            processed_count = 0
            successful_webhooks = 0
            failed_webhooks = 0
            archived_count = 0
            
            for email_data in emails:
                email_id = email_data.get('id', 'unknown')
                subject = email_data.get('subject', 'No Subject')
                sender = email_data.get('sender', 'Unknown Sender')
                
                logger.info(f"Processing email {email_id}: '{subject}' from {sender}")
                
                try:
                    # Check if email contains relevant data
                    if not self.check_data_relevance(email_data):
                        logger.info(f"Email {email_id} - No relevant data found, marking as processed")
                        self.db.mark_email_processed(email_data, webhook_sent=False)
                        processed_count += 1
                        continue
                    
                    # Extract universal JSON data
                    universal_data = self.extract_universal_json_data(email_data)
                    
                    # Check if we extracted meaningful customer data
                    has_customer_data = any([
                        universal_data["customer_info(お客様情報)"][0]["name(お名前)"],
                        universal_data["customer_info(お客様情報)"][0]["email(メールアドレス)"],
                        universal_data["customer_info(お客様情報)"][0]["phone_number(電話番号)"],
                        universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_text(お問い合わせ内容)"]
                    ])
                    
                    if not has_customer_data:
                        logger.info(f"Email {email_id} - No extractable customer data found")
                        self.db.mark_email_processed(email_data, universal_data, webhook_sent=False)
                        processed_count += 1
                        continue
                    
                    # Send to webhook
                    webhook_success = self.send_to_webhook(universal_data)
                    
                    if webhook_success:
                        logger.info(f"Successfully processed and sent webhook for email {email_id}")
                        successful_webhooks += 1
                    else:
                        logger.error(f"Failed to send webhook for email {email_id}")
                        failed_webhooks += 1
                    
                    # Mark as processed in database
                    self.db.mark_email_processed(email_data, universal_data, webhook_sent=webhook_success)
                    processed_count += 1
                    
                    # Archive email if configured
                    if self.archive_email(email_id):
                        archived_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing email {email_id}: {e}")
                    # Still mark as processed to avoid infinite retries
                    self.db.mark_email_processed(email_data, webhook_sent=False)
                    processed_count += 1
                    failed_webhooks += 1
                    continue
            
            # Update statistics
            self.db.update_daily_stats(processed_count, successful_webhooks, failed_webhooks)
            
            results = {
                'processed': processed_count,
                'successful_webhooks': successful_webhooks,
                'failed_webhooks': failed_webhooks,
                'archived': archived_count
            }
            
            logger.info(f"Processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Critical error in process_emails: {e}")
            return {
                'processed': 0,
                'successful_webhooks': 0,
                'failed_webhooks': 0,
                'archived': 0,
                'error': str(e)
            }
    
    def run_once(self) -> Dict:
        """Run processing once and return results"""
        logger.info("Running email processing once...")
        try:
            results = self.process_emails()
            logger.info(f"One-time processing completed: {results}")
            return results
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            return {'processed': 0, 'error': str(e)}
    
    def get_stats(self) -> Dict:
        """Get processing statistics from database"""
        return self.db.get_stats()
    
    def get_recent_emails(self) -> List[Dict]:
        """Get recently processed emails from database"""
        return self.db.get_recent_processed_emails()
    
    def clear_processed_data(self) -> bool:
        """Clear all processed email data"""
        try:
            self.db.clear_all_data()
            return True
        except Exception as e:
            logger.error(f"Error clearing processed data: {e}")
            return False


def main():
    """Main function to run the email processor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Gmail Email Processor for Japanese Real Estate Forms')
    parser.add_argument('--webhook', '-w', 
                       help='Webhook URL (or set WEBHOOK_URL env var)')
    parser.add_argument('--interval', '-i', type=int, 
                       help='Check interval in seconds (default: 60)')
    parser.add_argument('--once', action='store_true',
                       help='Run once instead of continuous mode')
    parser.add_argument('--stats', '-s', action='store_true',
                       help='Show processing statistics and exit')
    parser.add_argument('--setup-oauth', action='store_true',
                       help='Set up OAuth credentials (run this first)')
    
    args = parser.parse_args()
    
    # Initialize processor
    try:
        processor = GmailAPIProcessor(webhook_url=args.webhook)
        
        # Set up OAuth if requested
        if args.setup_oauth:
            logger.info("Setting up OAuth credentials...")
            if processor.authenticate():
                logger.info("OAuth setup completed successfully!")
                logger.info("You can now run the processor normally.")
            else:
                logger.error("OAuth setup failed!")
            return
        
        # Show stats if requested
        if args.stats:
            stats = processor.get_stats()
            recent_emails = processor.get_recent_emails()
            
            print("\n=== Email Processor Statistics ===")
            print(f"Total processed: {stats.get('total_processed', 0)}")
            print(f"Successful webhooks: {stats.get('successful_webhooks', 0)}")
            print(f"Failed webhooks: {stats.get('failed_webhooks', 0)}")
            print(f"Today processed: {stats.get('today_processed', 0)}")
            print(f"Today successful: {stats.get('today_successful', 0)}")
            
            if recent_emails:
                print("\n=== Recent Processed Emails ===")
                for email in recent_emails[:5]:
                    print(f"- {email['subject']} from {email['sender']} ({email['processed_date']})")
            return
        
        logger.info("Email processor initialized successfully")
        
        if args.once:
            # Run once
            results = processor.run_once()
            if 'error' in results:
                logger.error(f"Processing failed: {results['error']}")
                exit(1)
        else:
            # Run continuously - This won't work on Render, only for local testing
            interval = args.interval or int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))
            logger.info(f"Starting continuous email processing (checking every {interval} seconds)")
            
            while True:
                try:
                    logger.info("=" * 50)
                    logger.info("Checking for new emails...")
                    results = processor.process_emails()
                    
                    if results['processed'] > 0:
                        logger.info(f"✓ Processed {results['processed']} emails, "
                                  f"{results['successful_webhooks']} webhooks successful, "
                                  f"{results['archived']} archived")
                    else:
                        logger.info("✓ No new emails to process")
                    
                    logger.info(f"Waiting {interval} seconds until next check...")
                    time.sleep(interval)
                    
                except KeyboardInterrupt:
                    logger.info("Email processing stopped by user")
                    break
                except Exception as e:
                    logger.error(f"Error in continuous processing: {e}")
                    logger.info("Waiting 60 seconds before retrying...")
                    time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Email processor stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
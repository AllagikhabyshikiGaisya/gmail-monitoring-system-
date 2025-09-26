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
from typing import Dict, List, Optional, Any, Tuple
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
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import backoff

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

@dataclass
class ExtractedField:
    """Represents an extracted field with confidence and metadata"""
    value: str
    confidence: float
    source_pattern: str
    position: int = 0
    validation_passed: bool = True

@dataclass 
class ProcessingResult:
    """Result of email processing"""
    success: bool
    email_id: str
    extracted_fields: Dict[str, ExtractedField] = field(default_factory=dict)
    universal_data: Dict = field(default_factory=dict)
    webhook_sent: bool = False
    archived: bool = False
    error_message: str = ""

class FieldValidator:
    """Validates extracted field values"""
    
    @staticmethod
    def validate_email(value: str) -> Tuple[bool, str]:
        """Validate email address format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(pattern, value.strip()):
            return True, value.strip().lower()
        return False, value
    
    @staticmethod
    def validate_phone(value: str) -> Tuple[bool, str]:
        """Validate and normalize phone number"""
        # Remove all non-digit characters except hyphens and parentheses
        cleaned = re.sub(r'[^\d\-\(\)\s]', '', value)
        digits = re.sub(r'[^\d]', '', cleaned)
        
        # Japanese phone numbers: mobile (11 digits) or landline (10-11 digits)
        if len(digits) in [10, 11]:
            if len(digits) == 11 and digits.startswith('0'):
                # Format as XXX-XXXX-XXXX for mobile or 0X-XXXX-XXXX for landline
                if digits.startswith('090') or digits.startswith('080') or digits.startswith('070'):
                    formatted = f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
                else:
                    formatted = f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
                return True, formatted
            elif len(digits) == 10:
                formatted = f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
                return True, formatted
        
        return False, cleaned
    
    @staticmethod
    def validate_postal_code(value: str) -> Tuple[bool, str]:
        """Validate Japanese postal code"""
        digits = re.sub(r'[^\d]', '', value)
        if len(digits) == 7:
            formatted = f"{digits[:3]}-{digits[3:]}"
            return True, formatted
        return False, value
    
    @staticmethod
    def validate_age(value: str) -> Tuple[bool, str]:
        """Validate age value"""
        numbers = re.findall(r'\d+', value)
        if numbers:
            age = int(numbers[0])
            if 0 <= age <= 120:
                return True, str(age)
        return False, value
    
    @staticmethod
    def validate_url(value: str) -> Tuple[bool, str]:
        """Validate and normalize URL"""
        value = value.strip()
        if not value:
            return False, value
            
        # Add protocol if missing
        if not value.startswith(('http://', 'https://')):
            if value.startswith('www.'):
                value = 'https://' + value
            elif '.' in value:
                value = 'https://' + value
        
        # Basic URL validation
        url_pattern = r'^https?://[^\s<>"\[\]{}|\\^`]+\.[a-zA-Z]{2,}[^\s<>"\[\]{}|\\^`]*$'
        if re.match(url_pattern, value):
            return True, value
        return False, value

class EnhancedTextProcessor:
    """Enhanced text processing with better Japanese support"""
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """Comprehensive text normalization"""
        if not text:
            return ""
        
        # Unicode normalization
        text = unicodedata.normalize('NFKC', text)
        
        # Convert full-width to half-width for ASCII characters
        text = text.translate(str.maketrans(
            '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
            '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        ))
        
        # Normalize common punctuation
        replacements = {
            '：': ':', '；': ';', '，': ',', '．': '.', 
            '（': '(', '）': ')', '「': '"', '」': '"',
            '〜': '~', '～': '~', '・': '·'
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    @staticmethod
    def extract_structured_data(text: str) -> Dict[str, List[str]]:
        """Extract structured data using multiple approaches"""
        if not text:
            return {}
        
        normalized_text = EnhancedTextProcessor.normalize_text(text)
        extracted = {}
        
        # Split into logical sections
        sections = re.split(r'\n\s*\n', normalized_text)
        
        for section in sections:
            # Extract key-value pairs
            lines = section.split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Look for colon-separated pairs
                colon_match = re.search(r'^([^:：]+)[：:](.+)$', line)
                if colon_match:
                    key = colon_match.group(1).strip()
                    value = colon_match.group(2).strip()
                    if key and value:
                        if key not in extracted:
                            extracted[key] = []
                        extracted[key].append(value)
        
        return extracted

class SmartFieldExtractor:
    """Enhanced field extraction with multiple strategies"""
    
    def __init__(self):
        self.validator = FieldValidator()
        self.text_processor = EnhancedTextProcessor()
    
    def get_extraction_patterns(self) -> Dict[str, List[Dict]]:
        """Get improved extraction patterns with confidence scores"""
        return {
            'name': [
                {
                    'pattern': r'(?:お?名前|氏名|申込者名|ご依頼者)[：:\s]*([^\n\r]{1,50}?)(?:\s*(?:\n|$|フリガナ|ふりがな))',
                    'confidence': 0.9,
                    'description': 'Direct name field'
                },
                {
                    'pattern': r'([^\s\n]{2,10})\s*(?:様|さん|氏|殿)(?:\s|$)',
                    'confidence': 0.7,
                    'description': 'Name with honorific'
                },
                {
                    'pattern': r'お客様.*?[：:]([^\n\r]{2,20})(?:\n|$)',
                    'confidence': 0.6,
                    'description': 'Customer name context'
                }
            ],
            'furigana': [
                {
                    'pattern': r'(?:フリガナ|ふりがな|カナ)[：:\s]*([ァ-ヾ\s]{2,30})(?:\n|$)',
                    'confidence': 0.95,
                    'description': 'Direct furigana field'
                },
                {
                    'pattern': r'([ァ-ヾ\s]{4,20})(?:\s|$)',
                    'confidence': 0.5,
                    'description': 'Katakana sequence'
                }
            ],
            'email': [
                {
                    'pattern': r'(?:メールアドレス|E-?mail|e-?mail)[：:\s]*([^\s\n]+@[^\s\n]+\.[a-zA-Z]{2,})(?:\s|$)',
                    'confidence': 0.95,
                    'description': 'Direct email field'
                },
                {
                    'pattern': r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
                    'confidence': 0.9,
                    'description': 'Email pattern anywhere'
                }
            ],
            'phone': [
                {
                    'pattern': r'(?:電話番号|TEL|Tel|Phone|携帯)[：:\s]*([0-9\-\(\)\s]{8,20})(?:\n|$)',
                    'confidence': 0.9,
                    'description': 'Direct phone field'
                },
                {
                    'pattern': r'(\b(?:0\d{1,4}[-\s]?\d{2,4}[-\s]?\d{4}|0\d{9,10})\b)',
                    'confidence': 0.85,
                    'description': 'Japanese phone pattern'
                }
            ],
            'age': [
                {
                    'pattern': r'(?:年齢|Age)[：:\s]*(\d{1,3})(?:歳|才|$)',
                    'confidence': 0.95,
                    'description': 'Direct age field'
                },
                {
                    'pattern': r'(\d{1,3})(?:歳|才)(?:\s|$)',
                    'confidence': 0.8,
                    'description': 'Age with suffix'
                }
            ],
            'postal_code': [
                {
                    'pattern': r'(?:郵便番号|〒)[：:\s]*(\d{3}-?\d{4})(?:\s|$)',
                    'confidence': 0.95,
                    'description': 'Direct postal code field'
                },
                {
                    'pattern': r'〒\s*(\d{3}-?\d{4})',
                    'confidence': 0.9,
                    'description': 'Postal symbol pattern'
                }
            ],
            'address': [
                {
                    'pattern': r'(?:住所|ご住所|所在地)[：:\s]*([^\n]{5,100})(?:\n|$)',
                    'confidence': 0.9,
                    'description': 'Direct address field'
                },
                {
                    'pattern': r'([都道府県市区町村][^\n]{10,80})(?:\n|$)',
                    'confidence': 0.7,
                    'description': 'Japanese address pattern'
                }
            ],
            'inquiry_text': [
                {
                    'pattern': r'(?:お問い?合わせ内容|ご質問|相談内容|お問合せ)[：:\s]*([^\n]{10,500})(?:\n\n|$)',
                    'confidence': 0.9,
                    'description': 'Direct inquiry field'
                },
                {
                    'pattern': r'(?:メッセージ|内容|詳細|ご要望)[：:\s]*([^\n]{10,500})(?:\n\n|$)',
                    'confidence': 0.8,
                    'description': 'Message content field'
                }
            ],
            'company_name': [
                {
                    'pattern': r'(?:会社名|企業名|法人名)[：:\s]*([^\n]{2,50})(?:\n|$)',
                    'confidence': 0.9,
                    'description': 'Direct company field'
                },
                {
                    'pattern': r'([^\s\n]+(?:株式会社|有限会社|合同会社|LLC|Inc|Corp|Ltd))',
                    'confidence': 0.8,
                    'description': 'Company with suffix'
                }
            ],
            'property_name': [
                {
                    'pattern': r'(?:物件名|建物名)[：:\s]*([^\n]{2,50})(?:\n|$)',
                    'confidence': 0.9,
                    'description': 'Direct property field'
                },
                {
                    'pattern': r'([^\n]*(?:マンション|アパート|ハウス|レジデンス|パーク)[^\n]{0,30})(?:\n|$)',
                    'confidence': 0.7,
                    'description': 'Property type pattern'
                }
            ],
            'price': [
                {
                    'pattern': r'(?:価格|金額|販売価格)[：:\s]*([^\n]{3,30})(?:\n|$)',
                    'confidence': 0.9,
                    'description': 'Direct price field'
                },
                {
                    'pattern': r'(\d+(?:[,，]\d{3})*(?:万|億|千万)?円?)(?:\s|$)',
                    'confidence': 0.8,
                    'description': 'Japanese price pattern'
                }
            ],
            'url': [
                {
                    'pattern': r'(https?://[^\s\n<>"\[\]{}|\\^`]+)',
                    'confidence': 0.95,
                    'description': 'HTTP URL pattern'
                },
                {
                    'pattern': r'(?:URL|Link)[：:\s]*(www\.[^\s\n]+)',
                    'confidence': 0.8,
                    'description': 'WWW URL pattern'
                }
            ]
        }
    
    def extract_field(self, text: str, field_name: str) -> List[ExtractedField]:
        """Extract field with multiple patterns and confidence scoring"""
        if not text or not field_name:
            return []
        
        patterns = self.get_extraction_patterns().get(field_name, [])
        if not patterns:
            return []
        
        extracted_fields = []
        
        for pattern_info in patterns:
            pattern = pattern_info['pattern']
            base_confidence = pattern_info['confidence']
            description = pattern_info.get('description', '')
            
            try:
                matches = re.finditer(pattern, text, re.MULTILINE | re.DOTALL | re.IGNORECASE)
                
                for match in matches:
                    value = match.group(1).strip() if match.groups() else match.group(0).strip()
                    
                    if not value or len(value) < 1:
                        continue
                    
                    # Validate the extracted value
                    is_valid, cleaned_value = self.validate_field_value(field_name, value)
                    
                    if is_valid and cleaned_value:
                        # Calculate confidence based on context
                        context_bonus = self.calculate_context_confidence(text, match.start(), field_name)
                        final_confidence = min(0.99, base_confidence + context_bonus)
                        
                        extracted_field = ExtractedField(
                            value=cleaned_value,
                            confidence=final_confidence,
                            source_pattern=description,
                            position=match.start(),
                            validation_passed=True
                        )
                        
                        extracted_fields.append(extracted_field)
                        
            except Exception as e:
                logger.warning(f"Error processing pattern for field {field_name}: {e}")
                continue
        
        # Sort by confidence and position, remove duplicates
        extracted_fields.sort(key=lambda x: (-x.confidence, x.position))
        return self.deduplicate_fields(extracted_fields)
    
    def validate_field_value(self, field_name: str, value: str) -> Tuple[bool, str]:
        """Validate field value using appropriate validator"""
        validation_methods = {
            'email': self.validator.validate_email,
            'phone': self.validator.validate_phone,
            'postal_code': self.validator.validate_postal_code,
            'age': self.validator.validate_age,
            'url': self.validator.validate_url
        }
        
        if field_name in validation_methods:
            return validation_methods[field_name](value)
        
        # Default validation - clean and check length
        cleaned = self.clean_generic_value(value)
        is_valid = len(cleaned) >= 2 and not re.match(r'^[^\w\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]+$', cleaned)
        return is_valid, cleaned
    
    def clean_generic_value(self, value: str) -> str:
        """Clean generic field values"""
        if not value:
            return ""
        
        # Remove unwanted patterns
        unwanted_patterns = [
            r'^\s*[：:]\s*',  # Leading colon
            r'\s*[：:]\s*$',  # Trailing colon
            r'\s*様\s*$',     # Honorific suffix
            r'\s*さん\s*$',
            r'\s*殿\s*$'
        ]
        
        cleaned = value.strip()
        for pattern in unwanted_patterns:
            cleaned = re.sub(pattern, '', cleaned).strip()
        
        return cleaned
    
    def calculate_context_confidence(self, text: str, position: int, field_name: str) -> float:
        """Calculate confidence bonus based on surrounding context"""
        context_window = 100
        start = max(0, position - context_window)
        end = min(len(text), position + context_window)
        context = text[start:end].lower()
        
        context_keywords = {
            'name': ['名前', 'name', '氏名', 'お客様'],
            'email': ['メール', 'mail', 'アドレス', '連絡先'],
            'phone': ['電話', 'tel', 'phone', '携帯', '番号'],
            'address': ['住所', 'address', '所在地'],
            'inquiry': ['問い合わせ', 'inquiry', '質問', '相談']
        }
        
        keywords = context_keywords.get(field_name, [])
        bonus = 0.0
        
        for keyword in keywords:
            if keyword in context:
                bonus += 0.1
        
        return min(0.2, bonus)  # Max bonus of 0.2
    
    def deduplicate_fields(self, fields: List[ExtractedField]) -> List[ExtractedField]:
        """Remove duplicate field values, keeping highest confidence"""
        if not fields:
            return fields
        
        seen_values = {}
        deduplicated = []
        
        for field in fields:
            normalized_value = field.value.lower().strip()
            
            if normalized_value not in seen_values or seen_values[normalized_value].confidence < field.confidence:
                seen_values[normalized_value] = field
        
        return list(seen_values.values())

class EmailDatabase:
    """Enhanced SQLite database manager with better error handling and connection management"""
    
    def __init__(self, db_path: str = None):
        # Use /tmp directory for Render (writable filesystem)
        if db_path is None:
            self.db_path = os.path.join(tempfile.gettempdir(), "processed_emails.db")
        else:
            self.db_path = db_path
        
        self._connection_lock = threading.RLock()
        self._connection_pool = {}
        self.init_database()
        
    def get_connection(self):
        """Get a thread-safe database connection"""
        thread_id = threading.get_ident()
        
        with self._connection_lock:
            if thread_id not in self._connection_pool:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=10000")
                conn.execute("PRAGMA temp_store=MEMORY")
                self._connection_pool[thread_id] = conn
            
            return self._connection_pool[thread_id]
    
    def init_database(self):
        """Initialize the SQLite database with required tables"""
        try:
            with self._connection_lock:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Create processed emails table with better schema
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
                        json_data TEXT,
                        extraction_confidence REAL DEFAULT 0.0,
                        field_count INTEGER DEFAULT 0,
                        processing_time_ms INTEGER DEFAULT 0,
                        error_message TEXT,
                        content_hash TEXT
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
                        average_confidence REAL DEFAULT 0.0,
                        total_processing_time_ms INTEGER DEFAULT 0,
                        UNIQUE(date)
                    )
                ''')
                
                # Create extracted fields table for analysis
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS extracted_fields (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        email_id TEXT NOT NULL,
                        field_name TEXT NOT NULL,
                        field_value TEXT,
                        confidence REAL,
                        source_pattern TEXT,
                        validation_passed BOOLEAN DEFAULT TRUE,
                        extracted_date TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (email_id) REFERENCES processed_emails (email_id)
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_email_id ON processed_emails(email_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_processed_date ON processed_emails(processed_date)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_extraction_fields ON extracted_fields(email_id, field_name)')
                
                conn.commit()
                logger.info(f"Database initialized successfully: {self.db_path}")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def is_email_processed(self, email_id: str) -> bool:
        """Check if an email has already been processed"""
        try:
            with self._connection_lock:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM processed_emails WHERE email_id = ?", (email_id,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if email is processed: {e}")
            return False
    
    def mark_email_processed(self, result: ProcessingResult):
        """Mark an email as processed with detailed information"""
        try:
            with self._connection_lock:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Calculate metrics
                avg_confidence = 0.0
                if result.extracted_fields:
                    confidences = [field.confidence for field in result.extracted_fields.values()]
                    avg_confidence = sum(confidences) / len(confidences)
                
                field_count = len(result.extracted_fields)
                
                # Generate content hash for deduplication
                content_str = json.dumps(result.universal_data, sort_keys=True)
                content_hash = hashlib.md5(content_str.encode()).hexdigest()
                
                # Insert main record
                cursor.execute('''
                    INSERT OR REPLACE INTO processed_emails 
                    (email_id, subject, sender, received_date, webhook_sent, json_data,
                     extraction_confidence, field_count, error_message, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    result.email_id,
                    "",  # Will be filled by caller
                    "",  # Will be filled by caller  
                    "",  # Will be filled by caller
                    result.webhook_sent,
                    json.dumps(result.universal_data) if result.universal_data else None,
                    avg_confidence,
                    field_count,
                    result.error_message or None,
                    content_hash
                ))
                
                # Insert extracted fields details
                for field_name, field_data in result.extracted_fields.items():
                    cursor.execute('''
                        INSERT INTO extracted_fields
                        (email_id, field_name, field_value, confidence, source_pattern, validation_passed)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        result.email_id,
                        field_name,
                        field_data.value,
                        field_data.confidence,
                        field_data.source_pattern,
                        field_data.validation_passed
                    ))
                
                conn.commit()
                logger.info(f"Marked email {result.email_id} as processed (confidence: {avg_confidence:.2f})")
                
        except Exception as e:
            logger.error(f"Error marking email as processed: {e}")
    
    def get_stats(self) -> Dict:
        """Get comprehensive processing statistics"""
        try:
            with self._connection_lock:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                # Basic counts
                cursor.execute("SELECT COUNT(*) FROM processed_emails")
                total_processed = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM processed_emails WHERE webhook_sent = TRUE")
                successful_webhooks = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM processed_emails WHERE webhook_sent = FALSE")
                failed_webhooks = cursor.fetchone()[0]
                
                # Average confidence
                cursor.execute("SELECT AVG(extraction_confidence) FROM processed_emails WHERE extraction_confidence > 0")
                avg_confidence = cursor.fetchone()[0] or 0.0
                
                # Today's stats
                today = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("SELECT * FROM processing_stats WHERE date = ?", (today,))
                today_stats = cursor.fetchone()
                
                # Most common extracted fields
                cursor.execute('''
                    SELECT field_name, COUNT(*) as count, AVG(confidence) as avg_conf
                    FROM extracted_fields 
                    GROUP BY field_name 
                    ORDER BY count DESC 
                    LIMIT 10
                ''')
                field_stats = cursor.fetchall()
                
                return {
                    'total_processed': total_processed,
                    'successful_webhooks': successful_webhooks,
                    'failed_webhooks': failed_webhooks,
                    'average_confidence': round(avg_confidence, 3),
                    'today_processed': today_stats[2] if today_stats else 0,
                    'today_successful': today_stats[3] if today_stats else 0,
                    'today_failed': today_stats[4] if today_stats else 0,
                    'field_extraction_stats': [
                        {'field': row[0], 'count': row[1], 'avg_confidence': round(row[2], 3)}
                        for row in field_stats
                    ]
                }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {'total_processed': 0, 'successful_webhooks': 0, 'failed_webhooks': 0}
    
    def cleanup_old_connections(self):
        """Clean up old database connections"""
        with self._connection_lock:
            active_threads = {thread.ident for thread in threading.enumerate()}
            stale_connections = set(self._connection_pool.keys()) - active_threads
            
            for thread_id in stale_connections:
                try:
                    self._connection_pool[thread_id].close()
                    del self._connection_pool[thread_id]
                except:
                    pass

class UniversalJSONProcessor:
    """Enhanced JSON processor with flexible field mapping"""
    
    def __init__(self):
        self.field_extractor = SmartFieldExtractor()
    
    def get_universal_template(self) -> Dict[str, Any]:
        """Returns the universal JSON template structure"""
        return {
            "sender_email(送信元メールアドレス)": "",
            "timestamp(タイムスタンプ)": "",
            "subject(件名)": "",
            "processing_metadata": {
                "extraction_confidence": 0.0,
                "extracted_field_count": 0,
                "processing_time_ms": 0,
                "extraction_method": "smart_pattern_matching"
            },
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
    
    def extract_universal_json_data(self, email_data: Dict) -> Tuple[Dict, Dict[str, ExtractedField]]:
        """Extract and map data to universal JSON format with detailed field tracking"""
        start_time = time.time()
        
        # Get universal template
        universal_data = self.get_universal_template()
        
        # Basic email info
        universal_data["sender_email(送信元メールアドレス)"] = email_data.get('sender', '')
        universal_data["timestamp(タイムスタンプ)"] = email_data.get('formatted_date', '')
        universal_data["subject(件名)"] = email_data.get('subject', '')
        
        # Company info
        universal_data["company_info(会社情報)"]["received_datetime(受信日時)"] = email_data.get('formatted_date', '')
        universal_data["company_info(会社情報)"]["id(ＩＤ)"] = email_data.get('id', '')
        
        # Extract information using enhanced field extractor
        email_body = email_data.get('body', '')
        email_subject = email_data.get('subject', '')
        full_text = f"{email_subject}\n{email_body}"
        
        # Extract all fields
        all_extracted_fields = {}
        
        field_names = [
            'name', 'furigana', 'email', 'phone', 'age', 'postal_code', 
            'address', 'inquiry_text', 'company_name', 'property_name', 
            'price', 'url'
        ]
        
        for field_name in field_names:
            extracted_fields = self.field_extractor.extract_field(full_text, field_name)
            if extracted_fields:
                # Take the highest confidence field
                best_field = max(extracted_fields, key=lambda x: x.confidence)
                all_extracted_fields[field_name] = best_field
                logger.info(f"Extracted {field_name}: '{best_field.value}' (confidence: {best_field.confidence:.2f})")
        
        # Map extracted fields to universal JSON structure
        self.map_fields_to_universal_json(universal_data, all_extracted_fields)
        
        # Calculate processing metadata
        processing_time = int((time.time() - start_time) * 1000)
        avg_confidence = 0.0
        if all_extracted_fields:
            confidences = [field.confidence for field in all_extracted_fields.values()]
            avg_confidence = sum(confidences) / len(confidences)
        
        universal_data["processing_metadata"]["extraction_confidence"] = round(avg_confidence, 3)
        universal_data["processing_metadata"]["extracted_field_count"] = len(all_extracted_fields)
        universal_data["processing_metadata"]["processing_time_ms"] = processing_time
        
        return universal_data, all_extracted_fields
    
    def map_fields_to_universal_json(self, universal_data: Dict, extracted_fields: Dict[str, ExtractedField]):
        """Enhanced field mapping with better error handling"""
        try:
            for field_name, field_data in extracted_fields.items():
                value = field_data.value
                if not value or len(value.strip()) == 0:
                    continue
                
                # Customer info mappings
                if field_name == 'name':
                    universal_data["customer_info(お客様情報)"][0]["name(お名前)"] = value
                elif field_name == 'furigana':
                    universal_data["customer_info(お客様情報)"][0]["furigana(フリガナ)"] = value
                elif field_name == 'email':
                    universal_data["customer_info(お客様情報)"][0]["email(メールアドレス)"] = value
                elif field_name == 'phone':
                    universal_data["customer_info(お客様情報)"][0]["phone_number(電話番号)"] = value
                elif field_name == 'age':
                    if not value.endswith(("歳", "才")):
                        value = value + "歳"
                    universal_data["customer_info(お客様情報)"][0]["age(年齢)"] = value
                elif field_name == 'postal_code':
                    if not value.startswith("〒"):
                        value = "〒" + value
                    universal_data["customer_info(お客様情報)"][0]["postal_code(郵便番号)"] = value
                elif field_name == 'address':
                    universal_data["customer_info(お客様情報)"][0]["address(ご住所)"] = value
                elif field_name == 'inquiry_text':
                    universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_text(お問い合わせ内容)"] = value
                    universal_data["customer_info(お客様情報)"][0]["comments(ご意見・ご質問等)"] = value
                elif field_name == 'company_name':
                    universal_data["company_info(会社情報)"]["company_name(会社名)"] = value
                    universal_data["property_info(物件情報)"]["company_name(会社名)"] = value
                elif field_name == 'property_name':
                    universal_data["property_info(物件情報)"]["property_name(物件名)"] = value
                    universal_data["reservation_info(ご予約情報)"][0]["property_name(物件名)"] = value
                elif field_name == 'price':
                    universal_data["property_info(物件情報)"]["price(価格)"] = value
                    universal_data["reservation_info(ご予約情報)"][0]["price(価格)"] = value
                elif field_name == 'url':
                    # Map URL to most appropriate fields
                    universal_data["company_info(会社情報)"]["url(URL)"] = value
                    universal_data["event_info(イベント情報)"]["event_url(URL)"] = value
                    universal_data["property_info(物件情報)"]["property_url(物件詳細画面)"] = value
                        
        except Exception as e:
            logger.error(f"Error mapping fields to universal JSON: {e}")

class EnhancedGmailProcessor:
    """Enhanced Gmail API processor with improved reliability and error handling"""
    
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
    
    def __init__(self, webhook_url: str = None):
        """Initialize Gmail API processor with enhanced configuration"""
        # Get configuration from environment variables
        self.webhook_url = webhook_url or os.getenv('WEBHOOK_URL')
        self.max_emails = int(os.getenv('MAX_EMAILS_PER_CHECK', '20'))
        self.archive_processed = os.getenv('ARCHIVE_PROCESSED_EMAILS', 'true').lower() == 'true'
        self.min_confidence_threshold = float(os.getenv('MIN_CONFIDENCE_THRESHOLD', '0.3'))
        self.parallel_processing = os.getenv('PARALLEL_PROCESSING', 'false').lower() == 'true'
        
        # Initialize components
        self.service = None
        self.db = EmailDatabase()
        self.json_processor = UniversalJSONProcessor()
        self._auth_lock = threading.Lock()
        
        logger.info(f"Enhanced email processor initialized:")
        logger.info(f"  - Webhook configured: {bool(self.webhook_url)}")
        logger.info(f"  - Max emails per check: {self.max_emails}")
        logger.info(f"  - Archive processed: {self.archive_processed}")
        logger.info(f"  - Min confidence threshold: {self.min_confidence_threshold}")
        logger.info(f"  - Parallel processing: {self.parallel_processing}")
    
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
    
    @backoff.on_exception(backoff.expo, HttpError, max_tries=3)
    def authenticate(self) -> bool:
        """Authenticate with retry logic and better error handling"""
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
        """Get latest unprocessed emails with better error handling"""
        try:
            if not self.service:
                if not self.authenticate():
                    return []
            
            # Get list of emails with retry logic
            try:
                results = self.service.users().messages().list(
                    userId='me',
                    labelIds=['INBOX'],
                    maxResults=self.max_emails * 3  # Get more to account for already processed
                ).execute()
            except HttpError as e:
                if e.resp.status == 401:
                    logger.warning("Authentication expired, re-authenticating...")
                    if self.authenticate():
                        results = self.service.users().messages().list(
                            userId='me',
                            labelIds=['INBOX'],
                            maxResults=self.max_emails * 3
                        ).execute()
                    else:
                        return []
                else:
                    raise
            
            messages = results.get('messages', [])
            
            if not messages:
                logger.info("No messages found in inbox")
                return []
            
            # Process emails in parallel or sequential based on config
            if self.parallel_processing:
                emails = self._process_emails_parallel(messages)
            else:
                emails = self._process_emails_sequential(messages)
            
            logger.info(f"Retrieved {len(emails)} new emails for processing")
            return emails
            
        except Exception as e:
            logger.error(f"Error getting emails: {e}")
            return []
    
    def _process_emails_sequential(self, messages: List[Dict]) -> List[Dict]:
        """Process emails sequentially"""
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
        
        logger.info(f"Processed {len(emails)} new emails ({processed_count} already processed)")
        return emails
    
    def _process_emails_parallel(self, messages: List[Dict]) -> List[Dict]:
        """Process emails in parallel using ThreadPoolExecutor"""
        emails = []
        processed_count = 0
        
        # Filter out already processed emails
        unprocessed_messages = []
        for message in messages:
            if self.db.is_email_processed(message['id']):
                processed_count += 1
            else:
                unprocessed_messages.append(message)
                if len(unprocessed_messages) >= self.max_emails:
                    break
        
        if not unprocessed_messages:
            return emails
        
        # Process in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_message = {
                executor.submit(self._fetch_single_email, message['id']): message 
                for message in unprocessed_messages
            }
            
            for future in as_completed(future_to_message):
                try:
                    email_data = future.result()
                    if email_data:
                        emails.append(email_data)
                except Exception as e:
                    message = future_to_message[future]
                    logger.error(f"Error processing email {message['id']}: {e}")
        
        logger.info(f"Parallel processed {len(emails)} new emails ({processed_count} already processed)")
        return emails
    
    def _fetch_single_email(self, email_id: str) -> Optional[Dict]:
        """Fetch and extract single email data"""
        try:
            msg = self.service.users().messages().get(
                userId='me', 
                id=email_id,
                format='full'
            ).execute()
            
            return self.extract_email_data(msg)
        except Exception as e:
            logger.error(f"Error fetching email {email_id}: {e}")
            return None
    
    def extract_email_data(self, message: Dict) -> Optional[Dict]:
        """Extract structured data from Gmail API message with better error handling"""
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
                    email_data['sender'] = value
                elif name == 'to':
                    email_data['recipient'] = value
                elif name == 'subject':
                    email_data['subject'] = value
                elif name == 'date':
                    email_data['date'] = value
                elif name == 'message-id':
                    email_data['message_id'] = value
            
            # Extract body with better HTML handling
            body = self.extract_email_body(payload)
            email_data['body'] = EnhancedTextProcessor.normalize_text(body)
            
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
        """Enhanced email body extraction with better HTML and encoding handling"""
        try:
            body_parts = []
            
            def extract_part_body(part):
                """Recursively extract body from email parts with better handling"""
                mime_type = part.get('mimeType', '')
                
                if 'parts' in part:
                    for subpart in part['parts']:
                        extract_part_body(subpart)
                elif mime_type == 'text/plain':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        try:
                            decoded = base64.urlsafe_b64decode(data + '==').decode('utf-8', errors='ignore')
                            if decoded.strip():
                                body_parts.append(decoded)
                        except Exception as e:
                            logger.debug(f"Error decoding plain text: {e}")
                elif mime_type == 'text/html':
                    data = part.get('body', {}).get('data', '')
                    if data:
                        try:
                            decoded = base64.urlsafe_b64decode(data + '==').decode('utf-8', errors='ignore')
                            # Better HTML cleaning
                            text = self._clean_html_content(decoded)
                            if text.strip():
                                body_parts.append(text)
                        except Exception as e:
                            logger.debug(f"Error decoding HTML: {e}")
            
            extract_part_body(payload)
            
            # Combine all body parts with better deduplication
            full_body = self._combine_body_parts(body_parts)
            return full_body.strip()
            
        except Exception as e:
            logger.error(f"Error extracting email body: {e}")
            return ""
    
    def _clean_html_content(self, html_content: str) -> str:
        """Clean HTML content more thoroughly"""
        if not html_content:
            return ""
        
        # Remove script and style elements completely
        html_content = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Replace common HTML elements with meaningful text
        replacements = [
            (r'<br[^>]*>', '\n'),
            (r'<p[^>]*>', '\n'),
            (r'</p>', '\n'),
            (r'<div[^>]*>', '\n'),
            (r'</div>', '\n'),
            (r'<td[^>]*>', ' '),
            (r'</td>', ' '),
            (r'<tr[^>]*>', '\n'),
            (r'</tr>', '\n'),
        ]
        
        for pattern, replacement in replacements:
            html_content = re.sub(pattern, replacement, html_content, flags=re.IGNORECASE)
        
        # Remove remaining HTML tags
        text = re.sub(r'<[^>]+>', '', html_content)
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        
        return text
    
    def _combine_body_parts(self, body_parts: List[str]) -> str:
        """Combine body parts with intelligent deduplication"""
        if not body_parts:
            return ""
        
        if len(body_parts) == 1:
            return body_parts[0]
        
        # Remove very similar parts (likely HTML/plain text duplicates)
        unique_parts = []
        for part in body_parts:
            part_normalized = re.sub(r'\s+', ' ', part.lower()).strip()
            
            is_duplicate = False
            for existing_part in unique_parts:
                existing_normalized = re.sub(r'\s+', ' ', existing_part.lower()).strip()
                
                # Check similarity ratio
                if len(part_normalized) > 0 and len(existing_normalized) > 0:
                    similarity = len(set(part_normalized.split()) & set(existing_normalized.split())) / max(len(set(part_normalized.split())), len(set(existing_normalized.split())))
                    if similarity > 0.8:  # 80% similarity threshold
                        is_duplicate = True
                        break
            
            if not is_duplicate:
                unique_parts.append(part)
        
        return '\n\n'.join(unique_parts)
    
    def check_data_relevance(self, email_data: Dict) -> Tuple[bool, float]:
        """Enhanced relevance check with confidence scoring"""
        subject = email_data.get('subject', '').lower()
        body = email_data.get('body', '').lower()
        sender = email_data.get('sender', '').lower()
        email_text = f"{subject} {body} {sender}"
        
        # Enhanced relevance scoring system
        relevance_score = 0.0
        max_score = 100.0
        
        # High-value keywords (real estate, forms, inquiries)
        high_value_keywords = {
            # Customer info indicators
            'お名前': 15, 'name': 12, '氏名': 15, '申込者': 12,
            'メール': 12, 'email': 12, 'アドレス': 10,
            '電話': 12, 'tel': 10, 'phone': 10, '番号': 8,
            '住所': 12, 'address': 10, '所在地': 10,
            
            # Form and inquiry indicators  
            'フォーム': 20, 'form': 18, 'お問い合わせ': 25, '問い合わせ': 20,
            '申込': 20, '申し込み': 20, 'application': 15,
            '予約': 15, 'reservation': 12, 'booking': 12,
            '相談': 12, 'consultation': 10, '見学': 12,
            
            # Real estate keywords
            '物件': 18, 'property': 15, '不動産': 20, 'real estate': 18,
            '住宅': 15, 'house': 12, 'housing': 12,
            'マンション': 15, 'mansion': 12, 'アパート': 12,
            '戸建': 15, '一戸建て': 15,
            
            # Business indicators
            '会社': 8, 'company': 8, '企業': 8, '法人': 8
        }
        
        # Medium-value keywords
        medium_value_keywords = {
            '価格': 8, 'price': 8, '金額': 8, '料金': 8,
            '希望': 6, '要望': 6, 'request': 6,
            '質問': 8, 'question': 6, '回答': 6,
            '情報': 4, 'info': 4, 'information': 4,
            '詳細': 6, 'details': 6, '内容': 4
        }
        
        # Check for keywords
        for keyword, score in high_value_keywords.items():
            if keyword in email_text:
                relevance_score += score
                
        for keyword, score in medium_value_keywords.items():
            if keyword in email_text:
                relevance_score += score
        
        # Pattern-based scoring
        patterns_scores = {
            r'[：:]\s*[^\n]': 8,  # Colon patterns (form fields)
            r'お客様情報': 15,
            r'ご質問.*[：:]': 12,
            r'申し込み.*[：:]': 15,
            r'\d+-\d+-\d+': 10,  # Phone/postal patterns
            r'@[a-zA-Z0-9.-]+\.': 12,  # Email pattern
            r'[都道府県市区町村]': 8,  # Japanese address
            r'\d+(?:万|千|億)円': 10,  # Price patterns
            r'https?://': 6,  # URLs
            r'www\.': 4
        }
        
        for pattern, score in patterns_scores.items():
            if re.search(pattern, email_text, re.IGNORECASE):
                relevance_score += score
        
        # Structural indicators
        if len(re.findall(r'[：:]', email_text)) >= 3:
            relevance_score += 10  # Multiple colon patterns suggest form data
            
        if len(re.findall(r'\n', email_text)) >= 5:
            relevance_score += 5  # Multi-line content
        
        # Negative indicators (reduce score)
        negative_keywords = [
            'spam', 'advertisement', '広告', '宣伝', 
            'newsletter', 'unsubscribe', '配信停止',
            'notification', '通知', 'alert', 'アラート'
        ]
        
        for keyword in negative_keywords:
            if keyword in email_text:
                relevance_score -= 10
        
        # Normalize score
        confidence = min(1.0, relevance_score / max_score)
        is_relevant = confidence >= 0.3  # 30% threshold
        
        logger.info(f"Relevance check: score={relevance_score:.1f}, confidence={confidence:.2f}, relevant={is_relevant}")
        return is_relevant, confidence
    
    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=3)
    def send_to_webhook(self, data: Dict, email_id: str = None) -> bool:
        """Enhanced webhook sending with retry logic and better error handling"""
        if not self.webhook_url:
            logger.warning("No webhook URL configured, skipping webhook send")
            return False
        
        try:
            # Add metadata to webhook payload
            webhook_payload = {
                'email_id': email_id,
                'timestamp': datetime.now().isoformat(),
                'processor_version': '2.0',
                'data': data
            }
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Enhanced-Email-Processor/2.0',
                'X-Processor-Version': '2.0'
            }
            
            response = requests.post(
                self.webhook_url,
                json=webhook_payload,
                headers=headers,
                timeout=30
            )
            
            if response.status_code in [200, 201, 202, 204]:
                logger.info(f"Data successfully sent to webhook for email {email_id}")
                return True
            else:
                logger.warning(f"Webhook returned status {response.status_code} for email {email_id}: {response.text[:200]}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending to webhook for email {email_id}: {e}")
            return False
    
    def archive_email(self, email_id: str) -> bool:
        """Archive processed email with better error handling"""
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
            
            logger.debug(f"Archived email {email_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to archive email {email_id}: {e}")
            return False
    
    def process_single_email(self, email_data: Dict) -> ProcessingResult:
        """Process a single email and return detailed results"""
        start_time = time.time()
        email_id = email_data.get('id', 'unknown')
        
        result = ProcessingResult(
            success=False,
            email_id=email_id
        )
        
        try:
            subject = email_data.get('subject', 'No Subject')
            sender = email_data.get('sender', 'Unknown Sender')
            
            logger.info(f"Processing email {email_id}: '{subject}' from {sender}")
            
            # Check relevance first
            is_relevant, relevance_confidence = self.check_data_relevance(email_data)
            
            if not is_relevant:
                logger.info(f"Email {email_id} - Not relevant (confidence: {relevance_confidence:.2f})")
                result.success = True
                result.error_message = f"Low relevance (confidence: {relevance_confidence:.2f})"
                return result
            
            # Extract universal JSON data
            universal_data, extracted_fields = self.json_processor.extract_universal_json_data(email_data)
            
            # Calculate overall extraction confidence
            if extracted_fields:
                confidences = [field.confidence for field in extracted_fields.values()]
                avg_confidence = sum(confidences) / len(confidences)
            else:
                avg_confidence = 0.0
            
            # Check if we extracted meaningful data
            has_meaningful_data = (
                avg_confidence >= self.min_confidence_threshold and
                len(extracted_fields) >= 2  # At least 2 fields extracted
            )
            
            key_fields = ['name', 'email', 'phone', 'inquiry_text']
            has_key_field = any(field in extracted_fields for field in key_fields)
            
            if not has_meaningful_data and not has_key_field:
                logger.info(f"Email {email_id} - Insufficient meaningful data (confidence: {avg_confidence:.2f}, fields: {len(extracted_fields)})")
                result.success = True
                result.universal_data = universal_data
                result.extracted_fields = extracted_fields
                result.error_message = f"Insufficient data quality (confidence: {avg_confidence:.2f})"
                return result
            
            # Send to webhook if configured
            webhook_success = False
            if self.webhook_url:
                webhook_success = self.send_to_webhook(universal_data, email_id)
                result.webhook_sent = webhook_success
                
                if webhook_success:
                    logger.info(f"Successfully processed and sent webhook for email {email_id}")
                else:
                    logger.error(f"Failed to send webhook for email {email_id}")
            else:
                logger.info(f"Successfully processed email {email_id} (no webhook configured)")
                result.webhook_sent = True  # Consider success if no webhook needed
            
            # Archive email if configured and successful
            if result.webhook_sent and self.archive_email(email_id):
                result.archived = True
            
            # Mark as successful
            result.success = True
            result.universal_data = universal_data
            result.extracted_fields = extracted_fields
            
            processing_time = int((time.time() - start_time) * 1000)
            logger.info(f"Email {email_id} processed successfully in {processing_time}ms (confidence: {avg_confidence:.2f})")
            
            return result
            
        except Exception as e:
            error_msg = f"Error processing email {email_id}: {e}"
            logger.error(error_msg)
            result.error_message = str(e)
            return result
    
    def process_emails(self) -> Dict:
        """Enhanced main processing function with better error handling and metrics"""
        try:
            # Clean up old database connections
            self.db.cleanup_old_connections()
            
            # Get latest unprocessed emails
            emails = self.get_latest_emails()
            
            if not emails:
                logger.info("No new emails to process")
                return {
                    'processed': 0,
                    'successful_webhooks': 0,
                    'failed_webhooks': 0,
                    'archived': 0,
                    'average_confidence': 0.0,
                    'total_processing_time_ms': 0
                }
            
            # Process emails
            results = []
            total_start_time = time.time()
            
            for email_data in emails:
                result = self.process_single_email(email_data)
                
                # Update result with email metadata
                result.email_id = email_data.get('id', 'unknown')
                
                # Store in database
                self.db.mark_email_processed(result)
                
                results.append(result)
            
            total_processing_time = int((time.time() - total_start_time) * 1000)
            
            # Calculate metrics
            processed_count = len(results)
            successful_webhooks = sum(1 for r in results if r.webhook_sent)
            failed_webhooks = processed_count - successful_webhooks
            archived_count = sum(1 for r in results if r.archived)
            
            # Calculate average confidence
            confidences = []
            for result in results:
                if result.extracted_fields:
                    field_confidences = [field.confidence for field in result.extracted_fields.values()]
                    if field_confidences:
                        confidences.append(sum(field_confidences) / len(field_confidences))
            
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            # Update statistics
            self.db.update_daily_stats(processed_count, successful_webhooks, failed_webhooks)
            
            summary = {
                'processed': processed_count,
                'successful_webhooks': successful_webhooks,
                'failed_webhooks': failed_webhooks,
                'archived': archived_count,
                'average_confidence': round(avg_confidence, 3),
                'total_processing_time_ms': total_processing_time,
                'avg_processing_time_per_email_ms': total_processing_time // processed_count if processed_count > 0 else 0
            }
            
            logger.info(f"Processing completed: {summary}")
            return summary
            
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
        """Run processing once and return detailed results"""
        logger.info("Running enhanced email processing once...")
        try:
            results = self.process_emails()
            logger.info(f"One-time processing completed: {results}")
            return results
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            return {'processed': 0, 'error': str(e)}
    
    def get_stats(self) -> Dict:
        """Get comprehensive processing statistics"""
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
    
    def health_check(self) -> Dict:
        """Perform health check of all components"""
        health = {
            'overall_status': 'healthy',
            'components': {},
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # Check Gmail API authentication
            if self.authenticate():
                health['components']['gmail_api'] = {'status': 'healthy', 'authenticated': True}
            else:
                health['components']['gmail_api'] = {'status': 'unhealthy', 'authenticated': False}
                health['overall_status'] = 'degraded'
            
            # Check database
            try:
                stats = self.db.get_stats()
                health['components']['database'] = {
                    'status': 'healthy', 
                    'total_processed': stats.get('total_processed', 0)
                }
            except Exception as e:
                health['components']['database'] = {'status': 'unhealthy', 'error': str(e)}
                health['overall_status'] = 'degraded'
            
            # Check webhook if configured
            if self.webhook_url:
                try:
                    # Send a test ping (without actual data)
                    response = requests.head(self.webhook_url, timeout=10)
                    if response.status_code < 500:
                        health['components']['webhook'] = {'status': 'healthy', 'url_accessible': True}
                    else:
                        health['components']['webhook'] = {'status': 'degraded', 'url_accessible': False}
                except:
                    health['components']['webhook'] = {'status': 'unhealthy', 'url_accessible': False}
                    health['overall_status'] = 'degraded'
            else:
                health['components']['webhook'] = {'status': 'not_configured'}
            
        except Exception as e:
            health['overall_status'] = 'unhealthy'
            health['error'] = str(e)
        
        return health


def main():
    """Enhanced main function with better CLI and error handling"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Enhanced Gmail Email Processor for Japanese Real Estate Forms',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python script.py --once                    # Run once
  python script.py --webhook https://...     # Run once with webhook
  python script.py --stats                   # Show statistics
  python script.py --health-check           # Check system health
  python script.py --setup-oauth            # Set up OAuth credentials
        """
    )
    
    parser.add_argument('--webhook', '-w', 
                       help='Webhook URL (or set WEBHOOK_URL env var)')
    parser.add_argument('--interval', '-i', type=int, 
                       help='Check interval in seconds (default: 60)')
    parser.add_argument('--once', action='store_true',
                       help='Run once instead of continuous mode')
    parser.add_argument('--stats', '-s', action='store_true',
                       help='Show processing statistics and exit')
    parser.add_argument('--health-check', action='store_true',
                       help='Perform health check and exit')
    parser.add_argument('--setup-oauth', action='store_true',
                       help='Set up OAuth credentials (run this first)')
    parser.add_argument('--clear-data', action='store_true',
                       help='Clear all processed email data (DANGER!)')
    parser.add_argument('--min-confidence', type=float, default=0.3,
                       help='Minimum confidence threshold (0.0-1.0)')
    
    args = parser.parse_args()
    
    # Initialize processor
    try:
        # Set environment variable if provided
        if args.min_confidence:
            os.environ['MIN_CONFIDENCE_THRESHOLD'] = str(args.min_confidence)
        
        processor = EnhancedGmailProcessor(webhook_url=args.webhook)
        
        # Set up OAuth if requested
        if args.setup_oauth:
            logger.info("Setting up OAuth credentials...")
            if processor.authenticate():
                logger.info("OAuth setup completed successfully!")
                logger.info("You can now run the processor normally.")
            else:
                logger.error("OAuth setup failed!")
            return
        
        # Clear data if requested
        if args.clear_data:
            logger.warning("This will delete ALL processed email data!")
            confirm = input("Type 'YES' to confirm: ")
            if confirm == 'YES':
                if processor.clear_processed_data():
                    logger.info("All processed email data cleared successfully")
                else:
                    logger.error("Failed to clear data")
            else:
                logger.info("Data clearing cancelled")
            return
        
        # Health check if requested
        if args.health_check:
            health = processor.health_check()
            print(f"\n=== System Health Check ===")
            print(f"Overall Status: {health['overall_status'].upper()}")
            print(f"Timestamp: {health['timestamp']}")
            
            for component, status in health['components'].items():
                print(f"\n{component.replace('_', ' ').title()}:")
                for key, value in status.items():
                    print(f"  {key}: {value}")
            
            if health['overall_status'] != 'healthy':
                exit(1)
            return
        
        # Show stats if requested
        if args.stats:
            stats = processor.get_stats()
            recent_emails = processor.get_recent_emails()
            
            print("\n=== Enhanced Email Processor Statistics ===")
            print(f"Total processed: {stats.get('total_processed', 0)}")
            print(f"Successful webhooks: {stats.get('successful_webhooks', 0)}")
            print(f"Failed webhooks: {stats.get('failed_webhooks', 0)}")
            print(f"Average confidence: {stats.get('average_confidence', 0.0):.3f}")
            print(f"Today processed: {stats.get('today_processed', 0)}")
            print(f"Today successful: {stats.get('today_successful', 0)}")
            
            if stats.get('field_extraction_stats'):
                print("\n=== Field Extraction Statistics ===")
                for field_stat in stats['field_extraction_stats'][:5]:
                    print(f"  {field_stat['field']}: {field_stat['count']} times (avg confidence: {field_stat['avg_confidence']:.3f})")
            
            if recent_emails:
                print("\n=== Recent Processed Emails ===")
                for email in recent_emails[:5]:
                    print(f"  - {email['subject']} from {email['sender']} ({email['processed_date']})")
            return
        
        logger.info("Enhanced email processor initialized successfully")
        
        if args.once:
            # Run once
            results = processor.run_once()
            if 'error' in results:
                logger.error(f"Processing failed: {results['error']}")
                exit(1)
            else:
                logger.info("Processing completed successfully")
        else:
            # Run continuously - Note: This won't work on Render, only for local testing
            interval = args.interval or int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))
            logger.info(f"Starting continuous email processing (checking every {interval} seconds)")
            logger.info("Note: Continuous mode is for local development only")
            
            while True:
                try:
                    logger.info("=" * 50)
                    logger.info("Checking for new emails...")
                    results = processor.process_emails()
                    
                    if results['processed'] > 0:
                        logger.info(f"✓ Processed {results['processed']} emails, "
                                  f"{results['successful_webhooks']} webhooks successful, "
                                  f"{results['archived']} archived (avg confidence: {results.get('average_confidence', 0):.2f})")
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
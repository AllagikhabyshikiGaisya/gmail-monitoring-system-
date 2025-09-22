#!/usr/bin/env python3

import os
import json
import re
import requests
import base64
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import html
import unicodedata

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
                r'([^\s\n]{2,10})\s*(?:様|さん|氏|殿)(?:\s|$)',  # Name with honorific
            ],
            'furigana': [
                r'(?:フリガナ|ふりがな|カナ|Furigana)[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'(?:よみがな|読み仮名)[：:\s]*([^\n\r]+?)(?:\n|$)',
                r'([ァ-ヾ\s]{3,20})(?:\s|$)'  # Katakana pattern
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
            
            # Company Information Patterns (Enhanced)
            'company_name': [
                r'(?:会社名|企業名|法人名|Company)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:勤務先|お勤め先)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\s]+(?:株式会社|有限会社|合同会社|LLC|Inc|Corp|Ltd))',
                r'(株式会社[^\s\n]+)',
                r'([^\s]+会社)(?:\s|$)'
            ],
            'branch_name': [
                r'(?:支店名|店舗名|営業所|Branch)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\s]+(?:支店|店舗|営業所|支社))(?:\s|$)'
            ],
            
            # Event Information Patterns (Enhanced)
            'event_name': [
                r'(?:イベント名|セミナー名|講座名|Event)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:説明会|相談会|見学会)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\n]*(?:セミナー|講座|説明会|相談会|見学会|イベント)[^\n]*)(?:\n|$)'
            ],
            'event_date': [
                r'(?:開催日|実施日|日程|Date)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(\d{4}年\d{1,2}月\d{1,2}日)',
                r'(\d{4}/\d{1,2}/\d{1,2})',
                r'(\d{1,2}月\d{1,2}日)',
                r'([月火水木金土日]曜日)',
                r'(令和\d+年\d+月\d+日)',
                r'(平成\d+年\d+月\d+日)'
            ],
            'event_time': [
                r'(?:時間|開催時間|実施時間|Time)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(\d{1,2}:\d{2}(?:～|〜|-)\d{1,2}:\d{2})',
                r'(\d{1,2}時\d{2}分?(?:～|〜|-)\d{1,2}時\d{2}分?)',
                r'(\d{1,2}時(?:～|〜|-)\d{1,2}時)',
                r'(午前|午後)\d{1,2}時'
            ],
            'event_place': [
                r'(?:会場|場所|開催場所|Venue)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:所在地|住所).*?会場[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\n]*(?:ホール|会館|センター|ビル|館)[^\n]*)(?:\n|$)'
            ],
            
            # Reservation Information Patterns (Enhanced)
            'preferred_date': [
                r'(?:希望日|ご希望日|予約希望日|Preferred Date)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:見学希望日|訪問希望日)[：:\s]*([^\n]+?)(?:\n|$)',
                r'第\d希望[：:\s]*([^\n]+?)(?:\n|$)'
            ],
            'preferred_time': [
                r'(?:希望時間|ご希望時間|Preferred Time)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:見学希望時間|訪問希望時間)[：:\s]*([^\n]+?)(?:\n|$)',
                r'都合の良い時間[：:\s]*([^\n]+?)(?:\n|$)'
            ],
            
            # Inquiry Information Patterns (Enhanced)
            'inquiry_text': [
                r'(?:お問い?合わせ内容|ご質問|相談内容|Inquiry)[：:\s]*([^\n]*?)(?:\n\n|$)',
                r'(?:メッセージ|内容|詳細)[：:\s]*([^\n]*?)(?:\n\n|$)',
                r'(?:ご要望|要望|希望)[：:\s]*([^\n]*?)(?:\n\n|$)',
                r'その他.*?[：:\s]*([^\n]*?)(?:\n\n|$)'
            ],
            'inquiry_source': [
                r'(?:きっかけ|お問い?合わせの?きっかけ|Source)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:どちらで|どこで).*?知り.*?[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:媒体|メディア)[：:\s]*([^\n]+?)(?:\n|$)'
            ],
            
            # Budget Information Patterns (Enhanced)
            'budget_monthly': [
                r'(?:希望返済額|月々の返済額|Monthly Payment)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:毎月の支払い|月額)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(\d+(?:万|萬)円?)(?:\s*/月|\s*毎月)'
            ],
            'monthly_rent': [
                r'(?:月々の家賃|現在の家賃|Monthly Rent)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:家賃|賃料)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(\d+(?:万|萬)円?)(?:\s*/月|\s*毎月).*?(?:家賃|賃料)'
            ],
            
            # Property Information Patterns (Enhanced)
            'property_name': [
                r'(?:物件名|建物名|Property)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:マンション名|アパート名)[：:\s]*([^\n]+?)(?:\n|$)',
                r'([^\n]*(?:マンション|アパート|ハウス|レジデンス|パーク)[^\n]*)(?:\n|$)'
            ],
            'property_type': [
                r'(?:物件種別|建物種別|Property Type)[：:\s]*([^\n]+?)(?:\n|$)',
                r'((?:分譲|賃貸)?(?:マンション|アパート|一戸建て|戸建|土地))(?:\s|$)',
                r'(?:種別|タイプ)[：:\s]*([^\n]+?)(?:\n|$)'
            ],
            'price': [
                r'(?:価格|金額|Price|販売価格)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(?:坪単価|㎡単価)[：:\s]*([^\n]+?)(?:\n|$)',
                r'(\d+(?:万|億|千万)円?)(?:\s|$)',
                r'(\d+,\d{3}(?:,\d{3})*円?)(?:\s|$)'
            ],
            
            # URLs (Enhanced)
            'url': [
                r'(https?://[^\s\n]+)',
                r'(?:URL|Link)[：:\s]*(https?://[^\s\n]+)',
                r'(?:詳細|詳しく).*?(https?://[^\s\n]+)',
                r'(www\.[^\s\n]+)',
                r'([a-zA-Z0-9.-]+\.(?:com|co\.jp|jp|net|org)[^\s]*)'
            ],
            
            # Additional Japanese-specific patterns
            'prefecture': [
                r'([都道府県])',
                r'(東京都|大阪府|京都府|北海道|[a-zA-Z]{2,3}県)',
            ],
            'city': [
                r'([市区町村])',
                r'([^\s]+(?:市|区|町|村))',
            ],
            'building_type': [
                r'((?:木造|鉄筋|RC|SRC)(?:造|構造)?)',
                r'((?:\d+階建て?|\d+F))',
            ],
            'room_layout': [
                r'(\d+[LDKS]{1,4})',
                r'(\d+(?:部屋|室))',
                r'([1-9][LDKS](?:\+[LDKS])*)',
            ]
        }


class GmailAPIProcessor:
    """Enhanced Gmail API processor with improved Japanese text handling"""
    
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    
    def __init__(self, credentials_path: str, token_path: str, webhook_url: str):
        """
        Initialize Gmail API processor
        
        Args:
            credentials_path: Path to credentials.json file
            token_path: Path to token.json file
            webhook_url: Webhook URL to send processed data
        """
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.webhook_url = webhook_url
        self.service = None
        self.processed_emails = set()
        self.universal_json = UniversalJSONProcessor()
        
        # Load processed emails from file if exists
        self.load_processed_emails()
        
    def load_processed_emails(self):
        """Load previously processed email IDs from file"""
        try:
            if os.path.exists('processed_emails.json'):
                with open('processed_emails.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_emails = set(data.get('processed_ids', []))
                    logger.info(f"Loaded {len(self.processed_emails)} processed email IDs")
        except Exception as e:
            logger.error(f"Error loading processed emails: {e}")
    
    def save_processed_emails(self):
        """Save processed email IDs to file"""
        try:
            with open('processed_emails.json', 'w', encoding='utf-8') as f:
                json.dump({'processed_ids': list(self.processed_emails)}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving processed emails: {e}")
    
    def authenticate(self) -> bool:
        """Authenticate and build Gmail service using existing credentials"""
        try:
            creds = None
            
            # Load existing token
            if os.path.exists(self.token_path):
                try:
                    with open(self.token_path, 'rb') as token:
                        creds = pickle.load(token)
                except Exception as e:
                    logger.warning(f"Could not load existing token: {e}")
                    if os.path.exists(self.token_path):
                        os.remove(self.token_path)
            
            # If there are no valid credentials, run the OAuth flow
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                    except Exception as e:
                        logger.warning(f"Token refresh failed: {e}")
                        creds = None
                
                if not creds:
                    if not os.path.exists(self.credentials_path):
                        logger.error(f"Credentials file not found: {self.credentials_path}")
                        return False
                    
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, self.SCOPES)
                    creds = flow.run_local_server(port=0)
                
                # Save the credentials for the next run
                with open(self.token_path, 'wb') as token:
                    pickle.dump(creds, token)
            
            # Build the Gmail service
            self.service = build('gmail', 'v1', credentials=creds)
            logger.info("Gmail API authentication successful")
            return True
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False
    
    def get_latest_emails(self, max_results: int = 10) -> List[Dict]:
        """
        Get latest emails from inbox
        
        Args:
            max_results: Maximum number of emails to fetch
            
        Returns:
            List of email dictionaries with metadata
        """
        try:
            if not self.service:
                if not self.authenticate():
                    return []
            
            # Get list of emails (latest first)
            results = self.service.users().messages().list(
                userId='me',
                labelIds=['INBOX'],
                maxResults=max_results
            ).execute()
            
            messages = results.get('messages', [])
            
            if not messages:
                logger.info("No messages found in inbox")
                return []
            
            emails = []
            for message in messages:
                # Skip if already processed
                if message['id'] in self.processed_emails:
                    continue
                
                try:
                    # Get full message
                    msg = self.service.users().messages().get(
                        userId='me', 
                        id=message['id'],
                        format='full'
                    ).execute()
                    
                    # Extract email data
                    email_data = self.extract_email_data(msg)
                    if email_data:
                        emails.append(email_data)
                except Exception as e:
                    logger.error(f"Error processing email {message['id']}: {e}")
                    continue
            
            logger.info(f"Retrieved {len(emails)} new emails")
            return emails
            
        except HttpError as error:
            logger.error(f"Gmail API error: {error}")
            return []
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
                    email_data['sender'] = value
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
        """
        Enhanced relevance check for Japanese content
        
        Args:
            email_data: Email data dictionary
            
        Returns:
            True if email contains relevant data, False otherwise
        """
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
            '年齢', 'age', '歳', '才',
            
            # Form and inquiry keywords
            'フォーム', 'form', 'お問い合わせ', '問い合わせ', '問合せ', 'inquiry',
            '申込', '申し込み', 'application', '登録', 'registration',
            '予約', 'reservation', 'booking', 'よやく',
            '相談', 'consultation', '見学', 'けんがく',
            'セミナー', 'seminar', 'イベント', 'event',
            
            # Real estate keywords
            '物件', 'property', '不動産', 'real estate', 'ぶっけん',
            '住宅', 'house', 'housing', 'じゅうたく',
            'マンション', 'mansion', 'アパート', 'apartment',
            '戸建', '一戸建て', 'house', 'こだて',
            '土地', 'land', 'とち',
            '賃貸', 'rental', 'ちんたい',
            '売買', 'sales', 'ばいばい',
            
            # Company and service keywords
            '会社', 'company', '企業', 'きぎょう',
            'サービス', 'service', 'しゃーびす',
            '資料', 'materials', '資料請求', 'しりょう',
            '説明会', 'briefing', 'せつめいかい',
            
            # Lark specific
            'lark', 'larksuite', '飛書', 'feishu',
            'webhook', 'api'
        ]
        
        # Check if any relevance keywords are present
        keyword_found = False
        for keyword in relevance_keywords:
            if keyword in email_text:
                logger.info(f"Found relevant keyword: {keyword}")
                keyword_found = True
                break
        
        # Enhanced pattern checking for structured data
        patterns_found = 0
        structure_patterns = [
            r'[：:]\s*[^\n]',  # Colon patterns common in forms
            r'お客様情報',
            r'ご質問.*[：:]',
            r'申し込み.*[：:]',
            r'フォーム',
            r'[^\s]+\s*[：:]\s*[^\s]',  # General field: value pattern
            r'\d+-\d+-\d+',  # Phone or postal code pattern
            r'@[a-zA-Z0-9.-]+\.',  # Email pattern
            r'[都道府県市区町村]',  # Japanese address components
            r'\d+(?:万|千|億)円',  # Japanese price patterns
            r'[月火水木金土日]曜日',  # Japanese day patterns
        ]
        
        for pattern in structure_patterns:
            if re.search(pattern, email_text, re.IGNORECASE):
                patterns_found += 1
                logger.info(f"Found relevant pattern: {pattern}")
        
        # More lenient check - accept if keyword found OR multiple patterns found
        is_relevant = keyword_found or patterns_found >= 2
        
        if is_relevant:
            logger.info(f"Email marked as relevant (keywords: {keyword_found}, patterns: {patterns_found})")
        else:
            logger.info(f"Email not relevant (keywords: {keyword_found}, patterns: {patterns_found})")
        
        return is_relevant
    
    def extract_universal_json_data(self, email_data: Dict) -> Dict:
        """
        Extract and map data to universal JSON format with enhanced Japanese processing
        
        Args:
            email_data: Processed email data
            
        Returns:
            Universal JSON format dictionary
        """
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
                        if match.groups():
                            value = match.group(1).strip()
                        else:
                            value = match.group(0).strip()
                        
                        # Clean and validate the extracted value
                        value = self.clean_extracted_value(value, field)
                        
                        if value and len(value) > 0:
                            # Map to universal JSON structure
                            self.map_field_to_json(universal_data, field, value)
                            extracted_data[field] = value
                            logger.info(f"Extracted {field}: {value}")
                            break
                except Exception as e:
                    logger.warning(f"Error processing pattern {pattern} for field {field}: {e}")
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
            # Extract only the email address if there's extra text
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', value)
            if email_match:
                value = email_match.group(1)
        
        elif field == 'phone':
            # Normalize phone numbers
            value = re.sub(r'[^\d\-\(\)\s]', '', value)
            value = re.sub(r'\s+', ' ', value).strip()
        
        elif field == 'postal_code':
            # Ensure postal code format
            digits = re.findall(r'\d', value)
            if len(digits) == 7:
                value = f"{digits[0]}{digits[1]}{digits[2]}-{digits[3]}{digits[4]}{digits[5]}{digits[6]}"
            elif not re.match(r'\d{3}-\d{4}', value):
                return ""
        
        elif field in ['age']:
            # Extract numbers only for age
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
            # Ensure URL format
            if not value.startswith(('http://', 'https://')):
                if value.startswith('www.'):
                    value = 'https://' + value
                elif '.' in value and not value.startswith('//'):
                    value = 'https://' + value
        
        # Remove common unwanted suffixes/prefixes
        unwanted_patterns = [
            r'^\s*[：:]\s*',  # Leading colon
            r'\s*[：:]\s*',  # Trailing colon
            r'^.*?[：:]\s*',  # Everything before colon (only if there's text after)
            r'\s*様\s*',    # Honorific suffix
            r'\s*さん\s*',   # Honorific suffix
            r'\s*殿\s*',     # Honorific suffix
        ]
        
        for pattern in unwanted_patterns:
            value = re.sub(pattern, '', value).strip()
        
        # Return empty string if value is too short or contains only special characters
        if len(value) < 1 or re.match(r'^[^\w\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]+', value):
            return ""
        
        return value
    
    def map_field_to_json(self, universal_data: Dict, field: str, value: str):
        """Map extracted field to correct position in universal JSON"""
        try:
            if not value or len(value.strip()) == 0:
                return
            
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
            elif field == 'company_name':
                universal_data["company_info(会社情報)"]["company_name(会社名)"] = value
                universal_data["property_info(物件情報)"]["company_name(会社名)"] = value
            elif field == 'branch_name':
                universal_data["company_info(会社情報)"]["branch_name(支店名)"] = value
                universal_data["property_info(物件情報)"]["branch_name(支店名)"] = value
            elif field == 'event_name':
                universal_data["event_info(イベント情報)"]["event_name(イベント名)"] = value
            elif field == 'event_date':
                universal_data["event_info(イベント情報)"]["event_date(開催日)"] = value
            elif field == 'event_time':
                universal_data["event_info(イベント情報)"]["event_time(時間)"] = value
            elif field == 'event_place':
                universal_data["event_info(イベント情報)"]["event_place(会場)"] = value
                universal_data["reservation_info(ご予約情報)"][0]["meeting_place(集合場所)"] = value
            elif field == 'preferred_date':
                universal_data["reservation_info(ご予約情報)"][0]["preferred_date(ご希望日)"] = value
            elif field == 'preferred_time':
                universal_data["reservation_info(ご予約情報)"][0]["preferred_time(ご希望時間)"] = value
            elif field == 'inquiry_text':
                universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_text(お問い合わせ内容)"] = value
                universal_data["customer_info(お客様情報)"][0]["comments(ご意見・ご質問等)"] = value
            elif field == 'inquiry_source':
                universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_source(お問い合わせのきっかけ)"] = value
                universal_data["customer_info(お客様情報)"][0]["registration_reason(会員登録のきっかけ)"] = value
            elif field == 'budget_monthly':
                universal_data["customer_info(お客様情報)"][0]["monthly_payment(月々の返済額)"] = value
                universal_data["survey_info(アンケート情報)"]["budget_monthly(希望返済額)"] = value
            elif field == 'monthly_rent':
                universal_data["customer_info(お客様情報)"][0]["monthly_rent(月々の家賃)"] = value
            elif field == 'property_name':
                universal_data["property_info(物件情報)"]["property_name(物件名)"] = value
                universal_data["reservation_info(ご予約情報)"][0]["property_name(物件名)"] = value
            elif field == 'property_type':
                universal_data["property_info(物件情報)"]["property_type(物件種別)"] = value
                universal_data["reservation_info(ご予約情報)"][0]["property_type(物件種別)"] = value
            elif field == 'price':
                universal_data["property_info(物件情報)"]["price(価格)"] = value
                universal_data["reservation_info(ご予約情報)"][0]["price(価格)"] = value
            elif field == 'url':
                universal_data["company_info(会社情報)"]["url(URL)"] = value
                universal_data["event_info(イベント情報)"]["event_url(URL)"] = value
                universal_data["property_info(物件情報)"]["property_url(物件詳細画面)"] = value
            elif field == 'room_layout':
                universal_data["property_info(物件情報)"]["floor_plan(間取り)"] = value
            elif field == 'prefecture':
                # Add to address if not already there
                current_address = universal_data["customer_info(お客様情報)"][0]["address(ご住所)"]
                if not current_address:
                    universal_data["customer_info(お客様情報)"][0]["address(ご住所)"] = value
                elif value not in current_address:
                    universal_data["customer_info(お客様情報)"][0]["address(ご住所)"] = f"{value} {current_address}"
            elif field == 'city':
                # Add to address if not already there
                current_address = universal_data["customer_info(お客様情報)"][0]["address(ご住所)"]
                if not current_address:
                    universal_data["customer_info(お客様情報)"][0]["address(ご住所)"] = value
                elif value not in current_address:
                    universal_data["customer_info(お客様情報)"][0]["address(ご住所)"] = f"{current_address} {value}"
                    
        except Exception as e:
            logger.error(f"Error mapping field {field} with value '{value}': {e}")
    
    def send_to_webhook(self, data: Dict) -> bool:
        """Send processed data to webhook with retry logic"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                headers = {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Email-Processor/1.0'
                }
                
                response = requests.post(
                    self.webhook_url, 
                    json=data, 
                    headers=headers, 
                    timeout=30
                )
                
                if response.status_code in [200, 201, 204]:
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
    
    def save_processed_data(self, email_data: Dict, universal_data: Dict):
        """Save processed data to local file for debugging and backup"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"processed_data_{timestamp}.json"
            
            save_data = {
                'email_metadata': {
                    'id': email_data.get('id'),
                    'subject': email_data.get('subject'),
                    'sender': email_data.get('sender'),
                    'date': email_data.get('formatted_date'),
                    'body_length': len(email_data.get('body', ''))
                },
                'universal_json_data': universal_data,
                'processed_at': datetime.now().isoformat(),
                'webhook_url': self.webhook_url
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Processed data saved to: {filename}")
        except Exception as e:
            logger.error(f"Error saving processed data: {e}")
    
    def process_emails(self) -> int:
        """
        Main processing function - get emails, extract data, send to webhook
        
        Returns:
            Number of emails processed successfully
        """
        try:
            # Get latest emails
            emails = self.get_latest_emails()
            
            if not emails:
                logger.info("No new emails to process")
                return 0
            
            processed_count = 0
            
            for email_data in emails:
                email_id = email_data.get('id', 'unknown')
                subject = email_data.get('subject', 'No Subject')
                sender = email_data.get('sender', 'Unknown Sender')
                
                logger.info(f"Processing email {email_id}: '{subject}' from {sender}")
                
                try:
                    # Check if email contains relevant data
                    if not self.check_data_relevance(email_data):
                        logger.info(f"Email {email_id} - No relevant data found, skipping")
                        self.processed_emails.add(email_id)
                        continue
                    
                    # Extract universal JSON data
                    universal_data = self.extract_universal_json_data(email_data)
                    
                    # Check if we extracted any meaningful data
                    has_customer_data = any([
                        universal_data["customer_info(お客様情報)"][0]["name(お名前)"],
                        universal_data["customer_info(お客様情報)"][0]["email(メールアドレス)"],
                        universal_data["customer_info(お客様情報)"][0]["phone_number(電話番号)"],
                        universal_data["inquiry_info(お問い合わせ内容)"]["inquiry_text(お問い合わせ内容)"]
                    ])
                    
                    if not has_customer_data:
                        logger.info(f"Email {email_id} - No extractable customer data found")
                        self.processed_emails.add(email_id)
                        continue
                    
                    # Save processed data locally
                    self.save_processed_data(email_data, universal_data)
                    
                    # Send to webhook
                    if self.send_to_webhook(universal_data):
                        logger.info(f"Successfully processed email {email_id}")
                        processed_count += 1
                    else:
                        logger.error(f"Failed to send webhook for email {email_id}")
                    
                    # Mark as processed regardless of webhook success to avoid reprocessing
                    self.processed_emails.add(email_id)
                    
                except Exception as e:
                    logger.error(f"Error processing email {email_id}: {e}")
                    # Still mark as processed to avoid infinite retries on corrupted emails
                    self.processed_emails.add(email_id)
                    continue
            
            # Save processed email IDs
            self.save_processed_emails()
            
            logger.info(f"Processing completed: {processed_count} emails successfully processed")
            return processed_count
            
        except Exception as e:
            logger.error(f"Critical error in process_emails: {e}")
            return 0
    
    def run_continuous(self, interval_seconds: int = 20):
        """
        Run email processing continuously
        
        Args:
            interval_seconds: Seconds between email checks (default 20)
        """
        logger.info(f"Starting continuous email processing (checking every {interval_seconds} seconds)")
        
        while True:
            try:
                logger.info("=" * 50)
                logger.info("Checking for new emails...")
                processed = self.process_emails()
                
                if processed > 0:
                    logger.info(f"✓ Processed {processed} emails in this cycle")
                else:
                    logger.info("✓ No new emails to process")
                
                logger.info(f"Waiting {interval_seconds} seconds until next check...")
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("Email processing stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in continuous processing: {e}")
                logger.info("Waiting 60 seconds before retrying...")
                time.sleep(60)
    
    def run_once(self) -> int:
        """Run processing once and return number of processed emails"""
        logger.info("Running email processing once...")
        try:
            processed = self.process_emails()
            logger.info(f"One-time processing completed: {processed} emails processed")
            return processed
        except Exception as e:
            logger.error(f"Error in run_once: {e}")
            return 0


class EmailProcessorStats:
    """Statistics and monitoring for email processor"""
    
    def __init__(self):
        self.stats_file = 'processor_stats.json'
        self.stats = self.load_stats()
    
    def load_stats(self) -> Dict:
        """Load processing statistics"""
        try:
            if os.path.exists(self.stats_file):
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {
                'total_processed': 0,
                'successful_webhooks': 0,
                'failed_webhooks': 0,
                'last_run': None,
                'daily_stats': {},
                'created_at': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error loading stats: {e}")
            return {}
    
    def save_stats(self):
        """Save statistics to file"""
        try:
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving stats: {e}")
    
    def update_stats(self, processed: int, successful: int):
        """Update processing statistics"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        self.stats['total_processed'] += processed
        self.stats['successful_webhooks'] += successful
        self.stats['failed_webhooks'] += (processed - successful)
        self.stats['last_run'] = datetime.now().isoformat()
        
        if today not in self.stats['daily_stats']:
            self.stats['daily_stats'][today] = {
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'first_run': datetime.now().isoformat()
            }
        
        self.stats['daily_stats'][today]['processed'] += processed
        self.stats['daily_stats'][today]['successful'] += successful
        self.stats['daily_stats'][today]['failed'] += (processed - successful)
        self.stats['daily_stats'][today]['last_run'] = datetime.now().isoformat()
        
        self.save_stats()
        logger.info(f"Stats updated: Total processed={self.stats['total_processed']}, Today processed={self.stats['daily_stats'][today]['processed']}")
    
    def get_stats(self) -> Dict:
        """Get current statistics"""
        return self.stats
    
    def get_daily_stats(self, days: int = 7) -> Dict:
        """Get statistics for recent days"""
        daily_stats = {}
        for i in range(days):
            date = datetime.now().date() - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            daily_stats[date_str] = self.stats['daily_stats'].get(date_str, {
                'processed': 0,
                'successful': 0,
                'failed': 0
            })
        return daily_stats


def main():
    """Main function to run the email processor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Gmail Email Processor for Japanese Real Estate Forms')
    parser.add_argument('--credentials', '-c', default='credentials.json', 
                       help='Path to Gmail API credentials file (default: credentials.json)')
    parser.add_argument('--token', '-t', default='token.json',
                       help='Path to token file (default: token.json)')
    parser.add_argument('--webhook', '-w', required=True,
                       help='Webhook URL to send processed data')
    parser.add_argument('--interval', '-i', type=int, default=20,
                       help='Check interval in seconds for continuous mode (default: 20)')
    parser.add_argument('--once', action='store_true',
                       help='Run once instead of continuous mode')
    parser.add_argument('--max-emails', '-m', type=int, default=10,
                       help='Maximum number of emails to fetch per check (default: 10)')
    parser.add_argument('--stats', '-s', action='store_true',
                       help='Show processing statistics and exit')
    
    args = parser.parse_args()
    
    # Show stats if requested
    if args.stats:
        stats = EmailProcessorStats()
        current_stats = stats.get_stats()
        daily_stats = stats.get_daily_stats(7)
        
        print("\n=== Email Processor Statistics ===")
        print(f"Total processed: {current_stats.get('total_processed', 0)}")
        print(f"Successful webhooks: {current_stats.get('successful_webhooks', 0)}")
        print(f"Failed webhooks: {current_stats.get('failed_webhooks', 0)}")
        print(f"Last run: {current_stats.get('last_run', 'Never')}")
        
        print("\n=== Daily Statistics (Last 7 days) ===")
        for date, stats_data in daily_stats.items():
            if stats_data['processed'] > 0:
                print(f"{date}: {stats_data['processed']} processed, {stats_data['successful']} successful")
        return
    
    # Validate required files and parameters
    if not os.path.exists(args.credentials):
        logger.error(f"Credentials file not found: {args.credentials}")
        logger.error("Please download credentials.json from Google Cloud Console")
        return
    
    if not args.webhook:
        logger.error("Webhook URL is required")
        return
    
    # Initialize processor
    try:
        processor = GmailAPIProcessor(
            credentials_path=args.credentials,
            token_path=args.token,
            webhook_url=args.webhook
        )
        
        # Initialize stats tracking
        stats = EmailProcessorStats()
        
        logger.info("Email processor initialized successfully")
        logger.info(f"Credentials: {args.credentials}")
        logger.info(f"Token: {args.token}")
        logger.info(f"Webhook: {args.webhook}")
        
        if args.once:
            # Run once
            processed = processor.run_once()
            stats.update_stats(processed, processed)  # Assume all processed emails were successful
        else:
            # Run continuously
            processor.run_continuous(args.interval)
            
    except KeyboardInterrupt:
        logger.info("Email processor stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
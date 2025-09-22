#!/usr/bin/env python3
"""
Quick Start Script for Enhanced Email Processor
Run this to get started quickly with the email processor system
"""

import os
import sys
import subprocess
import json

def print_header():
    print("=" * 70)
    print("🚀 Enhanced Email Processor - Quick Start")
    print("=" * 70)
    print("This script will help you set up and test the email processor system.")
    print()

def check_python_version():
    """Check if Python version is adequate"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required.")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True

def check_files():
    """Check if required files exist"""
    required_files = {
        'credentials.json': 'Gmail API credentials (from your existing setup)',
        'email_processor.py': 'Main email processing logic',
        'app.py': 'Web application server',
        'dashboard.html': 'Web dashboard interface',
        'requirements.txt': 'Python package requirements'
    }
    
    missing_files = []
    for file, description in required_files.items():
        if os.path.exists(file):
            print(f"✅ Found: {file}")
        else:
            print(f"❌ Missing: {file} ({description})")
            missing_files.append(file)
    
    return len(missing_files) == 0

def install_dependencies():
    """Install Python dependencies"""
    print("\n📦 Installing Python dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def create_initial_config():
    """Create initial configuration file"""
    config = {
        "credentials_path": "credentials.json",
        "token_path": "token.json",
        "webhook_url": "https://y8xp2r4oy7i.jp.larksuite.com/base/automation/webhook/event/DuuGaDaKVw5FCFhFKogjybwepic",
        "check_interval": 20,
        "max_emails": 10,
        "log_level": "INFO",
        "filter_keywords": ["lark", "larksuite", "イベント", "申込", "問い合わせ"],
        "auto_start": False
    }
    
    try:
        with open('config.json', 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        print("✅ Created initial configuration file (config.json)")
        return True
    except Exception as e:
        print(f"❌ Failed to create config: {e}")
        return False

def test_gmail_auth():
    """Test Gmail API authentication"""
    print("\n🔐 Testing Gmail API authentication...")
    try:
        from email_processor import GmailAPIProcessor
        
        processor = GmailAPIProcessor(
            credentials_path='credentials.json',
            token_path='token.json', 
            webhook_url='dummy_url'
        )
        
        if processor.authenticate():
            print("✅ Gmail API authentication successful!")
            return True
        else:
            print("❌ Gmail API authentication failed")
            print("   Please check your credentials.json file")
            return False
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Please ensure all dependencies are installed")
        return False
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False

def run_quick_test():
    """Run a quick test of the email processor"""
    print("\n🧪 Running quick test...")
    try:
        from email_processor import GmailAPIProcessor
        
        processor = GmailAPIProcessor(
            credentials_path='credentials.json',
            token_path='token.json',
            webhook_url='https://y8xp2r4oy7i.jp.larksuite.com/base/automation/webhook/event/DuuGaDaKVw5FCFhFKogjybwepic'
        )
        
        processed = processor.run_once()
        print(f"✅ Quick test completed! Processed {processed} emails.")
        
        if processed > 0:
            print("   📧 Emails were found and processed!")
            print("   📊 Check the dashboard for details")
        else:
            print("   📭 No new emails found (this is normal)")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def start_dashboard():
    """Start the web dashboard"""
    print("\n🌐 Starting web dashboard...")
    print("📊 Dashboard will be available at: http://localhost:5000")
    print("🎮 Use the dashboard to control and monitor the email processor")
    print()
    print("Press Ctrl+C to stop the application")
    print("=" * 70)
    
    try:
        # Import and run the Flask app
        from app import run_app
        run_app()
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        print("Please ensure app.py is in the current directory")
    except Exception as e:
        print(f"\n❌ Dashboard error: {e}")
        print("Please check the logs for more details")

def main():
    """Main setup function"""
    print_header()
    
    # Check Python version
    if not check_python_version():
        input("Press Enter to exit...")
        return
    
    print("\n📁 Checking required files...")
    if not check_files():
        print("\n❌ Some required files are missing.")
        print("Please ensure all files are in the current directory:")
        print("  - credentials.json (your Gmail API credentials)")
        print("  - email_processor.py, app.py, dashboard.html")
        print("  - requirements.txt")
        input("\nPress Enter to exit...")
        return
    
    # Install dependencies
    print("\n" + "="*50)
    if not install_dependencies():
        input("Press Enter to exit...")
        return
    
    # Create config if it doesn't exist
    if not os.path.exists('config.json'):
        print("\n⚙️ Creating initial configuration...")
        create_initial_config()
    else:
        print("\n✅ Configuration file already exists")
    
    # Test Gmail authentication
    print("\n" + "="*50)
    if not test_gmail_auth():
        print("\n❌ Gmail authentication failed.")
        print("Please check:")
        print("  1. credentials.json is valid")
        print("  2. Gmail API is enabled in Google Cloud Console")
        print("  3. OAuth consent screen is configured")
        
        choice = input("\nContinue anyway? (y/n): ").lower().strip()
        if choice != 'y':
            return
    
    # Quick test
    print("\n" + "="*50)
    run_quick_test()
    
    # Ask what to do next
    print("\n" + "="*50)
    print("🎉 Setup completed successfully!")
    print("\nWhat would you like to do next?")
    print("1. Start web dashboard (recommended)")
    print("2. Run processor once and exit")
    print("3. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == '1':
            start_dashboard()
            break
        elif choice == '2':
            run_quick_test()
            break
        elif choice == '3':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, or 3.")
    
    input("\nPress Enter to exit...")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Setup interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Setup error: {e}")
        input("Press Enter to exit...")
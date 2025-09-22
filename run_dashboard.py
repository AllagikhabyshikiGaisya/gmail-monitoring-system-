#!/usr/bin/env python3
"""
Simple Dashboard Launcher
Run this to start the web dashboard directly
"""

import os
import sys

def main():
    print("🚀 Starting Email Processor Dashboard...")
    print("📊 Dashboard will be available at: http://localhost:5000")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    
    try:
        # Check if app.py exists
        if not os.path.exists('app.py'):
            print("❌ Error: app.py not found in current directory")
            print("Please ensure all project files are in the same folder")
            return
        
        # Import and run the app
        sys.path.insert(0, '.')
        from app import run_app
        run_app()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please ensure all required dependencies are installed:")
        print("pip install flask flask-cors requests google-api-python-client google-auth google-auth-oauthlib")
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Please check that all required files are present and try again")
    
    input("\nPress Enter to exit...")

if __name__ == '__main__':
    main()
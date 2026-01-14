#!/usr/bin/env python3
"""
MiniRDBMS Demo Script
Run this to showcase the complete system
"""

import sys
import os
import time
import threading
import webbrowser


def demo_repl():
    """Quick REPL demo"""
    print("\n QUICK REPL DEMO")
    print("-" * 30)
    
    from rdbms.parser import parse
    from rdbms.repl import execute
    
    demo_queries = [
        "SELECT * FROM users;",
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id;"
    ]
    
    for query in demo_queries:
        print(f"\nSQL: {query}")
        try:
            command = parse(query)
            result = execute(command)
            if isinstance(result, list):
                print(f"Results: {len(result)} rows found")
                for i, row in enumerate(result[:2], 1):
                    print(f"  {i}. {row}")
                if len(result) > 2:
                    print(f"  ... and {len(result) - 2} more")
            else:
                print(f"Result: {result}")
        except Exception as e:
            print(f"Error: {e}")

def start_web_app():
    """Start Flask web app"""
    print("\n STARTING WEB DEMO")
    print("-" * 30)
    print("Web app will be available at: http://localhost:5000")
    print("Press Ctrl+C to stop the web server")
    
    time.sleep(2)
    
    # Start Flask app
    from web.app import app
    app.run(debug=False, host="0.0.0.0", port=5000)

def main():
    """Main demo function"""
    
    print("\nChoose demo mode:")
    print("1. REPL demo")
    print("2. Web app demo")
    print("3. Both")
    
    try:
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == "1":
            demo_repl()
            print("\n REPL demo complete!")
            
        elif choice == "2":
            start_web_app()
            
        elif choice == "3":
            demo_repl()
            print("\n REPL demo complete! Starting web app...")
            start_web_app()
            
        else:
            print("Invalid choice. Starting web app...")
            start_web_app()
            
    except KeyboardInterrupt:
        print("\n\n Demo stopped by user")
    except Exception as e:
        print(f"\n Demo error: {e}")

if __name__ == "__main__":
    main()

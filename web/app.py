#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, request, redirect, url_for
from rdbms.engine import insert_into, select, update, delete_from

app = Flask(__name__)

@app.route("/")
def index():
    """Display all users"""
    try:
        users = select("users", ordered_by_pk=True)
        return render_template("index.html", users=users)
    except Exception as e:
        return render_template("index.html", users=[], error=str(e))

@app.route("/add", methods=["POST"])
def add():
    """Add a new user"""
    try:
        user_data = {
            "id": int(request.form["id"]),
            "name": request.form["name"],
            "email": request.form["email"]
        }
        result = insert_into("users", user_data)
        print(f"Insert result: {result}")
    except Exception as e:
        print(f"Insert error: {e}")
    
    return redirect("/")

@app.route("/edit/<int:id>", methods=["GET", "POST"])
def edit(id):
    """Edit a user"""
    if request.method == "POST":
        try:
            result = update("users", "name", request.form["name"], "id", id)
            print(f"Update result: {result}")
        except Exception as e:
            print(f"Update error: {e}")
        
        return redirect("/")
    else:
        # GET request - show edit form
        users = select("users", ordered_by_pk=True)
        user = None
        for u in users:
            if u["id"] == id:
                user = u
                break
        
        return render_template("edit.html", user=user)

@app.route("/delete/<int:id>")
def delete(id):
    """Delete a user"""
    try:
        result = delete_from("users", "id", id)
        print(f"Delete result: {result}")
    except Exception as e:
        print(f"Delete error: {e}")
    
    return redirect("/")

@app.route("/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy", "database": "MiniRDBMS"}

if __name__ == "__main__":
    print("🚀 Starting MiniRDBMS Demo App")
    print("📱 Open http://localhost:5000 in your browser")
    app.run(debug=True, host="0.0.0.0", port=5000)

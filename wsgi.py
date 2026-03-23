"""WSGI entry point for Railway deployment."""
import sys
import os

# Resolve paths from this file's location (repo root)
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.join(ROOT_DIR, "backend")
FRONTEND_DIR = os.path.join(ROOT_DIR, "frontend")

# Add backend to Python path so imports work
sys.path.insert(0, BACKEND_DIR)

# Set env var so app.py can find frontend
os.environ["THETAFLOW_FRONTEND"] = FRONTEND_DIR

from app import app

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5002))
    app.run(host="0.0.0.0", port=port)

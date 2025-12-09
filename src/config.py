"""
Environment configuration for Neo4j database connection.
Loads settings from .env file or environment variables.
"""

import os
from dotenv import load_dotenv

# Load .env file from project root
load_dotenv()

# Neo4j Configuration
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'neo4j_dev')

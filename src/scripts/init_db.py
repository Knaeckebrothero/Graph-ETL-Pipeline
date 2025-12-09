#!/usr/bin/env python3
"""
Neo4j Database Initialization Script.
Verifies connection and optionally resets the database.
"""

import argparse
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import Neo4jDatabase


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Initialize and manage Neo4j database connection'
    )
    parser.add_argument(
        '--uri',
        default=os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
        help='Neo4j bolt URI (default: bolt://localhost:7687)'
    )
    parser.add_argument(
        '--user',
        default=os.getenv('NEO4J_USER', 'neo4j'),
        help='Neo4j username (default: neo4j)'
    )
    parser.add_argument(
        '--password',
        default=os.getenv('NEO4J_PASSWORD', 'neo4j_dev'),
        help='Neo4j password (default: neo4j_dev)'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Clear all data from the database'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    logger = setup_logging(args.verbose)

    logger.info(f"Connecting to Neo4j at {args.uri}")

    # Create database instance with provided credentials
    db = Neo4jDatabase(
        uri=args.uri,
        user=args.user,
        password=args.password
    )

    # Check connection
    try:
        if db.is_connected():
            logger.info("Successfully connected to Neo4j")
        else:
            logger.error("Failed to connect to Neo4j")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        sys.exit(1)

    # Handle reset
    if args.reset:
        confirm = input("Are you sure you want to delete all data? (yes/no): ")
        if confirm.lower() == 'yes':
            db.clear_all()
            logger.info("Database cleared successfully")
        else:
            logger.info("Reset cancelled")

    # Show stats
    if args.stats or not args.reset:
        try:
            stats = db.get_stats()
            logger.info("Database Statistics:")
            logger.info(f"  Total nodes: {stats['total_nodes']}")
            logger.info(f"  Total relationships: {stats['relationship_count']}")
            if stats['node_counts']:
                logger.info("  Node counts by label:")
                for label, count in stats['node_counts'].items():
                    logger.info(f"    {label}: {count}")
            else:
                logger.info("  No nodes in database")
        except Exception as e:
            logger.warning(f"Could not retrieve stats: {e}")

    # Cleanup
    db.close()
    logger.info("Done")


if __name__ == '__main__':
    main()

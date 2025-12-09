"""
Neo4j Database Manager.
Provides connection management and query execution for Neo4j graph database.
"""

import logging
from contextlib import contextmanager
from typing import Generator

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

import sys
sys.path.insert(0, str(__file__).rsplit('/', 2)[0])
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

logger = logging.getLogger(__name__)


class Neo4jDatabase:
    """
    Neo4j database connection manager with lazy initialization.

    Usage:
        from db import neo4j_db

        # Check connection
        if neo4j_db.is_connected():
            results = neo4j_db.query("MATCH (n) RETURN n LIMIT 10")

        # Use session context manager
        with neo4j_db.session() as session:
            result = session.run("MATCH (n) RETURN count(n)")

        # Cleanup on shutdown
        neo4j_db.close()
    """

    def __init__(
        self,
        uri: str = None,
        user: str = None,
        password: str = None,
    ):
        """
        Initialize Neo4j database manager.

        Args:
            uri: Neo4j bolt URI (defaults to NEO4J_URI from config)
            user: Database username (defaults to NEO4J_USER from config)
            password: Database password (defaults to NEO4J_PASSWORD from config)
        """
        self.uri = uri or NEO4J_URI
        self.user = user or NEO4J_USER
        self.password = password or NEO4J_PASSWORD
        self._driver = None

    @property
    def driver(self):
        """Lazy initialization of Neo4j driver."""
        if self._driver is None:
            try:
                self._driver = GraphDatabase.driver(
                    self.uri,
                    auth=(self.user, self.password)
                )
                self._driver.verify_connectivity()
                logger.info(f"Connected to Neo4j at {self.uri}")
            except ServiceUnavailable as e:
                logger.error(f"Neo4j service unavailable at {self.uri}: {e}")
                raise
            except AuthError as e:
                logger.error(f"Neo4j authentication failed: {e}")
                raise
        return self._driver

    @contextmanager
    def session(self) -> Generator:
        """
        Context manager for Neo4j session.
        Ensures session is properly closed after use.

        Usage:
            with neo4j_db.session() as session:
                result = session.run("MATCH (n) RETURN n")
        """
        session = self.driver.session()
        try:
            yield session
        finally:
            session.close()

    def query(self, cypher: str, params: dict = None) -> list:
        """
        Execute a Cypher query and return results as list of dicts.

        Args:
            cypher: Cypher query string
            params: Optional query parameters

        Returns:
            List of dictionaries containing query results
        """
        with self.session() as session:
            result = session.run(cypher, params or {})
            return [record.data() for record in result]

    def is_connected(self) -> bool:
        """
        Check if database connection is available.

        Returns:
            True if connected, False otherwise
        """
        try:
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            logger.warning(f"Neo4j connection check failed: {e}")
            return False

    def get_stats(self) -> dict:
        """
        Get database statistics.

        Returns:
            Dictionary with node counts by label and total relationship count
        """
        node_counts = {}

        # Get all labels and their counts
        labels_result = self.query("CALL db.labels()")
        for record in labels_result:
            label = record.get('label')
            if label:
                count_result = self.query(
                    f"MATCH (n:`{label}`) RETURN count(n) as count"
                )
                node_counts[label] = count_result[0]['count'] if count_result else 0

        # Get relationship count
        rel_result = self.query("MATCH ()-[r]->() RETURN count(r) as count")
        rel_count = rel_result[0]['count'] if rel_result else 0

        return {
            'node_counts': node_counts,
            'relationship_count': rel_count,
            'total_nodes': sum(node_counts.values())
        }

    def clear_all(self) -> None:
        """
        Delete all nodes and relationships from the database.
        Use with caution!
        """
        logger.warning("Clearing all data from Neo4j database")
        self.query("MATCH (n) DETACH DELETE n")
        logger.info("All data cleared from Neo4j database")

    def close(self) -> None:
        """Close the database connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            logger.info("Neo4j connection closed")


# Singleton instance for convenience
neo4j_db = Neo4jDatabase()

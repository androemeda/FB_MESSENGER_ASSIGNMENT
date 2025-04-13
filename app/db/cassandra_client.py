"""
Cassandra client for the Messenger application.
This provides a connection to the Cassandra database.
"""
import os
import uuid
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from cassandra.cluster import Cluster, Session, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger(__name__)

class CassandraClient:
    """Singleton Cassandra client for the application."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the Cassandra connection."""
        if self._initialized:
            return
        
        self.host = os.getenv("CASSANDRA_HOST", "cassandra")  # default changed from "localhost" to "cassandra"
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        
        self.cluster = None
        self.session = None
        self.connect()
        
        self._initialized = True
    
    def connect(self) -> None:
        """Connect to the Cassandra cluster with retries."""
        retries = 10
        for i in range(retries):
            try:
                self.cluster = Cluster([self.host])
                self.session = self.cluster.connect(self.keyspace)
                self.session.row_factory = dict_factory
                logger.info(f"Connected to Cassandra at {self.host}:{self.port}, keyspace: {self.keyspace}")
                return
            except NoHostAvailable as e:
                logger.warning(f"[{i+1}/{retries}] Cassandra not available yet, retrying in 3s...")
                time.sleep(3)
            except Exception as e:
                logger.error(f"Unexpected error during Cassandra connection: {str(e)}")
                time.sleep(3)
        raise Exception("Failed to connect to Cassandra after multiple retries.")
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def execute(self, query: str, params: dict = None) -> List[Dict[str, Any]]:
        """
        Execute a CQL query.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            List of rows as dictionaries
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, params or {})
            return list(result)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_async(self, query: str, params: dict = None):
        """
        Execute a CQL query asynchronously.
        
        Args:
            query: The CQL query string
            params: The parameters for the query
            
        Returns:
            Async result object
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            return self.session.execute_async(statement, params or {})
        except Exception as e:
            logger.error(f"Async query execution failed: {str(e)}")
            raise
    
    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self.connect()
        return self.session


# Create a global instance (lazy)
cassandra_client = None

def get_cassandra_client():
    global cassandra_client
    if cassandra_client is None:
        cassandra_client = CassandraClient()
    return cassandra_client

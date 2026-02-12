from db.initate_connection_interface import InitiateConnectionInterface
from psycopg2 import pool
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PostgresConnection(InitiateConnectionInterface):
    def __init__(self, connection_string: str, minconn=1, maxconn=5):
        self.connection_string = connection_string
        self.minconn = minconn
        self.maxconn = maxconn
        self.pool = None

    def initiate_connection(self):
        self.pool = pool.SimpleConnectionPool(self.minconn, self.maxconn, self.connection_string)
        logger.info("PostgreSQL connection pool initialized.")

    @contextmanager
    def get_connection(self):
        """
        Provides a context manager for acquiring a connection from the pool.
        Ensures the connection is returned to the pool after use.

        :return: A connection object from the pool.
        """

        if not self.pool:
            raise RuntimeError("Connection pool is not initialized.")
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error occurred: {e}")
            raise
        finally:
            self.pool.putconn(conn)


    @contextmanager
    def get_cursor(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute_query(self, query, params: tuple = None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
        
    def close_connection(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None
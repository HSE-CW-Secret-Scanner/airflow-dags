import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base

from config.logger import logger

class SecretDatabase:
    def __init__(self):
        """
        Initializes the class with connection parameters.
        """
        host = os.environ['PG_HOST']
        port = os.environ['PG_PORT']
        db_name = os.environ['PG_DB']
        user = os.environ['PG_USER']
        password = os.environ['PG_PASSWORD']
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(self.connection_string)
        self.metadata = MetaData()
        self.Base = automap_base(metadata=self.metadata)
        self.Session = sessionmaker(bind=self.engine)

    def reflect_tables(self, table_names):
        """
        Reflect specified tables from the database and prepare the declarative base.

        Args:
            table_names (list): List of table names to reflect.
        """
        try:
            self.metadata.reflect(self.engine, only=table_names)
            self.Base.prepare()
        except Exception as e:
            logger.error(f'Error during reflecting tables: {e}')
            raise

    def get_table_class(self, table_name):
        """
        Get the mapped class for a given table name.

        Args:
            table_name (str): Name of the table.

        Returns:
            class: Mapped class for the table.
        """
        return getattr(self.Base.classes, table_name)

    def connect(self):
        """
        Establishes a connection to the database.
        """
        try:
            self.engine = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Secrets database connection established")
        except SQLAlchemyError as e:
            logger.info(f"Error connecting to the database: {e}")
            raise

    def execute_query(self, query):
        """
        Executes a SQL query and returns the result.
        """
        if not self.Session:
            raise ConnectionError("DPC database connection is not established. Call connect() first")
        session = self.Session()
        try:
            result = session.execute(query)
            session.commit()
            return result
        except SQLAlchemyError as e:
            session.rollback()
            logger.info(f"Error executing query: {e}")
            raise
        finally:
            session.close()

    def close(self):
        """
        Closes the database connection.
        """
        if self.engine:
            self.engine.dispose()
            logger.info("DPC database connection closed")

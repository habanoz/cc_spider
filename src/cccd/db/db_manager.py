from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .base import Base


class DatabaseManager:
    def __init__(self, database_url):
        self.engine = create_engine(database_url, pool_size=1, max_overflow=0, pool_recycle=-1)
        self.session_factory = sessionmaker(bind=self.engine)

        Base.metadata.create_all(self.engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
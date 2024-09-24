from datetime import datetime

from sqlalchemy import Column, DateTime, String, Integer, Boolean, Index
from sqlalchemy.dialects.postgresql import UUID

from ..base import Base

class WebDoc(Base):
    __tablename__ = 'docs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, index=True, unique=True)
    filename = Column(String, nullable=False)
    offset = Column(Integer, nullable=False)
    length = Column(Integer, nullable=False)
    result = Column(Boolean, default=None, doc="None=Not processed, True=Success, False=Fail")
    label = Column(Boolean, default=None, doc="None=Not processed, True=News, False=Not news")
    created = Column(DateTime, nullable=False, default=datetime.now)
    updated = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (Index('web_doc_idx_result', "result"), )
    
    def __repr__(self):
        return f"<WebDoc(url={self.url}>"

    def __eq__(self, other):
        if isinstance(other, WebDoc):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)
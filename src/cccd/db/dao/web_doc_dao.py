from ..db_manager import DatabaseManager
from ..model.web_doc import WebDoc
from ..utils.gzip import comp, decomp
from sqlalchemy import update
from sqlalchemy import insert
from sqlalchemy import and_
from cccd.model.web_doct import WebDocT, WebDocResultT
from datetime import datetime

class WebDocDao:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def add(self, doc: WebDocT):
        with self.db_manager.session_scope() as session:
            new_chat = WebDoc(
                doc.url,
                doc.filename,
                doc.offset,
                doc.length
            )

            session.add(new_chat)
            
    def add_batch(self, docs: list[WebDocT]):
        with self.db_manager.session_scope() as session:
            session.execute(
                insert(WebDoc),
                [
                    {
                        'url': doc.url, 
                        'filename': doc.filename, 
                        'offset': doc.offset, 
                        'length': doc.length
                    } for doc in docs
                ]
            )

    def update_done(self, doc: WebDocResultT):
        with self.db_manager.session_scope() as session:
            session.query(WebDoc).filter(WebDoc.url == doc['url']).update(
                {
                    'result': doc.raw is not None,
                     "updated": datetime.now()
                }
            )

    def update_label(self, url: str, label:bool):
        with self.db_manager.session_scope() as session:
            session.query(WebDoc).filter(WebDoc.url == url).update(
                {
                    'label': label,
                     "updated": datetime.now()
                }
            )
    
    def update_label_batch(self, ids:list[int], labels:list[bool]):
        with self.db_manager.session_scope() as session:
            session.execute(
                update(WebDoc),
                [
                    {
                        "id": id,
                        "label": label,
                        "updated": datetime.now()
                    }
                    for id,label in zip(ids, labels)
                ]
            )
            
    def update_done_batch(self, docs: list[WebDocResultT]):
        with self.db_manager.session_scope() as session:
            session.execute(
                update(WebDoc),
                [
                    {
                        "id": doc.id,
                        "result": doc.raw is not None,
                        "updated": datetime.now()
                    }
                    for doc in docs
                ]
            )

    def get_by_url(self, url) -> WebDocResultT:
        with self.db_manager.session_scope() as session:
            web_doc = session.get(WebDoc, url)
            if web_doc is None:
                return None

            return WebDocResultT(url=web_doc.url, raw=decomp(web_doc.raw))

    def get_incomplete(self, limit: int = 10_000, offset=None) -> list[WebDocT]:
        with self.db_manager.session_scope() as session:
            query = session.query(WebDoc).filter(and_(WebDoc.result == None))

            if limit is not None:
                query = query.limit(limit)
            
            if offset:
                query = query.offset(offset).order_by(WebDoc.id)
                
            web_docs = query.all()

            return [
                WebDocT(
                    id=web_doc.id,
                    url=web_doc.url,
                    filename=web_doc.filename,
                    offset=web_doc.offset,
                    length=web_doc.length
                )
                for web_doc in web_docs
            ]

    def get_unclassified_urls(self) -> list:
        with self.db_manager.session_scope() as session:
            query = session.query(WebDoc.id,WebDoc.url).filter(WebDoc.label == None)

            return query.all()
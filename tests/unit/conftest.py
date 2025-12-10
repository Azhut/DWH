
import pytest
import mongomock
from copy import deepcopy



class AsyncCursor:
    def __init__(self, iterable):
        self._iter = iter(iterable)
        self._items = list(iterable)

    def __aiter__(self):
        self._iter = iter(self._items)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def to_list(self, length=None):
        # length игнорируем, возвращаем все
        return list(self._items)

class AsyncCollectionWrapper:
    """Обёртка вокруг sync pymongo collection, дающая async API, достаточную для наших тестов."""
    def __init__(self, sync_coll):
        self._coll = sync_coll

    # find возвращает AsyncCursor
    def find(self, *args, **kwargs):
        docs = list(self._coll.find(*args, **kwargs))
        return AsyncCursor(docs)

    async def find_one(self, *args, **kwargs):
        return self._coll.find_one(*args, **kwargs)

    async def insert_one(self, doc):
        return self._coll.insert_one(deepcopy(doc))

    async def insert_many(self, docs):
        return self._coll.insert_many([deepcopy(d) for d in docs])

    async def update_one(self, query, update, upsert=False):
        return self._coll.update_one(query, update, upsert=upsert)

    async def delete_one(self, query):
        return self._coll.delete_one(query)

    async def delete_many(self, query):
        return self._coll.delete_many(query)

    async def count_documents(self, query):
        return self._coll.count_documents(query)

    # Для FlatDataService bulk_write — реализуем простую совместимость
    async def bulk_write(self, operations, ordered=False, bypass_document_validation=False):
        # ожидаем список InsertOne(документы) или подобных
        # извлекаем документ из operations[i]._doc если есть
        inserted = 0
        for op in operations:
            # pymongo.operations.InsertOne stores document in ._doc
            doc = None
            if hasattr(op, "_doc"):
                doc = op._doc
            elif hasattr(op, "document"):
                doc = op.document
            if doc is not None:
                self._coll.insert_one(deepcopy(doc))
                inserted += 1
        class DummyResult:
            inserted_count = inserted
        return DummyResult()

@pytest.fixture
def async_mongo_db(tmp_path):
    """
    Возвращаем базу (замотанную в async-обёртки) на mongomock.
    tests могут брать коллекции через db['Files'] и т.д.
    """
    client = mongomock.MongoClient()
    db = client['test_db']
    # оборачиваем коллекции по требованию
    class DBWrapper:
        def __getitem__(self, name):
            coll = db[name]
            return AsyncCollectionWrapper(coll)
        def get_collection(self, name):
            return self.__getitem__(name)
        # для случаев, где код ожидает .name / .database
        @property
        def name(self):
            return 'test_db'
    yield DBWrapper()
    # mongomock не требует закрытия

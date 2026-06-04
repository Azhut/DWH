from __future__ import annotations

import asyncio

from app.application.data.indexes import MongoIndexManager


class FakeCollection:
    def __init__(self, indexes: dict[str, dict] | None = None) -> None:
        self.indexes = indexes or {}
        self.created: list[dict] = []

    async def index_information(self) -> dict[str, dict]:
        return self.indexes

    async def create_index(self, keys, **kwargs):
        self.created.append({"keys": keys, **kwargs})
        self.indexes[kwargs["name"]] = {"key": keys}
        return kwargs["name"]

    async def drop_index(self, name: str) -> None:
        self.indexes.pop(name, None)


class FakeDatabase:
    def __init__(self, collections: dict[str, FakeCollection]) -> None:
        self.collections = collections

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections[name]


def test_create_file_indexes_skips_existing_indexes() -> None:
    files = FakeCollection(
        {
            "uniq_file_id": {},
            "uniq_filename_form_id": {},
        }
    )
    manager = MongoIndexManager(FakeDatabase({"Files": files}))

    asyncio.run(manager.create_file_indexes())

    assert files.created == []


def test_create_file_indexes_creates_missing_indexes_once() -> None:
    files = FakeCollection()
    manager = MongoIndexManager(FakeDatabase({"Files": files}))

    asyncio.run(manager.create_file_indexes())
    asyncio.run(manager.create_file_indexes())

    assert [index["name"] for index in files.created] == [
        "uniq_file_id",
        "uniq_filename_form_id",
    ]
    assert all(index["unique"] is True for index in files.created)

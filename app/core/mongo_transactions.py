"""Много-документные транзакции MongoDB и безопасный откат на выполнение без сессии."""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, TypeVar

from pymongo.errors import OperationFailure

from app.core.database import mongo_connection
from config.config import config

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _transaction_unsupported(exc: OperationFailure) -> bool:
    c = exc.code
    msg = str(exc).lower()
    if c in (20, 303, 116):
        return True
    if "replica set" in msg:
        return True
    if "transaction" in msg and ("not supported" in msg or "not allowed" in msg):
        return True
    return False


async def run_in_transaction(coro: Callable[[Any], Awaitable[T]]) -> T:
    """
    Выполняет асинхронную функцию с сессией транзакции.

    Если в настройках отключены транзакции, передаётся session=None (операции без много-документной атомарности).
    При ошибке «нет replica set» выполняется одна попытка без транзакции с session=None.
    """
    if not config.MONGO_USE_TRANSACTIONS:
        return await coro(None)

    client = mongo_connection.get_client()
    if client is None:
        raise RuntimeError("Клиент MongoDB не инициализирован")

    try:
        async with await client.start_session() as session:
            async with session.start_transaction():
                return await coro(session)
    except OperationFailure as exc:
        if _transaction_unsupported(exc):
            logger.warning(
                "Транзакции MongoDB недоступны, выполняется сценарий без транзакции: %s",
                exc,
            )
            return await coro(None)
        raise

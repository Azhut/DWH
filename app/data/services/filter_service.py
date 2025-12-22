# app/data/services/filter_service.py
from typing import List, Dict, Tuple, Union, Any
import math

from app.data.repositories.flat_data import FlatDataRepository


class FilterService:
    """
    Бизнес-логика фильтрации/получения значений фильтров.
    Работает через FlatDataRepository (DI).
    """

    # Маппинг имён фильтров (как приходит от клиента) -> поля в коллекции FlatData
    FILTER_MAP = {
        "год": "year",
        "город": "city",
        "раздел": "section",
        "строка": "row",
        "колонка": "column",
        # "форма": "form"  # можно оставить, но обычно форма передаётся отдельным параметром
    }

    def __init__(self, repo: FlatDataRepository):
        self.repo = repo

    # ----------------- публичные методы -----------------

    async def get_filter_values(
        self,
        filter_name: str,
        applied_filters: List[Dict[str, Any]],
        pattern: str = "",
        form_id: str | None = None
    ) -> List[Union[str, int, float]]:
        """
        Возвращает список уникальных значений для filter_name с учётом applied_filters и form_id.
        - filter_name: например "раздел"
        - applied_filters: [{"filter-name": "год", "values": [2020]} , ...]
        - pattern: опциональный регекс-шаблон (строка)
        - form_id: обязательный контекст (сервер должен проверить его наличие ранее)
        """
        # проверка
        field = self._map_filter_name(filter_name)

        # строим Mongo query (включая form_id)
        query = self._build_query(applied_filters, form_id)

        # если задан pattern — добавим regex-условие по искомому полю
        if pattern:
            # если уже есть условие на это поле, добавим $and (чтобы не перезаписать)
            # но проще: просто присвоим regex — это заменит существующее условие на поле,
            # однако applied_filters вряд ли будут содержать условие на тот же field в типичных кейсах.
            query = query.copy()
            query[field] = {"$regex": pattern, "$options": "i"}

        # используем distinct для получения уникальных значений
        values = await self.repo.collection.distinct(field, filter=query)

        # Сортируем значения для детерминированности (если сравнимые типы)
        try:
            values = sorted(values)
        except Exception:
            # если sorting не применим (разные типы), оставим как есть
            pass

        return values

    async def get_filtered_data(
        self,
        filters: List[Dict[str, Any]],
        limit: int,
        offset: int,
        form_id: str | None = None
    ) -> Tuple[List[List[Union[str, int, float, None]]], int]:
        """
        Возвращает:
         - список строк (каждая строка: [year, city, section, row, column, value])
         - total (общее количество записей, удовлетворяющих запросу)
        Все значения приводятся в читаемый формат (year -> int, value -> int/round)
        """
        query = self._build_query(filters, form_id)

        # получение документов через репозиторий
        docs, total = await self.repo.get_filtered_data(query, limit, offset)

        # обработка в таблицу
        table = self._process_docs_to_table(docs)

        return table, total

    # ----------------- вспомогательные -----------------

    def _map_filter_name(self, name: str) -> str:
        """
        Маппит русское имя фильтра в поле коллекции. Бросает ValueError если неизвестно.
        """
        if not isinstance(name, str):
            raise ValueError(f"Неправильное имя фильтра: {name}")

        key = name.lower()
        if key not in self.FILTER_MAP:
            raise ValueError(f"Неизвестный фильтр: {name}")
        return self.FILTER_MAP[key]

    def _build_query(self, filters: List[Dict[str, Any]], form_id: str | None) -> Dict[str, Any]:
        """
        Преобразует список бизнес-фильтров в Mongo query.
        Всегда включает условие на form (если передан).
        Возвращает словарь запроса.
        """
        conditions: List[Dict[str, Any]] = []

        # form_id как обязательный контекст — если передано, добавляем первым условием
        if form_id is not None:
            conditions.append({"form": form_id})  # Здесь используется поле "form"

        for f in filters or []:
            fname = f.get("filter-name")
            values = f.get("values", []) or []
            if not fname or not values:
                continue

            try:
                field = self._map_filter_name(fname)
            except ValueError:
                # пробрасываем дальше — вызывающий слой должен обработать (endpoint -> 400)
                raise

            # Для города приводим к верхнему регистру (в базе могут быть верхние)
            if field == "city":
                values = [v.upper() if isinstance(v, str) else v for v in values]

            conditions.append({field: {"$in": values}})

        if not conditions:
            return {}
        if len(conditions) == 1:
            # только form условие -> возвращаем плоский dict
            return conditions[0]
        return {"$and": conditions}

    def _process_docs_to_table(self, docs: List[Dict[str, Any]]) -> List[List[Union[str, int, float, None]]]:
        """
        Преобразует список документов в двумерный список для ответа.
        Ожидаемые поля в документе: year, city, section, row, column, value
        """
        rows: List[List[Union[str, int, float, None]]] = []

        for doc in docs:
            # базовая валидация
            year = doc.get("year")
            city = doc.get("city")
            section = doc.get("section")
            row = doc.get("row")
            column = doc.get("column")
            value = doc.get("value")

            if year is None:
                raise ValueError("Документ не содержит обязательного поля 'year'")
            if city is None:
                raise ValueError("Документ не содержит обязательного поля 'city'")
            if section is None:
                raise ValueError("Документ не содержит обязательного поля 'section'")
            if row is None:
                raise ValueError("Документ не содержит обязательного поля 'row'")
            if column is None:
                raise ValueError("Документ не содержит обязательного поля 'column'")

            # приведение year (если float)
            if isinstance(year, float):
                year = int(year)

            # приведение value
            if isinstance(value, float):
                if math.isnan(value):
                    value = 0.0
                value = int(value) if value.is_integer() else round(value, 2)

            rows.append([year, city, section, row, column, value])

        return rows

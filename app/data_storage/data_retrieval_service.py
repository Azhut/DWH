from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Dict

class DataRetrievalService:
    def __init__(self, db_uri: str, db_name: str):
        self.client = AsyncIOMotorClient(db_uri)
        self.db = self.client[db_name]
        self.sheets_collection = self.db.get_collection("Sheets")

    async def get_all_sections(self) -> List[str]:
        """
        Возвращает список всех уникальных значений sheet_name (разделов).
        """
        sections = await self.sheets_collection.distinct("sheet_name")
        return sections

    async def get_document_info(self, cities: List[str], years: List[int]) -> (List[str], List[int], List[str]):
        """
        Возвращает уникальные города, годы и секции, соответствующие фильтрам.
        """
        query = {}
        if cities:
            query["city"] = {"$in": cities}
        if years:
            query["year"] = {"$in": years}

        cursor = self.sheets_collection.find(query, {"city": 1, "year": 1, "sheet_name": 1})
        results = await cursor.to_list(length=None)

        unique_cities = list({doc["city"] for doc in results})
        unique_years = list({doc["year"] for doc in results})
        unique_sections = list({doc["sheet_name"] for doc in results})

        return unique_cities, unique_years, unique_sections

    async def get_document_fields(self, section: str, cities: List[str], years: List[int]) -> (List[str], List[str]):
        """
        Возвращает уникальные строки (rows) и столбцы (columns) для указанной секции и фильтров.
        """
        query = {"sheet_name": section}
        if cities:
            query["city"] = {"$in": cities}
        if years:
            query["year"] = {"$in": years}

        cursor = self.sheets_collection.find(query, {"headers": 1})
        results = await cursor.to_list(length=None)

        rows = set()
        columns = set()
        for doc in results:
            rows.update(doc.get("headers", {}).get("vertical", []))
            columns.update(doc.get("headers", {}).get("horizontal", []))

        return list(rows), list(columns)

    async def get_documents(
        self, section: str, cities: List[str], years: List[int], rows: List[str], columns: List[str]
    ) -> List[Dict]:
        """
        Возвращает документы с данными, соответствующими фильтрам.
        """
        query = {"sheet_name": section}
        if cities:
            query["city"] = {"$in": cities}
        if years:
            query["year"] = {"$in": years}

        cursor = self.sheets_collection.find(query)
        results = await cursor.to_list(length=None)

        documents = []
        for doc in results:
            filtered_data = []

            for col in doc.get("data", []):
                if columns and col["column_header"] not in columns:
                    continue

                filtered_values = []
                for value in col.get("values", []):
                    if rows and value["row_header"] not in rows:
                        continue
                    filtered_values.append(value)

                if filtered_values:
                    filtered_data.append({
                        "column_header": col["column_header"],
                        "values": filtered_values
                    })

            documents.append({
                "year": doc["year"],
                "city": doc["city"],
                "data": filtered_data
            })

        return documents

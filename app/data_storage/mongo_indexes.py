async def create_indexes(db):
    await db["FlatData"].create_index([
        ("year", 1),
        ("city", 1),
        ("section", 1),
        ("row", 1),
        ("column", 1)
    ])
    await db["Sheets"].create_index([("sheet_name", 1)])
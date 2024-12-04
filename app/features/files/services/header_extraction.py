class HeaderExtractionService:
    """
    Сервис для извлечения заголовков из данных секций.
    """
    def extract_headers(self, sections):
        """
        Извлекает заголовки из секций.
        :param sections: Словарь с секциями данных.
        :return: Заголовки данных.
        """
        headers = {}
        for sheet_name, data in sections.items():
            headers[sheet_name] = data.columns.tolist()  # Извлекаем заголовки из DataFrame
        return headers

from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Выбор базы данных
db = client['sport_data']


from app.core.database import mongo_connection
from app.data.repositories.file import FileRepository
from app.data.repositories.flat_data import FlatDataRepository
from app.data.repositories.logs import LogsRepository
from app.data.services.log_service import LogService
from app.data.services.flat_data_service import FlatDataService
from app.data.services.file_service import FileService

def get_db():
    """Возвращаем текущую DB (в тестах можно патчить mongo_connection.get_database)."""
    return mongo_connection.get_database()

def get_file_repo():
    db = get_db()
    return FileRepository(db.get_collection("Files"))

def get_flat_repo():
    db = get_db()
    return FlatDataRepository(db.get_collection("FlatData"))

def get_logs_repo():
    db = get_db()
    return LogsRepository(db.get_collection("Logs"))

def get_log_service():
    return LogService(get_logs_repo())

def get_flat_data_service():
    return FlatDataService(get_flat_repo())

def get_file_service():
    return FileService(get_file_repo())

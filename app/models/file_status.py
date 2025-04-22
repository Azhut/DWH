from enum import Enum
class FileStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PROCESSING = "processing"
    DUPLICATE = "duplicate"
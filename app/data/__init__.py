"""
Data module for the application
"""
from .index_manager import MongoIndexManager, create_indexes

__all__ = ['MongoIndexManager', 'create_indexes']
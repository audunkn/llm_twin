from loguru import logger
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from llm_engineering.settings import settings
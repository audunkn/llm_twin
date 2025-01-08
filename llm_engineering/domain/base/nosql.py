import uuid
from abc import ABC
from typing import Generic, Type, TypeVar

from loguru import logger
from pydantic import UUID4, BaseModel, Field
from pymongo import errors

from llm_engineering.domain.exceptions import ImproperlyConfigured
from llm_engineering.infrastructure.db.mongo import connection
from llm_engineering.settings import settings

_database = connection.get_database(settings.DATABASE_NAME)

# Next, we define a type variable T bound to the NoSQLBaseDocument class. 
# The variable leverages Python’s generic module, allowing us to generalize the class’s types. 
# For example, when we implement the ArticleDocument class, 
# which will inherit from the NoSQLBaseDocument class, all the instances where T was used
# will be replaced with the ArticleDocument type when analyzing the signature of functions 
# (more on Python generics: https://realpython.com/python312-typing):
T = TypeVar("T", bound="NoSQLBaseDocument") # signature, for inheritance tracking purposes

class NoSQLBaseDocument(BaseModel, Generic[T], ABC):
    id: UUID4 = Field(default_factory=uuid.uuid4)

    # Compares to instances of classes to check for equality of UUID4 ids
    # Usage: Duplicate control
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
            return False

        return self.id == value.id
    
    def __hash__(self) -> int:
        return hash(self.id)
    
    # Transforms a dictionary retrieved from MongoDB into an instance of the class:
    @classmethod
    def from_mongo(cls: Type[T], data: dict) -> T:
        """Convert "_id" (str object) into "id" (UUID object)."""

        if not data:
            raise ValueError("Data is empty.")

        id = data.pop("_id")

        return cls(**dict(data, id=id))
    
    # Convert the model instance into a dictionary suitable for MongoDB insertion:
    def to_mongo(self: T, **kwargs) -> dict:
        """Convert "id" (UUID object) into "_id" (str object)."""
        exclude_unset = kwargs.pop("exclude_unset", False)
        by_alias = kwargs.pop("by_alias", True)

        parsed = self.model_dump(exclude_unset=exclude_unset, by_alias=by_alias, **kwargs)

        if "_id" not in parsed and "id" in parsed:
            parsed["_id"] = str(parsed.pop("id"))

        for key, value in parsed.items():
            if isinstance(value, uuid.UUID):
                parsed[key] = str(value)

        return parsed

    # Converts the model instance into a dictionary
    def model_dump(self: T, **kwargs) -> dict:
        dict_ = super().model_dump(**kwargs)

        for key, value in dict_.items():
            if isinstance(value, uuid.UUID):
                dict_[key] = str(value)

        return dict_
    
    # The save() method allows an instance of the model to be inserted into a MongoDB collection. 
    # It retrieves the appropriate collection, converts the instance into a MongoDB-compatible document leveraging the to_mongo() method
    # and attempts to insert it into the database, handling any write errors that may occur:
    def save(self: T, **kwargs) -> T | None:
        collection = _database[self.get_collection_name()]
        try:
            collection.insert_one(self.to_mongo(**kwargs))

            return self
        except errors.WriteError:
            logger.exception("Failed to insert document.")

            return None

    # The get_or_create() class method attempts to find a document in the database matching the provided filter options. 
    # If a matching document is found, it is converted into an instance of the class. 
    # If not, a new instance is created with the filter options as its initial data and saved to the database:        
    @classmethod
    def get_or_create(cls: Type[T], **filter_options) -> T:
        collection = _database[cls.get_collection_name()]
        try:
            instance = collection.find_one(filter_options)
            if instance:
                return cls.from_mongo(instance)

            new_instance = cls(**filter_options)
            new_instance = new_instance.save()

            return new_instance
        except errors.OperationFailure:
            logger.exception(f"Failed to retrieve document with filter options: {filter_options}")

            raise
    
    # The bulk_insert() class method allows multiple documents to be inserted into the database at once:
    @classmethod
    def bulk_insert(cls: Type[T], documents: list[T], **kwargs) -> bool:
        collection = _database[cls.get_collection_name()]
        try:
            collection.insert_many(doc.to_mongo(**kwargs) for doc in documents)

            return True
        except (errors.WriteError, errors.BulkWriteError):
            logger.error(f"Failed to insert documents of type {cls.__name__}")

            return False

    # The find() class method searches for a single document in the database that matches the given filter options: 
    @classmethod
    def find(cls: Type[T], **filter_options) -> T | None:
        collection = _database[cls.get_collection_name()]
        try:
            instance = collection.find_one(filter_options)
            if instance:
                return cls.from_mongo(instance)

            return None
        except errors.OperationFailure:
            logger.error("Failed to retrieve document")

            return None
        
    # Similarly, the bulk_find() class method retrieves multiple documents matching the filter options. 
    # It converts each retrieved MongoDB document into a model instance, collecting them into a list:
    @classmethod
    def bulk_find(cls: Type[T], **filter_options) -> list[T]:
        collection = _database[cls.get_collection_name()]
        try:
            instances = collection.find(filter_options)
            return [document for instance in instances if (document := cls.from_mongo(instance)) is not None]
        except errors.OperationFailure:
            logger.error("Failed to retrieve documents")

            return []
    
    # The get_collection_name() class method determines the name of the MongoDB collection associated with the class. 
    # It expects the class to have a nested Settings class with a name attribute specifying the collection name. 
    # If this configuration is missing, an ImproperlyConfigured exception will be raised specifying that the subclass should define a nested Settings class:
    @classmethod
    def get_collection_name(cls: Type[T]) -> str:
        if not hasattr(cls, "Settings") or not hasattr(cls.Settings, "name"):
            raise ImproperlyConfigured(
                "Document should define an Settings configuration class with the name of the collection."
            )

        return cls.Settings.name
    # We can configure each subclass using the nested Settings class, such as defining the collection name, or anything else specific to that subclass.



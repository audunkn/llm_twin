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





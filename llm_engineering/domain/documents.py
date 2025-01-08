from abc import ABC
from typing import Optional

from pydantic import UUID4, Field

from .base import NoSQLBaseDocument # TODO - implement
from .types import DataCategory

# The UserDocument class is used to store and query all the users from the LLM Twin project:
class UserDocument(NoSQLBaseDocument):
    first_name: str
    last_name: str

    class Settings:
        name = "users"

    @property
    def full_name(self):
        return f"{self.first_name} {self.last_name}"

# The Document class is introduced as an abstract base model for other documents on top of the NoSQLBaseDocument ODM class. 
# It includes common attributes like content, platform, and author details, providing a standardized structure for documents that will inherit from it:
# By leveraging Pydantic to define the fields, we have out-of-the-box type validation    
class Document(NoSQLBaseDocument, ABC):
    content: dict
    platform: str
    author_id: UUID4 = Field(alias="author_id")
    author_full_name: str = Field(alias="author_full_name")

# Specific document type defined by extending the Document class.
# Represent different category of data, with unique fields and settings that specify its respective collection name in the database:
# All the CRUD functionality is delegated to the parent class. 
# By leveraging Pydantic to define the fields, we have out-of-the-box type validation
class RepositoryDocument(Document):
    name: str
    link: str

    class Settings:
        name = DataCategory.REPOSITORIES

# Specific document type defined by extending the Document class.
# Represent different category of data, with unique fields and settings that specify its respective collection name in the database:
# All the CRUD functionality is delegated to the parent class. 
# By leveraging Pydantic to define the fields, we have out-of-the-box type validation
class PostDocument(Document):
    image: Optional[str] = None
    link: str | None = None

    class Settings:
        name = DataCategory.POSTS

# Specific document type defined by extending the Document class.
# Represent different category of data, with unique fields and settings that specify its respective collection name in the database:
# All the CRUD functionality is delegated to the parent class. 
# By leveraging Pydantic to define the fields, we have out-of-the-box type validation
class ArticleDocument(Document):
    link: str

    class Settings:
        name = DataCategory.ARTICLES
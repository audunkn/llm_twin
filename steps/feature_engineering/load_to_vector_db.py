from loguru import logger
from typing_extensions import Annotated
from zenml import step

from llm_engineering.application import utils
from llm_engineering.domain.base import VectorBaseDocument


# As each article, post, or code repository sits in a different collection inside the vector DB, 
# we have to group all the documents based on their data category. Then, we load each group in bulk in the Qdrant vector DB
@step
def load_to_vector_db(
    documents: Annotated[list, "documents"],
) -> Annotated[bool, "successful"]:
    logger.info(f"Loading {len(documents)} documents into the vector database.")

    grouped_documents = VectorBaseDocument.group_by_class(documents)
    for document_class, documents in grouped_documents.items():
        logger.info(f"Loading documents into {document_class.get_collection_name()}")
        for documents_batch in utils.misc.batch(documents, size=4):
            try:
                document_class.bulk_insert(documents_batch)
            except Exception:
                logger.error(f"Failed to insert documents into {document_class.get_collection_name()}")

                return False

    return True
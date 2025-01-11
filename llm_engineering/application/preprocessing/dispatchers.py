from loguru import logger

from llm_engineering.domain.base import NoSQLBaseDocument, VectorBaseDocument
from llm_engineering.domain.types import DataCategory

from .chunking_data_handlers import (
    ArticleChunkingHandler,
    ChunkingDataHandler,
    PostChunkingHandler,
    RepositoryChunkingHandler,
)
from .cleaning_data_handlers import (
    ArticleCleaningHandler,
    CleaningDataHandler,
    PostCleaningHandler,
    RepositoryCleaningHandler,
)
from .embedding_data_handlers import (
    ArticleEmbeddingHandler,
    EmbeddingDataHandler,
    PostEmbeddingHandler,
    QueryEmbeddingHandler,
    RepositoryEmbeddingHandler,
)

# A dispatcher inputs a document and applies dedicated handlers based on its data category (article, post, or repository). A handler can either clean, chunk, or embed a document.
# The CleaningDispatcher implements a dispatch() method that inputs a raw document. Based on its data category, 
# it instantiates and calls a handler that applies the cleaning logic specific to that data point.
# The key in the dispatcher logic is the CleaningHandlerFactory(), which instantiates a different cleaning handler based on the document’s data category:
class CleaningHandlerFactory:
    @staticmethod
    def create_handler(data_category: DataCategory) -> CleaningDataHandler:
        if data_category == DataCategory.POSTS:
            return PostCleaningHandler()
        elif data_category == DataCategory.ARTICLES:
            return ArticleCleaningHandler()
        elif data_category == DataCategory.REPOSITORIES:
            return RepositoryCleaningHandler()
        else:
            raise ValueError("Unsupported data type")


class CleaningDispatcher:
    factory = CleaningHandlerFactory()

    @classmethod
    def dispatch(cls, data_model: NoSQLBaseDocument) -> VectorBaseDocument:
        data_category = DataCategory(data_model.get_collection_name())
        handler = cls.factory.create_handler(data_category)
        clean_model = handler.clean(data_model)

        logger.info(
            "Document cleaned successfully.",
            data_category=data_category,
            cleaned_content_len=len(clean_model.content),
        )

        return clean_model

# A dispatcher inputs a document and applies dedicated handlers based on its data category (article, post, or repository). A handler can either clean, chunk, or embed a document.
# The ChunkingDispatcher implements a dispatch() method that inputs a raw document. Based on its data category, 
# it instantiates and calls a handler that applies the chunking logic specific to that data point.
# The key in the dispatcher logic is the ChunkingHandlerFactory(), which instantiates a different chunking handler based on the document’s data category:
class ChunkingHandlerFactory:
    @staticmethod
    def create_handler(data_category: DataCategory) -> ChunkingDataHandler:
        if data_category == DataCategory.POSTS:
            return PostChunkingHandler()
        elif data_category == DataCategory.ARTICLES:
            return ArticleChunkingHandler()
        elif data_category == DataCategory.REPOSITORIES:
            return RepositoryChunkingHandler()
        else:
            raise ValueError("Unsupported data type")


class ChunkingDispatcher:
    factory = ChunkingHandlerFactory

    @classmethod
    def dispatch(cls, data_model: VectorBaseDocument) -> list[VectorBaseDocument]:
        data_category = data_model.get_category()
        handler = cls.factory.create_handler(data_category)
        chunk_models = handler.chunk(data_model)

        logger.info(
            "Document chunked successfully.",
            num=len(chunk_models),
            data_category=data_category,
        )

        return chunk_models

# A dispatcher inputs a document and applies dedicated handlers based on its data category (article, post, or repository). A handler can either clean, chunk, or embed a document.
# The EmbeddingDispatcher implements a dispatch() method that inputs a raw document. Based on its data category, 
# it instantiates and calls a handler that applies the embedding logic specific to that data point.
# The key in the dispatcher logic is the EmbeddingHandlerFactory(), which instantiates a different embedding handler based on the document’s data category:
class EmbeddingHandlerFactory:
    @staticmethod
    def create_handler(data_category: DataCategory) -> EmbeddingDataHandler:
        if data_category == DataCategory.QUERIES:
            return QueryEmbeddingHandler()
        if data_category == DataCategory.POSTS:
            return PostEmbeddingHandler()
        elif data_category == DataCategory.ARTICLES:
            return ArticleEmbeddingHandler()
        elif data_category == DataCategory.REPOSITORIES:
            return RepositoryEmbeddingHandler()
        else:
            raise ValueError("Unsupported data type")


class EmbeddingDispatcher:
    factory = EmbeddingHandlerFactory

    @classmethod
    def dispatch(
        cls, data_model: VectorBaseDocument | list[VectorBaseDocument]
    ) -> VectorBaseDocument | list[VectorBaseDocument]:
        is_list = isinstance(data_model, list)
        if not is_list:
            data_model = [data_model]

        if len(data_model) == 0:
            return []

        data_category = data_model[0].get_category()
        assert all(
            data_model.get_category() == data_category for data_model in data_model
        ), "Data models must be of the same category."
        handler = cls.factory.create_handler(data_category)

        embedded_chunk_model = handler.embed_batch(data_model)

        if not is_list:
            embedded_chunk_model = embedded_chunk_model[0]

        logger.info(
            "Data embedded successfully.",
            data_category=data_category,
        )

        return embedded_chunk_model
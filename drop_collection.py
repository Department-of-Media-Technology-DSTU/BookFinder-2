
# Просто скрипт для быстрого дропа коллекции, ничего необычного

from settings import _COLLECTION_NAME
from milvus_defs import drop_collection, create_connection

create_connection()
drop_collection(_COLLECTION_NAME)

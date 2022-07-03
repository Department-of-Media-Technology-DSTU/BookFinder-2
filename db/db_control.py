from pymilvus import (Collection, CollectionSchema, DataType, FieldSchema,
                      connections, utility)

import settings
from data_preprocessing.text_transformer import Transformer
import pandas as pd


def create_connection(alias='default'):
    """ Подключиться к БД."""
    connections.connect(alias=alias, host=settings._HOST, port=settings._PORT)


def create_collection():
    """ Созданть коллекцию. """
    field1 = FieldSchema(name=settings._ID_FIELD_NAME, dtype=DataType.INT64,
                         description="int64", is_primary=True)
    field2 = FieldSchema(name=settings._VECTOR_FIELD_NAME, dtype=DataType.FLOAT_VECTOR,
                         description="description_vector", dim=settings._DIM,
                         is_primary=False)
    schema = CollectionSchema(
        fields=[field1, field2], description=settings._COLLECTION_DESCRIPTION)
    collection = Collection(name=settings._COLLECTION_NAME, schema=schema)
    return collection


def has_collection(name):
    """ Проверить наличие коллекции. """
    return utility.has_collection(name)


def drop_collection(name):
    """  Удалить коллекцию. """
    collection = Collection(name)
    collection.drop()


def list_collections():
    """ Получить список коллекций. """
    collections = utility.list_collections()
    print(collections)
    return collections


def prepare_data(data):
    """ Подготовить данные к загрузке в коллекцию. """
    _id = []
    vector = []
    for i in range(len(data)):
        _id.append(int(data.loc[i][0]))
        vector.append(data.loc[i][1:].values.tolist())
    return [_id, vector]


def insert(collection, data):
    """ Вставить данные в коллекцию. """
    collection.insert(data)
    return data


def get_entity_num(collection):
    """ Получить количество векторов в коллекции. """
    noe = collection.num_entities
    print(f"\nThe number of entity: {noe}")
    return noe


def create_index(collection):
    """ Создать индекс. """
    index_param = {
        "index_type": settings._INDEX_TYPE,
        "params": {"nlist": settings._NLIST},
        "metric_type": settings._METRIC_TYPE}
    collection.create_index(settings._VECTOR_FIELD_NAME, index_param)
    print("\nCreated index:\n{}".format(collection.index().params))


def drop_index(collection):
    """ Удалить индекс. """
    collection.drop_index()


def load_collection(collection):
    """ Загрузить коллекцию в память"""
    collection.load()


def release_collection(collection):
    """ Выгрузить коллекцию из памяти. """
    collection.release()


def search(collection, users_text):
    """ Перевод пользовательского текста в вектор и поиск дистанции между ним и векторами в коллекции. """
    id_array = [i for i in range(collection.num_entities)]
    vectors_left = {
        "ids": id_array,
        "collection": settings._COLLECTION_NAME,
        "partition": "_default",
        "field": settings._VECTOR_FIELD_NAME
    }
    text_transformer = Transformer()
    vectors_right = {
        "float_vectors": [text_transformer.encode(users_text).astype(float)]
    }
    params = {
        "metric": settings._METRIC_TYPE,
        "dim": 512
    }
    results = utility.calc_distance(
        vectors_left=vectors_left,
        vectors_right=vectors_right,
        params=params)
    return results


def get_data():
    """ 
        Читает вектора с .csv файла. Если его нет - скачает.  
    """
    try:
        data = pd.read_csv('description_vectors.csv')
    except Exception:
        data = pd.read_csv(
            'https://drive.google.com/uc?export=download&id=1DN8vu6VFsevUnu5NmQ6nujmrz6y84erR')
    return data


def prepare_collection():
    """Подготовка коллекции к использованию. Если её нет - создаем и заполняем, если есть - забираем"""
    create_connection()
    if not has_collection(settings._COLLECTION_NAME):
        collection = create_collection()
        insert(collection, prepare_data(get_data()))
    else:
        collection = Collection(settings._COLLECTION_NAME)
        if get_entity_num(collection) == 0:
            insert(collection, prepare_data(get_data()))
    return collection

# coding: utf-8

import settings
import pandas as pd

import milvus_defs as mc


class MilvusSearcher:
    def __init__(self):
        self.collection = self.prepare_collection()
        mc.create_index(self.collection, settings._VECTOR_FIELD_NAME)

    def search_similar(self, sentence: str, n: int = 5):
        """
        Поиск N книг, наиболее близких к тексту пользователя\n
        Возвращает список
        """
        mc.load_collection(self.collection)
        results = mc.search(self.collection, sentence)
        mc.release_collection(self.collection)
        res = sorted(range(len(results)), key=lambda sub: results[sub])[:n]
        book_data = pd.read_csv('book_data.csv')
        similar_books = book_data.loc[res, [
            'name', 'author', 'description']].values
        return similar_books

    def get_data(self):
        """ 
            Читает вектора с .csv файла. Если его нет - запустит кодировщик.
            Если кодировщик не найдет данных книг, то запустит препроцессор.
            А если и он не найдет, то дальше заработает парсер. Наверное.    
        """
        try:
            data = pd.read_csv('description_vectors.csv')
        except Exception:
            import descriptions2vectors
            data = descriptions2vectors.main()
        return data

    def prepare_collection(self):
        """Подготовка коллекции к использованию. Если её нет - создаем и заполняем, если есть - забираем"""
        mc.create_connection()
        data = mc.prepare_data(self.get_data())
        if not mc.has_collection(settings._COLLECTION_NAME):
            collection = mc.create_collection(
                settings._COLLECTION_NAME, settings._ID_FIELD_NAME, settings._VECTOR_FIELD_NAME)
            mc.insert(collection, data)
        else:
            collection = mc.Collection(settings._COLLECTION_NAME)
            if mc.get_entity_num(collection) == 0:
                mc.insert(collection, data)
        return collection


if __name__ == '__main__':
    searcher = MilvusSearcher()
    description_test = "Столкновение принципов человечности с представлениями богатых людей о том, что весь мир служит только им, становятся основой конфликта, а кража друга Арто превращает мальчика Сережу в настоящего борца за справедливость."
    results = searcher.search_similar(description_test, n=3)

    print('Похожие книги:\n')
    for book in results:
        print(f"Название: {book[0]}\nАвтор: {book[1]}\nОписание: {book[2]}")
        print()

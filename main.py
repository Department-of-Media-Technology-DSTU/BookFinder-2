# coding: utf-8
import pandas as pd
import db.db_control as db


class MilvusSearcher:
    def __init__(self):
        self.collection = db.prepare_collection()
        db.create_index(self.collection)

    def search_similar(self, sentence: str, n: int = 5):
        """
        Поиск N книг, наиболее близких к тексту пользователя\n
        Возвращает список
        """
        db.load_collection(self.collection)
        results = db.search(self.collection, sentence)
        db.release_collection(self.collection)
        res = sorted(range(len(results)), key=lambda sub: results[sub])[:n]
        book_data = pd.read_csv('book_data.csv')
        similar_books = book_data.loc[res, [
            'name', 'author', 'description']].values
        return similar_books


if __name__ == '__main__':
    searcher = MilvusSearcher()

    description_test = "Столкновение принципов человечности с представлениями богатых людей о том, что весь мир служит только им, становятся основой конфликта, а кража друга Арто превращает мальчика Сережу в настоящего борца за справедливость."
    results = searcher.search_similar(description_test, n=3)

    print('Похожие книги:\n')
    for book in results:
        print(f"Название: {book[0]}\nАвтор: {book[1]}\nОписание: {book[2]}")
        print()

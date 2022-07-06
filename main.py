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
        try:
            book_data = pd.read_csv('book_data.csv')
        except Exception:
            book_data = pd.read_csv(
                'https://drive.google.com/uc?export=download&id=1vqPFd3UEuaUq7HkgsH1vjMLniYdRt9eT')
        similar_books = book_data.loc[res, [
            'name', 'author', 'description']].values
        return similar_books


if __name__ == '__main__':
    searcher = MilvusSearcher()
    description_test = "Варо Борха, обладатель крупнейшей в Испании коллекции книжных редкостей, нанимает Лукаса Корсо, опытного букиниста и охотника за уникальными книгами, чтобы установить подлинность приобретенного им экземпляра книги, некогда преданной огню инквизицией вместе с ее издателем и сохранившейся лишь в трех экземплярах."
    results = searcher.search_similar(description_test, n=5)

    print('Похожие книги:\n')
    for book in results:
        print(f"Название: {book[0]}\nАвтор: {book[1]}\nОписание: {book[2]}")
        print()

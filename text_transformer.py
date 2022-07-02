# -*- coding: utf-8 -*-

class Transformer:
    """ Переводчик на векторный. """

    def __init__(self, batch_size=6, max_seq_length=512):
        from sentence_transformers import SentenceTransformer
        self._BATCH_SIZE = batch_size
        self._MAX_SEQ_LENGTH = max_seq_length
        self._MODEL_NAME = 'distiluse-base-multilingual-cased-v1'

        self._MODEL = SentenceTransformer(self._MODEL_NAME)
        self._MODEL.max_seq_length = self._MAX_SEQ_LENGTH

    def encode(self, user_text):
        """ Перевод текста в вектор. """
        user_vector = self._MODEL.encode(
            sentences=user_text, batch_size=self._BATCH_SIZE)
        return user_vector

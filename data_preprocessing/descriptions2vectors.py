# -*- coding: utf-8 -*-

from sentence_transformers import SentenceTransformer
import pandas as pd
_CLEARED_BOOK_DATA = [
    'book_data.csv', 'https://drive.google.com/uc?export=download&id=1vqPFd3UEuaUq7HkgsH1vjMLniYdRt9eT']
model_name = 'distiluse-base-multilingual-cased-v1'
MAX_SEQ_LENGTH = 512
BATCH_SIZE = 6


def main():
    try:
        df = pd.read_csv(_CLEARED_BOOK_DATA[0])
    except Exception:
        df = pd.read_csv(_CLEARED_BOOK_DATA[1])
    df = df.rename({'Unnamed: 0': 'Id'}, axis=1)
    for i in range(len(df)):
        df.loc[i, 'Id'] = i
    descriptions = df['description'].tolist()

    model = SentenceTransformer(model_name)
    model.max_seq_length = MAX_SEQ_LENGTH

    sentence_vecs = model.encode(
        sentences=descriptions, batch_size=BATCH_SIZE, show_progress_bar=True)

    vd = dict()
    for id in range(len(sentence_vecs)):
        vd[id] = sentence_vecs[id]

    data = pd.DataFrame.from_dict(vd, orient='index')
    data.to_csv(
        'description_vectors.csv', index_label='Id')
    return data


if __name__ == '__main__':
    main()

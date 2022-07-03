# -*- coding: utf-8 -*-

import pandas as pd
_RAW_BOOK_DATA = [
    'cg_cat.csv', 'https://drive.google.com/uc?export=download&id=1eVWSItBmkAwYZkIDrIJPo01zrsc1lPk_']


def main():
    try:
        data = pd.read_csv(_RAW_BOOK_DATA[0])
    except Exception:
        data = pd.read_csv(_RAW_BOOK_DATA[1])
    data_cleared = data.drop_duplicates(subset=['name'])

    mask = (data_cleared['description'].str.len() > 200)
    data_cleared = data_cleared.loc[mask]

    mask = (data_cleared['description'].str.len() < 1300)
    data_cleared = data_cleared.loc[mask]

    data_cleared.reset_index(inplace=True, drop=True)

    data_cleared.to_csv('book_data.csv', index=True, index_label='Id')
    return data


if __name__ == '__main__':
    main()

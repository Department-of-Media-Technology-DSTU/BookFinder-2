# -*- coding: utf-8 -*-
import pandas as pd


def main():
    try:
        data = pd.read_csv("cg_cat.csv")
    except Exception:
        import parse_data
        data = parse_data.get_parsed_data()
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

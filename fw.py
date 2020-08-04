#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""сопоставление адресов вакансий с кодами МРИГО и имен вакансий с ОКПДТР"""
import fuzzywuzzy as fw
import os
import pandas as pd
import re

import misc.mrigo_splits as mrs
import misc.okpdtr_splits as oks
import misc.secondary_funcs as sf

SIMILARITY_LEVEL_MRIGO = 80


def main():
    # получение данных из БД
    vacancies = pd.read_csv(
    os.path.join(
        'tables',
        'vacancies.csv'
        )
    )
    # соотвествие названий МРИГО и кодов МРИГО
    mrigo_id_name = pd.read_csv(
        os.path.join(
            'tables',
            'id_mrigo_mrigo.csv'
            )
        )
    # соответствие имен ОКПДТР и кодов ОКПДТР
    okpdtr_id_name = pd.read_csv(
        os.path.join(
            'tables',
            'id_okpdtr_okpdtr.csv'
            )
        )
    
    # очистка адресов вакансий
    adresses_raw = vacancies['address'].tolist()
    adresses = []
    for adress in adresses_raw:
        s = adress.replace('Новосибирская область, ', '', 1)
        s = re.split(r'{}'.format('|'.join(mrs.dictionary)), s)[0]
        remove_digits = str.maketrans('', '', digits)
        s = s.translate(remove_digits)                              # удаляем цифры
        s = re.sub(r'[^\w\s]', '', s)                               # удаляем служебные символы
        s = ' '.join([word for word in s.split() if len(word) > 2])
        s = s.strip()
        adresses.append(s)
    print(f"\n> Очистка адресов в отношении 'Вакансии' прошла успешно.")
    
    id_mrigo = mrigo_id_name['id_mrigo'].tolist()
    mrigo = mrigo_id_name['mrigo'].tolist()
    mrigo_raw = mrigo.copy()
    mrigo = [re.sub(r'р[.]п[.]\s', '', s) for s in mrigo]
    
    print(f"> Началось сопоставление адресов с кодами МРИГО... (всего итераций -- {len(adresses)})")
    # сопоставление адресов вакансии с кодами МРИГО
    indexes = []
    for i in range(len(adresses)):
        print(f">> Текущая итерация -- {i}")
        sub_lst = []
        for j in range(len(mrigo)):
                sub_lst.append(fw.fuzz.token_sort_ratio(adresses[i], mrigo[j]))
        max_indexes = sf.find_locate_max(sub_lst)
        if max_indexes[0] < SIMILARITY_LEVEL_MRIGO:
            indexes.append(len(mrigo))
        else:
            indexes.append(max_indexes[1][0])
    id_mrigo.append(None)
    mrigo.append(None)
    print(f"> Cопоставление адресов с кодами МРИГО успешно заверешено.")

    vacancies.insert(
        6,
        'id_mrigo',
        [id_mrigo[indexes[i]] for i in range(len(adresses))],
        True
        )
    
    vacancies_test_mrigo = vacancies[['address', 'id_mrigo']]
    mrigo_raw.append(None)
    vacancies_test_mrigo.insert(2, 'mrigo', [mrigo_raw[indexes[i]] for i in range(len(adresses))], True)
    print(f"> Результат сопоставления кодов МРИГО с адресами:")
    print(vacancies_test_mrigo.isna().sum())
    vacancies_test_mrigo.to_excel(
        os.path.join(
            'tables',
            'vacancies_mrigo_test.xlsx'
            ), index=None, header=True)
    

if __name__ == "__main__":
    pass
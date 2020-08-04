#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""сопоставление адресов вакансий с кодами МРИГО и имен вакансий с ОКПДТР"""
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import os
import pandas as pd
import re
import string

import misc.mrigo_splits as mrs
import misc.okpdtr_splits as oks
import misc.secondary_funcs as sf

SIMILARITY_LEVEL_MRIGO = 60


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
    
    addresses = vacancies['address'].tolist()
    mrigo = mrigo_id_name['mrigo'].tolist()
    id_mrigo = mrigo_id_name['id_mrigo'].tolist()
    d = dict(zip(mrigo, id_mrigo))
    
    for i in range(len(addresses)):
        addresses[i] = addresses[i].replace('Новосибирская область, ', '', 1)
        addresses[i] = re.split(r'{}'.format('|'.join(mrs.dictionary)), addresses[i])[0]
    
    matched_list = []
    iteration = 1
    print(f"> Началось сопоставление адресов с кодами МРИГО... (всего итераций -- {len(addresses)})")
    for address in addresses:
        print(f">> Текущая итерация -- {iteration}")
        p = process.extractOne(address, mrigo)
        matched_list.append((address, p[0], d[p[0]], p[1]))
        iteration += 1
    
    df = pd.DataFrame(matched_list, columns=['address', 'mrigo', 'id_mrigo', 'score'])
    df.to_excel(
    os.path.join(
        'tables',
        'vacancies_mrigo_test.xlsx'
        ), index=None, header=True)
        
    
    '''   
    # очистка адресов вакансий
    adresses_raw = vacancies['address'].tolist()
    adresses = []
    for adress in adresses_raw:
        s = adress.replace('Новосибирская область, ', '', 1)
        s = re.split(r'{}'.format('|'.join(mrs.dictionary)), s)[0]
        remove_digits = str.maketrans('', '', string.digits)
        s = s.translate(remove_digits)                              # удаляем цифры
        s = re.sub(r'[^\w\s]', '', s)                               # удаляем служебные символы
        s = ' '.join([word for word in s.split() if len(word) > 2])
        s = s.strip()
        adresses.append(s)
    print(f"> Очистка адресов в отношении 'Вакансии' прошла успешно.")
    
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
                sub_lst.append(fuzz.token_sort_ratio(adresses[i], mrigo[j]))
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
    vacancies_test_mrigo.insert(2, 'mrigo_raw', [mrigo_raw[indexes[i]] for i in range(len(adresses))], True)
    vacancies_test_mrigo.insert(3, 'mrigo', [mrigo[indexes[i]] for i in range(len(adresses))], True)
    vacancies_test_mrigo.insert(4, 'address_ne_raw', [adresses[indexes[i]] for i in range(len(adresses))], True)
    print(f"> Результат сопоставления кодов МРИГО с адресами:")
    print(vacancies_test_mrigo.isna().sum())
    vacancies_test_mrigo.to_excel(
        os.path.join(
            'tables',
            'vacancies_mrigo_test.xlsx'
            ), index=None, header=True)
    '''
    

if __name__ == "__main__":
    main()
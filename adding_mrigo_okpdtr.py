#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""сопоставление адресов вакансий с кодами МРИГО и имен вакансий с ОКПДТР"""
from string import digits
import jellyfish
import os
import pandas as pd
import re

import misc.mrigo_splits as mrs
import misc.okpdtr_splits as oks
import misc.secondary_funcs as sf


SIMILARITY_LEVEL_MRIGO = 0.75       # порог сходтсва для МРИГО
SIMILARITY_LEVEL_OKPDTR = 0.85      # порог сходтсва для ОКПДТР


def main():
    # загрузка датафреймов из файлов
    vacancies = pd.read_csv(
        os.path.join(
            'tables',
            'vacancies.csv'
            )
        )
    mrigo_id_name = pd.read_csv(
        os.path.join(
            'tables',
            'id_mrigo_mrigo.csv'
            )
        )
    okpdtr_id_name = pd.read_csv(
        os.path.join(
            'tables',
            'id_okpdtr_okpdtr.csv'
            )
        )
    

    # очистка адресов
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
    print("Очистка адресов в отношении 'Вакансии' прошла успешно.")


    id_mrigo = mrigo_id_name['id_mrigo'].tolist()
    mrigo = mrigo_id_name['mrigo'].tolist()
    mrigo_raw = mrigo.copy()
    mrigo = [re.sub(r'р[.]п[.]\s', '', s) for s in mrigo]

    print(f"Началось сопоставление адресов с кодами МРИГО... (всего итераций -- {len(adresses)})")
    # сопоставление адресов вакансии с кодами МРИГО
    indexes = []
    for i in range(len(adresses)):
        print(f"Текущая итерация -- {i}")
        sub_lst = []
        for j in range(len(mrigo)):
            if sf.is_word_in_string(mrigo[j], adresses[i]):
                sub_lst.append(1.0)
            else:
                sub_lst.append(jellyfish.jaro_distance(adresses[i], mrigo[j]))
        max_indexes = sf.find_locate_max(sub_lst)
        if max_indexes[0] < SIMILARITY_LEVEL_MRIGO:
            indexes.append(len(mrigo))
        else:
            indexes.append(max_indexes[1][0])
    id_mrigo.append(None)
    mrigo.append(None)
    print(f"Cопоставление адресов с кодами МРИГО успешно заверешено.")

    vacancies.insert(
        6,
        'id_mrigo',
        [id_mrigo[indexes[i]] for i in range(len(adresses))],
        True
        )

    vacancies_test_mrigo = vacancies[['address', 'id_mrigo']]
    mrigo_raw.append(None)
    vacancies_test_mrigo.insert(2, 'mrigo', [mrigo_raw[indexes[i]] for i in range(len(adresses))], True)
    print("Результат сопоставления кодов МРИГО с адресами:")
    print(vacancies_test_mrigo.isna().sum())
    vacancies_test_mrigo.to_csv(
        os.path.join(
            'tables',
            'additionally',
            'vacancies_mrigo.csv'
            ),
        index=None,
        header=True
        )

    # ОКПДТР
    id_okpdtr_lst = okpdtr_id_name['id'].tolist()
    okpdtr_lst = okpdtr_id_name['name'].tolist()
    okpdtr_lst_raw = okpdtr_lst.copy()
    job_lst = vacancies['job-name'].tolist()

    # очистка наименований ОКПДТР
    for i in range(len(okpdtr_lst)):
        okpdtr_lst[i] = re.sub(r'\(|\)|[,]','', re.sub('-', ' ', str(okpdtr_lst[i])).lower())

    # очистка названий вакансий
    for i in range(len(job_lst)):
        job_lst[i] = re.sub(r'[^\w\s]', '', re.sub('-', ' ', str(job_lst[i])).lower())  # удаляем служебные символы, переводим в ниж. регистр
        remove_digits = str.maketrans('', '', digits)
        job_lst[i] = job_lst[i].translate(remove_digits)                                # удаляем цифры
        job_lst[i] = re.split(r'{}'.format('|'.join(oks.dictionary)), job_lst[i])[0]
    print("Очистка имен вакансий в отношении 'Вакансии' прошла успешно.")

    # сопоставление имен вакансий с кодами ОКПДТР  
    print(f"Началось сопоставление имен вакансий с кодами ОКПДТР... (всего итераций -- {len(job_lst)})")  
    indexes = []
    for i in range(len(job_lst)):
        print(f"Текущая итерация -- {i}")
        sub_lst = []
        for j in range(len(okpdtr_lst)):
            sub_lst.append(jellyfish.jaro_distance(okpdtr_lst[j], job_lst[i]))
        max_indexes = sf.find_locate_max(sub_lst)
        if max_indexes[0] < SIMILARITY_LEVEL_OKPDTR:
            indexes.append(len(okpdtr_lst))
        else:
            indexes.append(max_indexes[1][0])
    id_okpdtr_lst.append(None)
    okpdtr_lst.append(None)
    print(f"Cопоставление имен вакансий с кодами ОКПДТР успешно заверешено.")

    vacancies.insert(
        11,
        'id_okpdtr',
        [id_okpdtr_lst[indexes[i]] for i in range(len(job_lst))],
        True
        )
    vacancies.to_csv(
        os.path.join(
            'tables',
            'vacancies_updated.csv'
            ), 
        index=None,
        header=True)

    vacancies_test_okpdtr = vacancies[['job-name', 'id_okpdtr']]
    okpdtr_lst_raw.append(None)
    vacancies_test_okpdtr.insert(
        2,
        'okpdtr',
        [okpdtr_lst_raw[indexes[i]] for i in range(len(job_lst))],
        True
        )
    print("Результат сопоставления кодов ОКПДТР с адресами:")
    print(vacancies_test_okpdtr.isna().sum())
    vacancies_test_okpdtr.to_csv(
        os.path.join(
            'tables',
            'additionally',
            'vacancies_okpdtr.csv'
            ),
        index=None,
        header=True
        )
    print("Все данные сохранены")


if __name__ == "__main__":
    main()

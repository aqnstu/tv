#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""сопоставление адресов вакансий с кодами МРИГО и имен вакансий с ОКПДТР"""
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import numpy as np
import os
import pandas as pd
import re
import string

import misc.mrigo_splits as mrs
import misc.okpdtr_splits as oks
import misc.secondary_funcs as sf

SIMILARITY_LEVEL_MRIGO = 70
SIMILARITY_LEVEL_OKPDTR = 85

def main():
    # получение данных из БД
    vacancies = pd.read_csv(os.path.join('tables', 'vacancies.csv'))
    # соотвествие названий МРИГО и кодов МРИГО
    mrigo_id_name = pd.read_csv(os.path.join('tables', 'id_mrigo_mrigo.csv'))
    # соответствие имен ОКПДТР и кодов ОКПДТР
    okpdtr_id_name = pd.read_csv(os.path.join('tables', 'id_okpdtr_okpdtr.csv'))
    okpdtr_assoc_id_name = pd.read_csv(os.path.join('tables', 'id_okpdtr_okpdtr_assoc.csv'))
    okpdtr_id_name = pd.concat([okpdtr_id_name, okpdtr_assoc_id_name], sort=False, ignore_index=True)
    print(f"\n> Данные о вакансиях, МРИГО и ОКПДТР успешно загружены.")
    
    print(f"> Сопоставление вакансий с кодами МРИГО:")
    addresses = vacancies['address'].tolist()
    mrigo = mrigo_id_name['mrigo'].tolist()
    id_mrigo = mrigo_id_name['id_mrigo'].tolist()
    d1 = dict(zip(mrigo, id_mrigo))
    
    for i in range(len(addresses)):
        addresses[i] = addresses[i].replace('Новосибирская область, ', '', 1)
        addresses[i] = addresses[i].translate(str.maketrans('', '', string.digits))
        addresses[i] = re.sub(r'[^\w\s]', '', addresses[i])
        if re.search(r'Новосибирский', addresses[i]) != None:
            addresses[i] = re.search(r'Новосибирский', addresses[i]).group(0)
        if re.search(r'рн\s\w+\s', addresses[i]) != None:
            addresses[i] = re.search(r'рн\s\w+\s', addresses[i]).group(0)
        if re.search(r'г\s\w+\s', addresses[i]) != None:
            addresses[i] = re.search(r'г\s\w+\s', addresses[i]).group(0)
    print(f">> Очистка адресов в отношении 'Вакансии' прошла успешно.")

    matched_list = []
    print(f">> Началось сопоставление вакансий с кодами МРИГО... (всего итераций -- {len(addresses)})")
    for address in addresses:
        a = process.extractOne(address, mrigo, scorer=fuzz.token_set_ratio)
        matched_list.append((d1[a[0]], a[1]))
    print(f">> Сопоставление вакансий с кодами МРИГО завершено.")
    
    # из полученного списка кортежей получаем датафрейм
    df_with_id_mrigo = pd.DataFrame(matched_list, columns=['id_mrigo', 'score'])
    # если полученная оценка меньше заданной, то выбранный МРИГО не рассматривается
    df_with_id_mrigo['fix_id_mrigo'] = np.where(df_with_id_mrigo['score'] > SIMILARITY_LEVEL_MRIGO, df_with_id_mrigo['id_mrigo'], None)
    # печать результатов (статистики) сопоставления
    print(((df_with_id_mrigo.isnull() | df_with_id_mrigo.isna()).sum() * 100 / df_with_id_mrigo.index.size).round(2))
    
    # вставка кодов МРИГО в таблицу    
    vacancies.insert(6, 'id_mrigo', df_with_id_mrigo['fix_id_mrigo'].tolist(), True)
    
    vacancies = vacancies[:100]
    
    print(f">> Обновленное отношение 'Вакансии' успешно сохранено.")
    
    print(f"\n> Сопоставление вакансий с кодами ОКПДТР.")
    jobs = vacancies['job-name'].tolist()[:100]
    okpdtr = okpdtr_id_name['name'].tolist()
    id_okpdtr= okpdtr_id_name['id'].tolist()
    d2 = dict(zip(okpdtr, id_okpdtr))
    
    # очистка списка с сопоставленными данными
    matched_list[:] = []
    print(f">> Началось сопоставление вакансий с кодами ОКПТДР... (всего итераций -- {len(jobs)})")
    for job in jobs:
        j = process.extractOne(job, okpdtr, scorer=fuzz.token_set_ratio)
        matched_list.append((d2[j[0]], j[1]))
    print(f">> Сопоставление вакансий с кодами ОКПДТР завершено.")
    
    # из полученного списка кортежей получаем датафрейм   
    df_with_id_okpdtr = pd.DataFrame(matched_list, columns=['id_okpdtr', 'score'])
    # если полученная оценка меньше заданной, то выбранный МРИГО не рассматривается
    df_with_id_okpdtr['fix_id_okpdtr'] = np.where(df_with_id_okpdtr['score'] > SIMILARITY_LEVEL_OKPDTR, df_with_id_okpdtr['id_okpdtr'], None)
    # печать результатов (статистики) сопоставления
    print(((df_with_id_okpdtr.isnull() | df_with_id_okpdtr.isna()).sum() * 100 / df_with_id_okpdtr.index.size).round(2))
    
    # вставка кодов ОКПДТР в таблицу
    vacancies.insert(11, 'id_okpdtr', df_with_id_okpdtr['fix_id_okpdtr'].tolist(), True)
    vacancies.to_csv(os.path.join('tables', 'vacancies_updated.csv'), index=None, header=True)
    
if __name__ == "__main__":
    main()
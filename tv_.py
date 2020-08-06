#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
    получение данных через API ТРУДВСЕМ,
    распределение исходных данных  на два отношения -- 'Компании' и 'Вакансии' (2NF),
    получение таблиц с кодами и названиями ОКПДТР/МРИГО из БД,
    сопоставление адресов вакансий с кодами МРИГО и имен вакансий с ОКПДТР,
    выгрузка/обновление таблиц 'Компании' и дополненной таблицы 'Вакансии' в БД
"""

import html
import jellyfish
import json
import numpy as np
import os
import pandas as pd
import re
import sqlalchemy as sa
import string
import sys
import time
import urllib.request

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import misc.db as db
import misc.okpdtr_splits as oks
import misc.secondary_funcs as sf


def remove_tags(text):
    """
    Удалить HTML-тэги из произвольного текста.

    Входные параметры:
    text -- строка, в которой могут содержаться HTML-тэги
    """
    return html.escape(re.sub(r'(<!--.*?-->|<[^>]*>)', '', text))


def get_page_from_api(offset):
    """
    Получить данные с заданной страницы через API ТРУДВСЕМ.

    Входные параметры:
    offset -- сдвиг (текущая страница)
    """
    try:
        with urllib.request.urlopen("http://opendata.trudvsem.ru/api/v1/vacancies/region/54?offset=" + str(offset) + "&limit=100") as url:
            data = json.loads(url.read().decode("utf-8"))
            status = int(data['status'])
            if status != 200:
                return None, status
            return data, status
    except:
        print(f">>> Проблема с получением данных со страниц (см. get_page_from_api()).")
        return None, -1


def get_data_from_api(start_offset):
    """
    Получить доступные данные о вакансиях через API ТРУДВСЕМ.

    Входные параметры:
    start_offset -- начальная страница для загрузки данных
    """
    df_raw = pd.DataFrame()
    offset = start_offset
    print("> Загрузка данных через API TRUDVSEM...")
    while True:
        # print(f">> Текущая страница: -- {offset}")
        data, status = get_page_from_api(offset)
        if status != 200:
            break
        new_data = data['results']['vacancies']
        # приводим полученные данные к таблице
        df_tmp = pd.io.json.json_normalize(new_data)
        df_raw = pd.concat([df_raw, df_tmp], sort=False, ignore_index=True)
        offset += 1
    print("> Загрузка данных через API TRUDVSEM заверешна.")
    return df_raw


def main():
    ################################
    '''получение данных через API'''
    start = time.time()
    # получение "сырого" датафрейма с вакансиями
    try:
        df_raw = get_data_from_api(0)
    except:
        print(f">>> Сервера TRUDVSEM недоступны, либо данные были перенесены. Завершение работы.")
        sys.exit(1)

    if df_raw.empty:
        print(f">>> Новых данных не найдено. Завершение работы.")
        sys.exit(2)

    try:
        df_raw = df_raw.astype({
            'vacancy.region.region_code': 'str',
            'vacancy.company.inn': 'str',
            'vacancy.company.ogrn': 'str',
            'vacancy.company.kpp': 'str',
            'vacancy.addresses.address': 'str',
            'vacancy.duty': 'str',
            'vacancy.requirement.qualification': 'str',
        })
        df_raw = df_raw.replace({False: np.nan, np.nan: None})
        df_raw['download_time'] = pd.to_datetime('now')
        if not os.path.exists('tables'):
            os.makedirs('tables')
    except:
        print(f">>> Проблемы с исходным датафреймом (df_raw)[1]. Завершение работы.")
        sys.exit(3)

    try:
        # выбираем ОГРН первичным ключом
        df_raw = df_raw[pd.notnull(df_raw['vacancy.company.ogrn'])]
    except:
        print(f">>> Проблемы с исходным датафреймом (df_raw)[2]. Завершение работы.")
        sys.exit(4)
    
    
    ######################################################################################
    '''распределение исходных данных  на два отношения -- 'Компании' и 'Вакансии' (2NF)'''
    try:
        # отношение 'Компании'
        companies = df_raw[
            ['vacancy.company.companycode', 'vacancy.company.inn', 'vacancy.company.ogrn', 'vacancy.company.kpp',
            'vacancy.company.name',
            'vacancy.addresses.address',
            'vacancy.company.hr-agency',
            'vacancy.company.url', 'vacancy.company.site',
            'vacancy.company.phone', 'vacancy.company.fax', 'vacancy.company.email',
            'vacancy.company.code_industry_branch',
            ]]
        companies = companies.drop_duplicates(
            subset="vacancy.company.ogrn",
            keep='first'
            )
        companies = companies.reset_index(drop=True)
        companies = companies.rename(columns={
            'vacancy.company.ogrn': 'ogrn',
            'vacancy.company.inn': 'inn',
            'vacancy.company.kpp': 'kpp',
            'vacancy.company.companycode': 'companycode',
            'vacancy.company.name': 'name',
            'vacancy.addresses.address': 'address',
            'vacancy.company.hr-agency': 'hr-agency',
            'vacancy.company.url': 'url',
            'vacancy.company.site': 'site',
            'vacancy.company.phone': 'phone',
            'vacancy.company.fax': 'fax',
            'vacancy.company.email': 'email',
            'vacancy.company.code_industry_branch': 'code_industry_branch',
        })
        print("> Новое отношение 'Компании' успешно сформированно")
    except:
        print(f">>> Проблема с формированием отношения 'Компании'. Завершение работы.")
        sys.exit(5)

    try:
        # отношение 'Вакансии'
        vacancies = df_raw[
            ['vacancy.id', 'vacancy.company.ogrn',
            'vacancy.source',
            'vacancy.region.region_code', 'vacancy.region.name', 'vacancy.addresses.address',
            'vacancy.requirement.experience',
            'vacancy.employment', 'vacancy.schedule',
            'vacancy.job-name', 'vacancy.category.specialisation', 'vacancy.duty',
            'vacancy.requirement.education', 'vacancy.requirement.qualification',
            'vacancy.term.text', 'vacancy.social_protected',
            'vacancy.salary_min', 'vacancy.salary_max', 'vacancy.salary', 'vacancy.currency',
            'vacancy.vac_url',
            'vacancy.category.industry',
            'vacancy.creation-date', 'vacancy.modify-date', 'download_time',
            ]]

        vacancies = vacancies.rename(columns={
            'vacancy.id': 'id',
            'vacancy.company.ogrn': 'ogrn',
            'vacancy.source': 'source',
            'vacancy.region.region_code': 'region_code',
            'vacancy.region.name': 'region_name',
            'vacancy.addresses.address': 'address',
            'vacancy.requirement.experience': 'experience',
            'vacancy.employment': 'employment',
            'vacancy.schedule': 'schedule',
            'vacancy.job-name': 'job-name',
            'vacancy.category.specialisation': 'specialisation',
            'vacancy.duty': 'duty',
            'vacancy.requirement.education': 'education',
            'vacancy.requirement.qualification': 'qualification',
            'vacancy.term.text': 'term_text',
            'vacancy.social_protected': 'social_protected',
            'vacancy.salary_min': 'salary_min',
            'vacancy.salary_max': 'salary_max',
            'vacancy.salary': 'salary',
            'vacancy.currency': 'currency',
            'vacancy.vac_url': 'vac_url',
            'vacancy.category.industry': 'industry',
            'vacancy.creation-date': 'creation-date-from-api',
            'vacancy.modify-date': 'modify-date-from-api',
        })
        vacancies['is_closed'] = False
        print("> Новое отношение 'Вакансии' успешно сформированно.")
    except:
        print(f">>> Проблема с формированием отношения 'Вакансии'. Завершение работы.")
        sys.exit(6)

    ###############################################################
    '''получение таблиц с кодами и названиями ОКПДТР/МРИГО из БД'''
    try:
        # используем сортировку по убыванию кодов для дальнейшего удобного сопастовления
        mrigo_query = """
        SELECT DISTINCT blinov.mrigo.id_mrigo, blinov.mrigo.mrigo
        FROM blinov.mrigo
        """
        mrigo_id_name = db.get_table_from_query(mrigo_query)
        print(f"\n> Таблица с кодами и наименованиям МРИГО успешно загружена.")

        okptdr_query = """
                    SELECT blinov.okpdtr.id, blinov.okpdtr.name
                    FROM blinov.okpdtr
                    """
        okpdtr_id_name = db.get_table_from_query(okptdr_query)
        okptdr_assoc_query = """
                    SELECT blinov.okpdtr_assoc.id, blinov.okpdtr_assoc.name
                    FROM blinov.okpdtr_assoc;
                    """
        okpdtr_assoc_id_name = db.get_table_from_query(okptdr_assoc_query)
        print(f"> Таблица с кодами и наименованиям ОКПДТР успешно загружена.")
    except:
        print(f"> Нет доступа к БД в данный момент, либо проблемы с запросом. Заверешение работы.")
        sys.exit(7)

    ############################################################################
    '''сопоставление адресов вакансий с кодами МРИГО и имен вакансий с ОКПДТР'''
    # константы
    SIMILARITY_LEVEL_MRIGO = 70
    SIMILARITY_LEVEL_OKPDTR = 80

    print(f"\n> Сопоставление вакансий с кодами МРИГО:")
    addresses = vacancies['address'].tolist()
    mrigo = mrigo_id_name['mrigo'].tolist()
    id_mrigo = mrigo_id_name['id_mrigo'].tolist()
    d1 = dict(zip(mrigo, id_mrigo))

    for i in range(len(addresses)):
        addresses[i] = addresses[i].replace('Новосибирская область, ', '', 1)
        addresses[i] = re.sub(r"[\W\d]", '', addresses[i])
        if re.search(r'Новосибирский', addresses[i]) != None:
            addresses[i] = re.search(r'Новосибирский', addresses[i]).group(0)
        if re.search(r'рн\s\w+\s', addresses[i]) != None:
            addresses[i] = re.search(r'рн\s\w+\s', addresses[i]).group(0)
        if re.search(r'г\s\w+\s', addresses[i]) != None:
            addresses[i] = re.search(r'г\s\w+\s', addresses[i]).group(0)
    print(f">> Очистка адресов в отношении 'Вакансии' прошла успешно.")

    matched_list = []
    print(f">> Началось сопоставление вакансий с кодами МРИГО... (всего итераций -- {len(addresses)})")
    for _address in addresses:
        a = process.extractOne(_address, mrigo, scorer=fuzz.token_set_ratio)
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

    vacancies = vacancies[:200]

    print(f"\n> Сопоставление вакансий с кодами ОКПДТР:")
    jobs = vacancies['job-name'].tolist()[:200]
    okpdtr = okpdtr_id_name['name'].tolist()
    id_okpdtr= okpdtr_id_name['id'].tolist()
    d2 = dict(zip(okpdtr, id_okpdtr))
    
    # очистка наименований ОКПДТР
    for i in range(len(okpdtr)):
        okpdtr[i] = re.sub(r"[\W\d]", '', okpdtr[i].lower())
    
    # очистка названий вакансий
    for i in range(len(jobs)):
        jobs[i] = re.sub(r"[\W\d]", '', re.split(r'{}'.format('|'.join(oks.dictionary)), jobs[i].lower())[0])
    print("Очистка имен вакансий и наименований ОКПДТР прошла успешно.")
    
    # очистка списка с сопоставленными данными
    print(f">> Началось сопоставление вакансий с кодами ОКПТДР... (всего итераций -- {len(jobs)})")
    indexes = []
    for _job in jobs:
        sub_lst = []
        for _okpdtr in okpdtr:
            sub_lst.append(jellyfish.jaro_distance(_okpdtr, _job))
        max_indexes = sf.find_locate_max(sub_lst)
        if max_indexes[0] < SIMILARITY_LEVEL_OKPDTR / 100.0:
            indexes.append(len(okpdtr))
        else:
            indexes.append(max_indexes[1][0])
    id_okpdtr.append(None)
    okpdtr.append(None)
    fix_id_okpdtr = [id_okpdtr[indexes[i]] for i in range(len(jobs))]
    print(f">> Сопоставление вакансий с кодами ОКПДТР завершено.")
    
    fix_id_okpdtr_df = pd.DataFrame(fix_id_okpdtr, columns=['fix_id_okpdtr'])
    print(((fix_id_okpdtr_df.isnull() | fix_id_okpdtr_df.isna()).sum() * 100 / fix_id_okpdtr_df.index.size).round(2))

    # вставка кодов ОКПДТР в таблицу
    vacancies.insert(11, 'id_okpdtr', fix_id_okpdtr, True)
    vacancies.to_csv(os.path.join('tables', 'vacancies_updated.csv'), index=None, header=True)
    end = time.time()

    print(f"\n> Всего потребовалось времени: {end - start}")
    
    
    #################################################################################
    """выгрузка/обновление таблиц 'Компании' и дополненной таблицы 'Вакансии' в БД"""
    print(f"\n> Выгрузка полученных полученных данных в БД.")
    flag_companies = True
    # выгрузка компаний
    try:
        companies_query = """
                SELECT *
                FROM blinov.companies_tv
                """
        companies_tv_old = db.get_table_from_query(companies_query)
    except:
        flag_companies = False
    # если уже есть отношение 'Компании' в БД
    if flag_companies:
        print(f">> Отношение 'Компании' уже содержится в БД. Добавление новых записей...")
        
        cond = companies['ogrn'].isin(companies_tv_old['ogrn'])
        companies_tv_diff = companies.drop(companies[cond].index, inplace=False
            ).reset_index().drop(
                ['index'],
                axis=1
            )

        companies_tv_diff = companies_tv_diff.astype({
            'inn': 'str',
            'ogrn': 'str',
            'kpp': 'str',
        })
        for index, row in companies_tv_diff.iterrows():
            companies_tv_diff.at[index, 'inn'] = re.split(
                r'[.]',
                companies_tv_diff.at[index, 'inn']
                )[0]
            companies_tv_diff.at[index, 'ogrn'] = re.split(
                r'[.]',
                companies_tv_diff.at[index, 'ogrn']
                )[0]
            companies_tv_diff.at[index, 'kpp'] = re.split(
                r'[.]',
                companies_tv_diff.at[index, 'kpp']
                )[0]
        companies_tv_diff = companies_tv_diff.replace({'nan': np.nan})
        companies_tv_diff = companies_tv_diff.replace({np.nan: None})
        if not companies_tv_diff.empty:
            print(f">> Число новых компаний для обновления -- {companies_tv_diff.shape[0]}")
            companies_tv_diff.to_sql(
                'companies_tv',
                con=db.engine,
                schema='blinov',
                if_exists='append',
                index=False,
                chunksize=None,
                method='multi',
                dtype={
                    'ogrn': sa.String,
                    'inn': sa.String,
                    'kpp': sa.String,
                    'companycode': sa.String,
                    'name': sa.String,
                    'address': sa.String,
                    'hr-agency': sa.String,
                    'url': sa.String,
                    'site': sa.String,
                    'phone': sa.String,
                    'fax': sa.String,
                    'email': sa.String,
                })
            print(f">> Выгрузка новых данных в таблицу 'Компании' завершена.")
        else:
            print(f">> Нет новых компаний для выгрузки в БД.")
    else:
        print(f">> Отношение 'Компании' ранее не содержалось в БД. Создание таблицы и добавление новых записей, если они есть...")
        companies = companies.astype({
            'inn' : 'str',
            'ogrn' : 'str',
            'kpp' : 'str',
            })
        for index, row in companies.iterrows():
            companies.at[index, 'inn'] = re.split(r'[.]', companies.at[index, 'inn'])[0]
            companies.at[index, 'ogrn'] = re.split(r'[.]', companies.at[index, 'ogrn'])[0]
            companies.at[index, 'kpp'] = re.split(r'[.]', companies.at[index, 'kpp'])[0]
        companies = companies.replace({'nan' : np.nan})
        companies = companies.replace({np.nan : None})
        companies.to_sql(
            'companies_tv',
            con=db.engine,
            schema='blinov',
            if_exists='replace',
            index=False,
            chunksize=None,
            method='multi',
            dtype={
                'ogrn' : sa.String,
                'inn' : sa.String,
                'kpp' : sa.String,
                'companycode' : sa.String,
                'name' : sa.String,
                'address' : sa.String,
                'hr-agency' : sa.String,
                'url' : sa.String,
                'site' : sa.String,
                'phone' : sa.String,
                'fax' : sa.String,
                'email' : sa.String,
            })
        db.engine.execute('ALTER TABLE blinov.companies_tv ADD PRIMARY KEY(ogrn)')
        print(f">> Создание таблицы 'Компании' и выгрузка новых записей завершена.")

    # выгрузка вакансий
    flag_vacancies = True
    try:
        vacancies_query = """
                SELECT *
                FROM blinov.vacancies_tv
                """
        vacancies_tv_old = db.get_table_from_query(vacancies_query)
    except:
        flag_vacancies = False

    # если уже есть отношение 'Вакансии' в БД
    if flag_vacancies:
        print(f">> Отношение 'Вакансии' уже содержится в БД. Добавление новых записей, если они есть...")

        cond = vacancies['ogrn'].isin(vacancies_tv_old['ogrn'])
        vacancies_tv_diff = vacancies.drop(vacancies[cond].index, inplace=False
            ).reset_index().drop(
                ['index'],
                axis=1
            )

        vacancies_tv_diff = vacancies_tv_diff.astype({
            'region_code': 'str',
            'ogrn': 'str',
            'id_mrigo': 'str',
            'id_okpdtr': 'str',
        })
        for index, row in vacancies_tv_diff.iterrows():
            vacancies_tv_diff.at[index, 'region_code'] = re.split(
                r'[.]',
                vacancies_tv_diff.at[index, 'region_code']
                )[0]
            vacancies_tv_diff.at[index, 'ogrn'] = re.split(
                r'[.]',
                vacancies_tv_diff.at[index, 'ogrn']
                )[0]
            vacancies_tv_diff.at[index, 'id_mrigo'] = re.split(
                r'[.]',
                vacancies_tv_diff.at[index, 'id_mrigo']
                )[0]
            vacancies_tv_diff.at[index, 'id_okpdtr'] = re.split(
                r'[.]',
                vacancies_tv_diff.at[index, 'id_okpdtr']
                )[0]
        vacancies_tv_diff = vacancies_tv_diff.replace({'nan': np.nan})
        vacancies_tv_diff = vacancies_tv_diff.replace({np.nan: None})
        if not vacancies_tv_diff.empty:
            print(f">> Число новых компаний для обновления -- {vacancies_tv_diff.shape[0]}")
            vacancies_tv_diff.to_sql(
                'vacancies_tv',
                con=db.engine,
                schema='blinov',
                if_exists='append',
                index=False,
                chunksize=None,
                method='multi',
                dtype={
                    'id': sa.String,
                    'ogrn': sa.String,
                    'source': sa.String,
                    'region_code': sa.String,
                    'region_name': sa.String,
                    'address': sa.String,
                    'id_mrigo': sa.String,
                    'experience': sa.String,
                    'employment': sa.String,
                    'schedule': sa.String,
                    'job-name': sa.String,
                    'id_okpdtr': sa.String,
                    'specialisation': sa.String,
                    'duty': sa.String,
                    'education': sa.String,
                    'qualification': sa.String,
                    'term_text': sa.String,
                    'social_protected': sa.String,
                    'salary_min': sa.Float,
                    'salary_max': sa.Float,
                    'salary': sa.String,
                    'currency': sa.String,
                    'vac_url': sa.String,
                    'creation-date-from-api' : sa.DateTime,
                    'modify-modify-date-from-api' : sa.DateTime,
                    'download_time': sa.DateTime,
                    'is_closed': sa.Boolean,
                })
            print(">> Выгрузка новых данных в таблицу 'Вакансии' завершена.")
        else:
            print(">> Нет новых вакансий для выгрузки в БД.")
    else:
        print(f">> Отношение 'Вакансии' ранее не содержалось в БД. Создание таблицы и добавление новых записей...")
        vacancies = vacancies.astype({
            'region_code' : 'str',
            'ogrn' : 'str',
            'id_mrigo' : 'str',
            'id_okpdtr' : 'str',
            })
        for index, row in vacancies.iterrows():
            vacancies.at[index, 'region_code'] = re.split(r'[.]', vacancies.at[index, 'region_code'])[0]
            vacancies.at[index, 'ogrn'] = re.split(r'[.]', vacancies.at[index, 'ogrn'])[0]
            vacancies.at[index, 'id_mrigo'] = re.split(r'[.]', vacancies.at[index, 'id_mrigo'])[0]
            vacancies.at[index, 'id_okpdtr'] = re.split(r'[.]', vacancies.at[index, 'id_okpdtr'])[0]
        vacancies = vacancies.replace({'nan' : np.nan})
        vacancies = vacancies.replace({np.nan : None})
        vacancies.to_sql(
            'vacancies_tv',
            con=db.engine,
            schema='blinov',
            if_exists='replace',
            index=False,
            chunksize=None,
            method='multi',
            dtype={
                'id' : sa.String,
                'ogrn' : sa.String,
                'source' : sa.String,
                'region_code' : sa.String,
                'region_name' : sa.String,
                'address' : sa.String,
                'id_mrigo' : sa.String,
                'experience' : sa.String,
                'employment' : sa.String,
                'schedule' : sa.String,
                'job-name' : sa.String,
                'id_okpdtr' : sa.String,
                'specialisation' : sa.String,
                'duty' : sa.String,
                'education' : sa.String,
                'qualification' : sa.String,
                'term_text' : sa.String,
                'social_protected' : sa.String,
                'salary_min' : sa.Float,
                'salary_max' : sa.Float,
                'salary' : sa.String,
                'currency' : sa.String,
                'vac_url' : sa.String,
                'creation-date-from-api' : sa.DateTime,
                'modify-modify-date-from-api' : sa.DateTime,
                'download_time': sa.DateTime,
                'is_closed': sa.Boolean,
            })
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD PRIMARY KEY(id)')
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD CONSTRAINT vac_comp_f_key FOREIGN KEY (ogrn) REFERENCES blinov.companies_tv (ogrn)')
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD CONSTRAINT vac_mrigo_f_key FOREIGN KEY (id_mrigo) REFERENCES blinov.mrigo (id_mrigo)')
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD CONSTRAINT vac_okpdtr_f_key FOREIGN KEY (id_okpdtr) REFERENCES blinov.okpdtr (id)')
        print(f">> Создание таблицы 'Вакансии' и выгрузка новых записей завершена.")
    print(f">> Выгрузка данных в БД завершена.")
    

if __name__ == "__main__":
    main()
    print(f"\n> Программа успешно завершила свою работу.")

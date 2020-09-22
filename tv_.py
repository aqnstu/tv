#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
  - Получение данных через API ТРУДВСЕМ.
  - Распределение исходных данных на два отношения -- "Компании" и "Вакансии" (2NF).
  - Получение из БД таблиц с кодами и названиями МРИГО/ОКПДТР, а также с параметрами для сопоставления.
  - Cопоставление адресов вакансий с кодами МРИГО и имен вакансий с кодами ОКПДТР.
  - Выгрузка/обновление таблиц "Компании" и дополненной таблицы "Вакансии" в БД.
  - Логирование скрипта. 
  - Измерение времени выполнения.
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

from rapidfuzz import fuzz
from rapidfuzz import process

import misc.db as db
import misc.okpdtr_splits as oks


def find_locate_max(lst):
    """
    Поиск наибольшего значения в списке.

    Входные параметры:
    lst -- список с числами
    """
    biggest = max(lst)
    return biggest, [index for index, element in enumerate(lst) if biggest == element]


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
    with urllib.request.urlopen("http://opendata.trudvsem.ru/api/v1/vacancies/region/54?offset=" + str(offset) + "&limit=100") as url:
        data = json.loads(url.read().decode("utf-8"))
        status = int(data['status'])
        if status != 200:
            return None, status
        return data, status


def get_data_from_api(start_offset):
    """
    Получить доступные данные о вакансиях через API ТРУДВСЕМ.

    Входные параметры:
    start_offset -- начальная страница для загрузки данных
    """
    df_raw = pd.DataFrame()
    offset = start_offset
    print(">> Загрузка данных через API TRUDVSEM...")
    while True:
        # print(f">> Текущая страница: -- {offset}")
        data, status = get_page_from_api(offset)
        if status != 200:
            break
        new_data = data['results']['vacancies']
        # приводим полученные данные к таблице
        df_tmp = pd.json_normalize(new_data)
        df_raw = pd.concat([df_raw, df_tmp], sort=False, ignore_index=True)
        offset += 1
    print(">> Загрузка данных через API TRUDVSEM заверешна.")
    return df_raw


def main():
    ################################
    '''получение данных через API'''
    ################################
    print(f"> Получение данных:")
    start = time.time()
    # получение "сырого" датафрейма с вакансиями
    try:
        df_raw = get_data_from_api(0)
    except:
        s1 = "Сервера TRUDVSEM недоступны. Завершение работы."
        db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message) VALUES (:ep, :msg)").bindparams(ep=1, msg=s1))
        print(f">>> " + s1)
        sys.exit(1)

    if df_raw.empty:
        s2 = "Данных не найдено, возможно они были перенесены. Завершение работы."
        db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message) VALUES (:ep, :msg)").bindparams(ep=2, msg=s2))
        print(f">>> " + s2)
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
        
        df_raw['vacancy.addresses.address'] = df_raw['vacancy.addresses.address'].apply(lambda s: re.sub("'location': |{|\[|lng': |'lat': |}|\]|\'", '', s))
        df_raw['vacancy.duty'] = df_raw['vacancy.duty'].apply(remove_tags)
        df_raw['vacancy.requirement.qualification'] = df_raw['vacancy.requirement.qualification'].apply(remove_tags)
        df_raw = df_raw.replace({False: np.nan})
        
        # время загрузки данных (фактическое)
        df_raw['download_time'] = pd.to_datetime('now')
        
        # выбираем ОГРН первичным ключом
        df_raw['vacancy.company.ogrn'] = df_raw['vacancy.company.ogrn'].str.strip()
        df_raw['vacancy.company.ogrn'] = df_raw['vacancy.company.ogrn'].replace(['nan', 'NaN', 'Nan', 'naN', 'nAn', ''], np.nan)
        df_raw = df_raw[pd.notnull(df_raw['vacancy.company.ogrn'])]
        df_raw = df_raw.drop_duplicates(
            subset="vacancy.id",
            keep='last'
        )
        # df_raw.to_csv('df_raw.csv', index=False)
    except:
        s3 = "Проблемы с исходным датафреймом (df_raw). Завершение работы."
        db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message) VALUES (:ep, :msg)").bindparams(ep=3, msg=s3))
        print(f">>> " + s3)
        sys.exit(3)


    ######################################################################################
    '''распределение исходных данных на два отношения -- 'Компании' и 'Вакансии' (2NF)'''
    ######################################################################################
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
            'vacancy.company.hr-agency': 'hr_agency',
            'vacancy.company.url': 'url',
            'vacancy.company.site': 'site',
            'vacancy.company.phone': 'phone',
            'vacancy.company.fax': 'fax',
            'vacancy.company.email': 'email',
            'vacancy.company.code_industry_branch': 'code_industry_branch',
        })
        print(">> Новое отношение 'Компании' успешно сформированно")
    except:
        s4 = "Проблема с формированием отношения 'Компании'. Завершение работы."
        db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message) VALUES (:ep, :msg)").bindparams(ep=4, msg=s4))
        print(f">>> " + s4)
        sys.exit(4)

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
            'vacancy.job-name': 'job_name',
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
            'vacancy.creation-date': 'creation_date_from_api',
            'vacancy.modify-date': 'modify_date_from_api',
        })
        vacancies['is_closed'] = False
        vacancies['closing_time'] = np.nan
        print(">> Новое отношение 'Вакансии' успешно сформированно.")
    except:
        s5 = "Проблема с формированием отношения 'Вакансии'. Завершение работы."
        db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message) VALUES (:ep, :msg)").bindparams(ep=5, msg=s5))
        print(f">>> " + s5)
        sys.exit(5)

    ########################################################################################################
    '''получение из БД таблиц с кодами и названиями МРИГО/ОКПДТР, а также с параметрами для сопоставления'''
    ########################################################################################################
    try:
        mrigo_id_name = db.get_table_from_db_by_table_name('blinov.mrigo')
        print(f"\n> Таблица с кодами и наименованиям МРИГО успешно загружена.")

        okpdtr_id_name = db.get_table_from_db_by_table_name('blinov.okpdtr')
        okpdtr_assoc_id_name = db.get_table_from_db_by_table_name('blinov.okpdtr_assoc')
        okpdtr_id_name = pd.concat([okpdtr_id_name, okpdtr_assoc_id_name], sort=False, ignore_index=True)
        print(f"> Таблица с кодами и наименованиям ОКПДТР успешно загружена.")
        
        similarity_levels = db.get_table_from_db_by_table_name('vacs.tv_params')
        print(f"> Таблица с параметрами для сопоставления успешно загружена.")
    except:
        s6 = "Нет доступа к БД в данный момент, либо проблемы с запросом. Заверешение работы."
        db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message) VALUES (:ep, :msg)").bindparams(ep=6, msg=s6))
        print(f">>> " + s6)
        sys.exit(6)


    ###################################################################################
    '''сопоставление адресов вакансий с кодами МРИГО и имен вакансий с кодами ОКПДТР'''
    ###################################################################################
    # константы
    SIMILARITY_LEVEL_MRIGO = similarity_levels['similarity_level_mrigo'].tolist()[-1]
    SIMILARITY_LEVEL_OKPDTR = similarity_levels['similarity_level_okpdtr'].tolist()[-1]

    print(f"\n> Сопоставление вакансий с кодами МРИГО:")
    addresses = vacancies['address'].tolist()
    mrigo = mrigo_id_name['mrigo'].tolist()
    id_mrigo = mrigo_id_name['id_mrigo'].tolist()
    d1 = dict(zip(mrigo, id_mrigo))

    for i in range(len(addresses)):
        addresses[i] = addresses[i].replace('Новосибирская область, ', '', 1)
        addresses[i] = re.sub(r"[\,\-\.\d]", '', addresses[i])
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

    # из полученного списка кортежей получаем датафрейм
    df_with_id_mrigo = pd.DataFrame(matched_list, columns=['id_mrigo', 'score'])
    # если полученная оценка меньше заданной, то выбранный МРИГО не рассматривается
    df_with_id_mrigo['fix_id_mrigo'] = np.where(df_with_id_mrigo['score'] > SIMILARITY_LEVEL_MRIGO, df_with_id_mrigo['id_mrigo'], np.nan)
    # печать результатов (статистики) сопоставления
    df_with_id_mrigo_ = pd.DataFrame(df_with_id_mrigo['fix_id_mrigo'].tolist(), columns=['fix_id_mrigo'])
    print(((df_with_id_mrigo_.isnull() | df_with_id_mrigo_.isna()).sum() * 100 / df_with_id_mrigo_.index.size).round(2))
    
    # вставка кодов МРИГО в таблицу
    vacancies.insert(6, 'id_mrigo', df_with_id_mrigo['fix_id_mrigo'].tolist(), True)
    print(f">> Сопоставление вакансий с кодами МРИГО завершено.")

    print(f"\n> Сопоставление вакансий с кодами ОКПДТР:")
    jobs = vacancies['job_name'].tolist()
    okpdtr = okpdtr_id_name['name'].tolist()
    id_okpdtr= okpdtr_id_name['id'].tolist()
    # d2 = dict(zip(okpdtr, id_okpdtr))

    # очистка наименований ОКПДТР
    for i in range(len(okpdtr)):
        okpdtr[i] = re.sub(r"[\W\d]", '', okpdtr[i].lower())

    # очистка названий вакансий
    for i in range(len(jobs)):
        jobs[i] = re.sub(r"[\W\d]", '', re.split(r'{}'.format('|'.join(oks.dictionary)), jobs[i].lower())[0])
    print(f">> Очистка имен вакансий и наименований ОКПДТР прошла успешно.")

    # очистка списка с сопоставленными данными
    print(f">> Началось сопоставление вакансий с кодами ОКПТДР... (всего итераций -- {len(jobs)})")
    indexes = []
    for _job in jobs:
        sub_lst = []
        for _okpdtr in okpdtr:
            sub_lst.append(jellyfish.jaro_distance(_okpdtr, _job))
        max_indexes = find_locate_max(sub_lst)
        if max_indexes[0] < SIMILARITY_LEVEL_OKPDTR / 100.0:
            indexes.append(len(okpdtr))
        else:
            indexes.append(max_indexes[1][0])
    id_okpdtr.append(np.nan)
    okpdtr.append(np.nan)
    fix_id_okpdtr = [id_okpdtr[indexes[i]] for i in range(len(jobs))]
    
    fix_id_okpdtr_df = pd.DataFrame(fix_id_okpdtr, columns=['fix_id_okpdtr'])
    print(((fix_id_okpdtr_df.isnull() | fix_id_okpdtr_df.isna()).sum() * 100 / fix_id_okpdtr_df.index.size).round(2))

    # вставка кодов ОКПДТР в таблицу
    vacancies.insert(11, 'id_okpdtr', fix_id_okpdtr, True)
    # vacancies.to_csv(os.path.join('tables', 'vacancies_updated.csv'), index=None, header=True)
    print(f">> Сопоставление вакансий с кодами ОКПДТР завершено.")


    #################################################################################
    """выгрузка/обновление таблиц 'Компании' и дополненной таблицы 'Вакансии' в БД"""
    #################################################################################
    print(f"\n> Выгрузка полученных данных в БД:")
    companies_counter = 0
    vacancies_counter = 0
    flag_companies = True
    # выгрузка компаний
    try:
        companies_old = db.get_table_from_db_by_table_name('vacs.companies_tv')
    except:
        flag_companies = False
    # если уже есть отношение 'Компании' в БД
    if flag_companies:
        print(f">> Отношение 'Компании' уже содержится в БД. Добавление новых записей...")
        cond = companies['ogrn'].isin(companies_old['ogrn'])
        companies_diff = companies.drop(companies[cond].index, inplace=False).reset_index().drop(['index'],axis=1)

        companies_diff = companies_diff.astype({
            'inn': 'str',
            'ogrn': 'str',
            'kpp': 'str',
        })
        
        companies_diff['inn'] = companies_diff['inn'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        companies_diff['ogrn'] = companies_diff['ogrn'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        companies_diff['kpp'] = companies_diff['kpp'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        
        companies_diff = companies_diff.replace({'nan': np.nan})
        if not companies_diff.empty:
            try:
                # companies_diff.to_csv('companies_diff.csv', index=False)
                companies_counter = companies_diff.shape[0]
                print(f">> Число новых компаний для обновления -- {companies_counter}")
                companies_diff.to_sql(
                    'companies_tv_tmp',
                    con=db.engine,
                    schema='vacs',
                    if_exists='replace',
                    index=False,
                    method='multi',
                    dtype={
                        'ogrn': sa.String,
                        'inn': sa.String,
                        'kpp': sa.String,
                        'companycode': sa.String,
                        'name': sa.String,
                        'address': sa.String,
                        'hr_agency': sa.String,
                        'url': sa.String,
                        'site': sa.String,
                        'phone': sa.String,
                        'fax': sa.String,
                        'email': sa.String,
                    })
                db.engine.execute('INSERT INTO vacs.companies_tv (ogrn, inn, kpp, companycode, name, address, hr_agency, url, site, phone, fax, email) SELECT ogrn, inn, kpp, companycode, name, address, hr_agency, url, site, phone, fax, email FROM vacs.companies_tv_tmp ON CONFLICT (ogrn) DO NOTHING;')
                db.engine.execute('DROP TABLE vacs.companies_tv_tmp;')
            except Exception as e:
                s7 = "Проблема с обновлением отношения 'Компании'. Продолжение работы."
                db.engine.execute(sa.text("INSERT INTO vacs.tv_log (message) VALUES (:msg)").bindparams(msg=s7))
                companies_counter = 0
                print(f">>> " + s7 + str(e))
            else:
                print(f">> Выгрузка новых данных в таблицу 'Компании' завершена.")
        else:
            print(f">> Нет новых компаний для выгрузки в БД.")
    else:
        try:
            print(f">> Отношение 'Компании' ранее не содержалось в БД. Создание таблицы и добавление новых записей, если они есть...")
            companies = companies.astype({
                'inn' : 'str',
                'ogrn' : 'str',
                'kpp' : 'str',
                })
            
            companies['inn'] = companies['inn'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            companies['ogrn'] = companies['ogrn'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            companies['kpp'] = companies['kpp'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            
            companies = companies.replace({'nan' : np.nan})
            # companies.to_csv('companies.csv', index=False)
            companies_counter = companies.shape[0]
            print(f">> Число новых компаний для загрузки -- {companies_counter}")
            companies.to_sql(
                'companies_tv',
                con=db.engine,
                schema='vacs',
                if_exists='replace',
                index=False,
                method='multi',
                dtype={
                    'ogrn' : sa.String,
                    'inn' : sa.String,
                    'kpp' : sa.String,
                    'companycode' : sa.String,
                    'name' : sa.String,
                    'address' : sa.String,
                    'hr_agency' : sa.String,
                    'url' : sa.String,
                    'site' : sa.String,
                    'phone' : sa.String,
                    'fax' : sa.String,
                    'email' : sa.String,
                })
            db.engine.execute('ALTER TABLE vacs.companies_tv ADD PRIMARY KEY(ogrn)') 
        except:
            s8 = f"Проблема с выгрузкой нового отношения 'Компании'. Продолжение работы."
            db.engine.execute(sa.text("INSERT INTO vacs.tv_log (message) VALUES (:msg)").bindparams(msg=s8))
            companies_counter = 0
            print(f">>> " + s8)
        else:
            print(f">> Создание таблицы 'Компании' и выгрузка новых записей завершена.")
    
    # выгрузка вакансий
    flag_vacancies = True
    try:
        vacancies_old = db.get_table_from_db_by_table_name('vacs.vacancies_tv')
    except:
        flag_vacancies = False

    # если уже есть отношение 'Вакансии' в БД
    if flag_vacancies:
        print(f">> Отношение 'Вакансии' уже содержится в БД. Добавление новых записей, если они есть...")
        cond = vacancies['id'].isin(vacancies_old['id'])
        vacancies_diff = vacancies.drop(vacancies[cond].index, inplace=False).reset_index().drop(['index'], axis=1)

        # обновляем is_closed и closing_time
        id_from_old = set(vacancies_old.id) - set(vacancies.id)
        if id_from_old:
            print(f">> Всего закрытых вакансий (потенциально) -- {len(id_from_old)}")
            db.engine.execute(sa.text("UPDATE vacs.vacancies_tv SET is_closed = TRUE WHERE is_closed = FALSE AND id in :values").bindparams(values=tuple(id_from_old)))
            db.engine.execute(sa.text("UPDATE vacs.vacancies_tv SET closing_time = now() WHERE closing_time IS NULL AND id in :values").bindparams(values=tuple(id_from_old)))
        else:
            print(f">> Вакансий для закрытия не найдено.")
        vacancies_diff = vacancies_diff.astype({
            'region_code': 'str',
            'ogrn': 'str',
            'id_mrigo': 'str',
            'id_okpdtr': 'str',
        })
        
        vacancies_diff['region_code'] = vacancies_diff['region_code'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        vacancies_diff['ogrn'] = vacancies_diff['ogrn'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        vacancies_diff['id_mrigo'] = vacancies_diff['id_mrigo'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        vacancies_diff['id_okpdtr'] = vacancies_diff['id_okpdtr'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
        vacancies_diff = vacancies_diff.replace({'nan': np.nan})
        
        if not vacancies_diff.empty:
            try:
                # vacancies_diff.to_csv('vacancies_diff.csv', index=False)
                vacancies_counter = vacancies_diff.shape[0]
                print(f">> Число новых вакансий для загрузки -- {vacancies_counter}")
                vacancies_diff.to_sql(
                    'vacancies_tv_tmp',
                    con=db.engine,
                    schema='vacs',
                    if_exists='replace',
                    index=False,
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
                        'job_name' : sa.String,
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
                        'creation_date_from_api' : sa.DateTime,
                        'modify_date_from_api' : sa.DateTime,
                        'download_time': sa.DateTime,
                        'is_closed': sa.Boolean,
                        'closing_time': sa.DateTime,
                    })
                db.engine.execute('INSERT INTO vacs.vacancies_tv (id, ogrn, source, region_code, address, id_mrigo, experience, employment, schedule, job_name, id_okpdtr, specialisation, duty, education, qualification, term_text, social_protected, salary_min, salary_max, salary, currency, vac_url, creation_date_from_api, modify_date_from_api, download_time, is_closed, closing_time) SELECT id, ogrn, source, region_code, address, id_mrigo, experience, employment, schedule, job_name, id_okpdtr, specialisation, duty, education, qualification, term_text, social_protected, salary_min, salary_max, salary, currency, vac_url, creation_date_from_api, modify_date_from_api, download_time, is_closed, closing_time FROM vacs.vacancies_tv_tmp ON CONFLICT (id) DO NOTHING;')
                db.engine.execute('DROP TABLE vacs.vacancies_tv_tmp;')
            except Exception as e:
                s9 = "Проблема с обновлением отношения 'Вакансии'. Продолжение работы."
                db.engine.execute(sa.text("INSERT INTO vacs.tv_log (message) VALUES (:msg)").bindparams(msg=s9))
                vacancies_counter = 0
                print(f">>> " + s9 + str(e))
            else:
                print(">> Выгрузка новых данных в таблицу 'Вакансии' завершена.")    
        else:
            print(">> Нет новых вакансий для выгрузки в БД.")
    else:
        try:
            print(f">> Отношение 'Вакансии' ранее не содержалось в БД. Создание таблицы и добавление новых записей...")
            vacancies = vacancies.astype({
                'region_code' : 'str',
                'ogrn' : 'str',
                'id_mrigo' : 'str',
                'id_okpdtr' : 'str',
                })
            
            vacancies['region_code'] = vacancies['region_code'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            vacancies['ogrn'] = vacancies['ogrn'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            vacancies['id_mrigo'] = vacancies['id_mrigo'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            vacancies['id_okpdtr'] = vacancies['id_okpdtr'].apply(lambda s: re.split(r'[.]', s)[0] if not pd.isna(s) else s)
            vacancies = vacancies.replace({'nan' : np.nan})
            # vacancies.to_csv('vacancies.csv', index=False)
            vacancies_counter = vacancies.shape[0]
            print(f">> Число новых вакансий для загрузки -- {vacancies_counter}")
            vacancies.to_sql(
                'vacancies_tv',
                con=db.engine,
                schema='vacs',
                if_exists='replace',
                index=False,
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
                    'job_name' : sa.String,
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
                    'creation_date_from_api' : sa.DateTime,
                    'modify_date_from_api' : sa.DateTime,
                    'download_time': sa.DateTime,
                    'is_closed': sa.Boolean,
                    'closing_time': sa.DateTime,
                })
            db.engine.execute('ALTER TABLE vacs.vacancies_tv ADD PRIMARY KEY(id)')
            db.engine.execute('ALTER TABLE vacs.vacancies_tv ADD CONSTRAINT vac_comp_f_key FOREIGN KEY (ogrn) REFERENCES vacs.companies_tv (ogrn)')
            db.engine.execute('ALTER TABLE vacs.vacancies_tv ADD CONSTRAINT vac_mrigo_f_key FOREIGN KEY (id_mrigo) REFERENCES blinov.mrigo (id_mrigo)')
            db.engine.execute('ALTER TABLE vacs.vacancies_tv ADD CONSTRAINT vac_okpdtr_f_key FOREIGN KEY (id_okpdtr) REFERENCES blinov.okpdtr (id)')
        except:
            s10 = f"Проблема с выгрузкой нового отношения 'Вакансии'. Продолжение работы."
            db.engine.execute(sa.text("INSERT INTO vacs.tv_log (message) VALUES (:msg)").bindparams(msg=s10))
            vacancies_counter = 0
            print(f">>> " + s10)
        else:
            print(f">> Создание таблицы 'Вакансии' и выгрузка новых записей завершена.")
    print(f">> Выгрузка данных в БД завершена.")
    
    end = time.time()
    print(f"\n> Всего потребовалось времени: {end - start}")
    
    return companies_counter, vacancies_counter


if __name__ == "__main__":
    companies_counter, vacancies_counter = main()
    s0 = "Программа успешно завершила свою работу."
    db.engine.execute(sa.text("INSERT INTO vacs.tv_log (exit_point, message, num_of_companies, num_of_vacancies) VALUES (:ep, :msg, :noc, :nov)").bindparams(ep=0, msg=s0, noc=companies_counter, nov=vacancies_counter))
    print(f"\n> " + s0)
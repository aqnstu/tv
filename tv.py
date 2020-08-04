#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
    получение данных через API ТРУДВСЕМ,
    первичная очистка,
    распределение исходных данных  на два отношения -- 'Компании' и 'Вакансии' (2NF),
    получение таблиц с кодами и названиями ОКПДТР/МРИГО из БД,
    
"""

import html
import json
import numpy as np
import os
import pandas as pd
import re
import sys
import urllib.request

import misc.db as db


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
    print("> Начало загрузки данных через API TRUDVSEM...")
    while True:
        print(f">> Текущая страница: -- {offset}")
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
    # первичная очистка
        df_raw = df_raw.astype({
            'vacancy.region.region_code': 'str',
            'vacancy.company.inn': 'str',
            'vacancy.company.ogrn': 'str',
            'vacancy.company.kpp': 'str',
            'vacancy.addresses.address': 'str',
            'vacancy.duty': 'str',
            'vacancy.requirement.qualification': 'str',
        })

        for index, row in df_raw.iterrows():
            df_raw.at[index, 'vacancy.addresses.address'] = re.sub(
                "'location': |{|\[|lng': |'lat': |}|\]|\'", '', df_raw.at[index, 'vacancy.addresses.address'])
            df_raw.at[index, 'vacancy.duty'] = remove_tags(
                df_raw.at[index, 'vacancy.duty'])
            df_raw.at[index, 'vacancy.requirement.qualification'] = remove_tags(
                df_raw.at[index, 'vacancy.requirement.qualification'])
        df_raw = df_raw.replace({False: np.nan})
        df_raw = df_raw.replace({np.nan: None})
        # добавление времени и даты загрузки
        df_raw['download_time'] = pd.to_datetime('now')
        if not os.path.exists('tables'):
            os.makedirs('tables')
        df_raw.to_csv(os.path.join(
            'tables',
            'raw_dataframe.csv'
            ), index=None, header=True)
        print("> Первичная очистка данных завершена. Исходное отношение 'Вакансии' сохранено.")
    except:
        print(f">>> Проблемы с исходным датафреймом (df_raw)[1]. Завершение работы.")
        sys.exit(3)

    try:
        # выбираем ОГРН первичным ключом
        df_raw = df_raw[pd.notnull(df_raw['vacancy.company.ogrn'])]
    except:
        print(f">>> Проблемы с исходным датафреймом (df_raw)[2]. Завершение работы.")
        sys.exit(4)

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
        companies.to_csv(
            os.path.join(
            'tables',
            'companies.csv'
            ), index=None, header=True)
        
        """"""""""""
        companies.to_excel(
            os.path.join(
            'tables',
            'companies.xlsx'
            ), index=None, header=True)
        """"""""""""
        
        print("> Новое отношение 'Компании' успешно сформированно и сохранено.")
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
        vacancies.to_csv(
            os.path.join(
            'tables',
            'vacancies.csv'
            ), index=None, header=True)
        
        """"""""""""
        vacancies.to_excel(
            os.path.join(
            'tables',
            'vacancies.xlsx'
            ), index=None, header=True)
        """"""""""""
        
        print("> Новое отношение 'Вакансии' успешно сформированно и сохранено.")
    except:
        print(f">>> Проблема с формированием отношения 'Вакансии'. Завершение работы.")
        sys.exit(6)
        
        
    '''получение таблиц с кодами и названиями ОКПДТР/МРИГО из БД'''
    try:
        # используем сортировку по убыванию кодов для дальнейшего удобного сопастовления
        mrigo_query = """
        SELECT DISTINCT data.vf_btr_lines.id_mrigo, data.vf_btr_lines.mrigo
        FROM data.vf_btr_lines
        ORDER BY 1 DESC
        """
        id_mrigo_mrigo = db.get_table_from_query(mrigo_query)
        id_mrigo_mrigo.to_csv(os.path.join(
            'tables', 'id_mrigo_mrigo.csv'), index=None, header=True)
        print(f"\n> Таблица с кодами и наименованиям МРИГО успешно загружена.")

        okptdr_query = """
                    SELECT data.okpdtr.id, data.okpdtr.name
                    FROM data.okpdtr
                    ORDER BY 1
                    """
        id_okpdtr_okpdtr = db.get_table_from_query(okptdr_query)
        id_okpdtr_okpdtr.to_csv(os.path.join(
            'tables', 'id_okpdtr_okpdtr.csv'), index=None, header=True)
        print(f"> Таблица с кодами и наименованиям ОКПДТР успешно загружена.")
    except:
        print(f"> Нет доступа к БД в данный момент. Заверешение работы.")
        sys.exit(7)  

if __name__ == "__main__":
    main()
    print(f"> Программа успешно завершила свою работу.")

#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""выгрузка/обновление таблиц 'Компании' и дополненной таблицы 'Вакансии в БД'"""
import numpy as np
import os
import pandas as pd
import re
import sqlalchemy as sa

import misc.db as db

def main():
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
        print("Отношение 'Компании' уже содержится в БД. Добавление новых записей...")
        companies = pd.read_csv(
            os.path.join(
                'tables',
                'companies.csv'
                ),
            dtype=object
            )

        cond = companies['ogrn'].isin(companies_tv_old['ogrn'])
        companies_tv_diff = companies.drop(
            companies[cond].index,
            inplace=False
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
            print(f"Число новых компаний для обновления -- {companies_tv_diff.shape[0]}")
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
            print("Выгрузка новых данных в таблицу 'Компании' завершена.")
        else:
            print('Нет новых компаний для выгрузки в БД.')
    else:
        print("Отношение 'Компании' ранее не содержалось в БД. Создание таблицы и добавление новых записей, если они есть...")
        companies = pd.read_csv(
            os.path.join(
                'tables',
                'companies.csv'
                )
            )
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
        print("Создание таблицы 'Компании' и выгрузка новых записей завершена.")

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
        print("Отношение 'Вакансии' уже содержится в БД. Добавление новых записей, если они есть...")
        vacancies = pd.read_csv(
            os.path.join(
                'tables',
                'vacancies_updated.csv'
                ),
            dtype=object
            )

        cond = vacancies['ogrn'].isin(vacancies_tv_old['ogrn'])
        vacancies_tv_diff = vacancies.drop(
            vacancies[cond].index,
            inplace=False
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
            print(f"Число новых компаний для обновления -- {vacancies_tv_diff.shape[0]}")
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
                    'creation-date-from-api' : sa.DateTime,    # sa.DateTime,
                    'modify-modify-date-from-api' : sa.DateTime,      # sa.DateTime,
                    'download_time': sa.DateTime,
                    'is_closed': sa.Boolean,
                })
            print("Выгрузка новых данных в таблицу 'Вакансии' завершена.")
        else:
            print('Нет новых вакансий для выгрузки в БД.')
    else:
        print("Отношение 'Вакансии' ранее не содержалось в БД. Создание таблицы и добавление новых записей...")
        vacancies = pd.read_csv(os.path.join('tables', 'csv', 'vacancies_mrigo_okpdtr.csv'))
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
                'creation-date-from-api' : sa.DateTime,           # sa.DateTime,
                'modify-modify-date-from-api' : sa.DateTime,      # sa.DateTime,
                'download_time': sa.DateTime,
                'is_closed': sa.Boolean,
                
            })
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD PRIMARY KEY(id)')
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD CONSTRAINT vac_comp_f_key FOREIGN KEY (ogrn) REFERENCES blinov.companies_tv (ogrn)')
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD CONSTRAINT vac_mrigo_f_key FOREIGN KEY (id_mrigo) REFERENCES blinov.mrigo (id_mrigo)')
        db.engine.execute('ALTER TABLE blinov.vacancies_tv ADD CONSTRAINT vac_okpdtr_f_key FOREIGN KEY (id_okpdtr) REFERENCES blinov.okpdtr (id)')
        print("Создание таблицы 'Вакансии' и выгрузка новых записей завершена.")
    print("Выгрузка данных в БД завершена.")

if __name__ == "__main__":
    main()
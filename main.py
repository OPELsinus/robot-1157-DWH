import datetime
import os
import re
import shutil
import sys
from pathlib import Path

import openpyxl

from config import owa_username, owa_password, local_path, working_path, download_path, smtp_host, smtp_author, chat_id, bot_token

import psycopg2
import csv

from openpyxl import load_workbook
import time

import pandas as pd

from tools import update_credentials, send_message_by_smtp, send_message_to_tg


def sql_request(date):

    conn = psycopg2.connect(dbname='adb', host='172.16.10.22', port='5432',
                            user='rpa_robot', password='Qaz123123+')

    cur = conn.cursor()

    cur.execute(f"""select dpl.article as "Артикул", pg1.name_group_level5 as "Подгруппа", dpl.name_wares as "Наименование товара", dpl.extdesc28 as "Ценовой сегмент", ds.sale_obj_name as "Филиал", fwb.source_product_id as "Код товара", fwb.source_store_id as "Код филиала", sum(quantity_warehouse_acc) as "Учётные остатки", sum(quantity_warehouse) as "Фактические остатки", sum(quantity_free) as "Свободные остатки", sum(quantity_problem) as "Кол-во проблемных"
    from dwh_data.fact_wares_bal fwb
    left join dwh_data.dim_store ds on ds.source_store_id = fwb.source_store_id and current_date between ds.datestart and ds.dateend 
    left join dwh_data.dim_prod_group pg1 on pg1.source_prod_id = fwb.source_product_id and  fwb.rep_date between pg1.datestart and pg1.dateend
    left join dwh_data.dim_product_list dpl on dpl.code_wares = fwb.source_product_id and current_date between dpl.datestart and dpl.dateend 
    where fwb.rep_date = '{str(date)}' and pg1.code_group_level5 in ('1398', '1399', '1417', '1437', '3951', '1745', '1746', '1747', '1754')
    and ds.sale_obj_name = ds.sale_obj_name and dpl.extdesc28 = 'эконом'
    group by fwb.source_product_id, pg1.name_group_level5, dpl.name_wares, dpl.extdesc28, dpl.article, ds.sale_obj_name, fwb.source_store_id
    order by dpl.name_wares;
    """)

    rows = cur.fetchall()

    with open(os.path.join(working_path, 'all.csv'), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        for row in rows:
            writer.writerow(row)

    cur.close()
    conn.close()


def dividing_into_single_reports(working_path):

    try:
        os.makedirs(os.path.join(working_path, f'Splitted'))
    except:
        pass

    df = pd.read_csv(os.path.join(working_path, 'all.csv'), header=None)
    # df = df.drop(df.columns[11:], axis=1)
    df.columns = ["Артикул", "Подгруппа", "Наименование товара", "Ценовой сегмент", "Филиал", "Код товара", "Код филиала", "Учётные остатки", "Фактические остатки", "Свободные остатки", "Кол-во проблемных"]

    df1 = df[df['Наименование товара'].str.lower().str.contains('масло')].copy()
    for i in range(len(df1)):
        df1['Наименование товара'] = df1['Наименование товара'].str.replace(',', '.')

        multiplier = [float(s) for s in re.findall(r'[-+]?\d*\.\d+|\d+', df1['Наименование товара'].iloc[i])]

        if '0мл ' in df1['Наименование товара'].iloc[i].lower():
            df1['Фактические остатки'].iloc[i] *= 1
        else:
            if float(max(multiplier)) > 1:
                df1['Фактические остатки'].iloc[i] *= float(max(multiplier))

    for i in df1.index:
        df.loc[i, 'Фактические остатки'] = df1['Фактические остатки'].iloc[i - df1['Фактические остатки'].index[0]]
        
    print('Saving into files')

    for i in sorted(df['Филиал'].unique()):
        df1 = df.iloc[df[df['Филиал'] == i].index]

        df1.to_excel(os.path.join(working_path, f'Splitted\\{i}_{prev_date}.xlsx'), index=False)
        time.sleep(1)

        book = load_workbook(os.path.join(working_path, f'Splitted\\{i}_{prev_date}.xlsx'))

        worksheet = book.active

        column_widths = []
        for j, column in enumerate(df1.columns):
            column_widths.append(max(df1[column].astype(str).map(len).max(), len(column)) * 1.1)

        for j, width in enumerate(column_widths):
            worksheet.column_dimensions[worksheet.cell(row=1, column=j + 1).column_letter].width = width

        book.save(os.path.join(working_path, f'Splitted\\{i}_{prev_date}.xlsx'))


def archive_files(prev_date):

    folder_path = os.path.join(working_path, f'Splitted')

    try:
        os.makedirs(os.path.join(working_path, f'Splitted1'))
    except:
        pass
    destination_folder = os.path.join(working_path, f'Splitted1')

    zip_file_name = f'Все филиалы - 1157 за {prev_date}'
    zip_file_path = os.path.join(destination_folder, zip_file_name)

    shutil.make_archive(zip_file_path, 'zip', folder_path)

    return zip_file_path


if __name__ == '__main__':

    # print(working_path)

    update_credentials(Path(r'\\172.16.8.87\d'), owa_username, owa_password)

    df = pd.read_excel(r'\\172.16.8.87\d\Dauren\Производственный календарь 2023.xlsx')
    # curr_date = df['Day'].iloc[0]
    curr_date = datetime.datetime.now().strftime('%d.%m.%y')
    print(curr_date)
    day = int(curr_date.split('.')[0])
    month = int(curr_date.split('.')[1])
    year = int('20' + curr_date.split('.')[2])
    # print(datetime.date(year, month, day))
    prev_date = datetime.date(year, month, day)
    prev_date = (prev_date - datetime.timedelta(days=1))
    print(prev_date)

    sql_request(prev_date)

    print('Started dividing')

    dividing_into_single_reports(working_path)

    filepath = archive_files(prev_date)

    send_message_to_tg(bot_token=bot_token, message=f"Сегодня: {curr_date}\nОтрабатывал за: {prev_date.strftime('%d.%m.%y')}\nДата создания zip файла:\n{time.ctime(os.path.getctime(filepath + '.zip'))}", chat_id=chat_id)

    send_message_by_smtp(smtp_host, to=['Abdykarim.D@magnum.kz', 'Mukhtarova@magnum.kz', 'Narymbayeva@magnum.kz', 'KALDYBEK.B@magnum.kz', 'Sarieva@magnum.kz'], subject=f'Отчёт 1157 за {prev_date.strftime("%d.%m.%Y")}',
                         body='Результаты в приложении', username=smtp_author,
                         attachments=[filepath + '.zip'])

    Path(filepath + '.zip').unlink()

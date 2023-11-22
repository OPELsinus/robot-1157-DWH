import datetime
from time import sleep

import pandas as pd



































calendar = pd.read_excel(fr'\\vault.magnum.local\Common\Stuff\_05_Финансовый Департамент\01. Казначейство\Сверка\Сверка РОБОТ\Шаблоны для робота (не удалять)\Производственный календарь 2023.xlsx')

today = '27.03.23'

cur_day_index = calendar[calendar['Day'] == today]['Type'].index[0]
cur_day_type = calendar[calendar['Day'] == today]['Type'].iloc[0]

count = 0
day_ = None
found = False

for i in range(1, 31):

    try:
        day = int(calendar['Day'].iloc[cur_day_index + i].split('.')[0])
        print(calendar['Day'].iloc[cur_day_index + i], calendar['Weekday'].iloc[cur_day_index + i], calendar['Type'].iloc[cur_day_index + i])

    except:
        day = 1

    if day == 1:

        for j in range(1, 6):
            print(cur_day_index, i, j, cur_day_index + i - j)

            print('---', calendar['Day'].iloc[cur_day_index + i - j], calendar['Weekday'].iloc[cur_day_index + i - j], calendar['Type'].iloc[cur_day_index + i - j])

            if calendar['Type'].iloc[cur_day_index + i - j] == 'Working':
                count += 1
            if count == 2:
                found = True
                day_ = calendar['Day'].iloc[cur_day_index + i - j]
                break
    if found:
        break

print(cur_day_index, cur_day_type)

print(day_)

if today == day_: # * datetime.datetime.today().strftime('%d.%m.%Y') == day_:
    print('processing')
else:
    print('skipping')



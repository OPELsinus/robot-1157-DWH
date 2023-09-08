from shutil import make_archive

import os
import shutil


for file in os.listdir(r'\\172.16.8.87\d\Dauren\Отчёты 2т'):
    print(file.replace('_1.jpg', ''))
    shutil.move(fr'\\172.16.8.87\d\Dauren\Отчёты 2т\{file}', fr'\\172.16.8.87\d\Dauren\Отчёты 2т\{file.replace("_1", "")}')


import json
import logging
import os
import re
import shutil
import smtplib
import socket
import subprocess
import traceback
import urllib.parse
from contextlib import suppress
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from time import sleep
from typing import Union, List
from zipfile import ZipFile

import psutil
import pyautogui
import pyperclip
from win32api import GetUserNameEx, NameSamCompatible

from config import process_list_path

MONEY_FORMAT = '# ##0.00_-'


def dir_clear(path: Path, dirs=False):
    for path_ in list(path.iterdir()):
        if path_.is_file():
            path_.unlink()
        elif path_.is_dir() and dirs:
            path_.rmdir()


# ? tested
def send_message_by_smtp(*args, subject: str, url: str, to: Union[list, str], username: str, password: str = None,
                         html: str = None, attachments: List[Union[Path, str]] = None) -> None:
    body = ' '.join([str(i) for i in args])
    with smtplib.SMTP(url, 25) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        if password:
            smtp.login(username, password)

        msg = MIMEMultipart('alternative')
        msg["From"] = username
        msg["To"] = ';'.join(to) if type(to) is list else to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, 'plain'))

        if html:
            msg.attach(MIMEText(html, 'html'))

        if attachments and isinstance(attachments, list):
            for each in attachments:
                path = Path(each).resolve()
                with open(path.__str__(), 'rb') as f:
                    part = MIMEApplication(f.read(), Name=path.name)
                    part['Content-Disposition'] = 'attachment; filename="%s"' % path.name
                    msg.attach(part)

        smtp.send_message(msg=msg)


# ? tested
def net_use(resource: Union[Path, str], username: str, password: str, delete_all=False):
    if delete_all:
        command = f'net use * /delete /y'
        result = subprocess.run(command, shell=True, capture_output=True, encoding='cp866')
        print('delete', ' '.join(str(result.stdout).split(sep=None)))

    resource = str(resource)[:-1] if str(resource)[-1] == '\\' else str(resource)
    command = rf'net use "{resource}" /user:{username} {password}'.replace(r'\\\\', r'\\')
    result = subprocess.run(command, shell=True, capture_output=True, encoding='cp866')
    if len(result.stderr):
        print('net_use', resource, ' '.join(str(result.stdout).split(sep=None)))
    if len(result.stdout):
        print('net_use', resource, ' '.join(str(result.stdout).split(sep=None)))
    sleep(1)


# ? tested
def json_read(path: Union[Path, str]) -> Union[dict, list]:
    with open(str(path), 'r', encoding='utf-8') as fp:
        data = json.load(fp)
    return data


# ? tested
def json_write(path: Union[Path, str], data: Union[dict, list]) -> None:
    with open(str(path), 'w', encoding='utf-8') as fp:
        json.dump(data, fp, ensure_ascii=False)


# ? tested
def get_hostname() -> str:
    return socket.gethostbyname(socket.gethostname())


# ? tested
def get_username() -> str:
    return GetUserNameEx(NameSamCompatible)


# ? tested
def protect_path(value: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', value)


# ? tested
def protect_url(value: str) -> str:
    return urllib.parse.quote(value, safe='/:')


# ? tested
def check_file_downloaded(target: Union[Path, str], timeout: Union[int, float] = 60) -> Union[Path, None]:
    start_time = datetime.now()
    while True:
        target = Path(target)
        folder = target.parent
        files = folder.glob(target.name)
        for file_path in files:
            if not any(temp in str(file_path) for temp in ['.crdownload', '~$']):
                if file_path.is_file() and file_path.stat().st_size > 0:
                    return file_path
        if int((datetime.now() - start_time).seconds) > timeout:
            return None
        sleep(1)


# ? tested
def fix_excel_file_error(path: Union[Path, str]) -> Union[Path, None]:
    try:
        file_path = Path(path)
        tmp_folder = file_path.parent.joinpath('__temp__')
        with ZipFile(file_path.__str__()) as excel_container:
            excel_container.extractall(tmp_folder)
            excel_container.close()
        wrong_file_path = os.path.join(tmp_folder.__str__(), 'xl', 'SharedStrings.xml')
        correct_file_path = os.path.join(tmp_folder.__str__(), 'xl', 'sharedStrings.xml')
        os.rename(wrong_file_path, correct_file_path)
        file_path.unlink()
        shutil.make_archive(file_path.__str__(), 'zip', tmp_folder)
        os.rename(file_path.__str__() + '.zip', file_path.__str__())
        shutil.rmtree(tmp_folder.__str__(), ignore_errors=True)
    except Exception as e:
        traceback.print_exc()
        logging.warning(f"Error while trying to fix excel file: {e}")
        return None
    return file_path


# ? tested
def clipboard_set(value):
    pyperclip.copy(value)


# ? tested
def clipboard_get(raise_err=False, empty=False):
    result = pyperclip.paste()
    if not len(result):
        if raise_err:
            raise Exception('Clipboard is empty')
        else:
            return None
    if empty:
        clipboard_set('')
    return result


# ? tested
def hold_session() -> None:
    with suppress(Exception):
        pyautogui.press('volumedown')
        pyautogui.press('volumeup')


# ? tested
def make_screenshot(path: Union[Path, str]) -> None:
    pyautogui.screenshot(path.__str__())


# ? tested
def try_except_decorator(retry_cout=2, retry_delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(retry_cout):
                try:
                    result = func(*args, **kwargs)
                    return result
                except (Exception,):
                    traceback.print_exc()
                    sleep(retry_delay)
            raise Exception('retry_cout <= 0')

        return wrapper

    return decorator


# ? tested
def find_elements(timeout=30, **selector):
    from pywinauto.findwindows import find_elements
    from pywinauto.controls.uiawrapper import UIAWrapper
    from pywinauto.timings import wait_until_passes

    selector['top_level_only'] = selector['top_level_only'] if 'top_level_only' in selector else False

    def func():
        all_elements = find_elements(backend="uia", **selector)
        all_elements = [e for e in all_elements if e.control_type]
        all_elements = [UIAWrapper(e) for e in all_elements]
        if not len(all_elements):
            raise Exception('not found')
        return all_elements

    return wait_until_passes(timeout, 0.05, func)


# ? tested
def kill_exe(pid: int):
    process = psutil.Process(int(pid))
    root = psutil.Process(int(os.getppid()))
    if process.name() == root.name():
        return
    if process.is_running():
        children_ = process.children(recursive=True)
        for child_ in children_:
            if child_.is_running():
                child_.kill()
    if process.is_running():
        process.kill()


# ? tested
def kill_process_list():
    if process_list_path.is_file():
        with open(process_list_path.__str__(), 'r', encoding='utf-8') as pl_fp:
            process_list = json.load(pl_fp)
    else:
        process_list = list()

    username = get_username()
    for proc in psutil.process_iter():
        with suppress(Exception):
            proc_name = proc.name()
            if proc_name not in process_list:
                continue
            proc_username = proc.username()
            if proc_username != username:
                continue
            kill_exe(proc.pid)

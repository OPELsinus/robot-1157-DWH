import json
from contextlib import suppress
from pathlib import Path
from threading import Thread
from time import sleep

import psutil
from pyautogui import moveTo
from pywinauto.timings import wait_until_passes

from config import process_list_path, odines_username_rpa, odines_password_rpa, \
    logger_name, odines_username, odines_password
from rpamini import App, try_except_decorator
from tools import hold_session


class Odines(App):

    def __init__(self, timeout=60):
        super(Odines, self).__init__(Path(r'C:\Program Files\1cv8\common\1cestart.exe'), timeout=timeout,
                                     process_registry=process_list_path, logger=logger_name)
        self.fuckn_tooltip = {"class_name": "V8ConfirmationWindow", "control_type": "ToolTip", "visible_only": True,
                              "enabled_only": True, "found_index": 0}
        self.root_selector = {"title_re": "1С:Предприятие - Алматы центр / ТОО \"Magnum Cash&Carry\" "
                                          "/ Алматы  управление / .*", "class_name": "V8TopLevelFrame",
                              "control_type": "Window", "found_index": 0}
        hold_session()
        Thread(target=self.close_1c_config, daemon=True).start()

    def wait_fuckn_tooltip(self):
        with suppress(Exception):
            window = self.find_element(self.root_selector, use_window_element_info=False, parent=None)
            position = window.element.element_info.rectangle.mid_point()
            moveTo(position[0], position[1])
            self.wait_element(
                self.fuckn_tooltip, until=False, use_window_element_info=False, parent=None
            )

    # * ----------------------------------------------------------------------------------------------------------------
    @try_except_decorator
    def auth(self):
        self.run()
        self.switch({"title": "Запуск 1С:Предприятия", "class_name": "V8NewLocalFrameBaseWnd", "control_type": "Window",
                     "visible_only": True, "enabled_only": True, "found_index": 0})
        self.find_element({"title": "go_copy", "class_name": "", "control_type": "ListItem",
                           "visible_only": True, "enabled_only": True, "found_index": 0}).click(double=True)
        sleep(3)
        self.switch(
            {"title": "Доступ к информационной базе", "class_name": "V8NewLocalFrameBaseWnd", "control_type": "Window",
             "found_index": 0}, timeout=30)
        element_ = self.find_element(
            {"title": "", "class_name": "", "control_type": "ComboBox", "visible_only": True, "enabled_only": True,
             "found_index": 0})
        element_.click()
        element_.type_keys(odines_username, self.keys.TAB, clear=True)
        self.find_element(
            {"title": "", "class_name": "", "control_type": "Edit", "visible_only": True, "enabled_only": True,
             "found_index": 0}).set_text(odines_password)  # odines_password

        self.find_element(
            {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True, "enabled_only": True,
             "found_index": 0}).click()

        sleep(1)
        try:
            # Конфигурация БД не соответствует сохр конфигу
            self.find_element(
                {"title": "Да", "class_name": "", "control_type": "Button", "visible_only": True, "enabled_only": True,
                 "found_index": 0}).click()
        except:
            ...
        self.switch(self.root_selector, timeout=180)

        self.close_all_windows(10, 1)

    @try_except_decorator
    def quit(self):
        # * подключиться к окну если есть
        with suppress(Exception):
            self.switch(self.root_selector, timeout=0.1)

        # * закрыть окна
        with suppress(Exception):
            if self.window_element_info:
                self.close_all_windows(10, 1, True)
                self.open('Файл', 'Выход')
                if self.wait_element(
                        {"title": "Завершить работу с программой?", "class_name": "", "control_type": "Pane",
                         "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=5
                ):
                    self.find_element(
                        {"title": "Да", "class_name": "", "control_type": "Button", "visible_only": True,
                         "enabled_only": True, "found_index": 0}, timeout=1
                    ).click()
                    self.wait_element(
                        {"title": "Да", "class_name": "", "control_type": "Button", "visible_only": True,
                         "enabled_only": True, "found_index": 0}, timeout=5, until=False
                    )

        # * убийства )
        with suppress(Exception):
            def kill(parent_):
                if parent_.is_running():
                    children_ = parent_.children(recursive=True)
                    for child_ in children_:
                        if child_.is_running():
                            child_.kill()
                if parent_.is_running():
                    parent_.kill()

            for process in self.process_list:
                wait_until_passes(5, 0.05, kill, Exception, process)
            if self.process_registry:
                with open(str(self.process_registry), 'r', encoding='utf-8') as fp:
                    data = json.load(fp)
                for proc in psutil.process_iter():
                    if proc.name() in data:
                        wait_until_passes(5, 0.05, kill, Exception, proc)

    # * ----------------------------------------------------------------------------------------------------------------
    def open(self, *steps):
        sleep(1)
        self.wait_fuckn_tooltip()
        for n, step in enumerate(steps):
            if n:
                if not self.wait_element(
                        {"title": step, "class_name": "", "control_type": "MenuItem", "visible_only": True,
                         "enabled_only": True, "found_index": 0}, timeout=2
                ):
                    if n - 1:
                        self.find_element(
                            {"title": steps[n - 1], "class_name": "", "control_type": "MenuItem", "visible_only": True,
                             "enabled_only": True, "found_index": 0}
                        ).click()
                    else:
                        self.find_element(
                            {"title": steps[n - 1], "class_name": "", "control_type": "Button", "visible_only": True,
                             "enabled_only": True, "found_index": 0}
                        ).click()
                self.find_element(
                    {"title": step, "class_name": "", "control_type": "MenuItem", "visible_only": True,
                     "enabled_only": True, "found_index": 0}
                ).click()
            else:
                self.find_element(
                    {"title": step, "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}
                ).click()
        self.maximize_inner_window()

    def maximize_inner_window(self, timeout=0.1):
        if self.wait_element({"title": "Развернуть", "class_name": "", "control_type": "Button", "visible_only": True,
                              "enabled_only": True, "found_index": 0}, timeout=timeout):
            self.find_element({"title": "Развернуть", "class_name": "", "control_type": "Button", "visible_only": True,
                               "enabled_only": True, "found_index": 0}).click()

    def check_1c_error(self, count=1):
        while count > 0:
            count -= 1
            # * Ошибка при вызове метода контекста ---------------------------------------------------------------------
            if self.wait_element(
                    {"title": "Ошибка при вызове метода контекста (Выполнить)", "class_name": "",
                     "control_type": "Pane", "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "Ошибка при вызове метода контекста"
                raise Exception(error_message)

            # * Ошибка исполнения отчета -------------------------------------------------------------------------------
            if self.wait_element(
                    {"title": "Ошибка исполнения отчета", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "Ошибка исполнения отчета"
                raise Exception(error_message)

            # * Операция не выполнена ----------------------------------------------------------------------------------
            if self.wait_element(
                    {"title": "Операция не выполнена", "class_name": "", "control_type": "Pane", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "Операция не выполнена"
                raise Exception(error_message)

            # * Конфликт блокировок при выполнении транзакции ----------------------------------------------------------
            if self.wait_element(
                    {"title_re": "Конфликт блокировок при выполнении транзакции:.*", "class_name": "",
                     "control_type": "Pane", "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "Конфликт блокировок при выполнении транзакции"
                raise Exception(error_message)

            # * Введенные данные не отображены в списке, так как не соответствуют отбору -------------------------------
            if self.wait_element(
                    {"title": "Введенные данные не отображены в списке, так как не соответствуют отбору.",
                     "class_name": "", "control_type": "Pane", "visible_only": True, "enabled_only": True,
                     "found_index": 0}, timeout=0.2
            ):
                error_message = "Введенные данные не отображены в списке, так как не соответствуют отбору"
                raise Exception(error_message)

            # * critical В поле введены некорректные данные ------------------------------------------------------------
            if self.wait_element(
                    {"title_re": "В поле введены некорректные данные.*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "critical В поле введены некорректные данные"
                raise Exception(error_message)

            # * critical Не удалось провести ---------------------------------------------------------------------------
            if self.wait_element(
                    {"title_re": "Не удалось провести.*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "critical Не удалось провести"
                raise Exception(error_message)

            # * Сеанс работы завершен администратором ------------------------------------------------------------------
            if self.wait_element(
                    {"title": "Сеанс работы завершен администратором.", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "critical Сеанс работы завершен администратором"
                raise Exception(error_message)

            # * Сеанс отсутствует или удален ---------------------------------------------------------------------------
            if self.wait_element(
                    {"title_re": "Сеанс отсутствует или удален.*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "critical Сеанс отсутствует или удален"
                raise Exception(error_message)

            # * Неизвестное окно ошибки ---------------------------------------------------------------------------
            if self.wait_element(
                    {"title": "1С:Предприятие", "class_name": "V8NewLocalFrameBaseWnd", "control_type": "Window",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0.2
            ):
                error_message = "critical Неизвестное окно ошибки"
                raise Exception(error_message)

    def close_1c_error(self):
        # * Ошибка исполнения отчета -----------------------------------------------------------------------------------
        selector_ = {"title": "Ошибка исполнения отчета", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * Ошибка при вызове метода контекста -------------------------------------------------------------------------
        selector_ = {"title": "Ошибка при вызове метода контекста (Выполнить)", "class_name": "",
                     "control_type": "Pane", "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * Завершить работу с программой? -----------------------------------------------------------------------------
        selector_ = {"title": "Завершить работу с программой?", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "Да", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * Операция не выполнена --------------------------------------------------------------------------------------
        selector_ = {"title": "Операция не выполнена", "class_name": "", "control_type": "Pane", "visible_only": True,
                     "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * Конфликт блокировок при выполнении транзакции --------------------------------------------------------------
        selector_ = {"title_re": "Конфликт блокировок при выполнении транзакции:.*", "class_name": "",
                     "control_type": "Pane", "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * Введенные данные не отображены в списке, так как не соответствуют отбору -----------------------------------
        selector_ = {"title": "Введенные данные не отображены в списке, так как не соответствуют отбору.",
                     "class_name": "", "control_type": "Pane", "visible_only": True, "enabled_only": True,
                     "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * Данные были изменены. Сохранить изменения? -----------------------------------------------------------------
        selector_ = {"title": "Данные были изменены. Сохранить изменения?", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "Нет", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return False

        # * critical В поле введены некорректные данные ----------------------------------------------------------------
        selector_ = {"title_re": "В поле введены некорректные данные.*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "Да", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return True

        # * critical Не удалось провести -------------------------------------------------------------------------------
        selector_ = {"title_re": "Не удалось провести \".*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "OK", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return True

        # * Сеанс работы завершен администратором ----------------------------------------------------------------------
        selector_ = {"title": "Сеанс работы завершен администратором.", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "Завершить работу", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return True

        # * Сеанс отсутствует или удален -------------------------------------------------------------------------------
        selector_ = {"title_re": "Сеанс отсутствует или удален.*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}
        if self.wait_element(selector_, timeout=0.1):
            with suppress(Exception):
                self.find_element(
                    {"title": "Завершить работу", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=1
                ).click(double=True)
            self.wait_element(selector_, timeout=5, until=False)
            return True

    def close_1c_config(self):
        while True:
            with suppress(Exception):
                self.find_element(
                    {"title_re": "В конфигурацию ИБ внесены изменения.*", "class_name": "", "control_type": "Pane",
                     "visible_only": True, "enabled_only": True, "found_index": 0}, timeout=0, log=False
                )
                self.find_element(
                    {"title": "Нет", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True, "found_index": 0}, timeout=0, log=False
                ).click(log=False)

    def close_all_windows(self, count=10, idx=1, ext=False):
        if ext:
            with suppress(Exception):
                # * закрыть всплывашку
                self.close_1c_error()
                self.open('Окна', 'Закрыть все')
        while True:
            if len(self.find_elements(
                    {"title": "Закрыть", "class_name": "", "control_type": "Button", "visible_only": True,
                     "enabled_only": True}, timeout=1
            )) > idx:
                # * закрыть всплывашку
                self.close_1c_error()
                # * закрыть
                with suppress(Exception):
                    self.find_element(
                        {"title": "Закрыть", "class_name": "", "control_type": "Button", "visible_only": True,
                         "enabled_only": True, "found_index": idx}, timeout=1
                    ).click()
                # * закрыть всплывашку
                self.close_1c_error()
            else:
                break
            # ! выход
            count -= 1
            if count <= 0:
                raise Exception('Не все окна закрыты')
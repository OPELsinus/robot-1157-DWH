import ctypes
import json
import os
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import Union, List

import psutil
from pywinauto import win32functions
from pywinauto.controls.uiawrapper import UIAWrapper
from pywinauto.timings import wait_until_passes, wait_until
from selenium import webdriver
from selenium.webdriver import ChromeOptions, ActionChains, Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from win32api import GetMonitorInfo, MonitorFromPoint

from config import process_list_path
from tools import find_elements, kill_process_list
if ctypes.windll.user32.GetKeyboardLayout(0) != 67699721:
    __err__ = 'Смените раскладку на ENG'
    raise Exception(__err__)


# ? tested
class AppKeys:
    def __init__(self):
        pass

    CANCEL = '{VK_CANCEL}'  # ^break
    HELP = '{VK_HELP}'
    BACKSPACE = '{BACKSPACE}'
    BACK_SPACE = BACKSPACE
    TAB = '{VK_TAB}'
    CLEAR = '{VK_CLEAR}'
    RETURN = '{VK_RETURN}'
    ENTER = '{ENTER}'
    SHIFT = '{VK_LSHIFT}'
    LEFT_SHIFT = SHIFT
    CONTROL = '{VK_CONTROL}'
    LEFT_CONTROL = CONTROL
    ALT = '{VK_MENU}'
    LEFT_ALT = ALT
    PAUSE = '{VK_PAUSE}'
    ESCAPE = '{VK_ESCAPE}'
    SPACE = '{VK_SPACE}'
    PAGE_UP = '{PGUP}'
    PAGE_DOWN = '{PGDN}'
    END = '{VK_END}'
    HOME = '{VK_HOME}'
    LEFT = '{VK_LEFT}'
    ARROW_LEFT = LEFT
    UP = '{VK_UP}'
    ARROW_UP = UP
    RIGHT = '{VK_RIGHT}'
    ARROW_RIGHT = RIGHT
    DOWN = '{VK_DOWN}'
    ARROW_DOWN = DOWN
    INSERT = '{VK_INSERT}'
    DELETE = '{VK_DELETE}'

    NUMPAD0 = '{VK_NUMPAD0}'  # number pad keys
    NUMPAD1 = '{VK_NUMPAD1}'
    NUMPAD2 = '{VK_NUMPAD2}'
    NUMPAD3 = '{VK_NUMPAD3}'
    NUMPAD4 = '{VK_NUMPAD4}'
    NUMPAD5 = '{VK_NUMPAD5}'
    NUMPAD6 = '{VK_NUMPAD6}'
    NUMPAD7 = '{VK_NUMPAD7}'
    NUMPAD8 = '{VK_NUMPAD8}'
    NUMPAD9 = '{VK_NUMPAD9}'
    MULTIPLY = '{VK_MULTIPLY}'
    ADD = '{VK_ADD}'
    SEPARATOR = '{VK_SEPARATOR}'
    SUBTRACT = '{VK_SUBTRACT}'
    DECIMAL = '{VK_DECIMAL}'
    DIVIDE = '{VK_DIVIDE}'

    F1 = '{VK_F1}'  # function  keys
    F2 = '{VK_F2}'
    F3 = '{VK_F3}'
    F4 = '{VK_F4}'
    F5 = '{VK_F5}'
    F6 = '{VK_F6}'
    F7 = '{VK_F7}'
    F8 = '{VK_F8}'
    F9 = '{VK_F9}'
    F10 = '{VK_F10}'
    F11 = '{VK_F11}'
    F12 = '{VK_F12}'
    COMMAND = CONTROL

    CLEAN = '{VK_HOME}+{VK_END}{VK_DELETE}{VK_HOME}'


class App:
    keys = AppKeys

    class Element:
        keys = AppKeys

        def __init__(self, element: UIAWrapper, debug=False, logger=None):
            self.element = element
            self.debug = debug
            self.logger = logger

        # ? tested
        def __repr__(self):
            return self.element.__repr__()

        # ? tested
        def parent(self, count=1):
            element = self.element
            for i in range(count):
                if element.parent():
                    element_ = element.parent()
                    try_count = 10
                    while try_count > 0:
                        if element_.element_info.control_type is not None:
                            break
                        sleep(0.5)
                        element_ = element.parent()
                    if try_count <= 0:
                        raise RobotException('Parent is None', 'self.parent')
                    element = element_
                else:
                    break
                sleep(0.1)
            return App.Element(element, debug=self.debug)

        # ? tested
        def draw_outline(self) -> None:
            return self.element.draw_outline()

        # ? tested
        def close(self) -> None:
            return self.element.close()

        # ? tested
        def set_focus(self) -> None:
            return self.element.set_focus()

        # ? tested
        def click(self, coords=(None, None), double=False, right=False, set_focus=False) -> None:
            if set_focus:
                self.element.set_focus()
            if not right:
                self.element.click_input(double=double, coords=coords)
            else:
                self.element.right_click_input(coords=coords)

        # ? tested
        def select(self, item: Union[int, str], set_focus=False) -> None:
            if set_focus:
                self.element.set_focus()
            from pywinauto.controls.uia_controls import ComboBoxWrapper
            if isinstance(self.element, ComboBoxWrapper):
                self.element.select(item)
            else:
                raise Exception('Element is not instance of ComboBoxWrapper')

        # ? tested
        def get_text(self, attr='value', set_focus=False) -> str:
            if set_focus:
                self.element.set_focus()
            if attr == 'text':
                return str(' '.join(self.element.texts()))
            elif attr == 'value':
                return str(self.element.get_value())

        # ? tested
        def set_text(self, value=None, click=False, set_focus=False) -> None:
            if set_focus:
                self.element.set_focus()
            if click:
                self.element.click_input()
            return self.element.set_edit_text(value)

        # ? tested
        def type_keys(self, *value, click=False, clear=False, protect_first=False, set_focus=False) -> None:
            def replace(string):
                replace_list = ['(', ')', '{', '}', '^', '%', '+']
                string = ''.join([c if c not in replace_list else '{' + c + '}' for c in string])
                return string

            if click:
                self.element.click_input()
            if clear:
                self.element.type_keys(self.keys.CLEAR, set_foreground=set_focus)
            if protect_first:
                keys = ''.join(str(v) if n else replace(str(v)) for n, v in enumerate(value))
            else:
                keys = ''.join(str(v) for v in value)
            self.element.type_keys(keys, pause=0.03, with_spaces=True, with_tabs=True, with_newlines=True,
                                   set_foreground=set_focus)

        # TODO TEST
        def find_elements(self, selector, timeout: Union[int, float] = 60):
            selector['parent'] = self.element
            try:
                elements = find_elements(**selector, timeout=timeout)
            except (Exception,):
                elements = list()
            if not len(elements):
                raise Exception('Elements not found')
            if self.debug:
                for element in elements:
                    element.draw_outline()
            return [App.Element(element, debug=self.debug, logger=self.logger) for element in elements]

        # TODO TEST
        def find_element(self, selector, timeout: Union[int, float] = 60):
            selector['parent'] = self.element
            try:
                elements = find_elements(**selector, timeout=timeout)
            except (Exception,):
                elements = list()
            if not len(elements):
                raise Exception('Element not found')
            element = elements[0]
            if self.debug:
                element.draw_outline()
            return App.Element(element, debug=self.debug, logger=self.logger)

        # TODO TEST
        def wait_element(self, selector, timeout: Union[int, float] = 60, until=True, raise_if_false=False) -> bool:
            selector['parent'] = self.element

            def function():
                try:
                    elements = find_elements(**selector, timeout=0)
                    if len(elements) > 0:
                        flag = True
                        if self.debug:
                            elements[0].draw_outline()
                    else:
                        flag = False
                except (Exception,):
                    flag = False

                if flag != until:
                    raise Exception(f'Element not {"appeared" if until else "disappeared"}')

            try:
                wait_until_passes(timeout, 0.1, function)
                result = True
            except (Exception,):
                result = False

            if raise_if_false and not result:
                raise RobotException(f'{selector} not disappeared', 'wait_element')
            return result

    def __init__(self, path: Union[str, Path], timeout: Union[int, float] = 60, debug=False, logger=None):
        self.path = path
        self.timeout = timeout
        self.debug = debug
        self.logger = logger

        # noinspection PyTypeChecker
        _root: App.Element = None
        self._stack = {0: _root}
        self._highest_len = 1
        self._current_index = 0

    # ------------------------------------------------------------------------------------------------------------------
    # ? tested
    def run(self) -> None:
        self.quit()
        os.system(f'start "" "{self.path.__str__()}"')

    # tested
    @classmethod
    def quit(cls) -> None:
        kill_process_list()
        sleep(3)

    # ------------------------------------------------------------------------------------------------------------------
    # ? tested
    @property
    def root(self) -> Element:
        return self._stack[0]

    # ? tested
    @root.setter
    def root(self, root_window: Element) -> None:
        self._stack[0] = root_window

    # ------------------------------------------------------------------------------------------------------------------
    # ? tested
    @property
    def parent(self) -> Element:
        return self._stack[self._current_index]

    # ? tested
    @parent.setter
    def parent(self, window: Element) -> None:
        self._stack[self._current_index] = window

    # ? tested
    def _parent_switch_actions(self, set_focus, maximize, resize) -> None:
        target = self._stack[self._current_index]
        if set_focus:
            with suppress(Exception):
                target.element.set_focus()
        if maximize:
            with suppress(Exception):
                target.element.maximize()
        if resize:
            with suppress(Exception):
                r = GetMonitorInfo(MonitorFromPoint((0, 0))).get("Work")
                h = target.element.element_info.handle
                win32functions.MoveWindow(h, r[0], r[1], r[2], r[3] - 50, True)

    # ? tested
    def _parent_switch_serialize_process_list(self) -> None:
        target = self._stack[self._current_index]
        process = psutil.Process(target.element.element_info.process_id).name()
        if process_list_path.is_file():
            with open(process_list_path.__str__(), 'r', encoding='utf-8') as pl_fp:
                process_list = json.load(pl_fp)
        else:
            process_list = list()
        if process not in process_list:
            process_list.append(process)
            with open(process_list_path.__str__(), 'w+', encoding='utf-8') as pl_fp:
                json.dump(process_list, pl_fp, ensure_ascii=False)

    # ? tested
    def parent_switch(self, target: Union[Element, dict], timeout=None, set_focus=False, maximize=False,
                      resize=False) -> Element:
        timeout = timeout if timeout is not None else self.timeout
        # * target setting
        if isinstance(target, App.Element):
            pass
        elif isinstance(target, dict):
            if 'parent' not in target:
                target['parent'] = None
            target = self.find_element(target, timeout=timeout)
        else:
            raise Exception(f'{type(target)} is not supported')

        # * navigation
        self._stack[self._current_index + 1] = target
        self._current_index += 1
        self._highest_len = self._current_index + 1
        for i in [k for k in self._stack.keys() if k > self._current_index]:
            del self._stack[i]

        # * actions
        self._parent_switch_actions(set_focus, maximize, resize)
        self._parent_switch_serialize_process_list()

        target = self._stack[self._current_index]
        if self.debug:
            target.element.draw_outline()
        if self.logger:
            self.logger.info('-->', target)
        return target

    # ? tested
    def parent_back(self, steps: int, set_focus=False, maximize=False, resize=False) -> Element:
        index_to_visit = max(0, self._current_index - steps)
        self._current_index = index_to_visit

        # * actions
        self._parent_switch_actions(set_focus, maximize, resize)
        target = self._stack[self._current_index]
        if self.debug:
            target.element.draw_outline()
        if self.logger:
            self.logger.info('<--', target)
        return target

    # ? tested
    def parent_forward(self, steps: int, set_focus=False, maximize=False, resize=False) -> Element:
        index_to_visit = min(self._highest_len - 1, self._current_index + steps)
        self._current_index = index_to_visit

        # * actions
        self._parent_switch_actions(set_focus, maximize, resize)
        target = self._stack[self._current_index]
        if self.debug:
            target.element.draw_outline()
        if self.logger:
            self.logger.info('-->', target)
        return target

    # ------------------------------------------------------------------------------------------------------------------
    # ? tested
    def find_elements(self, selector, timeout: Union[int, float] = None) -> List[Element]:
        timeout = timeout if timeout is not None else self.timeout
        if 'parent' not in selector:
            selector['parent'] = self.parent
        if isinstance(selector['parent'], App.Element):
            selector['parent'] = selector['parent'].element
        try:
            elements = find_elements(**selector, timeout=timeout)
        except (Exception,):
            elements = list()
        if not len(elements):
            raise Exception('Elements not found')
        if self.debug:
            for element in elements:
                element.draw_outline()
        return [self.Element(element, debug=self.debug, logger=self.logger) for element in elements]

    # ? tested
    def find_element(self, selector, timeout: Union[int, float] = None) -> Element:
        timeout = timeout if timeout is not None else self.timeout
        if 'parent' not in selector:
            selector['parent'] = self.parent
        if isinstance(selector['parent'], App.Element):
            selector['parent'] = selector['parent'].element
        try:
            elements = find_elements(**selector, timeout=timeout)
        except (Exception,):
            elements = list()
        if not len(elements):
            raise Exception('Element not found')
        element = elements[0]
        if self.debug:
            element.draw_outline()
        return self.Element(element, debug=self.debug, logger=self.logger)

    # ? tested
    def wait_element(self, selector, timeout: Union[int, float] = None, until=True, raise_if_false=False) -> bool:
        timeout = timeout if timeout is not None else self.timeout
        if 'parent' not in selector:
            selector['parent'] = self.parent
        if isinstance(selector['parent'], App.Element):
            selector['parent'] = selector['parent'].element

        def function():
            try:
                elements = find_elements(**selector, timeout=0)
                if len(elements) > 0:
                    flag = True
                    if self.debug:
                        elements[0].draw_outline()
                else:
                    flag = False
            except (Exception,):
                flag = False

            if flag != until:
                raise Exception(f'Element not {"appeared" if until else "disappeared"}')

        try:
            wait_until_passes(timeout, 0.1, function)
            result = True
        except (Exception,):
            result = False

        if raise_if_false and not result:
            raise RobotException(f'{selector} not disappeared', 'wait_element')
        return result


# ? tested
class Web:
    keys = Keys

    # ? tested
    class Element:
        keys = Keys

        def __init__(self, element, selector, by, driver):
            self.element: WebElement = element
            self.selector = selector
            self.by = by
            self.driver: WebDriver = driver

        def page_load(self, url, timeout=60):
            def body():
                return url != self.driver.current_url

            wait_until(timeout, 0.5, body)

        # ? tested
        def scroll(self, delay=0):
            sleep(delay)
            ActionChains(self.driver).move_to_element(self.element).perform()

        # ? tested
        def clear(self, delay=0):
            sleep(delay)
            self.element.clear()

        # ? tested
        def click(self, double=False, delay=0, scroll=False, page_load=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            url = self.driver.current_url
            ActionChains(self.driver).double_click(self.element).perform() if double else self.element.click()
            if page_load:
                self.page_load(url)

        # ? tested
        def get_attr(self, attr='text', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            return getattr(self.element, attr) if attr in ['tag_name', 'text'] else self.element.get_attribute(attr)

        # ? tested
        def set_attr(self, value=None, attr='value', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            self.driver.execute_script(f"arguments[0].{attr} = arguments[1]", self.element, value)

        # ? tested
        def type_keys(self, *value, delay=0, scroll=False, clear=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            if clear:
                self.clear()
            self.element.send_keys(*value)

        # ? tested
        def select(self, value=None, select_type='select_by_value', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            select = Select(self.element)
            function = getattr(select, select_type)
            if value is None:
                if select_type == 'deselect_all':
                    return function()
                else:
                    return select
            else:
                return function(value)

        # TODO TEST
        def find_elements(self, selector, timeout=60, by='xpath'):
            selector = f'.{selector}' if selector[0] != '.' else selector
            if timeout:
                self.wait_element(selector, timeout, by)
            elements = self.element.find_elements(by, selector)
            selector = f'{self.selector}{selector[1:]}'
            elements = [Web.Element(element=element, selector=selector, by=by, driver=self.driver) for element in
                        elements]
            return elements

        # TODO TEST
        def find_element(self, selector, timeout=60, by='xpath'):
            selector = f'.{selector}' if selector[0] != '.' else selector
            if timeout:
                self.wait_element(selector, timeout, by)
            element = self.element.find_element(by, selector)
            selector = f'{self.selector}{selector[1:]}'
            element = Web.Element(element=element, selector=selector, by=by, driver=self.driver)
            return element

        # TODO TEST
        def wait_element(self, selector, timeout=60, by='xpath', until=True):
            selector = f'.{selector}' if selector[0] != '.' else selector

            def find():
                try:
                    self.element.find_element(by, selector)
                    return True
                except (Exception,):
                    return False

            try:
                return wait_until(timeout, 0.5, find, until)
            except (Exception,):
                return False

    def __init__(self, path=None, download_path=None, run=False, timeout=60):
        self.path = path or Path.home().joinpath(r"AppData\Local\.rpa\Chromium\chromedriver.exe")
        self.download_path = download_path or Path.home().joinpath('Downloads')
        self.run_flag = run
        self.timeout = timeout

        self.options = ChromeOptions()
        self.options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        self.options.add_experimental_option("useAutomationExtension", False)
        self.options.add_experimental_option("prefs", {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_settings.popups": 0,
            "download.default_directory": self.download_path.__str__(),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.options.add_argument("--start-maximized")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-print-preview")
        self.options.add_argument("--disable-extensions")
        self.options.add_argument("--disable-notifications")
        self.options.add_argument("--ignore-ssl-errors=yes")
        self.options.add_argument("--ignore-certificate-errors")

        # noinspection PyTypeChecker
        self.driver: WebDriver = None

    # ? tested
    def run(self):
        self.quit()
        self.driver = webdriver.Chrome(service=Service(self.path.__str__()), options=self.options)

    # ? tested
    def quit(self):
        if self.driver:
            self.driver.quit()

    # ? tested
    def close(self):
        self.driver.close()

    # ? tested
    def switch(self, switch_type='window', switch_index=-1, frame_selector=None):
        if switch_type == 'window':
            self.driver.switch_to.window(self.driver.window_handles[switch_index])
        elif switch_type == 'frame':
            if frame_selector:
                self.driver.switch_to.frame(self.find_elements(frame_selector)[switch_index].element)
            else:
                raise Exception('selected type is "frame", but didnt received frame_selector')
        elif switch_type == 'alert':
            self.driver.switch_to.alert.accept()
        raise Exception(f'switch_type "{switch_type}" didnt found')

    # ? tested
    def get(self, url):
        self.driver.get(url)

    # ? tested
    def find_elements(self, selector, timeout=None, event=None, by='xpath'):
        if event is None:
            event = expected_conditions.presence_of_element_located
        timeout = timeout if timeout is not None else self.timeout
        if timeout:
            self.wait_element(selector, timeout, event, by)
        elements = self.driver.find_elements(by, selector)
        elements = [self.Element(element=element, selector=selector, by=by, driver=self.driver) for element in elements]
        return elements

    # ? tested
    def find_element(self, selector, timeout=None, event=None, by='xpath'):
        if event is None:
            event = expected_conditions.presence_of_element_located
        timeout = timeout if timeout is not None else self.timeout
        if timeout:
            self.wait_element(selector, timeout, event, by)
        element = self.driver.find_element(by, selector)
        element = self.Element(element=element, selector=selector, by=by, driver=self.driver)
        return element

    # ? tested
    def wait_element(self, selector, timeout=None, event=None, by='xpath', until=True):
        if event is None:
            event = expected_conditions.presence_of_element_located
        try:
            timeout = timeout if timeout is not None else self.timeout
            wait = WebDriverWait(self.driver, timeout)
            event = event((by, selector))
            wait.until(event) if until else wait.until_not(event)
            return True
        except (Exception,):
            return False


# ? tested
class BusinessException(Exception):
    """Exception raised for business rule violations."""

    def __init__(self, message, function_name, data=None):
        self.message = message
        self.function_name = function_name
        self.data = data


# ? tested
class ApplicationException(Exception):
    """Exception raised for application errors."""

    def __init__(self, message, function_name, data=None):
        self.message = message
        self.function_name = function_name
        self.data = data


# ? tested
class RobotException(Exception):
    """Unexpected exceptions."""

    def __init__(self, message, function_name, data=None):
        self.message = message
        self.function_name = function_name
        self.data = data

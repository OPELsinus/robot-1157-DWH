import ctypes
import inspect
import json
import logging
import os
from contextlib import suppress
from pathlib import Path
from time import sleep

import psutil
from pywinauto import win32functions
from pywinauto.controls.uiawrapper import UIAWrapper
from pywinauto.timings import wait_until_passes
from pywinauto.win32structures import RECT
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait


def try_except_decorator(func):
    def wrapper(*args, **kwargs):
        logger = args[0].logger if getattr(args[0], 'logger') else logging.getLogger()
        if 'log' in kwargs.keys():
            log = kwargs['log']
            del kwargs['log']
        else:
            log = True
        if 'skip_error' in kwargs.keys():
            skip_error = kwargs['skip_error']
            del kwargs['skip_error']
        else:
            skip_error = False
        skip_error_str = f'skip_error={skip_error}'
        stack = inspect.stack()[1]
        file_name = Path(stack.filename).name
        line_num = stack.lineno
        code_context = [", ".join([str(i) for i in args[1:]]), str(kwargs)[1:-1]]
        code_context = f'{func.__name__}({", ".join([i for i in code_context if i != ""])})'
        result = None
        try:
            result = func(*args, **kwargs)
            if log:
                logger.debug(f'success||{skip_error_str}||{file_name}->line {str(line_num)}->{code_context}')
            return result
        except Exception as e:
            sttr = f'failed||{skip_error_str}||{file_name}->line {str(line_num)}->{code_context} - {str(e)}'
            if log:
                logger.debug(sttr)
            if not skip_error:
                raise e
        return result

    return wrapper


def find_elements(timeout=30, **selector):
    from pywinauto.findwindows import find_elements
    from pywinauto.controls.uiawrapper import UIAWrapper
    from pywinauto.timings import wait_until_passes

    selector['top_level_only'] = selector['top_level_only'] if 'top_level_only' in selector else False

    def func():
        all_elements = find_elements(backend="uia", **selector)
        all_elements = [UIAWrapper(e) for e in all_elements]
        if not len(all_elements):
            raise Exception('not found')
        return all_elements

    return wait_until_passes(timeout, 0.05, func)


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


class App:
    keys = AppKeys

    class Element:
        keys = AppKeys

        def __init__(self, element, logger=None):
            self.element: UIAWrapper = element
            self.logger = logger
            self.keys.CLEAR = '{VK_HOME}+{VK_END}{VK_DELETE}{VK_HOME}'
            if not logger:
                basic_format = '%(asctime)s||%(levelname)s||%(message)s'
                date_format = '%Y-%m-%d,%H:%M:%S'
                logging.basicConfig(level=logging.DEBUG, format=basic_format, datefmt=date_format)

        @try_except_decorator
        def click(self, double=False, right=False, coords=(None, None), delay=0, set_focus=False):
            sleep(delay)
            if set_focus:
                self.element.set_focus()
            if not right:
                self.element.click_input(double=double, coords=coords)
            else:
                self.element.right_click_input(coords=coords)

        @try_except_decorator
        def select(self, item: [int, str], delay=0, set_focus=False):
            sleep(delay)
            if set_focus:
                self.element.set_focus()
            from pywinauto.controls.uia_controls import ComboBoxWrapper
            if isinstance(self.element, ComboBoxWrapper):
                self.element.select(item)
            else:
                raise Exception('Element is not instance of ComboBoxWrapper')

        @try_except_decorator
        def get_text(self, attr='value', delay=0, set_focus=False):
            sleep(delay)
            if set_focus:
                self.element.set_focus()
            from pywinauto.controls.uia_controls import EditWrapper
            if isinstance(self.element, EditWrapper):
                if attr == 'text':
                    return str(' '.join(self.element.texts()))
                elif attr == 'value':
                    return str(self.element.get_value())
            else:
                raise Exception('Element is not instance of EditWrapper')

        @try_except_decorator
        def set_text(self, value=None, delay=0, set_focus=False, click=False):
            sleep(delay)
            if set_focus:
                self.element.set_focus()
            if click:
                self.element.click_input()
            from pywinauto.controls.uia_controls import EditWrapper
            if isinstance(self.element, EditWrapper):
                return self.element.set_edit_text(value)
            else:
                raise Exception('Element is not instance of EditWrapper')

        @try_except_decorator
        def type_keys(self, *value, delay=0, set_focus=True, clear=False, click=False, protect_first=False):
            def replace(string):
                replace_list = ['(', ')', '{', '}', '^', '%', '+']
                string = ''.join([c if c not in replace_list else '{' + c + '}' for c in string])
                return string

            sleep(delay)
            if set_focus:
                self.element.set_focus()
            if click:
                self.element.click_input()
            if clear:
                self.element.type_keys(self.keys.CLEAR)
            if protect_first:
                keys = ''.join(str(v) if n else replace(str(v)) for n, v in enumerate(value))
            else:
                keys = ''.join(str(v) for v in value)
            self.element.type_keys(keys, pause=0.05, with_spaces=True, with_tabs=True,
                                   with_newlines=True, set_foreground=set_focus)

    def __init__(self, path, logger=None, timeout=60, process_registry=None):
        self.path = path
        self.logger = logging.getLogger(logger) or logging.getLogger()
        if not logger:
            basic_format = '%(asctime)s||%(levelname)s||%(message)s'
            date_format = '%Y-%m-%d,%H:%M:%S'
            logging.basicConfig(level=logging.DEBUG, format=basic_format, datefmt=date_format)

        self.timeout = timeout
        self.process_registry = process_registry
        self.process_list = list()
        self.window_element_info = None

    @try_except_decorator
    def run(self):
        self.quit(skip_error=True, log=False)
        os.system(f'start "" "{self.path.__str__()}"')

    @try_except_decorator
    def quit(self):
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
        sleep(3)

    @try_except_decorator
    def switch(self, selector, timeout=None, alt_maximize=False):
        if isinstance(selector, App.Element):
            result = selector
        elif isinstance(selector, dict):
            if 'parent' not in selector:
                selector['parent'] = None
            result = self.find_element(selector, timeout=timeout, skip_error=False, log=False)
        else:
            raise Exception('Selector type unknown')
        with suppress(Exception):
            result.element.set_focus()
            result.element.maximize()
            if alt_maximize:
                if self.window_element_info:
                    r = self.window_element_info.rectangle
                else:
                    user32 = ctypes.windll.user32
                    r = RECT(0, 0, user32.GetSystemMetrics(0), user32.GetSystemMetrics(17))
                win32functions.MoveWindow(result.element.element_info.handle, r.left, r.top, r.right, r.bottom, True)

        self.window_element_info = result.element.element_info
        process = psutil.Process(self.window_element_info.process_id)
        if process not in self.process_list:
            self.process_list.append(process)
        if self.process_registry is not None:
            if self.process_registry.is_file():
                with open(str(self.process_registry), 'r', encoding='utf-8') as fp:
                    data = json.load(fp)
            else:
                data = list()
            process_name = process.name()
            if process_name not in data:
                data.append(process_name)
                with open(str(self.process_registry), 'w', encoding='utf-8') as fp:
                    json.dump(data, fp, ensure_ascii=False)

    @try_except_decorator
    def find_elements(self, selector, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        if 'parent' not in selector:
            selector['parent'] = self.window_element_info
        try:
            elements = find_elements(**selector, timeout=timeout)
        except (Exception,):
            elements = list()
        if not len(elements):
            raise Exception('Elements not found')
        return [self.Element(element, logger=self.logger) for element in elements]

    @try_except_decorator
    def find_element(self, selector, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        if 'parent' not in selector:
            selector['parent'] = self.window_element_info
        try:
            elements = find_elements(**selector, timeout=timeout)
        except (Exception,):
            elements = list()
        if not len(elements):
            raise Exception('Elements not found')
        element = elements[0]
        return self.Element(element, logger=self.logger)

    @try_except_decorator
    def wait_element(self, selector, timeout=None, until=True):
        timeout = timeout if timeout is not None else self.timeout
        if 'parent' not in selector:
            selector['parent'] = self.window_element_info

        def function():
            try:
                elements = find_elements(**selector, timeout=0)
                flag = len(elements) > 0
            except (Exception,):
                flag = False

            if flag != until:
                raise Exception(f'Element not {"appeared" if until else "disappeared"}')

        try:
            wait_until_passes(timeout, 0.1, function)
            return True
        except (Exception,):
            return False


class Web:
    keys = Keys

    class Element:
        keys = Keys

        def __init__(self, element, selector, by, driver, logger=None):
            self.element: WebElement = element
            self.selector = selector
            self.by = by
            self.driver: WebDriver = driver
            self.logger = logger
            if not logger:
                basic_format = '%(asctime)s||%(levelname)s||%(message)s'
                date_format = '%Y-%m-%d,%H:%M:%S'
                logging.basicConfig(level=logging.DEBUG, format=basic_format, datefmt=date_format)

        @try_except_decorator
        def scroll(self, delay=0):
            sleep(delay)
            ActionChains(self.driver).move_to_element(self.element).perform()

        @try_except_decorator
        def clear(self, delay=0):
            sleep(delay)
            self.element.clear()

        @try_except_decorator
        def click(self, double=False, delay=0, scroll=True):
            sleep(delay)
            if scroll:
                self.scroll(skip_error=True, log=False)
            ActionChains(self.driver).double_click(self.element).perform() if double else self.element.click()

        @try_except_decorator
        def wheel_click(self, delay=0, scroll=True):
            sleep(delay)
            if scroll:
                self.scroll(skip_error=True, log=False)
            self.driver.execute_script("window.open();", self.element.get_attribute("href"))
            # ActionChains(self.driver).key_down(Keys.COMMAND).send_keys("t").key_up(Keys.COMMAND).perform()

        @try_except_decorator
        def get_attr(self, attr='text', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                self.scroll(skip_error=True, log=False)
            return getattr(self.element, attr) if attr in ['tag_name', 'text'] else self.element.get_attribute(attr)

        @try_except_decorator
        def set_attr(self, value=None, attr='value', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                self.scroll(skip_error=True, log=False)
            self.driver.execute_script(f"arguments[0].{attr} = arguments[1]", self.element, value)

        @try_except_decorator
        def type_keys(self, *value, delay=0, scroll=True, clear=True):
            sleep(delay)
            if scroll:
                self.scroll(skip_error=True, log=False)
            if clear:
                self.clear(skip_error=True, log=False)
            self.element.send_keys(*value)

        @try_except_decorator
        def select(self, value=None, select_type='select_by_value', delay=0, scroll=True):
            sleep(delay)
            if scroll:
                self.scroll(skip_error=True, log=False)
            select = Select(self.element)
            function = getattr(select, select_type)
            if value is None:
                if select_type == 'deselect_all':
                    return function()
                else:
                    return select
            else:
                return function(value)

    def __init__(self, path=None, download_path=None, logger=None, run=False, timeout=60):
        self.path = path or Path.home().joinpath(r"AppData\Local\.rpa\Chromium\chromedriver.exe")
        self.download_path = download_path or Path.home().joinpath('Downloads')
        self.logger = logging.getLogger(logger) or logging.getLogger()
        if not logger:
            basic_format = '%(asctime)s||%(levelname)s||%(message)s'
            date_format = '%Y-%m-%d,%H:%M:%S'
            logging.basicConfig(level=logging.DEBUG, format=basic_format, datefmt=date_format)
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

    @try_except_decorator
    def run(self):
        self.quit(skip_error=False, log=False)
        self.driver = webdriver.Chrome(service=Service(self.path.__str__()), options=self.options)
        self.driver.set_page_load_timeout(3600)

    @try_except_decorator
    def quit(self):
        if self.driver:
            self.driver.quit()

    @try_except_decorator
    def close(self):
        self.driver.close()

    @try_except_decorator
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

    @try_except_decorator
    def get(self, url):
        self.driver.get(url)

    @try_except_decorator
    def find_elements(self, selector, timeout=None, event=None, by='xpath'):
        if event is None:
            event = expected_conditions.presence_of_element_located
        timeout = timeout if timeout is not None else self.timeout
        if timeout:
            self.wait_element(selector, timeout, event, by, log=False)
        elements = self.driver.find_elements(by, selector)
        elements = [self.Element(element=element, selector=selector, by=by, driver=self.driver) for element in elements]
        return elements

    @try_except_decorator
    def find_element(self, selector, timeout=None, event=None, by='xpath'):
        if event is None:
            event = expected_conditions.presence_of_element_located
        timeout = timeout if timeout is not None else self.timeout
        if timeout:
            self.wait_element(selector, timeout, event, by, log=False)
        element = self.driver.find_element(by, selector)
        element = self.Element(element=element, selector=selector, by=by, driver=self.driver)
        return element

    @try_except_decorator
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
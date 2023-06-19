from ctypes import byref
from ctypes.wintypes import COLORREF
from threading import Thread
from time import sleep

from keyboard import is_pressed
from pywinauto.controls.uiawrapper import UIAWrapper
from pywinauto.findwindows import find_elements
from pywinauto.uia_element_info import UIAElementInfo
from pywinauto.win32defines import PS_SOLID, BS_NULL, HS_DIAGCROSS
from pywinauto.win32functions import CreatePen, CreateBrushIndirect, CreateDC, SelectObject, Rectangle as Rect
from pywinauto.win32structures import LOGBRUSH
from win32api import GetCursorPos
from win32gui import InvalidateRect, WindowFromPoint, RedrawWindow

from se.config import Config


class Rectangle:
    @classmethod
    def clear(cls, rectangle=None, pen_handle=None):
        r = rectangle
        point = ((r.right - r.left) / 2, (r.bottom - r.top) / 2) if r else (0, 0)
        InvalidateRect(WindowFromPoint(point), r, True)
        sleep(0.05)
        rectangle = (r.left, r.top, r.right, r.bottom) if r else None
        RedrawWindow(WindowFromPoint(point), rectangle, None, 0)
        RedrawWindow(WindowFromPoint(point), None, pen_handle, 0)
        RedrawWindow(None, None, pen_handle, 4)
        RedrawWindow(None, None, pen_handle, 4)

    @classmethod
    def draw(cls, element_info, outline_thickness: int = 2, outline_color: COLORREF = 0x5C5CFF, clear=True):
        try:
            if element_info is None:
                return
            pen_handle = CreatePen(PS_SOLID, outline_thickness, outline_color)
            brush = LOGBRUSH()
            brush.lbStyle = BS_NULL
            brush.lbHatch = HS_DIAGCROSS
            dc = CreateDC("DISPLAY", None, None, None)
            SelectObject(dc, CreateBrushIndirect(byref(brush)))
            SelectObject(dc, pen_handle)
            rectangle = element_info.rectangle
            Rect(dc, rectangle.left, rectangle.top, rectangle.right, rectangle.bottom)
            sleep(0.1)
            if clear:
                cls.clear(rectangle, pen_handle)
                sleep(0.1)
        except (Exception,):
            cls.clear()


class Explorer:
    def __init__(self, config=None):
        self._listen = False
        self._parse = False
        self._element_info = None
        self.config = config or Config()
        Thread(target=self._init_listening, daemon=True).start()
        Thread(target=self._init_parsing, daemon=True).start()

    def _init_listening(self):
        while True:
            if self._listen:
                if is_pressed('ctrl'):
                    self._parse = True
                else:
                    self._parse = False
                if is_pressed('ctrl + alt'):
                    self._listen = False
                    self._parse = False
                if is_pressed('ctrl + shift'):
                    self._listen = False
                    self._parse = False
                if is_pressed('esc'):
                    self._listen = False
                    self._parse = False
                    self._element_info = None
            sleep(0.01)

    def _init_parsing(self):
        while True:
            if self._parse:
                self._element_info = UIAElementInfo.from_point(*GetCursorPos())
            sleep(0.01)

    def _build_selector(self, parent=None):
        if self._element_info is None:
            return None, None
        # * get selector flags
        flags = self.config.get()
        # * get element and its window
        element = UIAWrapper(self._element_info)

        # * build element selector
        selector = {
            'title': element.element_info.name if flags['title'] else None,
            'class_name': element.element_info.class_name if flags['class_name'] else None,
            'control_type': element.element_info.control_type if flags['control_type'] else None,
            'visible_only': element.element_info.visible if flags['visible_only'] else None,
            'enabled_only': element.element_info.enabled if flags['enabled_only'] else None
        }
        # ! parse all elements
        elements = find_elements(backend='uia', **selector, parent=parent, top_level_only=False)
        # ! get found_index
        found_index = [n for n, el in enumerate(elements) if el == element.element_info][0]
        selector_update = {'found_index': found_index if flags['found_index'] else None}
        selector.update(selector_update)
        self._element_info = None
        return element, selector

    # ? actions --------------------------------------------------------------------------------------------------------

    @classmethod
    def find_elements(cls, timeout=30, **selector):
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

    def get_selector(self, parent=None):
        self._listen = True
        while self._listen:
            if self._parse:
                Rectangle.draw(self._element_info)
            sleep(0.01)
        return self._build_selector(parent)


if __name__ == '__main__':
    _window, _element = Explorer().get_selector()
    _windows = Explorer.find_elements(**_window)
    _current = _windows[_window['found_index']]
    _current.draw_outline()
    if _element:
        _elements = Explorer.find_elements(parent=_current, **_element)
        _current = _elements[_element['found_index']]
        _current.draw_outline()

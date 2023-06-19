import json
from pathlib import Path

import socketio
from flask import Flask
from flask_socketio import SocketIO


class Config:
    def __init__(self):
        self.config_path = Path.home().joinpath('Appdata\\Local\\.rpa\\.se\\se.json')
        self.config_dir = self.config_path.parent
        self.config_dir.mkdir(exist_ok=True)

        self._title = True
        self._class_name = True
        self._control_type = True
        self._visible_only = True
        self._enabled_only = True
        self._found_index = True

        if not self.config_path.is_file():
            self.write()
        else:
            self.read()

    def get(self):
        data = {
            'title': self.title,
            'class_name': self.class_name,
            'control_type': self.control_type,
            'visible_only': self.visible_only,
            'enabled_only': self.enabled_only,
            'found_index': self.found_index
        }
        return data

    def write(self):
        with open(self.config_path, 'w+', encoding='utf-8') as f:
            json.dump(self.get(), f, ensure_ascii=False)

    def read(self):
        with open(self.config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        self.title = data['title']
        self.class_name = data['class_name']
        self.control_type = data['control_type']
        self.visible_only = data['visible_only']
        self.enabled_only = data['enabled_only']
        self.found_index = data['found_index']

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, flag):
        self._title = flag

    @property
    def class_name(self):
        return self._class_name

    @class_name.setter
    def class_name(self, flag):
        self._class_name = flag

    @property
    def control_type(self):
        return self._control_type

    @control_type.setter
    def control_type(self, flag):
        self._control_type = flag

    @property
    def visible_only(self):
        return self._visible_only

    @visible_only.setter
    def visible_only(self, flag):
        self._visible_only = flag

    @property
    def enabled_only(self):
        return self._enabled_only

    @enabled_only.setter
    def enabled_only(self, flag):
        self._enabled_only = flag

    @property
    def found_index(self):
        return self._found_index

    @found_index.setter
    def found_index(self, flag):
        self._found_index = flag


root_path = Path(__file__).parent
resources_path = root_path.joinpath('resources')
templates_path = resources_path
static_path = resources_path

io = SocketIO(ping_timeout=2, ping_interval=1)
app = Flask(__name__, static_folder=static_path.__str__(), template_folder=templates_path.__str__())
sio = socketio.Client(ssl_verify=False)
config = Config()

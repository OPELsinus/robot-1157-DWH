import json
import logging
import traceback
from threading import Thread
from time import sleep

from flask import render_template

from rpamini import App
from se._se import Explorer, Rectangle
from se.config import app, io, config


def print_(*args, **kwargs):
    args = [str(i) for i in args]
    kwargs = [f'{str(i)}: {kwargs[i]}' for i in kwargs]
    args.extend(kwargs)
    return ', '.join([str(i) for i in args])


global_data = {
    'parent': None,
    'element': None,
    'print': print_
}
explorer = Explorer(config)


@app.route('/')
def main():
    return render_template('index.html')


@io.on("connect")
def on_connect():
    try:
        data = config.get()
        io.emit('config', data)
    except Exception as e:
        traceback.print_exc()
        io.emit('status', {'level': logging.ERROR, 'message': str(e)})


@io.on("get")
def on_get():
    def func():
        try:
            global explorer
            global global_data
            explorer.config = config
            parent_: App.Element = global_data['parent']
            element, selector = explorer.get_selector(parent_.element if parent_ else parent_)
            selector = {key: selector[key] for key in selector if selector[key] is not None}
            selector = json.dumps(selector, ensure_ascii=False) if selector else None
            io.emit('fill', selector)
            io.emit('status', {'level': logging.INFO, 'message': 'selector found'})
            global_data['element'] = App.Element(element)
        except Exception as e:
            traceback.print_exc()
            io.emit('status', {'level': logging.ERROR, 'message': str(e)})

    Thread(target=func, daemon=True).start()


@io.on("check")
def on_check(*args):
    def func():
        try:
            global explorer
            global global_data
            explorer.config = config
            try:
                selector = json.loads(args[0])
                if 'found_index' not in selector:
                    selector['found_index'] = 0
                io.emit('status', {'level': logging.DEBUG, 'message': 'selector built'})
            except (Exception,):
                traceback.print_exc()
                io.emit('status', {'level': logging.WARN, 'message': 'selector is not valid'})
                return

            try:
                parent_: App.Element = global_data['parent']
                elements = explorer.find_elements(timeout=0, **selector, parent=parent_.element if parent_ else parent_)
                element = App.Element(elements[0])
                # noinspection PyTypeChecker
                global_element: App.Element = global_data['element']
                if element.element.element_info != global_element.element.element_info:
                    raise Exception('element not valid')
                io.emit('status', {'level': logging.DEBUG, 'message': 'element found'})
            except (Exception,):
                traceback.print_exc()
                io.emit('status', {'level': logging.ERROR, 'message': 'element not found'})
                return

            try:
                Rectangle.draw(element.element.element_info, clear=False)
                io.emit('status', {'level': logging.DEBUG, 'message': 'element focused'})
            except (Exception,):
                traceback.print_exc()
                io.emit('status', {'level': logging.ERROR, 'message': 'element cant be focus'})
            io.emit('status', {'level': logging.INFO, 'message': 'selector is valid'})

        except Exception as e:
            traceback.print_exc()
            io.emit('status', {'level': logging.ERROR, 'message': str(e)})

    Thread(target=func, daemon=True).start()


@io.on("alt_check")
def on_alt_check(*args):
    def func():
        try:
            global explorer
            global global_data
            explorer.config = config
            try:
                selector = json.loads(args[0])
                if 'found_index' not in selector:
                    selector['found_index'] = 0
                io.emit('status', {'level': logging.DEBUG, 'message': 'selector built'})
            except (Exception,):
                traceback.print_exc()
                io.emit('status', {'level': logging.WARN, 'message': 'selector is not valid'})
                return

            try:
                parent_: App.Element = global_data['parent']
                elements = explorer.find_elements(timeout=0, **selector, parent=parent_.element if parent_ else parent_)
                element = elements[0]
                io.emit('status', {'level': logging.DEBUG, 'message': 'element found'})
            except (Exception,):
                traceback.print_exc()
                io.emit('status', {'level': logging.ERROR, 'message': 'element not found'})
                return

            try:
                Rectangle.draw(element.element_info, clear=False)
                io.emit('status', {'level': logging.DEBUG, 'message': 'element focused'})
            except (Exception,):
                traceback.print_exc()
                io.emit('status', {'level': logging.ERROR, 'message': 'element cant be focus'})
            io.emit('status', {'level': logging.INFO, 'message': 'selector is valid'})

        except Exception as e:
            traceback.print_exc()
            io.emit('status', {'level': logging.ERROR, 'message': str(e)})

    Thread(target=func, daemon=True).start()


@io.on("set")
def on_set():
    try:
        global global_data
        global_data['parent'] = global_data['element']
        io.emit('status', {'level': logging.DEBUG, 'message': f'parent = {global_data["element"]}'})
    except Exception as e:
        traceback.print_exc()
        io.emit('status', {'level': logging.ERROR, 'message': str(e)})


@io.on("clean")
def on_clean():
    try:
        global global_data
        global_data = {
            'parent': None,
            'element': None,
            'print': print_
        }
        io.emit('status', {'level': logging.DEBUG, 'message': 'Done'})
    except Exception as e:
        traceback.print_exc()
        io.emit('status', {'level': logging.ERROR, 'message': str(e)})


@io.on("command")
def on_command(*args):
    def func():
        try:
            global global_data
            local_data = {}
            exec(f'result = {args[0]}', global_data, local_data)
            global_data.update(local_data)
            io.emit('status', {'level': logging.INFO, 'message': str(global_data['result']),
                               'global_data_keys': [key for key in global_data]})
        except Exception as e:
            traceback.print_exc()
            io.emit('status', {'level': logging.ERROR, 'message': str(e)})

    Thread(target=func, daemon=True).start()


@io.on("flag")
def on_flag(*args):
    try:
        setattr(config, args[0], args[1])
        config.write()
        data = config.get()
        io.emit('config', data)
    except Exception as e:
        traceback.print_exc()
        io.emit('status', {'level': logging.ERROR, 'message': str(e)})


def create_app():
    app.config['SECRET_KEY'] = 'qrpu4hYAmzpdbnMb5Glg3w'
    io.init_app(app, cors_allowed_origins="*")
    Thread(target=io.run, args=(app, '127.0.0.1', 6699), daemon=True).start()
    sleep(1)


if __name__ == '__main__':
    parent = None
    create_app()

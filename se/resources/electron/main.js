const {app, BrowserWindow, Tray, Menu} = require('electron')
const gotTheLock = app.requestSingleInstanceLock()
let mainWindow
let contextMenu
let trayIcon

function render() {
    app.whenReady().then(() => {
        mainWindow = new BrowserWindow({
            width: 800,
            height: 323,
            minWidth: 500,
            minHeight : 343,
            maxHeight : 343,
            show: false,
            // alwaysOnTop: true,
            backgroundColor: '#222222',
            // icon: './resources/app.ico',
        });
        mainWindow.loadURL('http://127.0.0.1:6699');
        mainWindow.setMenu(null);
        mainWindow.once('ready-to-show', () => {
            mainWindow.show();
        })
        // mainWindow.on('minimize', function (event) {
        //     event.preventDefault();
        //     mainWindow.hide();
        // });

        contextMenu = Menu.buildFromTemplate([{
            label: 'Show', click: function () {
                mainWindow.show();
            }
        }, {
            label: 'Quit', click: function () {
                app.isQuiting = true;
                app.quit();
            }
        }]);
        // trayIcon = new Tray('resources/app.ico');
        // trayIcon.setContextMenu(contextMenu);
        // trayIcon.on('click', function () {
        //     if (!mainWindow.isVisible()) {
        //         mainWindow.show();
        //     } else {
        //         mainWindow.hide();
        //     }
        // });
        // trayIcon.on('dbclick', function (event) {
        //     event.preventDefault()
        // });
    });
    app.on('before-quit', function (evt) {
        // trayIcon.destroy();
    });
}

if (gotTheLock) {
    render();
} else {
    app.quit();
}
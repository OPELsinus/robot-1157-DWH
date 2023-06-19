document.addEventListener('DOMContentLoaded', function () {
    const level = {
        50: 'text-danger',  /* CRITICAL FATAL */
        40: 'text-danger',  /* ERROR */
        30: 'text-warning',  /* WARNING WARN */
        20: 'text-success',  /* INFO */
        10: 'text-info',  /* DEBUG */
        0: 'text-muted'  /* NOTSET */
    }
    let socket = io();

    /* get */
    document.querySelector('input[name="get"]').addEventListener('click', () => {
        socket.emit('get');
    });
    socket.on('fill', function (json) {
        console.log('RECEIVED', json);
        let textarea = document.querySelector('textarea')
        textarea.value = ''
        textarea.value = json.replace(/true/g, 'True').replace(/false/g, 'False')
    });

    /* check */
    document.querySelector('input[name="check"]').addEventListener('click', () => {
        let textarea = document.querySelector('textarea')
        let selector = textarea.value.replace(/True/g, 'true').replace(/False/g, 'false')
        socket.emit('check', selector);
    });

    /* check */
    document.querySelector('input[name="alt_check"]').addEventListener('click', () => {
        let textarea = document.querySelector('textarea')
        let selector = textarea.value.replace(/True/g, 'true').replace(/False/g, 'false')
        socket.emit('alt_check', selector);
    });

    /* set */
    document.querySelector('input[name="set"]').addEventListener('click', () => {
        socket.emit('set');
    });

    /* clean */
    document.querySelector('input[name="clean"]').addEventListener('click', () => {
        document.querySelector('textarea').value = '';
        document.querySelector('input[name="command"]').value = '';
        document.querySelector('input[name="status"]').value = '';
        socket.emit('clean');
    });

    /* status */
    socket.on('status', function (json) {
        console.log('RECEIVED', json);
        let input = document.querySelector('input[name="status"]');
        input.value = json.message;
        for (let key in level) {
            document.querySelector('input[name="status"]').classList.remove(level[key]);
        }
        document.querySelector('input[name="status"]').classList.add(level[json.level]);
    });

    /* command */
    document.querySelector('input[name="command"]').addEventListener('keyup', (e) => {
        if (e.keyCode === 13) {
            let code = document.querySelector('input[name="command"]').value;
            socket.emit('command', code);
        }
    });

    /* config */
    socket.on('config', function (json) {
        for (let key in json) {
            if (json[key]) {
                document.querySelector(`input[title="${key}"]`).classList.remove('text-muted');
                document.querySelector(`input[title="${key}"]`).classList.remove('text-light');
                document.querySelector(`input[title="${key}"]`).classList.add('text-light');
            } else {
                document.querySelector(`input[title="${key}"]`).classList.remove('text-light');
                document.querySelector(`input[title="${key}"]`).classList.remove('text-muted');
                document.querySelector(`input[title="${key}"]`).classList.add('text-muted');
            }
        }
    });
    document.querySelectorAll('input[title]').forEach(el => {
        el.addEventListener('click', function() {
            socket.emit('flag', el.title, el.classList.contains('text-light')?false:true);
        });
    });

});
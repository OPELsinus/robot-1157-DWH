import sys
from subprocess import call

from se._app import create_app
from se.config import resources_path

create_app()
cmd = [str(resources_path.joinpath('electron\\electron\\electron.exe')), str(resources_path.joinpath('electron\\'))]
process = call(' '.join(cmd), stdout=sys.stdout, stderr=sys.stderr)

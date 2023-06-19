from pathlib import Path

from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from tools import json_read, json_write, net_use, get_hostname

disable_warnings(InsecureRequestWarning)

# ? ROOT
root_path = Path(__file__).parent

# ? LOCAL
local_path = Path.home().joinpath(f'AppData\\Local\\.rpa\\.agent')
local_path.mkdir(exist_ok=True, parents=True)
local_env_path = local_path.joinpath('agent.json')
local_env_data = json_read(local_env_path)
process_list_path = local_path.joinpath('process_list.json')
if not process_list_path.is_file():
    json_write(process_list_path, [])

# ? GLOBAL
global_path = Path(local_env_data['global_path'])
global_username = local_env_data['global_username']
global_password = local_env_data['global_password']
net_use(global_path, global_username, global_password)
global_env_path = global_path.joinpath('env.json')
global_env_data = json_read(global_env_path)

orc_host = global_env_data['orc_host']
tg_token = global_env_data['tg_token']
smtp_host = global_env_data['smtp_host']
smtp_author = global_env_data['smtp_author']
sprut_username = global_env_data['sprut_username']
sprut_password = global_env_data['sprut_password']
sprut_username_personal = global_env_data['sprut_username_personal']
sprut_password_personal = global_env_data['sprut_password_personal']
odines_username = global_env_data['odines_username']
odines_password = global_env_data['odines_password']
odines_username_rpa = global_env_data['odines_username_rpa']
odines_password_rpa = global_env_data['odines_password_rpa']
owa_username = global_env_data['owa_username']
owa_password = global_env_data['owa_password']
owa_username_compl = global_env_data['owa_username_compl']
owa_password_compl = global_env_data['owa_password_compl']
sed_username = global_env_data['sed_username']
sed_password = global_env_data['sed_password']
cups_host = global_env_data['cups_host']
cups_username = global_env_data['cups_username']
cups_password = global_env_data['cups_password']
cas_username = global_env_data['cas_username']
cas_password = global_env_data['cas_password']

# ? PROJECT
project_name = 'PROJECT_NAME'
host_name = get_hostname()
project_path = global_path.joinpath(f'.agent').joinpath(project_name).joinpath(host_name)
project_path.mkdir(exist_ok=True, parents=True)

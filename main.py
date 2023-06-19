import sys

from config import project_path, project_name
from core import init_logger

if __name__ == '__main__':
    # ? LOGGER
    if len(sys.argv) == 1:
        sys.argv.append('dev')
    logger = init_logger(tg_chat_id='531139435', orc_file_path=project_path.joinpath(f'{sys.argv[1]}.log'))
    logger.warning(project_name)

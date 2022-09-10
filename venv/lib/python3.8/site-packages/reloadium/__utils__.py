import os
import sys
from typing import Tuple

IMPORT_ERROR_MSG = 'It seems like your platform or Python version are not supported yet.\nWindows, Linux, macOS and Python >= 3.7 (3.9 for M1) are currently supported.\nPlease submit a github issue if you believe Reloadium should be working on your system at\nhttps://github.com/reloadware/reloadium\n'







def colored(inp: str, color: Tuple[int, int, int]) -> str:
    ret = ''.join(['\x1b[38;2;', '{:{}}'.format(color[0], ''), ';', '{:{}}'.format(color[1], ''), ';', '{:{}}'.format(color[2], ''), 'm', '{:{}}'.format(inp, ''), '\x1b[0m'])
    return ret


def pre_import_check() -> None:
    try:
        import reloadium.corium
    except Exception as e:
        sys.stderr.write(colored(IMPORT_ERROR_MSG, (255, 0, 0, )))
        sys.stderr.flush()

        if (os.environ.get('RW_DEBUG', 'False') != 'False'):
            raise 

        sys.exit(1)

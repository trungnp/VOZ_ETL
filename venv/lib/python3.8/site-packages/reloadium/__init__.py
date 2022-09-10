
from reloadium.__utils__ import pre_import_check

pre_import_check()

from reloadium.corium import start as reloader_start 
from reloadium.corium.config import BaseConfig
from reloadium.corium.public import *

__author__ = 'Damian Krystkiewicz'
__company__ = 'Reloadware'
__copyright__ = 'Copyright (C) 2022 Reloadware'
__stage__ = 'prod'
__license__ = 'Apache 2.0'


def start() -> None:
    import sys

    reloader_start(sys.argv)

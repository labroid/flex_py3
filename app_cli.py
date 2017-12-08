import collections
import glob
import logging
import os
import os.path
import shutil
import time
from pathlib import Path
from logging.config import dictConfig

import mongoengine as me

from models import Db_connect, Queue, State, Gphoto, Gphoto_parent
from utils import file_md5sum, Config

cfg = Config()
#dictConfig(cfg.logging)
#log = logging.getLogger(__name__)  # TODO:  Logging not correctly configured
Db_connect()

p = Path()

while True:
    os.system('cls')
    state = State.objects().first()
    print('Target: {}, Old Target: {}'.format(state.target, state.old_target))
    print('Mirror: {}, Purge: {}'.format(state.mirror_ok, state.purge_ok))
    print('Dir List: {}', state.dirlist)
    print('Files in Queue {}', Queue.objects.count())
    response = input("New Target or Null:").strip('"')
    if response != '':
        State.objects().update_one(target=response)


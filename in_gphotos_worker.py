import pymongo
import logging
import time
import os
import yaml
import collections
import sys
import pprint

from utils import file_md5sum, cfg_obj
from gphotos import Gphotos
from local_db import LocalDb


# Configuration
with open("config.yaml") as f:
    config = yaml.safe_load(f.read())
local_cfg = cfg_obj(config, 'local')
tq_cfg = cfg_obj(config, 'task_queue')

# TODO:  logging can be set via a dict, so put logging settings in config.yaml and (probably) move to main call
logger = logging.getLogger(__name__)
LOG_FILE = os.path.join(local_cfg.log_file_base, time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "photolog.txt")
logging.basicConfig(
    filename=LOG_FILE,
    format=local_cfg.log_format,
    level=logging.DEBUG,
    filemode='w'
)

# TODO:  Check depth of queue, and if it is over N, launch another instance of myself??

local_db = LocalDb().db
gphotos = Gphotos()

while True:
    gphotos.sync()
    photos = local_db.find({'$or': [{'in_gphotos': False}, {'in_gphotos': None}]})
    for photo in photos:
        local_db.update_one({'path': photo['path']}, {'$set': {'gphoto_check_out': time.time()}})
        record = gphotos.check_member(photo['md5sum'])
        if record:
            local_db.update_one({'path': photo['path']}, {'$set': {'gphoto_check_in': time.time(), 'in_gphotos': True, 'archive_meta': record}})
            logging.info('Worker X done: {} marked as in Gphotos'.format(photo['path']))
        else:
            if photo['in_gphotos'] is None:
                local_db.update_one({'path': photo['path']}, {'$set': {'gphoto_check_in': time.time(), 'in_gphotos': False}})
                logging.info('Worker X done: {} marked as not in Gphotos'.format(photo['path']))
    print("{} Waiting...".format(time.asctime()))
    time.sleep(100)
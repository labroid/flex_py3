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

gphotos = Gphotos()
local_db = pymongo.MongoClient(host=tq_cfg.host)[tq_cfg.database][tq_cfg.archive]

archived_count = 0
missing_count = 0
while True:
    # time.sleep(5) #Slow down process so I can see it run
    job = local_db.find_and_modify(
        query={'md5_out': None},
        update={'$set': {'md5_out': time.time()}},
        limit=1
    )
    if job is None:
        time.sleep(1)
        print("Waiting...")
    else:
        print("MD5 for", job['path'], end="")
        md5sum = file_md5sum(job['path'])  # TODO:  Should do something to notify user that MD5 failed on a bad file, path, or permissions
        print(md5sum, "-- done")
        local_db.update_one({'path': job['path']},
                            {'$set': {'md5sum': md5sum, 'md5_in': time.time(), 'gphoto_check_out': time.time()}})
        record = gphotos.check_member(md5sum)
        if record:
            update = {'gphoto_check_in': time.time(), 'in_gphotos': True, 'archive_meta': record}
        else:
            update = {'gphoto_check_in': time.time(), 'in_gphotos': False}
        local_db.update_one({"path": job['path']}, {'$set': update})
        logging.info("Worker X done: {}".format(job['path']))
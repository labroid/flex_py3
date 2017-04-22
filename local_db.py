import pymongo
import logging
import yaml

from utils import cfg_obj

with open("config.yaml") as f:
    config = yaml.safe_load(f.read())

local_cfg = cfg_obj(config, 'local')
tq_cfg = cfg_obj(config, 'task_queue')


class LocalDb():
    def __init__(self, host=tq_cfg.host, database=tq_cfg.database, collection=tq_cfg.collection):
        # TODO: Do a fast db present check, and start it if not present
        self.db = pymongo.MongoClient(host=host)[database][collection]  #TODO: Does this fail silently??

    # def __getattr__(self):
    #     self.db

    def name(self):
        return self.db.full_name

    def server_stat(self):
        if self.db.full_name:
            return True
        else:
            return False

    # TODO: Consider a 'statistics' call to make main routine clearer?
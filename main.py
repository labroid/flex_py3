import logging
import time
import pymongo
import json
import collections
import glob
import os
import os.path
import yaml
from flask import Flask, request
from flask_restful import Resource, Api, reqparse

from utils import cfg_obj
from gphotos import Gphotos

app = Flask(__name__)
api = Api(app)


def main():
    app.run(host='127.0.0.1', port=8080, debug=True)


class DirStats(Resource):
    def post(self):  # TODO:  Clear previous results on dir change??
        print("DirStats reached {}".format(dir(request)))
        json_data = request.get_json(force=True)  # TODO:  Why do we have to force this??
        print("Got JSON data {}".format(json_data))
        directory = json_data['directory'].replace('"', '')  # Remove surrounding quotes to make cut/paste easier
        print("Requested dir: {}".format(directory))
        return h.scan_dir(directory)


class StatServer(Resource):  # TODO: Might be better to return True/False so react.js can show color/text choices easily
    def get(self):
        if h.gphotos.server_stat():
            db_ok = 'Up'
        else:
            db_ok = 'Down'
        return {'app_ok': 'Up', 'db_ok': db_ok}


class StartCheck(Resource):
    def get(self):
        h.check_photos()
        return {'checkstatus': 'OK'}


class MoveToQueue(Resource):
    def get(self):
        # move_to_queue()
        return {'status': 'OK'}


class CheckStat(Resource):
    def get(self):
        total_files = h.local_db.count()
        md5_done = h.local_db.find({'md5sum': {'$ne': None}}).count()
        in_gphotos = h.local_db.find({'in_gphotos': True}).count()
        in_upload_queue = h.local_db.find({'queue_state': 'queued'}).count()
        stats = {'total_files': total_files, 'md5_done': md5_done, 'in_gphotos': in_gphotos,
                 'in_upload_queue': in_upload_queue}
        print("Queue Process stats: {}".format(stats))
        return stats

api.add_resource(DirStats, '/dirstats')
api.add_resource(StatServer, '/statserver')
api.add_resource(StartCheck, '/startcheck')
api.add_resource(CheckStat, '/checkstat')
api.add_resource(MoveToQueue, '/movetoqueue')


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


# Helper functions
class H():
    def __init__(self):
        with open("config.yaml") as f:
            config = yaml.safe_load(f.read())

        self.local_cfg = cfg_obj(config, 'local')
        self.tq_cfg = cfg_obj(config, 'task_queue')
        self.gphotos = Gphotos()
        self.local_db = pymongo.MongoClient(host=self.tq_cfg.host)[self.tq_cfg.database][self.tq_cfg.archive]
        self.photos = collections.defaultdict()

        print("Database=", self.local_db.full_name)
        print("Syncing Gphoto database...", end='')
        sync_stats = self.gphotos.sync()
        print(sync_stats)
        logging.info("Initial gphotos online db sync done, sync stats: {}".format(sync_stats))

    def scan_dir(self, target):
        self.photos.clear()  # TODO:  Make this a user option???
        logging.info("Start processing dir")
        dirsize = 0
        excluded_exts = {}

        start = time.time()
        target_list = list(glob.iglob(target))
        logging.info("target list: {}".format(target_list))
        for top in target_list:
            logging.info('Traversing tree at {} and storing paths.'.format(top))
            for root, dirs, files in os.walk(top):  # TODO:  Add error trapping argument
                for path in [os.path.join(root, x) for x in files]:
                    file_ext = os.path.splitext(path)[1].lower()
                    if file_ext in self.local_cfg.image_filetypes:  # TODO:  Check here if already in queue.  If so, count and size separately.  Probably put in queue marked as already in Gphotos so tree view will be valid if implemented
                        size = os.stat(path).st_size
                        self.photos[path] = size
                        dirsize += size
                    else:
                        if file_ext in excluded_exts:
                            excluded_exts[file_ext] += 1
                        else:
                            excluded_exts[file_ext] = 1
        if len(excluded_exts):
            excluded_list = [(str(k).replace(".", "") + "(" + str(v) + ")") for k, v in excluded_exts.items()]
        else:
            excluded_list = ["None"]
        response = json.dumps({'dirs': target_list, 'filecount': len(self.photos), 'dirsize': "{:.1f} MB".format(dirsize / 1e6),
                               'excluded_exts': excluded_list, 'elapsed_time': "{:.3f}".format(time.time() - start)})
        print("response = ", response)
        return response

    def check_photos(self):  # TODO:  Enable start/stop
        print("Syncing Gphoto database...", end='')
        self.gphotos.sync()
        print("Done")
        checked_start = time.time()
        count = 0
        for photo in self.photos:
            photometa = h.local_db.find_one({'path': photo})
            if not photometa:
                self.local_db.insert_one({
                    'path': photo,
                    'size': self.photos[photo],
                    'time_in': time.time(),
                    'md5_out': None,
                    'md5sum': None,
                    'md5_in': None,
                    'gphoto_check_out': None,
                    'in_gphotos': None,
                    'gphoto_check_in': None,
                    'queue_out': None,
                    'queue_state': None,
                    'queue_in': None,
                    'gphoto_meta': None
                                     })
                count += 1
            else:
                print("Duplicate in queue:", photometa['path'])
        print("Added {} jobs to work queue. Elapsed time = {:.3f}".format(count, time.time() - checked_start))
        return {'check_done': True}


if __name__ == '__main__':
    h = H()
    # TODO: change to rotating log file with dict config from yaml
    LOG_FILE = h.local_cfg.log_file_base + time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "photolog.txt"
    print(LOG_FILE)
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(
        filename=LOG_FILE,
        format=LOG_FORMAT,
        level=logging.DEBUG,
        filemode='w'
    )
    main()



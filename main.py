import logging
import time
import pymongo
import json
import collections
import glob
import os
import os.path
import yaml
import shutil
from pprint import pprint
from flask import Flask, request
from flask_restful import Resource, Api, reqparse

import gphotos

with open("config.yaml") as f:
    config = yaml.safe_load(f.read())

Gphoto_cfg = collections.namedtuple('Gphoto_cfg', config['gphotos'].keys())
gphoto_cfg = Gphoto_cfg(**config['gphotos'])

Local_cfg = collections.namedtuple('Local_cfg', config['local'].keys())
local_cfg = Local_cfg(**config['local'])

Tq_cfg = collections.namedtuple('Tq_cfg', config['task_queue'].keys())
tq_cfg = Tq_cfg(**config['task_queue'])

# TODO: change to rotating log file with dict config from yaml

LOG_FILE = local_cfg.log_file_base + time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "photolog.txt"
print(LOG_FILE)
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
logging.basicConfig(
    filename=LOG_FILE,
    format=LOG_FORMAT,
    level=logging.DEBUG,
    filemode='w'
)

logging.info("First log message to seed log")
app = Flask(__name__)
api = Api(app)
gphoto_db = None  # Gphotos cloud database reference
local_db = None  # Global local database reference
photos = collections.defaultdict()
gphoto_tools = None


def main():
    global gphoto_db, local_db, gphoto_tools
    gphoto_db = pymongo.MongoClient(host=gphoto_cfg.host)[gphoto_cfg.database][
        gphoto_cfg.collection]  # TODO:  Add fast check that databases are online; start it if we can
    gphoto_tools = gphotos.Gphotos(gphoto_cfg.host, gphoto_cfg.database, gphoto_cfg.collection)
    local_db = pymongo.MongoClient(host=tq_cfg.host)[tq_cfg.database][tq_cfg.archive]
    print("Database=", local_db.full_name)
    print("Syncing Gphoto database...", end='')
    gphoto_tools.sync()
    print("Done")
    app.run(host='127.0.0.1', port=8080, debug=True)


def scan_dir(target):
    global photos
    photos.clear()  # TODO:  Make this a user option???
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
                if file_ext in local_cfg.image_filetypes:  # TODO:  Check here if already in queue.  If so, count and size separately.  Probably put in queue marked as already in Gphotos so tree view will be valid if implemented
                    size = os.stat(path).st_size
                    photos[path] = size
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
    response = json.dumps({'dirs': target_list, 'filecount': len(photos), 'dirsize': "{:.1f} MB".format(dirsize / 1e6),
                           'excluded_exts': excluded_list, 'elapsed_time': "{:.3f}".format(time.time() - start)})
    print("response = ", response)
    return response


def check_photos():  # TODO:  Enable start/stop
    checked_start = time.time()
    count = 0
    for count, photo in enumerate(photos):
        photometa = local_db.find_one({'path': photo})
        if not photometa:
            print("That counts!")
            local_db.insert_one({'path': photo,
                                 'size': photos[photo],
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
        else:
            print("Duplicate in queue:", photometa['path'])
    print("Work queue filled with {} jobs. Elapsed time = {:.3f}".format(count, time.time() - checked_start))
    return {'check_done': True}





# ------------------------------Browser services start here-----------------------------------


@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return 'Hello World!  RESTful interface.  GET POST as necessary'


class DirStats(Resource):
    def post(self):  # TODO:  Clear previous results on dir change??
        print("DirStats reached {}".format(dir(request)))
        json_data = request.get_json(force=True)  # TODO:  Why do we have to force this??
        print("Got JSON data {}".format(json_data))
        directory = json_data['directory'].replace('"', '')  # Remove surrounding quotes to make cut/paste easier
        print("Requested dir: {}".format(directory))
        return scan_dir(directory)


class StatServer(Resource):
    def get(self):
        name = gphoto_db.full_name
        app_ok = 'Up'
        if name:
            db_ok = 'Up'
        else:
            db_ok = 'Down'
        return {'app_ok': app_ok, 'db_ok': db_ok}


class StartCheck(Resource):
    def get(self):
        check_photos()
        return {'checkstatus': 'OK'}


class MoveToQueue(Resource):
    def get(self):
        move_to_queue()
        return {'status': 'OK'}


class CheckStat(Resource):
    def get(self):
        total_files = local_db.count()
        md5_done = local_db.find({'md5sum': {'$ne': None}}).count()
        in_gphotos = local_db.find({'in_gphotos': True}).count()
        in_upload_queue = local_db.find({'queue_state': 'queued'}).count()
        stats = {'total_files': total_files, 'md5_done': md5_done, 'in_gphotos': in_gphotos,
                 'in_upload_queue': in_upload_queue}
        print("Queue Process stats: {}".format(stats))
        return stats


class GetMetadata(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('md5sums', type=str, help="Provide list of MD5 sums as strings")
        arguments = parser.parse_args()
        md5sums = json.loads(arguments['md5sums'])
        print("endpoint", request.path)
        response = {}
        for md5sum in md5sums:
            response[md5sum] = gphoto_db.find_one({'md5Checksum': md5sum}, {'_id': False})
            if request.path == '/members':
                if response[md5sum] is None:
                    response[md5sum] = False
                else:
                    response[md5sum] = True
        return response


class Count(Resource):
    def get(self):
        """Return number of items in database"""
        start = time.time()
        count = gphoto_db.count()
        elapsed = time.time() - start
        return {'elapsed': elapsed, 'count': count}


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


api.add_resource(Count, '/count')
api.add_resource(DirStats, '/dirstats')
api.add_resource(StatServer, '/statserver')
api.add_resource(GetMetadata, '/metadata', '/members')
api.add_resource(StartCheck, '/startcheck')
api.add_resource(CheckStat, '/checkstat')
api.add_resource(MoveToQueue, '/movetoqueue')

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    main()
# [END app]


# class TraverseCheck(Resource):
#     def get(self):
#         start = time.time()
#         found_count = 0
#         missing_count = 0
#         for photo in photos:
#             record = db.find_one({'md5Checksum': photos[photo]})
#             if record:
#                 # print("{} found {}".format(photo, record))
#                 found_count += 1
#             else:
#                 # print("{} not found".format(photo))
#                 missing_count += 1
#                 print("Copying {} to {}".format(photo, GPHOTO_UPLOAD_QUEUE))
#                 shutil.copy2(photo, GPHOTO_UPLOAD_QUEUE)
#             if not (found_count + missing_count) % 100:
#                 print("Processed {}".format(found_count + missing_count))
#         print("Done lookup. Found = {}, Missing = {}, Total ={}, Elapsed = {}, time/record = {}".format(found_count,
#                                                                                                         missing_count,
#                                                                                                         found_count + missing_count,
#                                                                                                         time.time() - start,
#                                                                                                         (
#                                                                                                             time.time() - start) / (
#                                                                                                             found_count + missing_count)))
#         logging.info("Total records saved for {}: {}".format(ARCHIVE_PATH, save_count))

# class sync(Resource):
#     """Sync Google Photos with cloud database"""
#     start = time.time()
#     photos = Gphotos(HOST, DATABASE, GPHOTOS_COLLECTION)
#     return json.dumps(photos.sync())


# @app.route('/sync')
# def sync():
#     """Sync Google Photos with cloud database"""
#     start = time.time()
#     photos = Gphotos(HOST, DATABASE, GPHOTOS_COLLECTION)
#     return json.dumps(photos.sync())
#
# @app.route('/check_membership')
# def check_membership(md5list):  # TODO: How do I pass in arguments??

#
# TODO:  Candidate code below to add database check at this point.  Probably convert photos dict to a dict of named tuples

#     record = db.find_one({'md5Checksum': photos[photo]})
#     if record:
#         # print("{} found {}".format(photo, record))
#         found_count += 1
#     else:
#         # print("{} not found".format(photo))
#         missing_count += 1
#         print("Copying {} to {}".format(photo, GPHOTO_UPLOAD_QUEUE))
#         shutil.copy2(photo, GPHOTO_UPLOAD_QUEUE)
#     if not (found_count + missing_count) % 100:
#         print("Processed {}".format(found_count + missing_count))
#
#
# print("Done lookup. Found = {}, Missing = {}, Total ={}, Elapsed = {}, time/record = {}".format(found_count,
#                                                                                                 missing_count,
#                                                                                                 found_count + missing_count,
#                                                                                                 time.time() - start,
#                                                                                                 (
#                                                                                                     time.time() - start) / (
#                                                                                                     found_count + missing_count)))
# logging.info("Total records saved for {}: {}".format(ARCHIVE_PATH, save_count))
#
#     db_member_start = time.time()
#     found_count = 0
#     missing_count = 0
#     for photo in photos:
#         record = db.find_one({'md5Checksum': photos[photo]})
#         if record:
#             # print("{} found {}".format(photo, record))
#             found_count += 1
#         else:
#             # print("{} not found".format(photo))
#             missing_count += 1
#             print("Copying {} to {}".format(photo, GPHOTO_UPLOAD_QUEUE))
#             shutil.copy2(photo, GPHOTO_UPLOAD_QUEUE)
#         if not (found_count + missing_count) % 100:
#             print("Processed {}".format(found_count + missing_count))
#     logging.info("Done lookup. Found = {}, Missing = {}, Total ={}, Elapsed = {}, time/record = {}".format(found_count,
#                                                                                                     missing_count,
#                                                                                                     found_count + missing_count,
#                                                                                                     time.time() - db_member_start,
#                                                                                                     (
#                                                                                                         time.time() - db_member_start) / (
#                                                                                                         found_count + missing_count)))
#


#     def get_stats(self):
#         answer = {'count': self.db.count()}
#         mime_types = self.db.distinct('mimeType')
#         for mime_type in mime_types:
#             answer[mime_type] = self.db.find({'mimeType': mime_type}).count()
#         return answer
#         # self.db.sales.aggregate([
#         #     {
#         #         '$group': {_id: {day: {$dayOfYear: "$date"}, year: { $year: "$date"}},
#         #                                 totalAmount: { $sum: { $multiply: ["$price", "$quantity"]}},
#         #                     count: { $sum: 1}}}
#         # ])
#
#

#

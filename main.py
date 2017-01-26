# Copyright 2015 Google Inc. All Rights Reserved.

#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]

#This is a change to test git


import logging
import time
import pymongo
import json
from flask import Flask, request
from flask_restful import Resource, Api

HOST = 'mongodb://labroid:mlab14@ds057176.mlab.com:57176/photo-meta'
DATABASE = 'photo-meta'
GPHOTOS_COLLECTION = 'gphotos'
db = None

app = Flask(__name__)
api = Api(app)

#This is a GIT test
#So is this one

def main():
    global db
    db = pymongo.MongoClient(host=HOST)[DATABASE][GPHOTOS_COLLECTION]
    app.run(host='127.0.0.1', port=8080, debug=True)


@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return 'Hello World!  RESTful interface.  GET POST as necessary'

# class check_memberships(Resource):
#     def get(self, md5sums):
#         response = {}
#         for md5sum in md5sums:
#             response[md5sum] = db.find_one({'md5Sum': md5sum})
#         return response

#    def put(self, todo_id):
#        pass
#        return {todo_id: todo_id}

#Test change

class count(Resource):
    def get(self):
        """Return number of items in database"""
        start = time.time()
        count = db.count()
        elapsed = time.time() - start
        return ({'elapsed': elapsed, 'count': count})

class stats(Resource):
    def get(self):
        """Return program status."""
        return {'count': 0, 'size': 10}

#@app.route('/count')
#def count():
    # """Return number of items in database"""
    # start = time.time()
    # db = pymongo.MongoClient(host=HOST)[DATABASE][GPHOTOS_COLLECTION]
    # count = db.count()
    # elapsed = time.time() - start
    # # print("Time to get count: {}".format(elapsed))
    # # return 'Total objects in database: {}, Time to get count: {}'.format(count, elapsed)
    # return(json.dumps({'elapsed': elapsed, 'count': count}))

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



@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500

#api.add_resource(photo, 'photo/<string:md5sum>')
api.add_resource(count, '/count')
api.add_resource(stats, '/stats')

# class Gphotos(object):
#     """
#     Gphotos:  A set of tools to aid management of local images and a Google Photos repository
#     """
#     def __init__(self, host, database, collection):
#         self.service = None
#         self.db = pymongo.MongoClient(host=host)[database][collection]
#         self.db.create_index('id')
#         self.db.create_index('md5Checksum')
#
#     def sync(self):
#         """
#         Synchronize database with google photos
#         """
#
#         if self.service is None:
#             self.get_service()
#
#         # TODO:  Make sure we don't 'find' files that are marked as trashed
#         database_changed = False
#         db_full_resync = False
#         new_count = 0
#         delete_count = 0
#
#         INIT_FIELDS = "files(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed), nextPageToken"
#         change_token_cursor = self.db.find({'change_token': {'$exists': True}})
#         assert change_token_cursor.count() <= 1  # TODO:  What do we do about assertion failures in a RESTful service?
#
#         if change_token_cursor == 0:
#             db_full_resync = True
#         if db_full_resync:  # If we have no change token, drop and resync the database
#             logging.info("No change token available - resyncing database")
#             self.db.drop()
#             database_changed = True
#             next_page_token = None
#             while True:
#                 file_list = self.service.files().list(pageToken=next_page_token,  #TODO:  Maybe exclude trashed here...what does includeRemoved default to?
#                                                  spaces='photos',
#                                                  pageSize=1000,
#                                                  fields=INIT_FIELDS).execute()
#                 if 'files' in file_list:
#                     file_count = len(file_list['files'])
#                 else:
#                     file_count = 0
#                 logging.info("Google sent {} records".format(file_count))
#                 db_status = self.db.insert_many(file_list.get('files'))
#                 logging.info("Mongodb stored {} records".format(len(db_status.inserted_ids)))
#                 assert file_count == len(  # TODO:  Do I need to do this or will Mongo throw exception on failure?
#                     db_status.inserted_ids), "Records stored != records from gPhotos.  Got {} gPhotos and {} ids".format(
#                     file_count, len(db_status.inserted_ids))
#                 if 'nextPageToken' in file_list:
#                     next_page_token = file_list['nextPageToken']
#                 else:
#                     break
#             # Once db is updated with all changes, get initial change token
#             change_token = self.service.changes().getStartPageToken().execute()
#             self.db.insert({'change_token': change_token['startPageToken']})
#         else:
#             logging.info('Have change token; updating database.')
#             change_token = change_token_cursor[0]['change_token']
#             UPDATE_FIELDS = 'changes(file(id,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed),fileId,removed,time),kind,newStartPageToken,nextPageToken'
#             while True:
#                 changes = self.service.changes().list(pageToken=change_token,
#                                                  spaces='photos',
#                                                  pageSize=1000,
#                                                  includeRemoved=True,
#                                                  fields=UPDATE_FIELDS).execute()
#                 change_count = len(changes.get('changes', []))
#                 logging.info("Google sent {} records".format(change_count))
#                 if change_count:
#                     database_changed = True
#                     for change in changes['changes']:
#                         if change['removed'] is True:
#                             db_status = self.db.delete_one({'id': change['fileId']})
#                             assert db_status.deleted_count == 1, "Deleted files count should be 1, got {}".format(
#                                 db_status.deleted_count)
#                             delete_count += 1
#                         else:
#                             db_status = self.db.replace_one({'id': change['file']['id']}, change['file'],
#                                                        upsert=True)  # TODO:  Make sure the data that comes with change is complete for insertion
#       #                      assert db_status.modified_count == 1, "Modified files count should be 1, got {}".format(
#       #                          db_status.modified_count)
#                             new_count += 1
#                 if 'nextPageToken' in changes:
#                     change_token = changes['nextPageToken']
#                 else:
#                     assert 'newStartPageToken' in changes, "newStartPageToken missing when nextPageToken is missing.  Should never happen."
#                     db_status = self.db.replace_one({'change_token': {'$exists': True}},
#                                                {'change_token': changes['newStartPageToken']})
#                     assert db_status.modified_count == 1, "Database did not update correctly"
#                     break  # All changes have been received
#             logging.info("Sync update complete.  New files: {} Deleted files: {}".format(new_count, delete_count))
#         full_count = self.db.count()
#         logging.info("Total records: {}".format(full_count))
#         logging.info('Done with database resync')
#
#         if database_changed:
#             self.__get_parents()
#             root_id = self.service.files().list(q='name="Google Photos"').execute()['files'][0]['id']
#             self.__set_paths(root_id, ['Google Photos'])
#             logging.info('Done set_paths')
#
#         return {'db_full_resync': db_full_resync, 'full_count': full_count, 'new_count': new_count, 'delete_count': delete_count}
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
#     def __get_parents(self):
#         """
#         Populate database entries for parent folders
#         :return: None.  Changes database
#         """
#         # TODO:  This delivers a datbase record with "My Drive" in it.  That is too high in the tree.....
#
#         if self.service is None:
#             self.get_service()
#
#         parents_needed = set(self.db.distinct('parents'))  # Seed not_in_db_set with all parents assuming none are present
#         ids_in_db = set(self.db.distinct('id'))
#         parents_needed.difference_update(ids_in_db)
#         while parents_needed:
#             parent_id = parents_needed.pop()
#             parent_meta = self.service.files().get(fileId=parent_id, fields='id,kind,md5Checksum,mimeType,name,ownedByMe,parents,size,trashed').execute()
#             self.db.insert(parent_meta)  #TODO Check write was successful?
#             ids_in_db.add(parent_id)
#             for parent in parent_meta.get('parents') or []:
#                 if parent not in ids_in_db:
#                     parents_needed.add(parent)
#         logging.info('Done getting parents')
#
#
#     def __set_paths(self, id, path):
#         """
#         Sets path ids for folders
#         :param id: Google Drive id of Google Photos folder
#         :param path: Google Drive path to file with Google Drive id
#         :return: None. Adds path to each folder in Google Photos
#         """
#         children = self.db.find({'mimeType': 'application/vnd.google-apps.folder', 'parents': id})
#         self.db.update_one({'id': id}, {'$set': {'path': path}})
#         if children.count() != 0:
#             for child in children:
#                 my_name = self.db.find_one({'id': child['id']})['name']
#                 path.append(my_name)
#                 self.__set_paths(child['id'], path)
#                 path.pop()
#
#
#     def get_service(self):
#         credentials = self.get_credentials()
#         http = credentials.authorize(httplib2.Http())
#         self.service = discovery.build('drive', 'v3', http=http)
#
#
#     def get_credentials(self):
#         """Gets valid user credentials from storage.
#
#         If nothing has been stored, or if the stored credentials are invalid,
#         the OAuth2 flow is completed to obtain the new credentials.
#
#         Returns:
#             Credentials, the obtained credential.
#         """
#         SCOPES = 'https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.photos.readonly'
#         CLIENT_SECRET_FILE = 'client_secret.json'
#         APPLICATION_NAME = 'Other Client 1'
#
#         try:
#             import argparse
#             flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
#         except ImportError:
#             flags = None
#
#         home_dir = os.path.expanduser('~')
#         credential_dir = os.path.join(home_dir, '.credentials')
#         if not os.path.exists(credential_dir):
#             os.makedirs(credential_dir)
#         credential_path = os.path.join(credential_dir,
#                                        'drive-batch.json')
#
#         store = oauth2client.file.Storage(credential_path)
#         credentials = store.get()
#         if not credentials or credentials.invalid:
#             flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
#             flow.user_agent = APPLICATION_NAME
#             if flags:
#                 credentials = tools.run_flow(flow, store, flags)
#             else:  # Needed only for compatibility with Python 2.6
#                 credentials = tools.run(flow, store)
#             #print('Storing credentials to ' + credential_path)
#         return credentials


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    main()
# [END app]

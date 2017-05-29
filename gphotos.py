import pymongo
import logging
import os
import yaml

import httplib2
from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

from utils import cfg_obj

with open("config.yaml") as f:
    config = yaml.safe_load(f.read())

gphoto_cfg = cfg_obj(config, 'gphotos')
local_cfg = cfg_obj(config, 'local')
# tq_cfg = cfg_obj(config, 'task_queue')


class Gphotos(object):
    """
    Gphotos:  A set of tools to aid management of local images and a Google Photos repository
    """

    def __init__(self, host=gphoto_cfg.host, database=gphoto_cfg.database, collection=gphoto_cfg.collection):
        self.service = None
        self.db = pymongo.MongoClient(host=host)[database][collection]
        self.db.create_index('id')
        self.db.create_index('md5Checksum')
        self.SCOPES = 'https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.photos.readonly'
        self.CLIENT_SECRET_FILE = 'client_secret.json'
        self.APPLICATION_NAME = 'Other Client 1'
        self.cred_store = None
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       'drive-batch.json')
        self.cred_store = oauth2client.file.Storage(credential_path)

    def credentials_ok(self):
        credentials = self.cred_store.get()
        if not credentials or credentials.invalid:
            return False
        else:
            return True

    def sync(self):
        """
        Synchronize database with google photos
        """

        if self.service is None:
            self.get_service()

        # TODO:  Make sure we don't 'find' files that are marked as trashed
        database_changed = False
        db_full_resync = False
        new_count = 0
        delete_count = 0

        INIT_FIELDS = "files(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed), nextPageToken"
        change_token_cursor = self.db.find({'change_token': {'$exists': True}})
        #assert change_token_cursor.count() <= 1  # Commented out as this was way too expensive call to Google cloud

        if change_token_cursor is None:  # If we have no change token, drop and resync the database
            db_full_resync = True
            logging.info("No change token available - resyncing database")
            self.db.drop()
            database_changed = True
            next_page_token = None
            while True:
                file_list = self.service.files().list(pageToken=next_page_token,
                                                      # TODO:  Maybe exclude trashed here...what does includeRemoved default to?
                                                      spaces='photos',
                                                      pageSize=1000,
                                                      fields=INIT_FIELDS).execute()
                if 'files' in file_list:
                    file_count = len(file_list['files'])
                else:
                    file_count = 0
                logging.info("Google sent {} records".format(file_count))
                db_status = self.db.insert_many(file_list.get('files'))
                logging.info("Mongodb stored {} records".format(len(db_status.inserted_ids)))
                next_page_token = file_list.get('nextPageToken')
                if next_page_token is None:
                    break
            # Once db is updated with all changes, get initial change token
            change_token = self.service.changes().getStartPageToken().execute()
            self.db.insert({'change_token': change_token['startPageToken']})
        else:
            logging.info('Have change token; updating database.')
            change_token = change_token_cursor[0]['change_token']
            UPDATE_FIELDS = 'changes(file(id,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed),fileId,removed,time),kind,newStartPageToken,nextPageToken'
            while True:
                changes = self.service.changes().list(pageToken=change_token,
                                                      spaces='photos',
                                                      pageSize=1000,
                                                      includeRemoved=True,
                                                      fields=UPDATE_FIELDS).execute()
                change_count = len(changes.get('changes', []))
                logging.info("Google sent {} records".format(change_count))
                if change_count:
                    database_changed = True
                    for change in changes['changes']:
                        if change['removed'] is True:
                            self.db.delete_one({'id': change['fileId']})
                            delete_count += 1
                        else:
                            # TODO:  Make sure the data that comes with change is complete for insertion
                            self.db.replace_one({'id': change['file']['id']}, change['file'], upsert=True)
                            new_count += 1
                if 'nextPageToken' in changes:
                    change_token = changes['nextPageToken']
                else:
                    assert 'newStartPageToken' in changes, "newStartPageToken missing when nextPageToken is missing.  Should never happen."
                    self.db.replace_one({'change_token': {'$exists': True}},
                                        {'change_token': changes['newStartPageToken']})
                    break  # All changes have been received
            logging.info("Sync update complete.  New files: {} Deleted files: {}".format(new_count, delete_count))
        full_count = self.db.count()
        logging.info("Total records: {}".format(full_count))
        logging.info('Done with database resync')

        if database_changed:
            self.__get_parents()
            root_id = self.service.files().list(q='name="Google Photos"').execute()['files'][0]['id']
            self.__set_paths(root_id, ['Google Photos'])
            logging.info('Done set_paths')

        return {'db_full_resync': db_full_resync, 'full_count': full_count, 'new_count': new_count,
                'delete_count': delete_count}

    def get_service(self):
        credentials = self.get_credentials()
        http = credentials.authorize(httplib2.Http())
        self.service = discovery.build('drive', 'v3', http=http)

    def get_credentials(self):  # TODO:  This still won't reset itself.
        """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
            Credentials, the obtained credential.
        """

        try:
            import argparse
            flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        except ImportError:
            flags = None

        credentials = self.cred_store.get()
        if not credentials or credentials.invalid:
            raise ValueError("Gotta fix this")
            # flow = client.flow_from_clientsecrets(self.CLIENT_SECRET_FILE, scope=self.SCOPES,
            #                                       redirect_uri=redirect_uri)
            # flow = client.flow_from_clientsecrets(self.CLIENT_SECRET_FILE, self.SCOPES)
            # flow.user_agent = self.APPLICATION_NAME
            # if flags:
            #     credentials = tools.run_flow(flow, store, flags)
            # else:  # Needed only for compatibility with Python 2.6
            #     credentials = tools.run(flow, store)
            #     # print('Storing credentials to ' + credential_path)
        return credentials

    def __get_parents(self):
        """
        Populate database entries for parent folders
        :return: None.  Changes database
        """
        # TODO:  This delivers a datbase record with "My Drive" in it.  That is too high in the tree.....

        if self.service is None:
            self.get_service()

        parents_needed = set(self.db.distinct('parents'))  # Seed not_in_db_set with all parents assuming none are present
        ids_in_db = set(self.db.distinct('id'))
        parents_needed.difference_update(ids_in_db)
        while parents_needed:
            parent_id = parents_needed.pop()
            parent_meta = self.service.files().get(fileId=parent_id, fields='id,kind,md5Checksum,mimeType,name,ownedByMe,parents,size,trashed').execute()
            self.db.insert(parent_meta)
            ids_in_db.add(parent_id)
            for parent in parent_meta.get('parents') or []:
                if parent not in ids_in_db:
                    parents_needed.add(parent)
        logging.info('Done getting parents')

    def __set_paths(self, id, path):
        """
        Sets path ids for folders
        :param id: Google Drive id of Google Photos folder
        :param path: Google Drive path to file with Google Drive id
        :return: None. Adds path to each folder in Google Photos
        """
        children = self.db.find({'mimeType': 'application/vnd.google-apps.folder', 'parents': id})
        self.db.update_one({'id': id}, {'$set': {'path': path}})
        if children.count() != 0:
            for child in children:
                my_name = self.db.find_one({'id': child['id']})['name']
                path.append(my_name)
                self.__set_paths(child['id'], path)
                path.pop()

    def check_member(self, md5):
        """
        If md5 is in Google Photos returns associated Gphoto metadata, otherwise returns None
        :param md5: MD5 sum of record possibly on Google Photos
        :return: dict of matching Google Photo metadata, returns None if not in Google Photos
        """
        meta = self.db.find_one({'md5Checksum': md5, 'trashed': False, 'explicitlyTrashed': False})
        if meta is not None:
            gphoto_path = os.path.join(*(self.db.find_one({'id': meta['parents'][0]})['path']))
            meta.update({'gpath': gphoto_path})
        return meta


    def server_stat(self):
        if self.db.full_name:
            return True
        else:
            return False

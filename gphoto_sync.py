import logging
from logging.config import dictConfig
import os

import mongoengine as me
import oauth2client
import yaml
from apiclient import discovery
from oauth2client import tools
import httplib2

from models import Gphoto, Gphoto_change, Gphoto_parent
from utils import Config

cfg = Config()
me.connect(db=cfg.gphotos.database, host=cfg.gphotos.host, alias=cfg.gphotos.gphoto_db_alias)


class Gphotos(object):
    """
    Gphotos:  A set of tools to synch a mongodb database to gphotos
    """

    def __init__(self):
        dictConfig(cfg.logging)  #TODO: Configure logging; remove print statements
        self.log = logging.getLogger(__name__)
        self.SCOPES = 'https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.photos.readonly'
        self.CLIENT_SECRET_FILE = 'client_secret.json'
        self.APPLICATION_NAME = 'Other Client 1'
        self.cred_store = None
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, 'drive-batch.json')
        self.cred_store = oauth2client.file.Storage(credential_path)
        self.service = None

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
            self.__get_service()

        database_changed = False
        db_full_resync = False
        new_count = 0
        delete_count = 0
        INIT_FIELDS = "files(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,trashed), nextPageToken"

        change_token_cursor = Gphoto_change.objects(type='change_start_page_token')
        # TODO: May be better to make unique in the database Make it default to None and check for None here
        assert change_token_cursor.count() <= 1, "Assertion failure: More than one Gphoto change_token in database"
        if change_token_cursor.count() > 0:
            change_token = change_token_cursor.first().value
        else:
            change_token = None
        if change_token is None:  # If we have no change token, drop and resync the database
            logging.info("No change token available - resyncing database")
            db_full_resync = True
            database_changed = True
            next_page_token = None
            Gphoto.drop_collection()
            while True:
                file_list = self.service.files().list(pageToken=next_page_token,
                                                      spaces='photos',
                                                      pageSize=1000,
                                                      fields=INIT_FIELDS).execute()
                file_count = len(file_list.get('files', []))
                logging.info("Google sent {} records".format(file_count))

                insert_list = []
                for item in file_list.get('files'):
                    if 'id' in item:  # Change all instance of key id to gid because mongoengine reserves id
                        item['gid'] = item.pop('id')
                    if 'size' in item: # mongoengine also reserves 'size' in modify operations
                        item['gsize'] = item.pop('size')
                    insert_list.append(Gphoto(**item))
                Gphoto.objects.insert(insert_list)
                next_page_token = file_list.get('nextPageToken')
                if next_page_token is None:
                    break
            # Once db is updated with all changes, get change token and save
            change_token = self.service.changes().getStartPageToken().execute()
            Gphoto_change.objects(type='change_start_page_token').modify(value=change_token['startPageToken'],
                                                                         upsert=True)
        else:
            logging.info('Have change token; updating database.')
            UPDATE_FIELDS = 'changes(file(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,trashed),fileId,removed),newStartPageToken,nextPageToken'
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
                            Gphoto.objects(gid=change['fileId']).delete()
                            delete_count += 1
                        else:
                            if 'id' in change['file']:  # Mongoengine reserves 'id'
                                change['file']['gid'] = change['file'].pop('id')
                            if 'size' in change['file']:  # Mongoengine reserves 'size' on modify command
                                change['file']['gsize'] = change['file'].pop('size')
                            Gphoto.objects(gid=change['file']['gid']).update_one(**change['file'], upsert=True)
                            new_count += 1
                            # print("Added #{}: {}".format(new_count, change.get('file').get('name')))
                if 'nextPageToken' in changes:
                    change_token = changes['nextPageToken']
                else:
                    assert 'newStartPageToken' in changes, "newStartPageToken missing when nextPageToken is missing.  Should never happen."
                    Gphoto_change.objects(type='change_start_page_token').update(value=changes['newStartPageToken'],
                                                                                 upsert=True)
                    break  # All changes have been received
            logging.info("Sync update complete.  New files: {} Deleted files: {}".format(new_count, delete_count))
        full_count = Gphoto.objects.count()
        logging.info("Done with database resync. Total records: {}".format(full_count))

        if database_changed:
            self.__get_parents()
            root_id = self.service.files().list(q='name="Google Photos"').execute()['files'][0]['id']
            self.__set_paths(root_id, ['Google Photos'])
            logging.info('Done set_paths')

        return {'db_full_resync': db_full_resync, 'full_count': full_count, 'new_count': new_count,
                'delete_count': delete_count} #Not sure who uses all this info

    def __get_service(self):
        credentials = self.__get_credentials()
        http = credentials.authorize(httplib2.Http())
        self.service = discovery.build('drive', 'v3', http=http)

    def __get_credentials(self):
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
            self.__get_service()

        parents_needed = set(
            Gphoto.objects.distinct('parents'))  # Seed not_in_db_set with all parents assuming none are present
        ids_in_parent_db = set(Gphoto_parent.objects.distinct('gid'))
        parents_needed.difference_update(ids_in_parent_db)
        while parents_needed:
            parent_id = parents_needed.pop()  # TODO:  Following query could be bulk query
            parent_meta = self.service.files().get(fileId=parent_id,
                                                   fields='id,mimeType,name,ownedByMe,parents,trashed').execute()  # TODO:  Is all this needed?? Handle trashed correctly?
            if 'id' in parent_meta:  # Change all instance of key id to gid because mongoengine reserves id
                parent_meta['gid'] = parent_meta.pop('id')
            Gphoto_parent(**parent_meta).save()
            # self.db.insert(parent_meta)
            ids_in_parent_db.add(parent_id)
            for parent in parent_meta.get('parents') or []:
                if parent not in ids_in_parent_db:
                    parents_needed.add(parent)
        logging.info('Done getting parents')

    def __set_paths(self, gid, path):
        """
        Sets path ids for folders
        :param gid: Google Drive id of Google Photos folder
        :param path: Google Drive path to file with Google Drive id
        :return: None. Adds path to each folder in Google Photos
        """
        logging.info('Setting path for {}'.format(path))
        Gphoto_parent.objects(gid=gid).update(path=path)
        children = Gphoto_parent.objects(parents=gid)
        if children.count() != 0:
            for child in children:
                my_name = Gphoto_parent.objects.get(gid=child.gid).name
                path.append(my_name)
                self.__set_paths(child['gid'], path)
                path.pop()

    def server_stat(self): # TODO: This needs to be a service that answers if sync is running.  Should it return refresh logs??
        db = me.Document._get_db()
        client_count = db.command("serverStatus") # TODO: Use debugger to see what I get here

        if client_count['something']:  # TODO:  Fix me!
            return True
        else:
            return False

def main():
    gp = Gphotos()
    # print("Logging to:{}".format(gp.log)) # TODO: fix this once logging is enabled
    if not gp.credentials_ok():
        print("Go get new credentials")
    gp.sync()

if __name__ == '__main__':
    main()


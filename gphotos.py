import logging
from logging.config import dictConfig
import os

import mongoengine as me
import yaml

from utils import Config
from models import Gphoto, Gphoto_parent

# Constants
GPHOTO_DB_ALIAS = 'gphotos'
cfg = Config()


me.connect(db=cfg.gphotos.database, host=cfg.gphotos.host, alias=GPHOTO_DB_ALIAS)


class Gphotos(object):
    """
    Gphotos:  Tools to aid management of local images and a Google Photos repository
    """
    def __init__(self):
        dictConfig(cfg.logging)
        self.log = logging.getLogger(__name__)

    def check_member(self, md5):
        """
        Return True if md5 in Gphotos db, False if not
        :return: Boolean
        """
        if Gphoto.objects(me.Q(md5Checksum=md5) & me.Q(trashed=False)).count() > 0:
            return True
        else:
            return False

    def get_metadata(self, md5):
        """
        If md5 is in Google Photos returns associated Gphoto metadata, otherwise returns None
        :param md5: MD5 sum of record possibly on Google Photos
        :return: dict of matching Google Photo metadata and parent path, returns None if not in Google Photos
        """
        queryset = Gphoto.objects(me.Q(md5Checksum=md5) & me.Q(trashed=False))
        hit_count = queryset.count()
        if hit_count > 0:
            if hit_count > 1:
                self.log.warning('More than one gphoto entry for MD5 = {}; got {} hits.'.format(md5, hit_count))
            photo_meta = queryset.first()
            parent = photo_meta.parents[0]
            if parent is not None:
                parent_meta = Gphoto_parent.objects(gid=parent)
                parent_count = parent_meta.count()
                if parent_count < 1:
                    self.log.debug('No parent record for MD5 {} gid {}'.format(md5, parent))
                    path = None
                elif parent_count >= 1:
                    if parent_count > 1:
                        self.log.debug('More than one parent record for MD5 {} gid {}'.format(md5, parent))
                    record = parent_meta.first()
                    path = os.path.join(*record.path)
                photo_meta = photo_meta.to_mongo().to_dict()
                photo_meta.update({'gphotos_path': path})
            else:
                self.log.error('{} in Gphoto database has no parent'.format(photo_meta.gid))
                raise ValueError('{} in Gphoto database has no parent'.format(photo_meta.gid))

            # gphoto_path = os.path.join(*(self.db.find_one({'id': meta['parents'][0]})['path']))
            return photo_meta
        else:
            return None

    def server_stat(self):  # TODO: This probably should only be in the server
        db = me.Document._get_db()
        client_count = db.command("serverStatus")  # TODO: Use debugger to see what I get here

        if client_count['something']:  # TODO:  Fix me!
            return True
        else:
            return False

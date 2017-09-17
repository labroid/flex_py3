import logging
from logging.config import dictConfig
import time
import os
import sys
import os.path
import yaml
import fs
from fs.osfs import OSFS
from fs.path import basename
from pathlib import Path
import mongoengine as me
from gphotos import Gphotos
import shutil

from utils import cfg_obj, file_md5sum

class Photo(me.Document):  # TODO:  Consider putting these in utils or models
    path = me.StringField(default=None, required=True)
    size = me.IntField(default=None)
    md5sum = me.StringField(default=None)
    in_gphotos = me.BooleanField(default=False)
    queue_state = me.StringField(default=None, choices=[None, 'enqueued', 'done'])
    gphoto_meta = me.DictField(default=None)

me.connect(db='metest')

def main():
    """
    Queue maintenance runs continuously. Analyze queue before adding new files (since we need to know if files
    are already in the queue as well as already in gphotos before adding them).

    for each file in queue
        put file in db and mark as in queue
    for each file in db
        update MD5 sum
        update gphoto membership
        mirror files already in gphotos
        remove files in gphotos from queue and mark done in db
        purge files if source still avalable
    check for new files to be added (separate process or database for selecting and adding?)
        Get dir list from user
        Put stats from dir list in fresh db
        Copy upload candidates to gphoto queue.  Assure no name collision by unique directory name.

    for photos in not done:
        if in Gphotos:
            copy to mirror
            remove from queue
            remove from source
            mark state 'done'
        else:
            if queue_state = None:
                    move to queue
                    mark state queued
    :return:
    """
    QueueWorker()


class QueueWorker:
    def __init__(self):
        with open("config.yaml") as f:
            config = yaml.safe_load(f.read())
        self.local_cfg = cfg_obj(config, 'local')
        self.tq_cfg = cfg_obj(config, 'task_queue')
        dictConfig(config['logging'])
        self.log = logging.getLogger(__name__)

        self.photo_queue = OSFS(self.local_cfg.gphoto_upload_queue)
        self.mirror = OSFS(self.local_cfg.mirror_root)
        self.gphotos = Gphotos()
        self.sync_db_to_queue()
        self.process_queue()

    def process_queue(self):  # TODO:  Make this async
        self.gphotos.sync()  # TODO:  Make this async
        for photo in Photo.objects(me.Q(md5sum__ne=None) & me.Q(in_gphotos=False)):
            self.check_gphotos()  # TODO:  Make this async
        for photo in Photo.objects(md5sum=None):
            photo.md5sum = file_md5sum(photo.path)  # TODO: Make this async
            photo.save()
            self.check_gphotos(photo)
        for photo in Photo.objects(queue_state__ne='done'):  # TODO:  Why is there a done? To keep count I think.
            if photo.in_gphotos:
                self.dequeue(photo)
            else:
                if photo.queue_state == None:
                    self.enqueue(photo)

    def check_gphotos(self, photo):
        if photo.gphoto_meta is None:
            photo.gphoto_meta = self.gphotos.get_metadata(photo.md5sum)  # TODO: Make this async
        if photo.gphoto_meta:
            photo.in_gphotos = True
        else:
            photo.in_gphotos = False
        photo.save()

    def sync_db_to_queue(self):
        Photo.drop_collection()  #TODO:  Make more efficient by not dropping but checking size/mtime for changes
        for path, info in self.photo_queue.walk.info(namespaces=['details']):
            if info.is_file:
                photo = Photo()
                # photo.path = os.path.join(self.photo_queue.root_path, basename(path))
                photo.path = self.photo_queue.getsyspath(path)
                photo.size = info.size
                photo.queue_state = 'enqueued'
                photo.save()
        # TODO:  Kick off process_queue

    def enqueue(self, photo):
        dest = os.path.join(self.photo_queue.root_path, os.path.splitdrive(photo.path)[1])
        if os.path.isfile(dest): # Check for name collision
            path_parts = os.path.split(dest)
            dest = os.path.join(path_parts[0], 'duplicate_name', path_parts[1])
            if os.path.isfile(dest):  # If still have name collision after putting in dup name dir, give up
                logging.warning("Photo queue name {} exists; skipping".format(dest))
        logging.info(photo['path'], "not in gphotos, non-dup, putting in queue")
        shutil.copy2(photo.path, dest)
        photo.queue_state = 'enqueued'
        photo.save()

    def dequeue(self, photo):
        """
        Copy photo to mirror, remove from Google photos upload queue, purge from source
        :param photo: photo object from queue database
        :param queue_filenames: base filenames from photo queue
        :return:
        """
        self.mirror(photo)
        self.rm_from_gphoto_queue(photo)
        self.purge(photo)
        #remove from database

    def mirror(self, photo):  # TODO: Something seems wrong - database does not have parents in it.
        dest = photo.gphoto_meta['parent'] # TODO: Need google path here
    #
    # def rm_from_gphoto_queue(self, photo):
    #     pass
    #     # Check MD5 to be sure before removing?
    #
    #
    # def purge(self, photo):
    #     pass
    #
    #
    #     if os.path.basename(photo['path']) in queue_filenames:
    #         gpath = photo.get('archive_meta').get('gpath')
    #         if gpath:
    #             os.makedirs(os.path.join(self.local_cfg.gphoto_root, gpath), exist_ok=True)
    #             source = os.path.join(self.local_cfg.gphoto_upload_queue, os.path.basename(photo['path']))
    #             dest = os.path.join(self.local_cfg.gphoto_root, gpath, os.path.basename(photo['path']))
    #             if not Path(dest).exists():
    #                 try:
    #                     os.rename(source, dest)
    #                     self.mark_done(photo, "mirrored")
    #                     print("Moved {} to {}".format(source, dest))
    #                 except:
    #                     print("File access error:", sys.exc_info()[0])  # If contention for file, catch it on next cycle
    #     else:
    #         self.mark_done(photo, "done")
    #
    # def mark_done(self, photo, state):
    #     self.local_db.update_one({'path': photo['path']}, {'$set': {'in_gphotos': True, 'queue_state': state}})


if __name__ == '__main__':
    main()
import logging
from logging.config import dictConfig
import time
import os
import sys
import os.path
import fs
from fs.osfs import OSFS
from fs.path import basename, normpath

from pathlib import Path
import mongoengine as me
from gphotos import Gphotos
import shutil

from utils import cfg_obj, file_md5sum, Config
from models import Db_connect, Queue, Candidates, State

Db_connect()


def main():
    initialize_state()
    QueueWorker()
    print("Main Done")
    """
    Queue maintenance runs continuously. Analyze queue before adding new files (since we need to know if files
    are already in the queue as well as already in gphotos to know if we want to add them to the queue).

    drop queue db (in future make durable and check)
    for each file in gphotos queue directory
        put file stats in queue db and mark as in queue
    while True: (async loop?)
        for each file in queue db
            update missing MD5 sums
            update missing gphoto membership
            mirror files already in gphotos and not mirrored
            optionally purge files already in gphotos if source still avalable
            remove files in gphotos from queue and mark done in db
        check for new files to be added (separate process or database for selecting and adding?)
            Get dir list from user and add to candidates
            Update missing MD5 sums for candidates
            update missing gphoto membership for queue
            if not in gphotos and not in gphoto queue add to gphoto queue
            mirror files already in gphotos and not mirrored
            Add upload candidates to gphoto queue.  Assure no name collision by unique directory name.

    *******Async try1*******

    drop queue db (TODO: make durable and check)
    for each file in gphotos queue directory
        put file stats in queue db and mark as in queue

    async process_files()
        get next file not in process
        mark db as in process
        await purge_file()

    async purge_file():
        await mirror_file()
        if purge_enabled:
            delete file

    async mirror_file():
        await file_in_gphotos()
        if mirror_enabled:
            copy to mirror()

    async file_in_gphotos():
        await get_md5()
        await check_gphotos()

*******Async try 2########
    drop queue db (in future make durable and check)
    drop candidates db
    for each file in gphotos queue directory
        put file stats in queue db and mark as in queue

    async queue_worker():
        while True:
            for each unprocessed file in queue db
                mark file as processing
                await update missing MD5 sums(_id list)
                await update missing gphoto membership(md5 list)
                await mirror files already in gphotos and not mirrored
                await optionally purge files already in gphotos if source still avalable
                await remove files in gphotos from queue and mark done in db

    async candidate_worker():
        while True:
            check for new files to be added (separate process or database for selecting and adding?)
                await Get dir list from user and add file stats to candidate
                await Update missing MD5 sums for candidates
                await update missing gphoto membership for candidates
                await if not in gphotos and not in gphoto queue add to gphoto queue
                await mirror files already in gphotos and not mirrored
                await Add upload candidates to gphoto queue.  Assure no name collision by unique directory name.
                await optionally purge files in gphotos if source still available
                await remove files from candidates that are mirrored and in gphotos

    """


def initialize_state():
    State.drop_collection()
    state = State()
    state.dirlist = ""
    state.purge_ok = True
    state.mirror_ok = True
    state.save()


class QueueWorker:
    def __init__(self):
        self.cfg = Config()
        dictConfig(self.cfg.logging)
        self.log = logging.getLogger(__name__) #TODO:  Logging not correctly configured

        self.photo_queue = OSFS(self.cfg.local.gphoto_upload_queue)
        self.mirror = OSFS(self.cfg.local.mirror_root)
        self.gphotos = Gphotos()
        self.sync_db_to_queue()
        self.process_queue()

    def sync_db_to_queue(self):
        print('Creating db synced to queue')
        Queue.drop_collection()  # TODO:  Make more efficient by not dropping but checking size/mtime for changes
        for path, info in self.photo_queue.walk.info(namespaces=['details']):
            if info.is_file:
                photo = Queue()  # TODO:  Photo here is hosted remotely, not locally. Need to redefine where the databases are
                photo.queue_path = path
                photo.src_path = None #TODO: Don't know source if we start fresh; maybe make this smarter
                photo.size = info.size
                photo.queue_state = 'enqueued'
                photo.save()
        print('Done syncing db to queue')

    def process_queue(self):  # TODO:  Make this async
        while True:
            # self.update_fstats(Queue.objects(size=None))
            self.update_md5s(Queue.objects(md5sum=None))
            self.check_gphotos(Queue.objects(me.Q(md5sum__ne=None) & me.Q(in_gphotos=False)))
            self.dequeue(Queue.objects(me.Q(in_gphotos=True) & me.Q(queue_state__ne='done')))
            print("Waiting...")
            time.sleep(5)
    #         self.enqueue()

    def update_md5s(self, photos):
        for photo in photos:
            photo.md5sum = file_md5sum(self.photo_queue.getsyspath(photo.queue_path))  # TODO: Make this async
            photo.save()
        print("MD5 Done")

    def check_gphotos(self, photos):
        for photo in photos:
            photo.gphoto_meta = self.gphotos.get_metadata(photo.md5sum)  # TODO: Make this async
            if photo.gphoto_meta:
                photo.in_gphotos = True
            else:
                photo.in_gphotos = False
            photo.save()
        print("Check Gphotos done")

    def mirror_file(self, photo):
        dest_dir = photo.gphoto_meta['gphotos_path']
        dest_path = fs.path.combine(dest_dir, photo.gphoto_meta['originalFilename'])
        if not self.mirror.isfile(fs.path.normpath(dest_path)):
            self.mirror.makedirs(dest_dir, recreate=True)
            fs.copy.copy_file(self.photo_queue, photo.queue_path, self.mirror, dest_path) #TODO: No error trapping; let's see how this works
            self.log.info("Mirrored {} to {}".format(photo.queue_path, dest_path))

    def dequeue(self, photos):
        for photo in photos:
            if State.objects.get().mirror_ok:
                self.mirror_file(photo)
            if self.photo_queue.isfile(photo.queue_path):
                print("This is where we would delete from queue {}".format(photo.queue_path))
                self.photo_queue.remove(photo.queue_path)
            if State.objects.get().purge_ok and photo.src_path and os.path.isfile(photo.src_path):
                print("This is where we would delete from source {}".format(photo.queue_path))
                os.remove(photo.src_path)
            photo.queue_state = 'done'
            photo.save()

    # def enqueue(self, photo):  #TODO: This is old and must be updated *************
    #
    #     self.update_md5s(Candidates)  #TODO:  Stopped here
    #     dest = os.path.join(self.photo_queue.root_path, os.path.splitdrive(photo.path)[1])
    #     if os.path.isfile(dest):  # Check for name collision
    #         path_parts = os.path.split(dest)
    #         dest = os.path.join(path_parts[0], 'duplicate_name', path_parts[1])
    #         if os.path.isfile(dest):  # If still have name collision after putting in dup name dir, give up
    #             logging.warning("Photo queue name {} exists; skipping".format(dest))
    #     logging.info(photo['path'], "not in gphotos, non-dup, putting in queue")
    #     shutil.copy2(photo.path, dest)
    #     photo.queue_state = 'enqueued'
    #     photo.save()

if __name__ == '__main__':
    main()

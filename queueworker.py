import logging
from logging.config import dictConfig
import time
import os
import sys
import os.path
import yaml
# import fs
# from fs.osfs import OSFS
from pathlib import Path
import shutil

from local_db import LocalDb
from utils import cfg_obj


def main():
    """
    Manages Google Photos (gphotos) upload queue
    Copy upload candidates to gphoto queue.  Make sure there will be no name collisions and withhold files that
    would collide.  Notify user of collisions and number to be transferred. When done move photo to mirror Gphotos
    if possible.
    :return:
    """
    QueueWorker()


    # Configuration
class QueueWorker:
    def __init__(self):
        with open("config.yaml") as f:
            config = yaml.safe_load(f.read())
        self.local_cfg = cfg_obj(config, 'local')
        self.tq_cfg = cfg_obj(config, 'task_queue')
        dictConfig(config['logging'])
        self.log = logging.getLogger(__name__)

        # with OSFS(self.local_cfg.gphoto_upload_queue) as queue_fs:
        # queue_fs = OSFS(self.local_cfg.gphoto_upload_queue)

        self.local_db = LocalDb().db
        self.sync_db()
        while True:
            queue_filenames = self.dir_filenames(self.local_cfg.gphoto_upload_queue)
            # archive_candidates = local_db.find({'queue_state': {'$ne': 'done'}})
            for photo in self.local_db.find():  # TODO:  What if a photo isn't in the queue, or a queue photo isn't in the database?
                queue_state = photo['queue_state']
                in_gphotos = photo['in_gphotos']
                assert in_gphotos in [None, True, False], \
                    "photos['in_gphotos'] contains {}".format(in_gphotos)
                assert queue_state in [None, 'enqueued', 'pending', 'done', 'mirrored'], \
                    "photos['queue_state'] contains {}".format(photo['queue_state'])
                if in_gphotos is None:
                    pass  # Let other services determine if in gphotos before taking action
                elif in_gphotos is True:
                    if queue_state is None:
                        self.mark_done(photo, "done")  # TODO:  This should mirror
                    elif queue_state == "enqueued":
                        self.dequeue_photo(photo, queue_filenames)
                    elif queue_state == 'pending':
                        self.mark_done(photo, "done")
                    elif queue_state == 'done':
                        self.dequeue_photo(photo, queue_filenames)
                    elif queue_state == 'mirrored':
                        pass
                elif in_gphotos is False:
                    if queue_state is None:
                        self.conditionaly_enqueue(photo, queue_filenames)
                    elif queue_state == "enqueued":
                        pass
                    elif queue_state == 'pending':
                        self.conditionaly_enqueue(photo, queue_filenames)
                    elif queue_state == 'done' or queue_state == 'mirrored':
                        err_msg = "This should never happen, photo queue_state is 'done' or 'mirroed' while in_gphotos is False"
                        print(err_msg)
                        raise RuntimeError("{}".format(err_msg))
            print("{} Waiting...".format(time.asctime()))
            time.sleep(10)  # TODO:  Extend this after debugging done

    def dir_filenames(self, path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    def sync_db(self):
        queue_filenames = self.dir_filenames(self.local_cfg.gphoto_upload_queue)
        for name in os.path.join(self.local_cfg.gphoto_upload_queue,)


    def conditionaly_enqueue(self, photo, queue_filenames):
        if os.path.basename(photo['path']) in queue_filenames:
            print(photo['path'], "not in gphotos, dup name, setting to pending")
            self.local_db.update_one({'path': photo['path']}, {'$set': {'queue_state': 'pending'}})
        else:
            print(photo['path'], "not in gphotos, non-dup, putting in queue")
            shutil.copy2(photo['path'], self.local_cfg.gphoto_upload_queue)
            self.local_db.update_one({'path': photo['path']}, {'$set': {'queue_state': 'enqueued'}})

    def dequeue_photo(self, photo, queue_filenames):
        if os.path.basename(photo['path']) in queue_filenames:
            gpath = photo.get('archive_meta').get('gpath')
            if gpath:
                os.makedirs(os.path.join(self.local_cfg.gphoto_root, gpath), exist_ok=True)
                source = os.path.join(self.local_cfg.gphoto_upload_queue, os.path.basename(photo['path']))
                dest = os.path.join(self.local_cfg.gphoto_root, gpath, os.path.basename(photo['path']))
                if not Path(dest).exists():
                    try:
                        os.rename(source, dest)
                        self.mark_done(photo, "mirrored")
                        print("Moved {} to {}".format(source, dest))
                    except:
                        print("File access error:", sys.exc_info()[0])  # If contention for file, catch it on next cycle
        else:
            self.mark_done(photo, "done")

    def groom_queue(self):
        # TODO: For all the files in the queue check their MD5 sum against gphotos and dequeue
        pass

    def mark_done(self, photo, state):
        self.local_db.update_one({'path': photo['path']}, {'$set': {'in_gphotos': True, 'queue_state': state}})


if __name__ == '__main__':
    main()

import pymongo
import logging
import time
import os
import os.path
import yaml
from pathlib import Path
import collections
import pprint
import shutil

from utils import cfg_obj


def main():
    """
    Manages Google Photos (gphotos) upload queue
    Copy upload candidates to gphoto queue.  Make sure there will be no name collisions and withhold files that
    would collide.  Notify user of collisions and number to be transferred. When done move photo to mirror Gphotos
    if possible.
    :return:
    """
    queueworker = QueueWorker()

    # Configuration
class QueueWorker:
    def __init__(self):
        with open("config.yaml") as f:
            config = yaml.safe_load(f.read())

        self.local_cfg = cfg_obj(config, 'local')
        self.tq_cfg = cfg_obj(config, 'task_queue')

        # TODO:  logging can be set via a dict, so put logging settings in config.yaml and (probably) move to main call
        logger = logging.getLogger(__name__)
        LOG_FILE = os.path.join(self.local_cfg.log_file_base, time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "photolog.txt")
        logging.basicConfig(
            filename=LOG_FILE,
            format=self.local_cfg.log_format,
            level=logging.DEBUG,
            filemode='w'
        )

        self.local_db = pymongo.MongoClient(host=self.tq_cfg.host)[self.tq_cfg.database][self.tq_cfg.archive]

        while True:
            queue_filenames = [f for f in os.listdir(self.local_cfg.gphoto_upload_queue) if
                               os.path.isfile(os.path.join(self.local_cfg.gphoto_upload_queue, f))]
            # archive_candidates = local_db.find({'queue_state': {'$ne': 'done'}})
            archive_candidates = self.local_db.find()
            for photo in archive_candidates:
                queue_state = photo['queue_state']
                assert photo['in_gphotos'] in [None, True, False], \
                    "photos['in photos'] contains {}".format(photo['in_gphotos'])
                assert queue_state in [None, 'enqueued', 'pending', 'done', 'mirrored'], \
                    "photos['queue_state'] contains {}".format(photo['queue_state'])
                if photo['in_gphotos'] is None:
                    pass  # Let other services determine if in gphotos before taking action
                elif photo['in_gphotos']:
                    if queue_state is None:
                        self.mark_done(photo, "done")
                    elif queue_state == "enqueued":
                        self.dequeue_photo(photo, queue_filenames)
                    elif queue_state == 'pending':
                        self.mark_done(photo, "done")
                    elif queue_state == 'done':
                        self.dequeue_photo(photo, queue_filenames)
                    elif queue_state == 'mirrored':
                        pass
                    else:
                        raise ValueError("Should never get here, queue_state = {}".format(photo['queue_state']))
                else:
                    if queue_state is None:
                        self.conditionaly_enqueue(photo, queue_filenames)
                    elif queue_state == "enqueued":
                        pass
                    elif queue_state == 'pending':
                        self.conditionaly_enqueue(photo, queue_filenames)
                    elif queue_state == 'done' or queue_state == 'mirrored':
                        raise RuntimeError("This should never happen, photo queue_state is 'done' while in_gphotos is False")
                    else:
                        raise ValueError("Should never get here, queue_state = {}".format(queue_state))
            print("Waiting...")
            time.sleep(10)


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
                    os.rename(source, dest)
                    self.mark_done(self.local_db, photo, "mirrored")
            print("Moved {} to {}".format(source, dest))
        else:
            self.mark_done(photo, "done")


    def mark_done(self, photo, state):
        self.local_db.update_one({'path': photo['path']}, {'$set': {'in_gphotos': True, 'queue_state': state}})


if __name__ == '__main__':
    main()

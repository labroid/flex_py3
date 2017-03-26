import pymongo
import logging
import time
import os
import os.path
import yaml
import collections
import pprint
import shutil

from utils import cfg_obj


def main():
    """
    Manages Google Photos (gphotos) upload queue
    if photo is in gphotos
        set in_gphotos True
        if enqueued
            remove from queue (TODO: move from queue to local gphoto mirror with collision renaming)
            set queue_state done
        if pending or None
            mark as done
        if done
            do nothing
        else
            bad state
    else
        if None
            move to queue (TODO:  Limit size of queue)
            mark enqueued
        if equeued
            do nothing
        if pending
            if no name collision
                move to queue
                mark enqueued
        if done
            assert warning as this is a bad state
        else
            bad state

So, more optimized this would be:

find all records !done:
    if none:
        sleep 60 seconds
    else:
        for each record:
            if in_gphotos:
                set in_gphotos true
                set queue_state done
                if in queue:
                    remove from queue
            else
                if queue_state not enqueued:
                    if duplicate name:
                        set queue_state: pending
                    else:
                        move to queue
                        set queue_state: enqueued

    Copy upload candidates to gphoto queue.  Make sure there will be no name collisions and withhold files that
    would collide.  Notify user of collisions and number to be transferred
    :return:
    """

    # Configuration

    with open("config.yaml") as f:
        config = yaml.safe_load(f.read())

    gphoto_cfg = cfg_obj(config, 'gphotos')
    local_cfg = cfg_obj(config, 'local')
    tq_cfg = cfg_obj(config, 'task_queue')

    print(local_cfg)
    # Gphoto_cfg = collections.namedtuple('Gphoto_cfg', config['gphotos'].keys())
    # gphoto_cfg = Gphoto_cfg(**config['gphotos'])
    #
    # Local_cfg = collections.namedtuple('Local_cfg', config['local'].keys())
    # local_cfg = Local_cfg(**config['local'])
    #
    # Tq_cfg = collections.namedtuple('Tq_cfg', config['task_queue'].keys())
    # tq_cfg = Tq_cfg(**config['task_queue'])

    # TODO:  loggind can be set via a dict, so put logging settings in config.yaml and (probably) move to main call
    logger = logging.getLogger(__name__)
    LOG_FILE = os.path.join(local_cfg.log_file_base, time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "photolog.txt")
    logging.basicConfig(
        filename=LOG_FILE,
        format=local_cfg.log_format,
        level=logging.DEBUG,
        filemode='w'
    )

    local_db = pymongo.MongoClient(host=tq_cfg.host)[tq_cfg.database][tq_cfg.archive]

    while True:
        # TODO:  Add gphotos state so operations are on True or False.  None should trigger a check.
        queue_filenames = [f for f in os.listdir(local_cfg.gphoto_upload_queue) if
                           os.path.isfile(os.path.join(local_cfg.gphoto_upload_queue, f))]
        archive_candidates = local_db.find({'queue_state': {'$ne': 'done'}})
        for photo in archive_candidates:
            if photo['in_gphotos']:
                print(photo['path'], "in gphotos setting queue_state done")
                local_db.update_one({'path': photo['path']},
                                    {'$set': {
                                        'in_gphotos': True,
                                        'queue_state': 'done'
                                            }
                                    }
                                    )
                if os.path.basename(photo['path']) in queue_filenames:
                    rm_target = os.path.join(local_cfg.gphoto_upload_queue, os.path.basename(photo['path']))
                    os.remove(rm_target)  # TODO: Change to move photos to local Gphotos tree
                    print("Intend to remove: {} since it is in gphotos".format(rm_target))
                    print(photo)
            else:
                if photo['archive_state'] != 'enqueued':
                    if os.path.basename(photo['path']) in queue_filenames:
                        print(photo['path'], "not in gphotos, dup name, setting to pending")
                        local_db.update_one({'path': photo['path']},
                                            {'$set': {'queue_state': 'pending'}})
                    else:
                        print(photo['path'], "not in gphotos, non-dup, putting in queue")
                        shutil.copy2(photo['path'], local_cfg.gphoto_upload_queue)
                        local_db.update_one({'path': photo['path']},
                                            {'$set': {'queue_state': 'enqueued'}})
        print("Waiting...")
        time.sleep(60)

if __name__ == '__main__':
    main()

import mongoengine as me
from utils import Config

cfg = Config()

class Db_connect():
    def __init__(self):
        me.connect(db=cfg.gphotos.database, alias=cfg.gphotos.collection, host=cfg.gphotos.host)
        me.connect(db=cfg.local.database, alias=cfg.local.database, host=None)


class Gphoto(me.Document):  # TODO: Remove strict: false from metadata once db is clean
    gid = me.StringField()
    imageMediaMetadata = me.DictField()
    md5Checksum = me.StringField()
    mimeType = me.StringField()
    name = me.StringField()
    originalFilename = me.StringField()
    ownedByMe = me.BooleanField()
    parents = me.ListField()
    gsize = me.IntField()
    trashed = me.BooleanField()
    meta = {
        'db_alias': cfg.gphotos.collection,
        'indexes': ['gid', 'md5Checksum'],
        'strict': False
    }


class Gphoto_change(me.Document):
    type = me.StringField()
    value = me.StringField()
    meta = {'db_alias': cfg.gphotos.collection}


class Gphoto_parent(me.Document):
    gid = me.StringField()
    mimeType = me.StringField()
    name = me.StringField()
    ownedByMe = me.BooleanField()
    parents = me.ListField()
    trashed = me.BooleanField()
    path = me.ListField()
    meta = {'db_alias': cfg.gphotos.collection}


class Photo(me.Document):
    src_path = me.StringField(default=None)
    queue_path = me.StringField(default=None, required=True)
    size = me.IntField(default=None)
    md5sum = me.StringField(default=None)
    in_gphotos = me.BooleanField(default=False)
    queue_state = me.StringField(default=None, choices=[None, 'enqueued', 'done'])
    gphoto_meta = me.DictField(default=None)
    meta = {'allow_inheritance': True}

class Queue(Photo):
    meta = {'db_alias': cfg.local.database}

class Candidates(Photo):
    meta = {'db_alias': cfg.local.database}

class State(me.Document):
    dirlist = me.StringField(default=None)
    mirror_ok = me.BooleanField(default=True)
    purge_ok = me.BooleanField(default=False)
    meta = {'db_alias': cfg.local.database}

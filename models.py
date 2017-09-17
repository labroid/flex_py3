import mongoengine as me

GPHOTO_DB_ALIAS = 'gphotos'

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
        'db_alias': GPHOTO_DB_ALIAS,
        'indexes': ['gid', 'md5Checksum'],
        'strict': False
    }


class Gphoto_change(me.Document):
    type = me.StringField()
    value = me.StringField()
    meta = {'db_alias': GPHOTO_DB_ALIAS}


class Gphoto_parent(me.Document):
    gid = me.StringField()
    mimeType = me.StringField()
    name = me.StringField()
    ownedByMe = me.BooleanField()
    parents = me.ListField()
    trashed = me.BooleanField()
    path = me.ListField()
    meta = {'db_alias': GPHOTO_DB_ALIAS}
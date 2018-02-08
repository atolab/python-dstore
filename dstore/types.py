from cdds import TopicType

class KeyValue(TopicType):
    def __init__(self, version, key, value, sid):
        self.version = version
        self.key = key
        self.value = value
        self.sid = sid

    def gen_key(self):
        return self.key

    def __str__(self):
        return 'KeyValue(version = {0}, key = {1}, value = {2}, sid = {3})'.format(self.version, self.key, self.value, self.sid)


class StoreInfo(TopicType):
    def __init__(self, sid, sroot, shome):
        self.sid = sid
        self.sroot = sroot
        self.shome = shome

    def gen_key(self):
        return self.sid

    def __str__(self):
        return 'StoreInfo(sid = {0}, root = {1}, home= {2})'.format(self.sid, self.sroot, self.shome)


class CacheMiss(TopicType):
    def __init__(self, source_sid, key):
        self.source_sid = source_sid
        self.key = key

    def gen_key(self):
       return self.key

    def __str__(self):
        return 'CacheMiss(source_sid = {0}, key = {1})'.format(self.source_sid, self.key)

class CacheHit(TopicType):
    def __init__(self, source_sid, dest_sid, key, value, version):
        self.source_sid = source_sid
        self.dest_sid = dest_sid
        self.key = key
        self.value = value
        self.version = version

    def gen_key(self):
       return self.key

    def __str__(self):
        return 'CacheHit(source_sid = {0}, dest_sid = {1}, key = {2}, value = {3}, version = {4})'.format(self.source_sid, self.dest_sid, self.key, self.value, self.version)

class CacheMissMV(TopicType):
    def __init__(self, source_sid, key):
        self.source_sid = source_sid
        self.key = key

    def gen_key(self):
       return self.key

    def __str__(self):
        return 'CacheMissMV(source_sid = {0}, key = {1})'.format(self.source_sid, self.key)

class CacheHitMV(TopicType):
    def __init__(self, source_sid, dest_sid, key, kvave):
        self.source_sid = source_sid
        self.dest_sid = dest_sid
        self.key = key
        self.kvave= kvave # (key, value, version)

    def gen_key(self):
       return self.key

    def __str__(self):
        return 'CacheHitMV(source_sid = {0}, dest_sid = {1}, key = {2}, kvave= {3})'.format(self.source_sid, self.dest_sid, self.key, self.kvave)

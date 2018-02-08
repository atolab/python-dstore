class Observer(object):

    def onPut(self, uri, val, ver):
        raise NotImplemented('Not yet...')

    # One of these for each operation on the cache...
    def onPput(self, uri, value, version):
        raise NotImplemented('Not yet...')

    # @TODO: The semantics of dput and persistence has to be refined.
    def onDput(self, uri, value, version):
        raise NotImplemented('Not yet...')

    def onGet(self, uri):
        raise NotImplemented('Not yet...')

    def onRemove(self, uri):
        raise NotImplemented('Not yet...')

    def onObserve(self, uri, action):
        raise NotImplemented('Not yet...')

    def onMiss(self):
        raise NotImplemented('Not yet...')

    def onConflict(self):
        raise NotImplemented('Not yet...')


class AbstractController(object):

    def start(self):
        '''
            This method starts the controller and 'connects' the cache to the rest of the system.

        '''
        raise NotImplemented('Not yet...')

    def pause(self):
        '''
            Pauses the execution of the controller. The incoming updates are not lost.
        '''
        raise NotImplemented('Not yet...')

    def resume(self):
        '''
            Resumes the execution of the controller and applies all pending changes received from the network.
        '''
        raise NotImplemented('Not yet...')


    def stop(self):
        '''
            Stops a controller and releases all resources used to receive/send data on the network.
        '''

    def resolve(self, uri):
        '''
            Tries to resolve this URI on across the distributed caches
        :param uri: the URI to be resolved
        :return: the value, if something is found
        '''
        raise NotImplemented('Not yet...')


class AbstractStore(object):

    def __init__(self):
        pass


    def keys(self):
        '''
        :return: the keys contained by this store.
        '''
    def put(self,uri,value):
        '''
        uri eg. xrce://{nodeid or */** (* is only node ids, ** is all informations) or list of nodeids}/resurce
        value json obj
        '''
        raise NotImplementedError

    def pput(self,uri,value):
        '''
        Persistent put, in other terms this (k,v) will be restored when re-creating a cache with the same scope.

        uri eg. xrce://{nodeid or */** (* is only node ids, ** is all informations) or list of nodeids}/resurce
        value json obj
        '''
        raise NotImplementedError

    def dput(self, uri, value):
        '''

        :param uri: the URI for which a delta-put has to be applied. A delta put updates only the
                    attributes inlined in the URI, e.g.
                    dput('fos://<system_id>/<node_id>/runtime/<runtime_id>/entity/<entity_id>#status=run&entity_data.memory=2GB')
                    The other attributes, if any, associated with the
                    current value of he URI are left unchanged.

        :param uri: the URI for which a delta-put has to be applied. A delta put updates only the
                    attributes provided by the value. The other attributes, if any, associated with the
                    current value of he URI are left unchanged.
        '''

    def get(self,uri):
        '''
        :return: dict (uri,value)
        '''
        raise NotImplementedError

    def getAll(self, uri):
        '''
                :return: [(uri,value)]
        '''
        raise NotImplementedError
    def remove(self, uri):
        '''
            removes the entry associated to this URI from the distributed cache.
        '''
        raise NotImplementedError

    def observe(self, uri, action):
        '''
        action(cache) is a lambda
        '''
        raise NotImplementedError

    def iterate(self):
        '''
        iterate into local cache
        '''
        raise NotImplementedError

    def register_metaresource(self, resource, action):
        raise NotImplementedError

    def get_metaresources(self):
        raise NotImplementedError

    def miss_handler(self,action):
        raise NotImplementedError

    def conflict_handler(self,action):
        raise NotImplementedError


    def close(self):
        '''
            Cloese a store and releases all associated resources
        '''

        raise NotImplementedError

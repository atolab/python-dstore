import fnmatch
import json
from .abstract_store import AbstractStore
from .controller import StoreController
import time

class Store(AbstractStore):
    """This class provides the API to interact with the distributed store."""

    def __init__(self, store_id, root, home, cache_size):
        """Creates a new store.

        :param store_id: the string representing the global store identifier.
        :param root: the *root* of the store, in other terms the scope of resolutions
                    for keys. URI an only resolved if the *root* is a prefixself.
        :param home: the *home* of the store, all keys that have the *home* as
                     prefix are kept in memory.
        :param cache_size: the size of the cache that will be holding keys that
                           have the root as a prefix but not the home.
        """
        super(Store, self).__init__()
        self.root = root
        self.home = home
        self.store_id = store_id
        self.__store = {}  # This stores URI whose prefix is **home**
        self.discovered_stores = {}  # list of discovered stores not including self
        self.__cache_size = cache_size
        self.__local_cache = {}  # this is a cache that stores up
        # to __cache_size entry for URI whose prefix is not **home**
        self.__observers = {}
        self.__controller = StoreController(self)
        self.__controller.start()
        self.logger = self.__controller.logger

        self.__metaresources = {}

        self.register_metaresource('keys', self.__get_keys_under)
        self.register_metaresource('stores', self.__get_stores)
        time.sleep(2)


    def keys(self):
        """Get the lisy of keys available in the current store instance

        :return: List of string
        """
        return list(self.__store.keys())

    def is_stored_value(self, uri):
        if uri.startswith(self.home):
            return True
        else:
            return False

    def is_cached_value(self, uri):
        return not self.is_stored_value(uri)

    def get_version(self, uri):
        version = None
        v = None
        if self.is_stored_value(uri):
            if uri in self.__store:
                v = self.__store[uri]
        else:
            if uri in self.__local_cache:
                v = self.__local_cache[uri]

        if v is not None:
            version = v[1]

        return version

    def get_value(self, uri):
        v = None

        if uri in self.__store.keys():
            v = self.__store[uri]
        elif uri in self.__local_cache.keys():
            v = self.__local_cache[uri]

        return v

    def next_version(self, uri):
        nv = 0
        v = self.get_version(uri)
        if v is not None:
            nv = v + 1

        return nv

    def __unchecked_store_value(self, uri, value, version):
        if self.is_stored_value(uri):
            self.__store[uri] = (value, version)
        else:
            self.__local_cache[uri] = (value, version)

    def update_value(self, uri, value, version):
        succeeded = False
        if self.__is_metaresource(uri):
            self.logger.error('update_value({})'.format(uri), 'This is a metaresource should never be stored in cache!!!!')
            return False

        current_version = self.get_version(uri)
        #print('Store', 'Updating URI: {0} to value: {1} and version = {2} -- older version was : {3}'.format(uri, value, version, current_version))
        self.logger.debug('Store',  'Updating URI: {0} to value: {1} and version = {2} -- older version was : {3}'.format(uri, value, version, current_version))
        if current_version is not None:
            #print('Store', 'Updating URI: Version is not None')
            self.logger.debug('Store', 'Updating URI: Version is not None')
            if current_version < version:
                self.__unchecked_store_value(uri, value, version)
                succeeded = True
        else:
            self.logger.debug('Store', 'Updating URI: Version is {0}'.format(version))
            self.__unchecked_store_value(uri, value, version)
            succeeded = True

        return succeeded

    def notify_observers(self, uri, value, v):
        ##print('Store', ">>>>>>>> notify_observers")
        ##print('Store', 'URI {0}'.format(uri))

        self.logger.debug('Store', ">>>>>>>> notify_observers")
        self.logger.debug('Store', 'URI {0}'.format(uri))
        self.logger.debug('Store', 'URI  TYPE {0}'.format(type(uri)))

        self.logger.debug('Store', 'URI STR CAST {0}'.format(str(uri)))
        self.logger.debug('Store', 'URI  TYPE {0}'.format(type(uri)))

        self.logger.debug('Store', 'OBSERVER SIZE {0}'.format(len(list(self.__observers.keys()))))
        for key in list(self.__observers.keys()):
            #print('Store', 'OBSERVER KEY {0}'.format(key))
            self.logger.debug('Store', 'OBSERVER KEY {0}'.format(key))
            if fnmatch.fnmatch(uri, key) or fnmatch.fnmatch(key, uri):
                #print('Store', ">>>>>>>> notify_observers inside if")
                self.logger.debug('Store', ">>>>>>>> notify_observers inside if")
                self.__observers.get(key)(uri, value, v)

    def put(self, uri, value):
        '''Store the  **<key, value>** tuple on the distributed store.

        :param uri: key
        :param value: value
        :return: the version
        '''

        if not self.__check_writing_rights(uri):
            self.logger.debug('Store', 'No writing right for URI {0}'.format(type(uri)))
            return None

        v = self.get_version(uri)
        if v == None:
            v = 0
        else:
            v = v + 1
        self.update_value(uri, value, v)

        # It is always the observer that inserts data in the cache
        self.__controller.onPut(uri, value, v)
        ##print("notify_observers in put")
        self.notify_observers(uri, value, v)
        return v

    def pput(self, uri, value):
        '''Persistently store the  **<key, value>** tuple on the distributed store.
           This operation requires a DDS durability service in order to really
           store data persistently.

        :param uri: key
        :param value: value
        :return: the version
        '''

        if not self.__check_writing_rights(uri):
            self.logger.debug('Store', 'No writing right for URI {0}'.format(type(uri)))
            return None

        v = self.next_version(uri)
        self.__unchecked_store_value(uri, value, v)
        self.__controller.onPput(uri, value, v)
        ##print("notify_observers in pput")
        self.notify_observers(uri, value, v)

    def conflict_handler(self, action):
        pass

    def dput(self, uri, values=None):
        '''

        Same as put but for delta updates, fields to be update can be part of the uri after an hashtag eg. /root/home/key#value3=newvalue

        WARNING: this works only if values are dictionary/json data structures

        :param uri: the uri rapresenting the resource, can contain delta updates
        :param values: the delta update value can be none
        :return: the new version
        '''

        if not self.__check_writing_rights(uri):
            self.logger.debug('Store', 'No writing right for URI {0}'.format(type(uri)))
            return None

        self.logger.debug('Store', '>>> dput >>> URI: {0} VALUE: {1}'.format(uri, values))
        uri_values = ''
        if values is None:
            ##status=run&entity_data.memory=2GB
            uri = uri.split('#')
            uri_values = uri[-1]
            uri = uri[0]

        data = self.get(uri)
        self.logger.debug('Store', '>>> dput resolved {0} to {1}'.format(uri, data))
        self.logger.debug('Store', '>>> dput resolved type is {0}'.format(type(data)))
        version = 0
        if data is None or data == '':
            data = {}
        else:
            data = json.loads(data)
            version = self.next_version(uri)

        # version = self.next_version(uri)
        # data = {}
        # for key in self.__local_cache:
        #     if fnmatch.fnmatch(key, uri):
        #         data = json.loads(self.__local_cache.get(key)[0])
        #
        #
        # # @TODO: Need to resolve this miss
        # if len(data) == 0:
        #     data = self.get(uri)
        #     if data is None:
        #         return
        #     else:
        #         self.__unchecked_store_value(uri, data, self.next_version(uri))
        #         for key in self.__local_cache:
        #             if fnmatch.fnmatch(key, uri):
        #                 data = json.loads(self.__local_cache.get(key)[0])
        #         version = self.next_version(uri)

        self.logger.debug('Store', '>>>VALUES {0} '.format(values))
        self.logger.debug('Store', '>>>VALUES TYPE {0} '.format(type(values)))
        if values is None:
            uri_values = uri_values.split('&')
            self.logger.debug('Store', '>>>URI VALUES {0} '.format(uri_values))
            for tokens in uri_values:
                self.logger.debug('Store', 'INSIDE for tokens {0}'.format(tokens))
                v = tokens.split('=')[-1]
                k = tokens.split('=')[0]
                # if len(k.split('.')) < 2:
                #    data.update({k: v})
                #    self.logger.debug('Store','>>>merged data  {0} '.format(data))
                # else:
                d = self.dot2dict(k, v)

                data = self.data_merge(data, d)
                self.logger.debug('Store', '>>>merged data  {0} '.format(data))
        else:
            # #print('{0} type {1}'.format(values,type(values)))
            jvalues = json.loads(values)
            self.logger.debug('Store', 'dput delta value = {0}, data = {1}'.format(jvalues, data))
            data = self.data_merge(data, jvalues)

        self.logger.debug('Store', 'dput merged data = {0}'.format(data))

        value = json.dumps(data)
        self.__unchecked_store_value(uri, value, version)
        self.__controller.onDput(uri, value, version)
        ##print("notify_observers in dput")
        self.notify_observers(uri, value, version)
        return version

    def observe(self, uri, action):
        '''

        Register an observer to a specified key, key can contain wildcards eg. /root/home/myvalues/*

        action has to take 3 parametes (uri, value, version)

        :param uri: the uri to observe
        :param action: the function to notify
        :return: None
        '''
        self.__observers.update({uri: action})

    def remove(self, uri):
        '''

        Remove a resource identified by uri from the store

        :param uri: the key to remove
        :return: None
        '''
        if not self.__check_writing_rights(uri):
            self.logger.debug('Store', 'No writing right for URI {0}'.format(type(uri)))
            return None

        self.__controller.onRemove(uri)
        if uri in list(self.__local_cache.keys()):
            self.__local_cache.pop(uri)
        elif uri in list(self.__store.keys()):
            self.__store.pop(uri)
        else:
            pass
            self.logger.debug('Store', "REMOVE KEY {0} NOT PRESENT".format(uri))

        self.notify_observers(uri, None, None)

    def remote_remove(self, uri):
        if not self.__check_writing_rights(uri):
            self.logger.debug('Store', 'No writing right for URI {0}'.format(type(uri)))
            return None

        if uri in list(self.__local_cache.keys()):
            self.__local_cache.pop(uri)
        elif uri in list(self.__store.keys()):
            self.__store.pop(uri)
        else:
            pass
            self.logger.debug('Store', "REMOVE KEY {0} NOT PRESENT".format(uri))

        self.notify_observers(uri, None, None)

    def get(self, uri):
        '''

        Retrive a single value from the store, if not present in cache will resolve from remote stores

        :param uri: key to retrieve
        :return: the value
        '''


        if self.__is_metaresource(uri):
            if uri.startswith(self.home):
                u = uri.split('/')[-1]
                return self.__metaresources.get(u)(uri.rsplit(u, 1))
            else:
                return self.resolve(uri)

        v = self.get_value(uri)
        if v is None:
            self.__controller.onMiss()
            self.logger.debug('DStore', 'Resolving: {0}'.format(uri))
            return self.resolve(uri)
        else:
            return v[0]
        # v = self.get_value(uri)
        # if v == None:
        #     self.__controller.onMiss()
        #     self.logger.debug('Store', 'Resolving: {0}'.format(uri))
        #     rv = self.__controller.resolve(uri)
        #     if rv != None:
        #         self.logger.debug('Store',  'URI: {0} was resolved to val = {1} and ver = {2}'.format(uri, rv[0], rv[1]))
        #         self.update_value(uri, rv[0], rv[1])
        #         self.notify_observers(uri, rv[0], rv[1])
        #         return rv[0]
        #     else:
        #         return None
        # else:
        #     return v[0]


    def resolve(self, uri):
        '''

        Same as get, but always tries to resolve from remote stores

        :param uri: the key to resolve
        :return: the value
        '''
        rv = self.__controller.resolve(uri)
        # #print('Store', 'Resolve {} {}'.format(uri, rv))
        if rv != (None, -1):
            self.logger.debug('Store', 'URI: {0} was resolved to val = {1} and ver = {2}'.format(uri, rv[0], rv[1]))
            # #print('Store', 'URI: {0} was resolved to val = {1} and ver = {2}'.format(uri, rv[0], rv[1]))
            ##print('IS URI A METARESOURCE {}'.format(self.__is_metaresource(uri)))
            if not self.__is_metaresource(uri):
                self.update_value(uri, rv[0], rv[1])
            self.notify_observers(uri, rv[0], rv[1])
            return rv[0]
        else:
            return None


    def getAll(self, uri):
        '''

        Same as get but key can containt wildcards, this will not cause a resolve in case of cache miss

        :param uri: the uri of resources
        :return: a list of (key, value, version)
        '''
        xs = []
        u = uri.split('/')[-1]
        if u.endswith('~') and u.startswith('~'):
            if u in self.__metaresources.keys():
                return [(uri, self.__metaresources.get(u)(uri.rsplit(u, 1)), 0)]
            else:
                return None

        for k, v in self.__store.items():
            if fnmatch.fnmatch(k, uri):
                xs.append((k, v[0], v[1]))
        for k, v in self.__local_cache.items():
            if fnmatch.fnmatch(k, uri):
                xs.append((k, v[0], v[1]))

        self.logger.debug('Store', '>>>>>> getAll({0}) = {1}'.format(uri, xs))
        return xs

    def resolveAll(self, uri):
        '''

        Same as getAll but always resolve

        :param uri: the uri of resources
        :return: a list of (key, value, version)
        '''
        xs = self.__controller.resolveAll(uri)
        # #print('Store', 'Resolve All {} {}'.format(uri, xs))
        self.logger.debug('Store', ' Resolved resolveAll = {0}'.format(xs))
        ys = self.getAll(uri)

        xs_dict = {k: (k, va, ve) for (k, va, ve) in xs}
        # xy_dict = {k: (k, va, ve) for (k, va, ve) in ys}

        for (k, va, ve) in ys:
            if k not in xs_dict:
                xs_dict.update({k: (k, va, ve)})
            else:
                if ve > xs_dict.get(k)[2]:
                    xs_dict.update({k: (k, va, ve)})

        return list(xs_dict.values())

    def miss_handler(self, action):
        pass

    def iterate(self):
        pass

    def __str__(self):
        ret = ''
        for key, value in self.__local_cache.items():
            ret = '{}{}'.format(ret, 'Key: {} - Value {}'.format(key, value))

        return ret

    # convert dot notation to a dict
    def dot2dict(self, dot_notation, value=None):
        ld = []

        tokens = dot_notation.split('.')
        n_tokens = len(tokens)
        for i in range(n_tokens, 0, -1):
            if i == n_tokens and value is not None:
                ld.append({tokens[i - 1]: value})
            else:
                ld.append({tokens[i - 1]: ld[-1]})

        return ld[-1]

    def data_merge(self, base, updates):
        # self.logger.debug('Store','data_merge base = {0}, updates= {1}'.format(base, updates))
        # self.logger.debug('Store','type of base  = {0} update = {1}'.format(type(base), type(updates)))
        if base is None or isinstance(base, int) or isinstance(base, str) or isinstance(base, float):
            base = updates
        elif isinstance(base, list):
            if isinstance(updates, list):
                names = [x.get('name') for x in updates]
                item_same_name = [item for item in base if item.get('name') in [x.get('name') for x in updates]]
                # self.logger.debug('Store',names)
                # self.logger.debug('Store',item_same_name)
                if all(isinstance(x, dict) for x in updates) and len(
                        [item for item in base if item.get('name') in [x.get('name') for x in updates]]) > 0:
                    for e in base:
                        for u in updates:
                            if e.get('name') == u.get('name'):
                                self.data_merge(e, u)
                else:
                    base.extend(updates)
            else:
                base.append(updates)
        elif isinstance(base, dict):
            if isinstance(updates, dict):
                for k in updates.keys():
                    if k in base.keys():
                        base.update({k: self.data_merge(base.get(k), updates.get(k))})
                    else:
                        base.update({k: updates.get(k)})
        return base

    def on_store_discovered(self, sid):
        raise NotImplemented

    def on_store_disappeared(self, sid):
        raise NotImplemented

    def register_metaresource(self, resource, action):
        '''

        Allow to register a metaresouce and the action to be taken when someone access that metaresource

        :param resource: the metaresource name
        :param action: the action to retrieve the metaresource value
        :return: None
        '''
        #
        # reserved = gen - delims / sub - delims
        #
        # gen - delims = ":" / "/" / "?" / "#" / "[" / "]" / "@"
        #
        # sub - delims = "!" / "$" / "&" / "'" / "(" / ")"
        #                   / "*" / "+" / "," / ";" / "="
        #
        #
        # TODO: use ~name~ ?

        r = '~{}~'.format(resource)
        self.__metaresources.update({r: action})

    def __is_metaresource(self, uri):
        u = uri.split('/')[-1]
        if u.endswith('~') and u.startswith('~'):
            return True
        return False

    def get_metaresources(self):
        return self.__metaresources

    def __get_stores(self, uri):
        self.logger.debug('__get_stores', 'uri {}'.format(uri))
        return self.discovered_stores

    def __get_keys_under(self, uri):
        keys = self.keys()
        ks = []

        if isinstance(uri, list):
            uri = uri[0]

        if '*' in uri:
            uri = uri + '*'
            for k in keys:
                self.logger.debug('__get_keys_under', '{} match {}? {}'.format(k, uri, fnmatch.fnmatch(k, uri)))
                if fnmatch.fnmatch(k, uri):
                    ks.append(k)
        else:
            for k in keys:
                self.logger.debug('__get_keys_under', '{} starts with {}? {}'.format(k, uri, k.startswith(uri)))
                if k.startswith(uri):
                    ks.append(k)
        return ks

    def __check_writing_rights(self, uri):
        # TODO add system_id to store values
        # if uri.startswith('afos://{}/{}'.format('<sys-id>', self.store_id)):
        #     return True
        # elif uri.startswith('dfos://'):
        #     return True
        # elif not uri.startswith('dfos://') and not uri.startswith('afos://'):
        #     return True
        # else:
        #     return False
        return True

    def close(self):
        self.__controller.stop()

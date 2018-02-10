import fnmatch
import json
from .abstract_store import AbstractStore
from .controller import StoreController

class Store(AbstractStore):

    def __init__(self, store_id, root, home, cache_size):
        super(Store, self).__init__()
        self.root = root
        self.home = home
        self.store_id = store_id
        self.__store = {}  # This stores URI whose prefix is **home**
        self.discovered_stores = []  # list of discovered stores not including self
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

        '''
        @GB: As discussed with Erik and Angelo, maybe can be better to have 2 `store` for local information
        one with Desired state (that can be written by all nodes and readed only by the owner node)
        and one with actual state (that can be written only by the owner node and readed by all nodes)

        This means that plugins and agent works to make the actual state match the Desired state,
        this is also very useful in the case we want to know if some entity/plugin/whatever state changed.

        So this means we should have two or more different put,get, dput, and observer


        +-------------------------------------------------------------------+
        |                                                  Agent1           |
        |                                                                   |
        |    Desired Store                          Actual Store            |
        |    +-------+                               +-------+              |
        |    |       |                               |       |              |
        |    |       |        +---------------+      |       |              |
        |    |       +------->| Plugins/Agent |----->+       |              |
        |    |       |        +---------------+      |       |              |
        |    |       |                               |       |              |
        |    |       |                               |       |              |
        |    +---^---+                               +---+---+              |
        |        |                                       |                  |
        |        |                                       |                  |
        +--------|---------------------------------------|------------------+
                 |                                       |
                 | put(dfos:/s1/a1/...)                  |   get(afos://s1/a1/....)
                 |                                       |
         +-------+---------------------------------------V--------------------+
         |                                                  Agent2           |
         |                                                                   |
         |                                                                   |
         |    +-------+                               +-------+              |
         |    |       |                               |       |              |
         |    |       |        +---------------+      |       |              |
         |    |       |------->| Plugins/Agent |----->|       |              |
         |    |       |        +---------------+      |       |              |
         |    |       |                               |       |              |
         |    |       |                               |       |              |
         |    +-------+                               +-------+              |
         |    Desired Store                          Actual Store            |
         |                                                                   |
         +-------------------------------------------------------------------+


        So the data flow should follow this diagram.


        This semplify a lot waiting for some entity/plugin to state changed
        eg. someone decide to deploy a vm, so send a define, then a configure
        during configuring the actual state remain to defined until the configuration is done
        then the kvm plugin update the state in the `Actual Store`, and so someone that deployed the vm
        can now be sure that is configured and can send a state transition to run, this is the same case
        as above, sto the actual state remains to configured until kvm is sure that the vm is 
        started and ready to serve (so can also populate monitoring information immediately after
        the state transition.

        So someone that want to be updated about state transition can simply register an observer
        or can do a busy wait by doing some get (beacuse in this case get and observer are linked only 
        to the actual state)




        After some discussion we add some meta-reources, than help the discoverty of informations inside the stores of the others nodes
        these meta-resources end with !

        <a|d>fos://<sys-id>/<node_id or *>/keys!  the value is the list of all keys in that store

        <a|d>fos://<sys-id>/<node_id or *>/discovered_stores! returns the list of all discovered stores for that specific node

        '''

    def keys(self):
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

        current_version = self.get_version(uri)
        self.logger.debug('Store',
                          'Updating URI: {0} to value: {1} and version = {2} -- older version was : {3}'.format(uri,
                                                                                                                value,
                                                                                                                version,
                                                                                                                current_version))
        if current_version != None:
            self.logger.debug('Store', 'Updating URI: Version None')
            if current_version <= version:
                self.__unchecked_store_value(uri, value, version)
                succeeded = True
        else:
            self.logger.debug('Store', 'Updating URI: Version is {0}'.format(version))
            self.__unchecked_store_value(uri, value, version)
            succeeded = True

        return succeeded

    def notify_observers(self, uri, value, v):
        self.logger.debug('Store', ">>>>>>>> notify_observers")
        self.logger.debug('Store', 'URI {0}'.format(uri))
        self.logger.debug('Store', 'URI  TYPE {0}'.format(type(uri)))

        self.logger.debug('Store', 'URI STR CAST {0}'.format(str(uri)))
        self.logger.debug('Store', 'URI  TYPE {0}'.format(type(uri)))

        self.logger.debug('Store', 'OBSERVER SIZE {0}'.format(len(list(self.__observers.keys()))))
        for key in list(self.__observers.keys()):
            self.logger.debug('Store', 'OBSERVER KEY {0}'.format(key))
            if fnmatch.fnmatch(uri, key) or fnmatch.fnmatch(key, uri):
                self.logger.debug('Store', ">>>>>>>> notify_observers inside if")
                self.__observers.get(key)(uri, value, v)

    def put(self, uri, value):

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
        self.notify_observers(uri, value, v)
        return v

    def pput(self, uri, value):
        if not self.__check_writing_rights(uri):
            self.logger.debug('Store', 'No writing right for URI {0}'.format(type(uri)))
            return None

        v = self.next_version(uri)
        self.__unchecked_store_value(uri, value, v)
        self.__controller.onPput(uri, value, v)
        self.notify_observers(uri, value, v)

    def conflict_handler(self, action):
        pass

    def dput(self, uri, values=None):

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
            # print('{0} type {1}'.format(values,type(values)))
            jvalues = json.loads(values)
            self.logger.debug('Store', 'dput delta value = {0}, data = {1}'.format(jvalues, data))
            data = self.data_merge(data, jvalues)

        self.logger.debug('Store', 'dput merged data = {0}'.format(data))

        value = json.dumps(data)
        self.__unchecked_store_value(uri, value, version)
        self.__controller.onDput(uri, value, version)
        self.notify_observers(uri, value, version)
        return True

    def observe(self, uri, action):
        self.__observers.update({uri: action})

    def remove(self, uri):
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

        u = uri.split('/')[-1]
        if u.endswith('~') and u.startswith('~'):
            if u in self.__metaresources.keys():
                return self.__metaresources.get(u)(uri.rsplit(u, 1))
            else:
                return None

        v = self.get_value(uri)
        if v == None:
            self.__controller.onMiss()
            self.logger.debug('Store', 'Resolving: {0}'.format(uri))
            rv = self.__controller.resolve(uri)
            if rv != None:
                self.logger.debug('Store',
                                  'URI: {0} was resolved to val = {1} and ver = {2}'.format(uri, rv[0], rv[1]))
                self.update_value(uri, rv[0], rv[1])
                self.notify_observers(uri, rv[0], rv[1])
                return rv[0]
            else:
                return None
        else:
            return v[0]

    def getAll(self, uri):
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
        xs = self.__controller.resolveAll(uri)
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

    def get_metaresources(self):
        return self.__metaresources

    def __get_stores(self, uri):
        return self.discovered_stores

    def __get_keys_under(self, uri):
        keys = self.keys()
        ks = []

        if '*' in uri:
            pass
            # do search with fnmatch
        else:
            for k in keys:
                if k.startswith(uri):
                    ks.append(k)
        return ks

    def __check_writing_rights(self, uri):
        # TODO add system_id to store values
        if uri.startswith('afos://{}/{}'.format('<sys-id>', self.store_id)):
            return True
        elif uri.startswith('dfos://'):
            return True
        elif not uri.startswith('dfos://') and not uri.startswith('afos://'):
            return True
        else:
            return False

    def close(self):
        self.__controller.stop()



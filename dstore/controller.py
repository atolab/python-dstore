from .abstract_store import *
from .types import  *
from .logger import *
from cdds import *
import copy
import time
from time import sleep
import random

the_dds_controller = None


class DDSController:

    def __init__(self):
        #print(">>> Initializing DDSController")
        self.dds_runtime = Runtime.get_runtime()
        self.dp = Participant(0)

        self.store_info_topic = FlexyTopic(self.dp, "FOSStoreInfo")
        self.key_value_topic = FlexyTopic(self.dp, "FOSKeyValue")

        self.hit_topic = FlexyTopic(self.dp, "FOSStoreHit")
        self.miss_topic = FlexyTopic(self.dp, "FOSStoreMiss")

        self.missmv_topic = FlexyTopic(self.dp, "FOSStoreMissMV")
        self.hitmv_topic = FlexyTopic(self.dp, "FOSStoreHitMV")
        self.pubMap = {}
        self.subMap = {}

    def get_pub(self, path):
        p = None
        if path in self.pubMap.keys():
            p = self.pubMap[path]
        else:
            p = Publisher(self.dp, Publisher.partition(path))
            self.pubMap[path] = p

        return p

    def get_sub(self, path):
        s = None
        if path in self.subMap.keys():
            s = self.subMap[path]
        else:
            s = Subscriber(self.dp, Publisher.partition(path))
            self.pubMap[path] = s

        return s


    @staticmethod
    def controller():
        global the_dds_controller
        if the_dds_controller is not None:
            return the_dds_controller
        else:
            the_dds_controller = DDSController()
            return the_dds_controller

    def close(self):
        self.dds_runtime.close()

class StoreController (AbstractController, Observer):
    MAX_SAMPLES = 64
    DISPOSED_INSTANCE = 32

    def __init__(self, store):
        super(StoreController, self).__init__()
        self.dds_controller = DDSController.controller()
        self.logger = DLogger()
        self.__store = store

        self.dp = self.dds_controller.dp

        self.pub = self.dds_controller.get_pub(self.__store.root)
        self.sub = self.dds_controller.get_sub(self.__store.root)

        self.store_info_topic = self.dds_controller.store_info_topic
        self.key_value_topic = self.dds_controller.key_value_topic

        self.hit_topic = self.dds_controller.hit_topic
        self.miss_topic = self.dds_controller.miss_topic

        self.missmv_topic = self.dds_controller.missmv_topic
        self.hitmv_topic = self.dds_controller.hitmv_topic


        self.store_info_writer = FlexyWriter(self.pub,
                                             self.store_info_topic,
                                             DDS_State)

        self.store_info_reader = FlexyReader(self.sub,
                                            self.store_info_topic,
                                            self.cache_discovered,
                                             DDS_State)


        self.store_info_reader.on_liveliness_changed(self.cache_disappeared)

        self.key_value_writer = FlexyWriter(self.pub,
                                            self.key_value_topic,
                                            DDS_State)

        self.key_value_reader = FlexyReader(self.sub,
                                            self.key_value_topic,
                                            self.handle_remote_put,
                                            DDS_State)

        self.key_value_reader = FlexyReader(self.sub,
                                            self.key_value_topic,
                                            self.log_samples,
                                            DDS_State)


        self.miss_writer = FlexyWriter(self.pub,
                                       self.miss_topic,
                                       DDS_Event)

        self.miss_reader = FlexyReader(self.sub,
                                       self.miss_topic,
                                       self.handle_miss,
                                       DDS_Event)


        self.hit_writer = FlexyWriter(self.pub,
                                       self.hit_topic,
                                       DDS_Event)

        self.hit_reader = FlexyReader(self.sub,
                                       self.hit_topic,
                                      None,
                                      DDS_Event)


        self.missmv_writer = FlexyWriter(self.pub,
                                         self.missmv_topic,
                                         DDS_Event)

        self.missmv_reader = FlexyReader(self.sub,
                                         self.missmv_topic,
                                         lambda r: self.handle_miss_mv(r),
                                         DDS_Event)



        self.hitmv_writer = FlexyWriter(self.pub,
                                        self.hitmv_topic,
                                        DDS_Event)


        self.hitmv_reader = FlexyReader(self.sub,
                                        self.hitmv_topic,
                                        None,
                                        DDS_Event)



    def log_samples(self, dr):
        for (s, i) in dr.read(all_samples()):
            if i.valid_data:
                self.logger.debug('DController', str(s))

    def handle_miss(self, r):
        self.logger.debug('DController.handle_miss','Handling Miss for store {0}'.format(self.__store.store_id))
        samples = r.take(all_samples())
        v = None
        for (d, i) in samples:
            if i.valid_data and (d.source_sid != self.__store.store_id):

                if self.__is_metaresource(d.key) and d.key.startswith(self.__store.home):
                    u = d.key.split('/')[-1]
                    if u in self.__store.get_metaresources().keys():
                        uri = '/'.join(d.key.split('/')[:-1])
                        mr = self.__store.get_metaresources().get(u)(uri)
                        v = (mr, 0)

                else:
                    v = self.__store.get_value(d.key)

                if v is not None:
                    self.logger.debug('DController.handle_miss', 'Serving Miss for {0}'.format(d.key))
                    h = CacheHit(self.__store.store_id, d.source_sid, d.key, v[0], v[1])
                    self.hit_writer.write(h)
                else:
                    self.logger.debug('DController.handle_miss', 'Store {0} did not resolve remote miss on key {1}'.format(
                        self.__store.store_id, d.key))
                    h = CacheHit(self.__store.store_id, d.source_sid, d.key, None, -1)
                    self.hit_writer.write(h)



    def handle_miss_mv(self, r):
        self.logger.info('DController','>>>> Handling Miss MV for store {0}'.format(self.__store.store_id))
        samples = r.take(all_samples())
        xs = []
        for (d, i) in samples:
            if i.valid_data and (d.source_sid != self.__store.store_id):

                if self.__is_metaresource(d.key):
                    u = d.key.split('/')[-1]
                    if u in self.__store.get_metaresources().keys():
                        va = self.__store.get_metaresources().get(u)(''.join(d.key.rsplit(u, 1)))
                        xs = [(d.key, va, 0)]
                else:
                    xs = self.__store.getAll(d.key)

                if len(xs) == 0:
                    xs = None

                self.logger.debug('DController','>>>> Serving Miss MV for key {} store: {} data: {}'.format(d.key, d.source_sid, xs))
                h = CacheHitMV(self.__store.store_id, d.source_sid, d.key, xs)
                r_sleep = random.randint(1, 75)/100
                time.sleep(r_sleep)
                self.hitmv_writer.write(h)


    def handle_remove(self, uri):
        self.logger.debug('DController','>>>> Removing {0}'.format(uri))
        self.__store.remote_remove(uri)

    def handle_remote_put(self, reader):
        #print(">>>>>>>>>>>>. handle_remote_put")
        samples = reader.take(DDS_ANY_SAMPLE_STATE)

        for (d, i) in samples:
            self.logger.debug('DController', ">>>>>>>> Handling remote put d.key {0}".format(d.key))
            #print('DController', ">>>>>>>> Handling remote put d.key {0}".format(d.key))
            #print('\t\tSOURCE TIMESTAMP {}'.format(i.source_timestamp))
            #print('\t\tRECEPTION TIMESTAMP {}'.format(i.reception_timestamp))
            if i.is_disposed_instance():
                #print('>>>>>>>>>>>>. handle_remote_put for DISPOSE INSTANCE ', '>>>>> D {0}'.format(d.key))
                self.logger.debug('DController','>>>>> D {0}'.format(d.key))
                self.handle_remove(d.key)
            elif i.valid_data:
                #print('>>>>>>>>>>>>. handle_remote_put for UPDATED INSTANCE ', '>>>>> D {0}'.format(d.key))
                rkey = d.key
                rsid = d.sid
                rvalue = d.value
                rversion = d.version
                self.logger.debug('DController', '>>>>> SID {0} Key {1} Version {2} Value {3}'.format(rsid, rkey, rversion, rvalue))
                self.logger.debug('DController', ' MY STORE ID {0} MY HOME {1}'.format(self.__store.store_id,  self.__store.home))

                self.logger.debug('DController', 'Current store value {0}'.format(self.__store.get_value(rkey)))
                self.logger.debug('DController', 'self put? {0}'.format(rsid != self.__store.store_id))
                # We eagerly add all values to the cache to avoid problems created by inversion of miss and store
                if rsid != self.__store.store_id:
                    self.logger.debug('DController',">>>>>>>> Handling remote put in for key = " + rkey)
                    if not self.__is_metaresource(rkey):
                        r = self.__store.update_value(rkey, rvalue, rversion)
                        if r:
                            #print(">> Updated " + rkey)
                            self.logger.debug('DController', ">> Updated " + rkey)
                            self.__store.notify_observers(rkey, rvalue, rversion)
                    else:
                        self.logger.debug('DController',">> Received old version of " + rkey)
                else:
                    self.logger.debug('DController',">>>>>> Ignoring remote put as it is a self-put")
            else:
                self.logger.debug('DController',">>>>>> Some store unregistered instance {0}".format(d.key))

    def cache_discovered(self, reader):
        self.logger.debug('DController', 'New Cache discovered, current view = {0}'.format(self.__store.discovered_stores))
        samples = reader.take(DDS_ANY_SAMPLE_STATE)
        t_now = time.time()

        for (d, i) in samples:
            if i.valid_data:
                rsid = d.sid
                self.logger.debug('DController', ">>> Discovered store with id: " + rsid)
                if rsid != self.__store.store_id:
                    if rsid not in self.__store.discovered_stores.keys():
                        self.logger.debug('DController', ">>> Store with id: {} is new!".format(rsid))
                        self.__store.discovered_stores.update({rsid: time.time()})
                        self.advertise_presence()
                    elif rsid in self.__store.discovered_stores.keys():
                        t_old = self.__store.discovered_stores.get(rsid)
                        self.logger.debug('DController', ">>> Store with id: {} is old t_old-t_now={}!".format(rsid, t_now - t_old))
                        if t_now - t_old > 7:
                            self.advertise_presence()
                            self.logger.debug('DController', ">>> Responding to advertising at store id: {}".format(rsid))

                        self.__store.discovered_stores.update({rsid: time.time()})

            elif i.is_disposed_instance():
                rsid = d.key
                self.logger.debug('DController', ">>> Store {0} has been disposed".format(rsid))
                if rsid in self.__store.discovered_stores:
                    self.logger.debug('DController', ">>> Removing Store id: " + rsid)
                    self.__store.discovered_stores.pop(rsid)

    # def cache_discovered(self,reader):
    #     self.logger.debug('DController','New Cache discovered, current view = {0}'.format(self.__store.discovered_stores))
    #     samples = reader.take(DDS_ANY_SAMPLE_STATE)
    #
    #     for (d, i) in samples:
    #         if i.valid_data:
    #             rsid = d.sid
    #             self.logger.debug('DController',">>> Discovered new store with id: " + rsid)
    #             if rsid != self.__store.store_id and rsid not in self.__store.discovered_stores:
    #                 self.__store.discovered_stores.append(rsid)
    #                 self.advertise_presence()
    #         elif i.is_disposed_instance():
    #             rsid = d.key
    #             self.logger.debug('DController',">>> Store {0} has been disposed".format(rsid))
    #             if rsid in self.__store.discovered_stores:
    #                 self.logger.debug('DController',">>> Removing Store id: " + rsid)
    #                 self.__store.discovered_stores.remove(rsid)




    def cache_disappeared(self, reader, status):
        self.logger.debug('DController',">>> Cache Lifecycle-Change")
        self.logger.debug('DController','Current Stores view = {0}'.format(self.__store.discovered_stores))
        samples = reader.take(DDS_NOT_ALIVE_NO_WRITERS_INSTANCE_STATE | DDS_NOT_ALIVE_DISPOSED_INSTANCE_STATE)
        for (d, i) in samples:
            if i.valid_data:
                rsid = d.sid
                if rsid != self.__store.store_id:
                    if rsid in self.__store.discovered_stores:
                        self.__store.discovered_stores.remove(rsid)
                        self.logger.debug('DController',">>> Store with id {0} has disappeared".format(rsid))
                    else:
                        self.logger.debug('DController',">>> Store with id {0} has disappeared, but for some reason we did not know it...")


    def onPut(self, uri, val, ver):
        # self.logger.debug('DController',">> uri: " + uri)
        # self.logger.debug('DController',">> val: " + val)
        v = KeyValue(key = uri , value = val, sid = self.__store.store_id, version = ver)
        self.key_value_writer.write(v)


    # One of these for each operation on the cache...
    def onPput(self, uri, val, ver):
        v = KeyValue(key = uri , value = val, sid = self.__store.store_id, version = ver)
        self.key_value_writer.write(v)

    def onDput(self, uri, val, ver):
        v = KeyValue(key = uri , value = val, sid = self.__store.store_id, version = ver)
        self.key_value_writer.write(v)


    def onGet(self, uri):
        pass
        # self.logger.debug('DController',"onGet Not yet...")

    def onRemove(self, uri):
        v = KeyValue(key=uri, value=uri, sid=self.__store.store_id, version=0)
        self.key_value_writer.dispose_instance(v)


    def onObserve(self, uri, action):
        pass
        # self.logger.debug('DController',"onObserve Not yet...")

    def onMiss(self):
        pass
        # self.logger.debug('DController',">> onMiss")

    def onConflict(self):
        pass
        # self.logger.debug('DController',"onConflict Not yet...")

    def resolveAll(self, uri, timeout = None):
        self.logger.info('DController','>>>> Handling {0} Miss MV for store {1}'.format(uri, self.__store.store_id))

        self.logger.info('DController',">> Trying to resolve {}".format(uri))
        """
            Tries to resolve this URI (with wildcards) across the distributed caches
            :param uri: the URI to be resolved
            :return: the [value], if something is found
        """
        # @TODO: This should be in the config...
        #delta = 0.010
        delta = 0.150
        if timeout is None:
            timeout = delta

        m = CacheMissMV(self.__store.store_id, uri)
        self.missmv_writer.write(m)


        # maxRetries = max(min(len(peers), 3),  3)
        retries = 0
        values = []
        peers = copy.deepcopy(self.__store.discovered_stores)
        peers_id = []
        answers = []

        while len(peers) != len(answers):
            self.logger.debug('DController', ">>>>>>>>>>>>> Resolver starting loop #{} with peers: {} answers: {}".format(retries, len(peers), len(answers)))
            sleep(0.2)
            samples = list(self.hitmv_reader.take(DDS_ANY_STATE))
            if retries > 0 and (retries % 10) == 0:
                self.missmv_writer.write(m)
                sleep(0.4)


            self.logger.debug('DController',">>>> Resolve loop #{} got {} samples -> {}".format(retries, len(samples), samples))
            for s in samples:
                d = s[0]
                i = s[1]
                self.logger.debug('DController',"Is valid data: {0}".format(i.valid_data))
                self.logger.debug('DController',"Key: {0}".format(d.key))
                if i.valid_data:
                    self.logger.debug('DController',"Reveived data from store {0} for store {1} on key {2}".format(d.source_sid, d.dest_sid, d.key))
                    self.logger.debug('DController',"I was looking to resolve uri: {0}".format(uri))
                    self.logger.debug('DController','>>>>>>>>> VALUE {0} KVAVE {1}'.format(values, d.kvave))
                    # Only remove if this was an answer for this key!
                    # if d.source_sid in peers and uri == d.key:
                    #     peers.remove(d.source_sid)
                    if d.source_sid not in answers:
                        answers.append(d.source_sid)
                        if d.key == uri and d.kvave is not None: # and d.dest_sid == self.__store.store_id:
                            values = values + d.kvave
                    else:
                        self.logger.debug('DController', "Already got an answer from {}".format(d.source_sid))

            self.logger.debug('DController', ">>>>>>>>>>>>> Resolver finishing loop #{} with peers: {} answers: {}".format(retries, len(peers), len(answers)))
            retries = retries+1



        # now we need to consolidate values
        self.logger.debug('DController', 'Resolved Values = {0}'.format(values))

        #values = list(set(values))

        filtered_values = {}



        for (k, va, ve) in values:
            if k not in filtered_values:
                filtered_values.update({k: (k ,va ,ve)})
            else:
                if ve > filtered_values.get(k)[2]:
                    filtered_values.update({k: (k, va, ve)})



        self.logger.debug('DController',"Filtered Values = {0}".format(filtered_values))
        return list(filtered_values.values())

    def resolve(self, uri, timeout = None):
        self.logger.debug('DController','>>>> Handling {0} Miss for store {1}'.format(uri, self.__store.store_id))

        self.logger.debug('DController',">> Trying to resolve {}".format(uri))
        """
            Tries to resolve this URI on across the distributed caches
            :param uri: the URI to be resolved
            :return: the value, if something is found
        """

        # @TODO: This should be in the config...
        #delta = 0.250
        delta = 0.050
        if timeout is None:
            timeout = delta


        m = CacheMiss(self.__store.store_id, uri)
        self.miss_writer.write(m)

        peers = copy.deepcopy(self.__store.discovered_stores)
        # answers = []
        self.logger.debug('DController',"Trying to resolve {0} with peers {1}".format(uri, peers))
        maxRetries = max(len(peers),  10)

        retries = 0
        v = (None, -1)
        #peers != [] and
        while retries < maxRetries:
        # while peers != answers:
        #     peers = copy.deepcopy(self.__store.discovered_stores)
            #sleep(0.2)
            samples = list(self.hitmv_reader.take(DDS_ANY_STATE))
            time.sleep(timeout + max(retries-1, 0) * delta)
            self.logger.debug('DController', ">>>> Resolve loop #{} got {} samples -> {}".format(retries, len(samples), samples))

            # sn = 0

            for (d, i) in samples:
                # sn += 1
                if i.valid_data and d.key == uri:
                    self.logger.debug('DController',"Reveived data from store {0} for store {1} on key {2}".format(d.source_sid, d.dest_sid, d.key))
                    self.logger.debug('DController',"I was looking to resolve uri: {0}".format(uri))
                        # # Only remove if this was an answer for this key!
                        # if d.source_sid in peers and uri == d.key and d.dest_sid == self.__store.store_id:
                        #     peers.remove(d.source_sid)
                    if d.key == uri and d.dest_sid == self.__store.store_id:
                        if int(d.version) > int(v[1]):
                            v = (d.value, d.version)
            retries = retries + 1


        return v
        # if v[0] is not None:
        #     return v
        #
        #     # retries += 1
        #
        # return v


    def __is_metaresource(self, uri):
            u = uri.split('/')[-1]
            if u.endswith('~') and u.startswith('~'):
                return True
            return False

    def start(self):
        self.logger.debug('DController', "Advertising Store with Id {0}".format(self.__store.store_id))

        import threading
        threading.Thread(target=self.advertise_presence_timer, args=[5]).start()

    def advertise_presence_timer(self, timer):
        self.logger.debug('DController', "Advertising Store with Id {} every {}".format(self.__store.store_id, timer))
        while True:
            self.logger.debug('DController', "Advertising Store with Id {}".format(self.__store.store_id))
            info = StoreInfo(sid=self.__store.store_id, sroot=self.__store.root, shome=self.__store.home)
            self.store_info_writer.write(info)
            time.sleep(timer)
    # def start(self):
    #     self.logger.debug('DController',"Advertising Store with Id {0}".format(self.__store.store_id))
    #     self.advertise_presence()

    def advertise_presence(self):
        info = StoreInfo(sid=self.__store.store_id, sroot=self.__store.root, shome=self.__store.home)
        self.store_info_writer.write(info)

    def pause(self):
        """
            Pauses the execution of the controller. The incoming updates are not lost.
        """
        pass
        # self.logger.debug('DController',"Pausing..")


    def resume(self):
        """
            Resumes the execution of the controller and applies all pending changes received from the network.
        """
        pass
        # self.logger.debug('DController',"Resuming..")


    def stop(self):
        info = StoreInfo(sid=self.__store.store_id, sroot=self.__store.root, shome=self.__store.home)
        self.store_info_writer.dispose_instance(info)
        DDSController.controller().close()



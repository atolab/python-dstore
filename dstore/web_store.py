import asyncio
import websockets
import logging
import sys
from .store import Store



# The commands accepted by this server have the format:
#     command store-id arg1 arg2 ... argn
# where the command represents the method to execute on the given store-id and the args are the
# arguments for the command.
#
# The commands currently supported are:
#
#    create  sid root home cache-size   -> OK | NOK
#    close   sid                        -> OK | NOK
#
#    gkeys    sid                        -> keys sid key1,key2,...,keyn
#    put     sid uri val                -> OK | NOK
#    dput     sid uri [val]             -> OK | NOK
#    get     sid uri                    -> value sid key value
#    aget     sid uri                    -> values sid key key1@value1,key2@value2,...,keyn@valuen
#    resolve  sid uri                    -> value sid key value
#    aresolve sid uri                    -> values sid key key1@value1,key2@value2,...,keyn@valuen
#    remove  sid uri                    -> OK | NOK
#
#    observe sid uri cookie             -> stream notify sid cookie key value


class Dispatcher (object):
    def __init__(self, cookie, wsock):
        self.cookie = cookie
        self.wsock = wsock


    def dispatch(self, key, val, ver):
        print("Dispatching observer")
        result = '{} {} {}'.format(self.cookie, key, val)
        asyncio.ensure_future(self.wsock.send(result))


class WebStore (object):
    '''

    This class provides the API to interact with the distributed store as well as the WebSocket service to access the Store

    The commands accepted by this server have the format:
        command store-id arg1 arg2 ... argn
    where the command represents the method to execute on the given store-id and the args are the
    arguments for the command.
    
    The commands currently supported are:
    
       create  sid root home cache-size   -> OK | NOK

       close   sid                        -> OK | NOK
    
       gkeys    sid                        -> keys sid key1,key2,...,keyn

       put     sid uri val                -> OK | NOK

       dput     sid uri [val]             -> OK | NOK

       get     sid uri                    -> value sid key value

       aget     sid uri                    -> values sid key key1@value1,key2@value2,...,keyn@valuen

       resolve  sid uri                    -> value sid key value

       aresolve sid uri                    -> values sid key key1@value1,key2@value2,...,keyn@valuen

       remove  sid uri                    -> OK | NOK
    
       observe sid uri cookie             -> stream notify sid cookie key value




    '''
    def __init__(self, port, auth = None):
        '''

        Create the websocket server


        :param port: port to listen
        :param auth: authorization string
        '''
        self.port = port
        self.auth = auth
        self.svc = None

        self.logger_impl = logging.getLogger('websockets')
        self.logger_impl.setLevel(logging.DEBUG)
        self.logger_impl.addHandler(logging.StreamHandler())
        # self.logger = DLogger()
        # self.logger.logger = self.logger_impl
        self.storeMap = {}

    @asyncio.coroutine
    def process(self, websocket, cmd):
        if cmd is not None:
            xs = [x for x in cmd.split(' ') if x is not '']
            if len(xs) < 2:
                print(">> Received invalid command {}".format(str(cmd)))
            else:
                cid = xs[0]
                sid = xs[1]
                args = xs[2:]
                yield from self.handle_command(websocket, cid, sid, args)


    def create(self, sid, args):
        if len(args) < 3:
            return None
        else:
            return Store(sid, args[0], args[1], int(args[2]))

    def close(self, sid):
        if sid in self.storeMap.keys():
            store = self.storeMap.pop(sid)
            store.close()

        return True

    def put(self, store, args):
        if len(args) < 2:
            return False
        else:
            store.put(args[0], ' '.join(args[1:]))
            return True

    def get(self, store, args):
        v = ''
        if len(args) > 0:
            v = store.get(args[0])
            if v is None:
                v = ''
            return v

    def getAll(self, store, args):
        xs = []
        if len(args) > 0:
            vs = store.getAll(args[0])
            xs = []
            for (key, val, ver) in vs:
                xs.append('{}@{}'.format(key, val))

        return xs

    def resolve(self, store, args):
        v = ''
        if len(args) > 0:
            v = store.resolve(args[0])
            if v is None:
                v = ''
            return v

    def resolveAll(self, store, args):
        xs = []
        if len(args) > 0:
            vs = store.resolveAll(args[0])
            xs = []
            for (key, val, ver) in vs:
                xs.append('{}@{}'.format(key, val))

        return xs

    def remove(self, store, args):
        if len(args) > 0:
            store.remove(args[0])
            return True
        else:
            return False

    def dput(self, store, args):
        result  = False
        if len(args) == 1:
            store.dput(args[0])
            result = True
        elif len(args) > 1:
            store.dput(args[0], ' '.join(args[1:]))
            result = True

        return result


    def observe(self, store, sid, args, websocket):
        success = False
        print("len(args) {}".format(len(args)))
        if len(args) > 1:
            success = True
            cookie = 'notify {} {}'.format(sid, args[1])
            disp = Dispatcher(cookie, websocket)
            store.observe(args[0], disp.dispatch)
        else:
            print("Observe failed!")
        print("success = {}".format(success))
        return success


    @asyncio.coroutine
    def send_error(self, websocket, val):
        yield from websocket.send("NOK {}".format(val))


    @asyncio.coroutine
    def send_success(self, websocket, val):
        yield from websocket.send("OK {}".format(val))


    @asyncio.coroutine
    def handle_command(self, websocket, cid, sid, args):
        # self.logger.debug("fog05ws", ">> Handling command {}".format(cid))
        # print(">> Handling command {}".format(cid))

        result = '{} {}'.format(cid,sid)
        prefix = 'NOK'

        # -- Create
        if cid == 'create':
            if not (sid in self.storeMap.keys()):
                s = self.create(sid, args)
                if s is not None:
                    self.storeMap[sid] = s
                    prefix = 'OK'
                else:
                    prefix = 'NOK'
            else:
                prefix = 'OK'

        elif cid == 'close':
            if self.close(sid):
                prefix = 'OK'
            else:
                prefix = 'NOK'

        else:
            store = None
            if sid in self.storeMap.keys():
                store = self.storeMap.get(sid)

                # -- Put
                if cid == 'put':
                    if self.put(store, args):
                        result = '{} {} {}'.format(cid, sid, args[0])
                        prefix = 'OK '

                # -- DPut
                if cid == 'dput':
                    if self.dput(store, args):
                        result = '{} {} {}'.format(cid, sid, args[0])
                        prefix = 'OK'

                # -- Remove
                if cid == 'remove':
                    if self.remove(store, args):
                        result = "{} {} {}".format(cid, sid, args[0])
                        prefix = 'OK'

                # -- Get
                elif cid == 'get':
                    v = self.get(store, args)
                    result = "{} {} {} {}".format('value', sid, args[0], v)
                    prefix = ''

                # -- GetAll
                elif cid == 'aget':
                    vs = self.getAll(store, args)
                    result = "{} {} {} {}".format('values', sid, args[0], '|'.join(vs))
                    prefix = ''

                elif cid == 'resolve':
                    v = self.resolve(store, args)
                    result = "{} {} {} {}".format('value', sid, args[0], v)
                    prefix = ''


                elif cid == 'aresolve':
                    vs = self.resolveAll(store, args)
                    result = "{} {} {} {}".format('values', sid, args[0], '|'.join(vs))
                    prefix = ''

                # -- Keys
                elif cid == 'gkeys':
                    ks = store.keys()
                    result = "{} {} {}".format('keys', sid, '|'.join(ks))
                    prefix = ''

                # -- Observe
                elif cid == 'observe':
                    if self.observe(store, sid, args, websocket):
                        result = "{} {} {}".format(cid, args[0], args[1])
                        prefix = 'OK'


        yield from websocket.send('{} {}'.format(prefix, result))
        # if success:
        #     yield from self.send_success(websocket, result)
        # else:
        #     yield from self.send_error(websocket, result)

    def authenticate(self, client):
        if self.auth is None:
            return True
        else:
            return client == self.auth


    @asyncio.coroutine
    def dispatch(self, websocket, path):
        try:
            while True:
                raddr = websocket.remote_address
                client_auth = path.split('/')[1]
                if self.authenticate(client_auth):
                    while True:
                        message = yield from websocket.recv()
                        print(">> Processing message {}".format(message))
                        yield from self.process(websocket, message)

                else:
                    print(">> Closing connection because of invalid authentication.")
        except:
            print(">> Remote peer closed the connection, doing the same.")
            websocket.close()

    def stop(self):
            try:
                for id in list(self.storeMap.keys()):
                    store = self.storeMap.pop(id)
                    store.close()
            except Exception as e:
                print('Error on exiting {}'.format(e))
            finally:
                return


    def start(self):
        if self.svc is None:
            # self.logger.info("fog05ws", ">> fog05 web-socket service is listening on port {}".format(self.port))
            print(">> fog05 web-socket service is listening on port {}".format(self.port))
            self.svc = websockets.serve(self.dispatch, '0.0.0.0', self.port)
            asyncio.get_event_loop().run_until_complete(self.svc)
            asyncio.get_event_loop().run_forever()
        else:
            raise RuntimeError("Service Already Running")


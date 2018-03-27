#!/usr/bin/env python3
from flask import Flask, Response, request
from . import Store
import logging
import json
import time


# {'result': bool, "store_id":sting , "data": [{'key':string, 'value':string, 'version': int}]}

class RestStore(object):

    def __init__(self, address="0.0.0.0", port=5000):
        self.address = address
        self.port = port
        self.stores = {}
        self.app = Flask(address)
        self.logger = logging.Logger('dstore rest')

    @app.route('/')
    def index(self):
        return json.dumps({'STORE REST API': {'version': 0.1}})

    @app.route('/create/<store_id>', methods=['POST'])
    def create(self, store_id):
        root = request.form.get('root')
        home = request.form.get('home')
        size = int(request.form.get('size'))

        store = Store(store_id, root, home, size)
        self.stores.update({store_id: (store, time.time())})
        return json.dumps({'result': True, "data": None})

    @app.route('/get/<store_id>/<path:uri>', methods=['GET'])
    def get(self, store_id, uri):
        v = None

        print('GET -> {}'.format(uri))
        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": [{'key': uri, 'value': None, 'version': None}]})
        store = store[0]

        if '*' in uri:
            v = store.resolveAll(uri)
        else:
            v = store.get(uri)

        if v is not None or len(v) == 0:
            if isinstance(v, list):
                data = []
                for (key, val, ver) in v:
                    data.append({'key': key, 'value': val, 'version':ver})
                return json.dumps({'result': True, "store_id": store_id, 'data': data})
            else:
                return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': v, 'version': None}]})
        else:
            return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': None, 'version': None}]})

    @app.route('/put/<store_id>/<path:uri>', methods=['PUT'])
    def put(self, store_id, uri):
        value = request.form.get('value')
        print('PUT -> {} -> {}'.format(uri, value))

        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]

        version = store.put(uri, value)
        return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': value, 'version': version}]})

    @app.route('/dput/<store_id>/<path:uri>/', methods=['PATCH'])
    def dput(self, store_id, uri):

        value = request.form.get('value')

        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]

        version = store.dput(uri, value)
        return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': value, 'version': version}]})

    @app.route('/remove/<store_id>/<path:uri>', methods=['DELETE'])
    def remove(self, store_id, uri):
        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]

        if store.remove(uri):
            return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': None, 'version': None}]})
        else:
            return json.dumps({'result': False, "store_id": store_id, "data": [{'key': uri, 'value': None, 'version': None}]})

    @app.route('/destroy/<store_id>', methods=['DELETE'])
    def destroy(self, store_id):
        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]
        store.close()
        self.stores.pop(store_id)
        return json.dumps({'result': True, "store_id": store_id, "data": None})

    def start(self):

        try:
            self.app.run(debug=True, host=self.address, port=self.port)
        finally:
            for k in list(self.stores.keys()):
                s = self.stores.get(k)
                s.close()





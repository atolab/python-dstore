from flask import Flask, request

from . import Store
import logging
import json
import time


# {'result': bool, "store_id":string , "data": [{'key':string, 'value':string, 'version': int}]}

class RestStore(object):
    """
    This class provides the API to interact with the distributed store as well as the HTTP RESTful service to access the Store

    All response message from this server have this structure

    {'result': bool, "store_id":string , "data": [{'key':string, 'value':string, 'version': int}]}

    """

    def __init__(self, address="0.0.0.0", port=5000):
        """

        Create the REST Store Service with given parameters

        :param address: IP address where the server has to be accesible (default value 0.0.0.0 [all address of this host])
        :param port: Port number to listen (default 5000)
        """
        self.address = address
        self.port = port
        self.stores = {}
        self.app = Flask(__name__)
        self.logger = self.app.logger
        self.app.add_url_rule('/', 'index', self.index, methods=['GET'])
        self.app.add_url_rule('/get/<store_id>/<path:uri>', 'get', self.get, methods=['GET'])
        self.app.add_url_rule('/create/<store_id>', 'create', self.create, methods=['POST'])
        self.app.add_url_rule('/put/<store_id>/<path:uri>', 'put', self.put, methods=['PUT'])
        self.app.add_url_rule('/dput/<store_id>/<path:uri>', 'dput', self.dput, methods=['PATCH'], )
        self.app.add_url_rule('/remove/<store_id>/<path:uri>', 'remove', self.destroy, methods=['DELETE'])
        self.app.add_url_rule('/destroy/<store_id>', 'destroy',self.destroy, methods=['DELETE'])


    def __close_all_store(self):
        """

        Private method to close all stores before shutting down

        :return:
        """
        for k in list(self.stores.keys()):
            s = self.stores.get(k)
            s.close()

    #@app.route('/')
    def index(self):
        """

        Retrive information for the service

        URL: /
        METHOD: GET

        :return: {'STORE REST API': {'version': version}}
        """
        return json.dumps({'STORE REST API': {'version': 0.1}})

    #@app.route('/create/<store_id>', methods=['POST'])
    def create(self, store_id):
        """

        Create a store
        URL: /create/<store_id>
        METHOD: POST


        root, home and size should be passed as parameter in the request body

        eg. curl


        curl --request POST \
            --url http://127.0.0.1:5000/create/123 \
            --form 'home: r/h' \
            --form 'root: r' \
            --form 'size: 100'

        :param store_id: id of the store to be created
        :return: JSON as described in init
        """

        print('{}'.format(request.form))
        root = request.form.get('root')
        home = request.form.get('home')
        size = int(request.form.get('size', 0))

        print('CREATE {} -> {} -> {} -> {}'.format(store_id, root, home, size))

        store = Store(store_id, root, home, size)
        self.stores.update({store_id: (store, time.time())})
        return json.dumps({'result': True, "data": None})

    #@app.route('/get/<store_id>/<path:uri>', methods=['GET'])
    def get(self, store_id, uri):
        """

        Get from a store

        URL: /get/<store_id>/<path:uri>
        METHOD: GET


        :param store_id: id of the store to use
        :param uri: URI of the resource to retrieve
        :return: JSON as described in init
        """

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

        print('V-> {}'.format(v))
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

    #@app.route('/put/<store_id>/<path:uri>', methods=['PUT'])
    def put(self, store_id, uri):
        """

        PUT inside a store

        URL: /put/<store_id>/<path:uri>
        METHOD: PUT

        The value should be passed inside the request body under key 'value'

        example using curl

        curl --request PUT \
            --url http://127.0.0.1:5000/put/yaks://some/key \
            --form 'value: key value'

        :param store_id: id of the store to use
        :param uri: URI of the resource to put
        :return: JSON as described in init
        """

        value = request.form.get('value')
        print('PUT -> {} -> {}'.format(uri, value))

        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]

        version = store.put(uri, value)
        return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': value, 'version': version}]})

    #@app.route('/dput/<store_id>/<path:uri>/', methods=['PATCH'])
    def dput(self, store_id, uri):
        """
        Delta put, to provide a delta update, this not support in URI update so value need to be passed inside the request body

        URL: /dput/<store_id>/<path:uri>
        METHOD: PATCH


        WARNING: this works only if value are JSON object

         The value should be passed inside the request body under key 'value'

        example using curl

        curl --request PATCH \
            --url http://127.0.0.1:5000/dput/yaks://some/key/to/update \
            --form 'value: field3=newvalue'


        :param store_id: id of the store to use
        :param uri: URI of the resource to delta update
        :return: JSON as described in init
        """

        value = request.form.get('value')

        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]

        version = store.dput(uri, value)
        return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': value, 'version': version}]})

    #@app.route('/remove/<store_id>/<path:uri>', methods=['DELETE'])
    def remove(self, store_id, uri):
        """

        Remove a resource from the store

        URL: /remove/<store_id>/<path:uri>
        METHOD: DELETE

        :param store_id: id of the store to use
        :param uri: URI of the resource to remove
        :return: JSON as described in init
        """


        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]

        if store.remove(uri):
            return json.dumps({'result': True, "store_id": store_id, "data": [{'key': uri, 'value': None, 'version': None}]})
        else:
            return json.dumps({'result': False, "store_id": store_id, "data": [{'key': uri, 'value': None, 'version': None}]})

    #@app.route('/destroy/<store_id>', methods=['DELETE'])
    def destroy(self, store_id):
        """

        destroy a store

        URL: /destroy/<store_id>
        METHOD: DELETE

        :param store_id: id of the store to destroy
        :return: JSON as described in init
        """


        store = self.stores.get(store_id, None)
        if store is None:
            return json.dumps({'result': False, "store_id": store_id, "data": None})
        store = store[0]
        store.close()
        self.stores.pop(store_id)
        return json.dumps({'result': True, "store_id": store_id, "data": None})

    def stop(self):
        """

        Stop the service

        :return:
        """
        self.__close_all_store()
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()


    def start(self):
        """

        Start the service

        This is used in dstore-rest-server

        :return:
        """

        try:
            self.app.run(debug=True, host=self.address, port=self.port)
        finally:
            self.__close_all_store()






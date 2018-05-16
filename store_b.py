from dstore import Store
import uuid
import random
s = Store('b','afos://0','afos://0/b',1024)

while True:
    input('Press enter to put something in the store!')
    k = 'afos://0/b/{}'.format(uuid.uuid4()).split('-')[0]
    v = 'test-{}'.format(random.randint(0,65535))
    print('<<<< {} :-> {} (v: {})'.format(k,v,s.put(k,v))) 

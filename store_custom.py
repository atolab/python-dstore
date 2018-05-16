from dstore import Store
import sys

if len(sys.argv) < 2:
    print('[Usage] {} <store id>'.format(sys.argv[0]))
    exit(-1)
sid = sys.argv[1]
s = Store(sid, 'afos://0', 'afos://0/{}'.format(sid), 1024)
i = 0
while True:
    input('Press enter to see discovered stores and do a resolve all on root')
    print('>>>> DISCOVERED STORES: {}'.format(s.discovered_stores))
    print('>>>> RESOLVE ALL on ROOT: {}'.format(s.resolveAll('afos://0/*')))


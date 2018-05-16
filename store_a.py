from dstore import Store
s = Store('a','afos://0','afos://0/a',1024)
i = 0
while True:
    input('Press enter to see discovered stores and do a resolve all on root')
    print('>>>> DISCOVERED STORES: {}'.format(s.discovered_stores))
    print('>>>> RESOLVE ALL on ROOT: {}'.format(s.resolveAll('afos://0/*')))
    k = 'afos://0/a/run'
    v = i+1
    print('<<<< {} :-> {} (ver: {})'.format(k,v,s.put(k,v)))
    i=i+1


from dstore import Store
s = Store('c','afos://0','afos://0/c',1024)
i = 0
while True:
    input('Press enter to see discovered stores and do a resolve all on root')
    print('>>>> DISCOVERED STORES: {}'.format(s.discovered_stores))
    print('>>>> RESOLVE ALL on ROOT: {}'.format(s.resolveAll('afos://0/*')))


from dstore import Store
import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        s = Store(sys.argv[1], '/', '/home', 1024)
        k = input()
        v = input()
        s.put(k, v)
        k = input()
        v = s.get(k)
        print('get({}) -> {}'.format(k,v))
        input()

    else:
        print('Usage:\n\t python3 test_store.py <store-id>')

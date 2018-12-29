import rpyc
import sys


"""
The BlockStore service is an in-memory data store that stores blocks of data,
indexed by the hash value.  Thus it is a key-value store. It supports basic
get() and put() operations. It does not need to support deleting blocks of
data–we just let unused blocks remain in the store. The BlockStore service only
knows about blocks–it doesn't know anything about how blocks relate to files.
"""


class BlockStore(rpyc.Service):
    def __init__(self):
        self._store = {}


    def exposed_store_block(self, h, block):
        """
        Stores block b in the key-value store, indexed by hash value h.

        :param h: hash value
        :param block: the block to be stored
        :return: None
        """
        self._store[h] = block


    def exposed_get_block(self, h):
        """
        Retrieves a block indexed by hash value h.

        :param h: hash value
        :return: the retrieved block
        """
        return self._store[h]


    def exposed_has_block(self, h):
        """
        Signals whether block indexed by h exists in the BlockStore service.

        :param h: hash value
        :return: true if existing, false if not
        """
        return h in self._store


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer

    port = int(sys.argv[1])
    server = ThreadPoolServer(BlockStore(), port=port)
    server.start()

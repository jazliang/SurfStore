import rpyc
import hashlib
import os
import sys


"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""


class SurfStoreClient:

    def __init__(self, config):
        """
        Initialize the client and set up connections to the block stores and
        metadata store using the config file

        :param config: the filename of config file
        """
        # TODO: Handle malformed config file.
        with open(config) as f:
            lines = f.readlines()
            self._n_block = int(lines[0].split(':')[1].strip())
            self._metadata_host = lines[1].split(':')[1].strip()
            self._metadata_port = int(lines[1].split(':')[2].strip())
            self._block_stores = []
            for i in range(self._n_block):
                self._block_stores.append((lines[i + 2].split(':')[1].strip(),
                                           int(lines[i + 2].split(':')[2].strip())))

        self.conn_metadata = rpyc.connect(self._metadata_host,
                                          self._metadata_port,
                                          config={'instantiate_custom_exceptions': True,
                                                  'allow_pickle': True})
        self.conn_block_stores = [rpyc.connect(bs[0], bs[1]) for bs in self._block_stores]


    # TODO: Static method in Python
    @staticmethod
    def _compute_hash(block):
        s = hashlib.sha256(block).hexdigest()
        return s


    def _find_block_id(self, h):
        # TODO: int(n, 16)
        return int(h, 16) % self._n_block


    def upload(self, filepath):
        """
        Reads the local file, creates a set of hashed blocks and
        uploads them onto the MetadataStore.

        (and potentially the BlockStore if they were not already present there).

        :param filepath: path to the local file to be uploaded
        :return: None
        """
        hashlist = []
        block_size = 4096
        hash_to_block = {}

        if not os.path.isfile(filepath):
            print('Not Found')
            return

        with open(filepath, 'rb') as f:
            block = f.read(block_size)
            while block:
                h = self._compute_hash(block)
                hashlist.append(h)
                hash_to_block[h] = block
                # block_id = self._find_block_id(h)
                # if not self.conn_block_stores[block_id].root.has_block(h):
                #     self.conn_block_stores[block_id].root.store_block(h, block)
                block = f.read(block_size)

        filename = filepath.split('/')[-1]

        while True:
            try:
                v, _ = self.conn_metadata.root.read_file(filename)
                self.eprint('Version:', v)
                self.conn_metadata.root.modify_file(filename, v + 1, hashlist)
                print('OK')
                return
            # TODO:
            # How data types on the client/server are translated to
            # the corresponding implementation on the remote endpoint.
            except rpyc.core.vinegar.GenericException as e:
                # TODO: Exception or ErrorResponse?
                if e.error_type == 1:
                    for h in eval(e.missing_blocks):
                        # print('missing: ', h)
                        block_id = self._find_block_id(h)
                        if not self.conn_block_stores[block_id].root.has_block(h):
                            block = hash_to_block[h]
                            self.conn_block_stores[block_id].root.store_block(h, block)


    def delete(self, filename):
        """
        Signals the MetadataStore to delete a file.

        :param filename: the name of remote file to be deleted
        :return: None
        """
        while True:
            try:
                v, _ = self.conn_metadata.root.read_file(filename)
                self.eprint('Version:', v)
                self.conn_metadata.root.delete_file(filename, v + 1)
                print('OK')
                return
            except rpyc.core.vinegar.GenericException as e:
                self.eprint('Error:', e)
                # # File has not been created before.
                # if e.error_type == 3:
                #     print('Not Found')
                #     return


    def download(self, filename, local_path):
        """
        Downloads a file from SurfStore and saves it to (dst) folder.

        Ensures not to download unnecessary blocks.

        :param filename: file to be downloaded
        :param local_path: local destination
        :return: None
        """
        v, hashlist_str = self.conn_metadata.root.read_file(filename)
        self.eprint('Version:', v)

        if hashlist_str is not None:
            hashlist = eval(hashlist_str)

        # TODO: If a file was deleted... Return hashlist = []?
        if v == 0 or hashlist is None:
            print('Not Found')
            return

        if local_path[-1] != '/':
            local_path += '/'

        # TODO: Anki, Python w/r/wb/rb/a/ab/w+/r+ to a file
        with open(local_path + filename, 'wb') as f:
            for h in hashlist:
                if os.path.isfile(local_path + h):
                    with open(local_path + h, 'rb') as bf:
                        f.write(bf.read())
                else:
                    new_block = self.conn_block_stores[self._find_block_id(h)].root.get_block(h)
                    with open(local_path + h, 'wb') as bf:
                        bf.write(new_block)
                    f.write(new_block)

        print('OK')


    def eprint(*args, **kwargs):
        """
        Use eprint to print debug messages to stderr

        Example:
            - self.eprint("This is a debug message")

        :param args:
        :param kwargs:
        :return:
        """
        print(*args, file=sys.stderr, **kwargs)


if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    operation = sys.argv[2]
    if operation == 'upload':
        client.upload(sys.argv[3])
    elif operation == 'download':
        client.download(sys.argv[3], sys.argv[4])
    elif operation == 'delete':
        client.delete(sys.argv[3])
    else:
        print("Invalid operation")

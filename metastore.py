import rpyc
import sys
import threading
import copy

'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''


class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3


# TODO: Debug techniques for RPC or distributed programming.
# TODO: anki, Threading -> lock, see: http://effbot.org/zone/thread-synchronization.htm
# TODO: Key to synchronization -> just think for a moment what shared resources are.
# TODO: GIL
# TODO: Learn more about Python pickle.


'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''


class MetadataStore(rpyc.Service):

    def __init__(self, config):
        """
        Initialize the class using the config file provided.

        :param config: file name of the provided config file
        """

        self._filename_to_v_bl = {}
        self._deleted_files = set()
        self._lock = threading.Lock()

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
            self.conn_block_stores = [rpyc.connect(bs[0], bs[1]) for bs in self._block_stores]


    def _find_block_id(self, h):
        # TODO: int(n, 16)
        return int(h, 16) % self._n_block


    # TODO: Handle race conditions
    def exposed_modify_file(self, filename, version, hashlist):
        """
        Modifies file f so that it now contains the contents
        referred to by the hashlist hl.

        The version provided, v, must be exactly one larger than
        the current version that the MetadataStore maintains.

        :param filename: the file to be modified
        :param version: the provided version number
        :param hashlist:
        :return: None
        """
        # hashlist = eval(hashlist_str)
        hashlist = copy.deepcopy(hashlist)
        # Check filename.
        filename = filename.split('/')[-1]

        # TODO: Create a file that existed before.
        with self._lock:
            missing_blocks = []
            for h in hashlist:
                block_id = self._find_block_id(h)
                if not self.conn_block_stores[block_id].root.has_block(h):
                    missing_blocks.append(h)

            if missing_blocks:
                err = ErrorResponse('Missing blocks.')
                err.missing_blocks(str(missing_blocks))
                raise err

            if filename in self._filename_to_v_bl:
                if version == self._filename_to_v_bl[filename][0] + 1:
                    # If the file existed before, remove it from the set of deleted files.
                    if filename in self._deleted_files:
                        self._deleted_files.remove(filename)
                    self._filename_to_v_bl[filename] = [version, hashlist]
                else:
                    err = ErrorResponse('Wrong version.')
                    current_version = self._filename_to_v_bl[filename][0]
                    err.wrong_version_error(current_version)
                    raise err
            else:
                if version == 1:
                    self._filename_to_v_bl[filename] = [1, hashlist]
                else:
                    err = ErrorResponse('Wrong version.')
                    err.wrong_version_error(0)
                    raise err
            # print(self._filename_to_v_bl)


    # TODO: Handle race conditions
    def exposed_delete_file(self, filename, version):
        """
        Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        :param filename: the file to be deleted
        :param version: the provided version number
        :return: None
        """
        # Check filename.
        filename = filename.split('/')[-1]

        with self._lock:
            if filename in self._filename_to_v_bl:
                if version == self._filename_to_v_bl[filename][0] + 1:
                    self._deleted_files.add(filename)
                    self._filename_to_v_bl[filename] = [version, None]
                else:
                    err = ErrorResponse("Wrong version.")
                    current_version = self._filename_to_v_bl[filename][0]
                    err.wrong_version_error(current_version)
                    raise err
            else:
                # TODO: Delete a file that does not exist. "Not Found" or "OK"?? Version + 1 or not?
                # err = ErrorResponse("File not found.")
                # err.file_not_found()
                # raise err
                if version == 1:
                    self._deleted_files.add(filename)
                    self._filename_to_v_bl[filename] = [version, None]
                else:
                    err = ErrorResponse("Wrong version.")
                    current_version = self._filename_to_v_bl[filename][0]
                    err.wrong_version_error(current_version)
                    raise err
            # print(self._filename_to_v_bl)


    def exposed_read_file(self, f):
        """
        Reads the file with filename f.

        :param f: filename
        :return: If the file exist, return the string representation of (v, hl),
                    where v is the most up-to-date version number and
                    hl is the corresponding hashlist.
                 If the file does not exist, return the string representation of (0, []).

        """
        # Check filename.
        f = f.split('/')[-1]

        with self._lock:
            if f in self._filename_to_v_bl:
                version = self._filename_to_v_bl[f][0]
                print('version:', version)
                # print('version:', version)
                if f in self._deleted_files:
                    # TODO: Return `None` instead of `str(None)`.
                    return version, None
                else:
                    return version, str(self._filename_to_v_bl[f][1])
            else:
                # print('version:', 0)
                print('version:', 0)
                return 0, str([])


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
    from rpyc.utils.server import ThreadPoolServer

    # rpyc.core.vinegar._generic_exceptions_cache['ErrorResponse'] = ErrorResponse
    config_filename = sys.argv[1]
    with open(config_filename, 'r') as config_f:
        port = int(config_f.readlines()[1].split(':')[2].strip())
    server = ThreadPoolServer(MetadataStore(sys.argv[1]), port=port)
    server.start()

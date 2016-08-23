from abe.db.api import MongodbClient
from abe.block.api import BlockAPI
from abe.token.api import TokenAPI
from abe.tx.api import TxAPI
from abe import flags
from multiprocessing import Process
from abe.decorator import signal_watcher
import os
import sys
import gflags

FLAGS = flags.FLAGS

BASE_PATH = os.getcwd()


@signal_watcher
def block_process():
    block_api = BlockAPI()
    block_api.synchronize()

def sync_blocks():
    blk = Process(target = block_process, args = ())
    blk.start()
    blk.join()

@signal_watcher
def token_process(token_name, api):
    api.synchronize(token_name)

def sync_token(tname):
    token_api = TokenAPI()
    p = Process(target = token_process, args = (tname, token_api))
    p.start()
    p.join()


def main(argv):
    try:
        argv = FLAGS(argv)
        if FLAGS.sync_block:
            print "blocks begin to sync"
            sync_blocks()
        if FLAGS.sync_token:
            if FLAGS.token == "":
                print "no token specified !"
                print 'Usage: %s  command [option]\n%s' % (sys.argv[0], FLAGS)
            else:
                print "token %s begin to sync!" % FLAGS.token
                sync_token(FLAGS.token)
    except gflags.FlagsError, e:
        print '%s\nUsage: %s args\n%s' % (e, sys.argv[0], FLAGS)
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)


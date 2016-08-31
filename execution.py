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



def main(argv):
    try:
        argv = FLAGS(argv)
        if FLAGS.sync_block:
            begin = FLAGS.begin
            end = FLAGS.end
            sync_balance = FLAGS.sync_balance
            if begin != -1 and end != -1 and end > begin:
                block_api = BlockAPI()
                block_api.synchronize(begin, end, sync_balance)
                return

        elif FLAGS.check_block:
            begin = FLAGS.begin
            end = FLAGS.end
            sync_balance = FLAGS.sync_balance
            if begin != -1 and end != -1 and end > begin:
                block_api = BlockAPI()
                block_api.check(begin, end, sync_balance)
                return

        elif FLAGS.sync_block_forever:
            block_api = BlockAPI()
            block_api.synchronize()
            return


        elif FLAGS.sync_token:
            if FLAGS.token != "":
                print "token %s begin to sync!" % FLAGS.token
                token_api = TokenAPI()
                api.synchronize(FLAGS.token)
                return

        
        print '%s\nUsage: %s args\n%s' % (e, sys.argv[0], FLAGS)
        sys.exit(1)

    except gflags.FlagsError, e:
        print '%s\nUsage: %s args\n%s' % (e, sys.argv[0], FLAGS)
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)


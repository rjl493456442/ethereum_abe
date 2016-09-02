from abe.db.api import MongodbClient
from abe.block.api import BlockAPI
from abe.token.api import TokenAPI
from abe.tx.api import TxAPI
from abe import flags
from multiprocessing import Process
from abe.decorator import signal_watcher
from scripts import tools
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

        elif FLAGS.sync_block_forever:
            block_api = BlockAPI()
            block_api.synchronize_forever()
            return

        elif FLAGS.check_block:
            shardId = FLAGS.shardId
            sync_balance = FLAGS.sync_balance
        
            if shardId != -1:
                block_api = BlockAPI()
                block_api.check(shardId, sync_balance)
                return

        elif FLAGS.sync_balance:
            shardId = FLAGS.shardId
            if shardId != -1:
                block_api = BlockAPI()
                block_api.sync_balance(shardId)
                return

        elif FLAGS.sync_it:
            log_path = FLAGS.log_location
            shardId = FLAGS.shardId
            if log_path != '':
                block_api = BlockAPI()
                block_api.sync_it(log_path, shardId)
                return

        elif FLAGS.sync_token:
            if FLAGS.token != "":
                print "token %s begin to sync!" % FLAGS.token
                token_api = TokenAPI()
                api.synchronize(FLAGS.token)
                return

        elif FLAGS.tool and FLAGS.service != '':
            tool = tools.Tool()
            tool.service(FLAGS.service)
            return 

        print 'Usage: %s args\n%s' % (sys.argv[0], FLAGS)
        sys.exit(1)

    except gflags.FlagsError, e:
        print '%s\nUsage: %s args\n%s' % (e, sys.argv[0], FLAGS)
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)


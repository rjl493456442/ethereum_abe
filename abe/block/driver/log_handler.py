from abe import logger
from abe import constant
from abe import flags
import time
from abe import utils
import itertools
FLAGS = flags.FLAGS

class LogHandler(object):
    def __init__(self, db_proxy):
        self.logger = logger
        self.db_proxy = db_proxy
        super(LogHandler, self).__init__()

    def run(self, filename):
        with open(filename, "r") as f:
            while True:
                next_n_lines = list(itertools.islice(f, 1000))
                if not next_n_lines:
                    break
                else:
                    self.process_lines(next_n_lines)
    

    def process_lines(self, lines):
        for line in lines:
            try:
                if line = "": continue
                info = utils.regular_extract(line)
                if info:    
                    self.save_to_db(info)
            except Exception, e:
                self.logger.info("Error: %s, Invalid line %s" % (e, line))
            
    def save_to_db(self, info):
        blocknumber = info["blocknumber"]
        blockhash = info["blockhash"]
        txhash = info["txhash"]
        
        if info['type'] == 0:
            # normal internal tx log record    
            it = {
                "to" : info['to'],
                "value" : info['value'],
                "timestamp" : info['timestamp'],
                "depth" : info['depth'],
                "pc": info['pc'],
                "calltype": info['calltype'],
                "error" : info['error'],
            }

            objectId = self.db_proxy.insert(FLAGS.internaltx_prefix, it, block_height = blocknumber).inserted_id
            
            res = self.db_proxy.get(FLAGS.block_it, {"block_hash":blockhash}, multi=False)
            if res:
                query = {
                    "block_hash" : blockhash,
                    "txs" : {"$elemMatch" : {"tx_hash":txhash}},
                }
                res = self.db_proxy.get(FLAGS.block_it, query, multi = False)
                if res:
                    operation = {
                        "$addToSet" : {"txs.$.internal_txs" : objectId},
                    }
                    self.db_proxy.update(FLAGS.block_it, query, operation)
                else:
                    operation = {
                        "$addToSet" : {"txs" :  
                            {
                                "tx_hash" : txhash,
                                "status" : "",
                                "internal_txs" : [objectId],
                            },
                        },
                    }
                    self.db_proxy.update(FLAGS.block_it, {"block_hash":blockhash}, operation)
            else:
                operation = {
                    "$set" : {"block_hash" : blockhash, "block_number" : blocknumber},
                    "$addToSet" : {
                        "txs": {
                            "tx_hash" : txhash,
                            "status" : "",
                            "internal_txs" : [objectId],
                        },
                    }, 
                }
                self.db_proxy.update(FLAGS.block_it, {"block_hash":blockhash}, operation, upsert = True)

        elif info['type'] == 1:
            res = self.db_proxy.get(FLAGS.block_it, {"block_hash":blockhash, "txs.tx_hash":txhash}, multi = False)
            if res and info['depth'] == 0:
                self.db_proxy.update(FLAGS.block_it, {"block_hash":blockhash, "txs.tx_hash":txhash}, {"$set":{"txs.$.status": info["status"]}})
            else:
                self.logger.info("Not found err tx, ", info)

        else:
            self.logger.info("Invalid type:", info)


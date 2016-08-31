from abe import logger
from abe import constant
from abe import flags
import time
from abe import utils
import itertools
import time
import os
FLAGS = flags.FLAGS

class LogHandler(object):
    def __init__(self, db_proxy):
        self.logger = logger
        self.db_proxy = db_proxy
        super(LogHandler, self).__init__()

    def run(self, filename):
        file_handler = open(filename, "r")
        for data in self.read_logs(file_handler):
            self.process_log(data)

    def read_logs(self, file_handler):
        data = []
        txhash = None
        while True:
            line = file_handler.readline()
            if line == "":
                if file_handler.tell() == os.fstat(file_handler.fileno()).st_size:
                    yield data
                    break
                else:
                    continue
            try:
                info = utils.regular_extract(line)
                if txhash is None: 
                    txhash = info["txhash"]
                    data.append(info)
                else:
                    if txhash != info["txhash"]:
                        file_handler.seek(-1 * len(line), 1)
                        yield data
                        txhash = None
                        data = []
                    else:
                        data.append(info)
            except Exception, e:
                self.logger.info("Error: %s, Invalid line %s" % (e, line))
            
    def process_log(self, data):
        # preprocess
        for _,info in enumerate(data):
            if info['type'] == 0 and info['error'] != '' or info['type'] == 1 and info['depth'] == 0: 
                cursor = _ - 1
                while cursor >= 0:
                    if data[cursor]['type'] == 1: pass
                    elif data[cursor]['depth'] > info['depth']:
                        if info['type'] == 0:
                            data[cursor]['error'] = info['error']
                        if info['type'] == 1:
                            data[cursor]['error'] = info['status']
                    elif data[cursor]['depth'] == info['depth']:
                        break
                    cursor = cursor - 1

        for info in data:
            self.save_to_db(info)

            
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

        elif info['type'] == 1 and info['depth'] == 0:
            self.db_proxy.update(FLAGS.block_it, {"block_hash":blockhash, "txs.tx_hash":txhash}, {"$set":{"txs.$.status": info["status"]}})

        else:
            self.logger.info("Invalid type:", info)

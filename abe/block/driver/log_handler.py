from abe import logger
from abe import constant
from abe import flags
import time
from abe import utils
import itertools
import time
import os
import hashlib
FLAGS = flags.FLAGS

class LogHandler(object):
    def __init__(self, db_proxy):
        self.logger = logger
        self.db_proxy = db_proxy
        super(LogHandler, self).__init__()

    def sync_it(self, block_height):
        self.block_height = block_height
        self.cursor = self._get_cursor()

        file_handler = open(self.cursor["filename"], "r")
        
        data = self._find_log(file_handler, block_height)
        for d in data:print d
        print len(data)
        if len(data) != 0: self.process_log(data)
        self._save_cursor(file_handler)

    def _get_cursor(self):
        res = self.db_proxy.get(FLAGS.meta, {"sync_record":"ethereum"}, multi = False)
        if res and res.has_key("cursor"):
            cursor = res["cursor"]
            if cursor['fileId'] != self.block_height/FLAGS.table_capacity:
                cursor = {
                    "filename":FLAGS.log_location+"/"+FLAGS.internaltxlog+str(self.block_height/FLAGS.table_capacity),
                    "position":0,
                    "fileId": self.block_height/FLAGS.table_capacity,
                }
        else:
            cursor = {
                "filename":FLAGS.log_location+"/"+FLAGS.internaltxlog+str(self.block_height/FLAGS.table_capacity),
                "position":0,
                "fileId": self.block_height/FLAGS.table_capacity,
            }
        return cursor

    def _save_cursor(self, file_handler):
        self.cursor['position'] = file_handler.tell()
        operation = {
            "$set" : {"cursor" : self.cursor}, 
        }
        self.db_proxy.update(FLAGS.meta, {"sync_record":"ethereum"}, operation, upsert = True)

    def _find_log(self, file_handler, block_height):
        data = []
        while True:
            line = file_handler.readline()
            if line == "":
                if file_handler.tell() == os.fstat(file_handler.fileno()).st_size:
                    return data
                else:
                    continue
            try:
                info = utils.regular_extract(line)
                if not info: continue
                if info['blocknumber'] != block_height: 
                    file_handler.seek(-1 * len(line), 1)
                    return data
                else:
                    data.append(info)
            except Exception, e:
                self.logger.info("Error: %s, Invalid line %s, ignore it" % (e, line))

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
                if not info: continue
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
                self.logger.info("Error: %s, Invalid line %s, ignore it" % (e, line))
            
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
                "hash" : info['hash'],
                "from" : info['from'],
                "to" : info['to'],
                "value" : info['value'],
                "timestamp" : info['timestamp'],
                "depth" : info['depth'],
                "pc": info['pc'],
                "calltype": info['calltype'],
                "error" : info['error'],
            }

            objectId = self.db_proxy.update(FLAGS.internaltx_prefix, {"hash":it['hash']}, {"$set":it}, block_height = blocknumber, upsert = True, multi = False).upserted_id
            if objectId is  None:
                self.logger.info("internal tx %s has been add, ignore it", info['hash'])
                return

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


from abe.token.driver import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
import time
from ethereum import abi as _abi
from multiprocessing import Process
import signal
from abe import utils
import abe.token.utils as _utils
FLAGS = flags.FLAGS

class BuiltinDriver(base.TokenBuiltinBase):

    @property
    def type(self):
        return 'DIGIX'

    @property
    def event(self):
        return constant.DIGIX_EVENT

    @property
    def last_block(self):
        return self._last_block

    def __init__(self):
        self.logger = logger
        self.rpc_cli = JSONRPCClient(host = FLAGS.rpc_host, port = FLAGS.rpc_port, print_communication = False)
        super(BuiltinDriver, self).__init__()
        
    def initialize(self):
        self.add_indexes_for_token(self.type)
        res = self.db_proxy.get(FLAGS.token_prefix + self.type, {"type": self.event}, multi = True, sort_key = "block", ascend = False, limit = 1)
        if res:
            self._last_block = res[0]['block']
        else:
            self._last_block = None

        # add token basic info
        source_code = self.get_source_code(self.type)
        abi = self.get_abi(self.type, constant.DIGIX_CONTRACT_NAME)
        basic_info = dict(
            token=self.type,
            source_code=source_code,
            abi=abi,
            address=constant.DIGIX_ADDR
        )
        self.db_proxy.update(FLAGS.token_basic, {"token":self.type}, {"$set":basic_info}, upsert = True)


    def get_past_logs(self):
        abi = self.get_abi(self.type, constant.DIGIX_CONTRACT_NAME)
        
        event_id = self.get_event_id(abi, self.event)

        from_block = hex(self.last_block) if self.last_block else "0x0" 

        params =  {
          "fromBlock": from_block,
          "toBlock": "latest",
          "address": constant.DIGIX_ADDR,
          "topics": [event_id, None, None]
        }

        self.filter_id = self.rpc_cli.call(constant.METHOD_NEW_FILTER, params)
        res = self.rpc_cli.call(constant.METHOD_GET_FILTER_LOGS, self.filter_id)

        for log in res:
            self.handle_log(log)


    def handle_log(self, log):
        hash = _utils.hash_log(log)
        transfer_table = FLAGS.token_prefix + "DGD"
        balance_table = FLAGS.balance_prefix + "DGD"

        user = '0x' + log['topics'][1].lower()[26:]
        amount = int(log['topics'][2], 16)
        badge = int(log['topics'][3], 16)
                    
        operation = {
            "$set": {
                "hash" : hash,
                "from" : f,
                "to" : to,
                "value" : value,
                "transactionHash" : log["transactionHash"],
                "block" : int(log["blockNumber"], 16),
                "blockHash" : log["blockHash"],
                "type" : self.event
            }
        }

        objectId = self.db_proxy.update(transfer_table, {"hash":hash}, operation, multi = False, upsert = True).upserted_id

        if objectId is  None:
            self.logger.info("event log %s has been add, ignore it", hash)
            return
        # update balance
        # TODO parse demical of token
        operation = {
            "$inc" : {"balance" : amount}
        }
        self.db_proxy.update(balance_table, {"account" : user}, operation, upsert = True)


    def revert_log(self, log):
        hash = _utils.hash_log(log)
        transfer_table = FLAGS.token_prefix + "DGD"
        balance_table = FLAGS.balance_prefix + "DGD"

        user = '0x' + log['topics'][1].lower()[26:]
        amount = int(log['topics'][2], 16)
        badge = int(log['topics'][3], 16)
                    
        deleted_count = self.db_proxy.delete(transfer_table, {
            "hash" : hash,
            "from" : f,
            "to" : to,
            "value" : value,
            "transactionHash" : log["transactionHash"],
            "block" : int(log["blockNumber"], 16),
            "blockHash" : log["blockHash"],
            "type" : self.event,
        }, multi = False).deleted_count

        if deleted_count == 0:
            self.logger.info("event log %s has been deleted, ignore it", hash)
            return

        # update balance
        # TODO parse demical of token
        operation = {
            "$inc" : {"balance" : amount * -1}
        }
        self.db_proxy.update(balance_table, {"account" : user}, operation, upsert = True)

    def wait(self):
        self.listener.join()





    

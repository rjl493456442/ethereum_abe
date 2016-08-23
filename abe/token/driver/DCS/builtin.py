from abe.token.driver import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
import time
from ethereum import abi as _abi
import abe.token.utils as _utils

FLAGS = flags.FLAGS

class BuiltinDriver(base.TokenBuiltinBase):

    @property
    def type(self):
        return 'DCS'

    @property
    def event(self):
        return constant.DCS_EVENT

    @property
    def last_block(self):
        return self._last_block

    def __init__(self):
        self.logger = logger
        self.abi = self.get_abi(self.type, constant.DCS_CONTRACT_NAME)
        super(BuiltinDriver, self).__init__()
        

    def initialize(self):
        self.add_indexes_for_token(self.type)
        res = self.db_proxy.get(FLAGS.token_prefix + self.type, {"type": self.event}, multi = True, sort_key = "block", ascend = False, limit = 1)
        if res:
            self._last_block = res[0]['block']
        else:
            self._last_block = None
        self.init_tx = self.rpc_cli.call(constant.METHOD_GET_TX_BY_HASH, constant.DCS_INIT_TX_HASH)
        init_acct = self.init_tx['from']
        res = self.db_proxy.get(FLAGS.balance_prefix + self.type, {"account": init_acct}, multi = False)
        if not res:
            self.get_initial_supply()

         # add token basic info
        source_code = self.get_source_code(self.type)
        abi = self.get_abi(self.type, constant.DCS_CONTRACT_NAME)
        basic_info = dict(
            token=self.type,
            source_code=source_code,
            abi=abi,
            supply=10000000
        )
        self.db_proxy.update(FLAGS.token_basic, {"token":self.type}, {"$set":basic_info}, upsert = True)


    def get_initial_supply(self):
        operation = {
            "$inc" : {"balance" : 10000000}
        }
        balance_table = FLAGS.balance_prefix + self.type
        self.db_proxy.update(balance_table, {"account" : self.init_tx['from']}, operation, upsert = True)

    def get_past_logs(self):
        event_id = self.get_event_id(self.abi, self.event)

        from_block = hex(self.last_block) if self.last_block else "0x0" 

        params =  {
          "fromBlock": from_block,
          "toBlock": "latest",
          "address": constant.DCS_ADDR,
          "topics": [event_id, None, None]
        }

        self.filter_id = self.rpc_cli.call(constant.METHOD_NEW_FILTER, params)
        res = self.rpc_cli.call(constant.METHOD_GET_FILTER_LOGS, self.filter_id)

        for log in res:
            self.handle_log(log)


    def handle_log(self, log):
        transfer_table = FLAGS.token_prefix + self.type
        balance_table = FLAGS.balance_prefix + self.type

        data = log['data'][2:].decode('hex')
        data_params = self.data_params(self.abi, self.event)

        value = _abi.decode_abi(data_params, data)[0]

        f = '0x' + log['topics'][1].lower()[26:]
        to = '0x' + log['topics'][2].lower()[26:]

        self.db_proxy.insert(transfer_table, {
            "from" : f,
            "to" : to,
            "value" : value,
            "transactionHash" : log["transactionHash"],
            "block" : int(log["blockNumber"], 16),
            "type" : self.event
        })

        # update balance
        # TODO parse demical of token
        operation = {
            "$inc" : {"balance" : value  * -1}
        }
        self.db_proxy.update(balance_table, {"account" : f}, operation, upsert = True)

        operation2 = {
            "$inc" : {"balance" : value }
        }
        self.db_proxy.update(balance_table, {"account" : to}, operation2, upsert = True)

    def revert_log(self, log):
        transfer_table = FLAGS.token_prefix + self.type
        balance_table = FLAGS.balance_prefix + self.type

        data = log['data'][2:].decode('hex')
        data_params = self.data_params(self.abi, self.event)

        value = _abi.decode_abi(data_params, data)[0]

        f = '0x' + log['topics'][1].lower()[26:]
        to = '0x' + log['topics'][2].lower()[26:]

        self.db_proxy.delete(transfer_table, {
            "from" : f,
            "to" : to,
            "value" : value,
            "transactionHash" : log["transactionHash"],
            "block" : int(log["blockNumber"], 16),
            "type" : self.event
        }, multi = False)

        # update balance
        # TODO parse demical of token
        operation = {
            "$inc" : {"balance" : value  * 1}
        }
        self.db_proxy.update(balance_table, {"account" : f}, operation, upsert = True)

        operation2 = {
            "$inc" : {"balance" : value * -1}
        }
        self.db_proxy.update(balance_table, {"account" : to}, operation2, upsert = True)


    def wait(self):
        self.listener.join()





    

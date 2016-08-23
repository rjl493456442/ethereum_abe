from abe.db import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags 
from abe.token import utils as _utils
import time
from ethereum import abi
from abe import BASE_PATH
import os
import json
from multiprocessing import Process
import signal
from abe import utils

FLAGS = flags.FLAGS

class TokenBuiltinBase(base.Base):

    def __init__(self):
        self.logger = logger
        self.rpc_cli = JSONRPCClient(host = FLAGS.rpc_host, port = FLAGS.rpc_port, print_communication = False)
        super(TokenBuiltinBase, self).__init__()

    def synchronize(self):
        self.initialize()
        self.get_past_logs()
        self.listen()
        self.wait()

    def initialize(self):
        pass

    def get_past_logs(self):
        pass

    def wait(self):
        pass

    def get_source_code(self, filename):
        path = "abe/token/driver/%s/%s.sol" % (filename, filename)
        contract_source_path = os.path.join(BASE_PATH, path)
        if os.path.exists(contract_source_path):
            with open(contract_source_path, "r") as f:
                return f.readlines()

    def get_abi(self, filename, contract_name):
        path = "abe/token/driver/%s/compiled.txt" % filename
        contract_path = os.path.join(BASE_PATH, path)
        if os.path.exists(contract_path):
            with open(contract_path, "r") as f:
                abi = json.load(f)
                return abi[contract_name]['abi']
        else:
            path = "abe/token/driver/%s/%s.sol" % (filename, filename)
            contract_source_path = os.path.join(BASE_PATH, path)

            if os.path.exists(contract_source_path):
                compiled = _utils.compile_contract(contract_source_path)
                # write to file
                abi_path = "abe/token/driver/%s/compiled.txt" % filename
                with open(os.path.join(BASE_PATH, abi_path), "w") as f:
                    compiled = _utils.remove_binary_data(compiled)
                    json.dump(compiled, f)
                return compiled[contract_name]['abi']
            else:
                return None

    def get_inputs(self, abi, name):
        """ get function input param types """
        for item in abi:
            if not item.has_key('name') or item.has_key('name') and item['name'] != name:
                continue
            return item['inputs']
        return None

    def get_inputs_for_constructor(self, abi):
        for item in abi:
            if item.has_key("type") and item['type'] == "constructor":
                return item['inputs']
        return None

    def data_params(self, abi, name):
        inputs = self.get_inputs(abi, name)
        params = [param['type'] for param in inputs if param["indexed"] == False]
        return params


    def get_event_id(self, abi, event):
        inputs = self.get_inputs(abi, event)
        if inputs:
            params = [param['type'] for param in inputs]
            event_id = _utils.event_id(event, params)
            return hex(event_id)[:-1]
        else:
            self.logger.info("not find the event in abi")

    def listen(self):
        self.listener = Process(target = self.listener_proc, args = (self.rpc_cli, self.logger, self.filter_id))
        self.listener.daemon = True
        self.listener.start()

    def listener_proc(self, rpc_cli, logger, filter_id):
        signal.signal(signal.SIGINT, utils.signal_handler)
        try:
            while True:
                res = rpc_cli.call(constant.METHOD_GET_FILTER_CHANGES, filter_id)
                if res:
                    logger.info("new token arrive: %s" % res)
                    for log in res:
                        if not "removed" in log.keys():
                            self.handle_log(log)
                        else:
                            if log['removed']:
                                logger.info("an removed log occur, revert it")
                                self.revert_log(log)
                            else:
                                self.handle_log(log)
                else:
                    time.sleep(FLAGS.poll_interval)
        except:
            # connect failed, ignore it
            time.sleep(FLAGS.poll_interval)
            pass


    def handle_log(log):
        pass
    




    

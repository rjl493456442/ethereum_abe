from abe.db import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
import time
from abe import utils
import json
FLAGS = flags.FLAGS

class BlockHandler(object):
    def __init__(self, rpc_cli, logger, db_proxy, sync_balance = False, sync_it = False):
        self.rpc_cli = rpc_cli
        self.logger = logger
        self.db_proxy = db_proxy
        self.sync_balance = sync_balance
        super(BlockHandler, self).__init__()

    def execute(self, blockinfo, fork_check):
        if isinstance(blockinfo, int):
            if blockinfo == 0:
                self.process_genesis()
                return True

            method_name = constant.METHOD_GET_BLOCK_BY_NUMBER

        elif isinstance(blockinfo, str) or isinstance(blockinfo, unicode):
            method_name = constant.METHOD_GET_BLOCK_BY_HASH

        try:
            blk = self.rpc_cli.call(method_name, blockinfo, True)
            if blk:
                self.blk_number = utils.convert_to_int(blk['number'])

                if fork_check:
                    res = self.db_proxy.get(FLAGS.blocks, {"number":self.blk_number}, block_height = self.blk_number)
                    if res and res["hash"] != blk["hash"]:
                        '''
                        chain head change
                        (1) different txs
                        (2) undo txs in old block but not in new one
                        (3) apply txs in new block but not in old one
                        (4) replace the origin 
                        '''
                        self.process_fork(res, blk)
                    else:
                        success = self.process_block(blk)
                        return True
                else:
                    # no necessary to check whether is a fork block
                    success = self.process_block(blk)
                return success
            else:
                self.logger.info("block %s not exist in blockchain, discard" % blk["number"])
                return False
        except Exception, e:
            self.logger.error(e)
            return False
    
    def process_genesis(self):
        ''' add genesis block from file to db '''
        filename = FLAGS.genesis_data
        with open(filename, "r") as f:
            data = json.load(f)
            data['transactions'] = []
            for to in data['alloc'].keys():
                value = data['alloc'][to]['balance']
                value = hex(int(value))
                if value.endswith('L'): value = value[:-1]
                data['transactions'].append({
                    "hash" : "GENESIS_" + to,
                    "from" : "GENESIS",
                    "to" : '0x'+ to,
                    "value" : value,
                })

            del data['alloc']
        
        block = data
        txs = block['transactions']
        timestamp = block['timestamp']
        
        for tx in txs:
            tx['blockNumber'] = "0x0"
            tx['timestamp'] = timestamp
            operation = {
                "$set" : {"address" : tx["to"]},
            }
            self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation, upsert = True, multi = False)
            self.db_proxy.update(FLAGS.txs, {"hash":tx["hash"]}, {"$set":tx}, upsert = True, multi = False, block_height = 0)
        block['txs_num'] = len(block['transactions'])
            
        del block['transactions']
        self.db_proxy.update(FLAGS.blocks, {"number":0}, {"$set":block}, block_height = 0, upsert = True)
        return True

    def process_block(self, block):
        txs = block['transactions']
        timestamp = block['timestamp']      
        # process tx 
        total_fee = 0
        for tx in txs:
            res = self.process_tx(tx, timestamp)
            if res:
                total_fee = total_fee + res
        block['number'] = utils.convert_to_int(block['number'])

        # get tx related accounts
        accounts = []
        for tx in txs:
            accounts.append(tx['from'])
            accounts.append(tx['to'])

        block['txs_num'] = len(block['transactions'])
        del block['transactions']

        # update miner account
        basic_reward = FLAGS.default_reward + len(block['uncles']) * FLAGS.default_reward * 1.0 / 32
        basic_reward = utils.unit_convert_from_ether(basic_reward)
        block['reward'] = total_fee + basic_reward
    
        operation = {
            "$set" : {"address" : block["miner"], "miner":1},
        }

        self.db_proxy.update(FLAGS.accounts, {"address":block["miner"]}, operation, upsert = True)
        # process uncle
        uncle_miners = self.process_uncle(block)
        accounts.extend(uncle_miners)
        accounts.append(block['miner'])

        if self.sync_balance: self.set_balance(accounts, self.blk_number)
        # insert block
        self.db_proxy.update(FLAGS.blocks, {"number":block['number']}, {"$set":block}, block_height = self.blk_number, upsert = True)
        return True

    def process_fork(self, old_block, new_block):
        '''
        chain head change
        (1) different txs
        (2) undo txs in old block but not in new one
        (3) apply txs in new block but not in old one
        (4) mark the origin
        '''
        self.logger.info("chain head change at height %d" % old_block["number"])
        accounts = old_hashes = new_hashes = []
        old_txs = self.db_proxy.get(FLAGS.txs, {"blockNumber" : hex(old_block['number'])}, block_height = self.blk_number, multi = True)

        old_hashes = [tx['hash'] for tx in old_txs]
        new_hashes = [tx['hash'] for tx in new_block['transactions']]    

        # undo_txs
        undo_txs = []
        undo_fee = 0
        for _,thash in enumerate(old_hashes):
            if thash not in new_hashes:
                undo_txs.append(old_txs[_])
                undo_fee = undo_fee + old_txs[_]['fee']
                accounts.append(old_txs[_]['from'])
                accounts.append(old_txs[_]['to'])

        # apply_txs
        apply_txs = []
        for _,tx in enumerate(new_hashes):
            if tx not in old_hashes:
                apply_txs.append(new_block['transactions'][_])
                accounts.append(new_block['transactions'][_]['from'])
                accounts.append(new_block['transactions'][_]['to'])

        # revert
        self.revert(old_block, undo_txs)
        revert_uncle_miners = self.revert_uncle(old_block)
        accounts.extend(revert_uncle_miners)

        # process new block
        apply_fee = 0
        for tx in apply_txs:
            fee = self.process_tx(tx, new_block['timestamp'])
            apply_fee = apply_fee + fee

        # save new block info
        new_block["number"] = utils.convert_to_int(new_block['number'])
        diff_uncle_reward = (len(new_block['uncles']) - len(old_block['uncles'])) * FLAGS.default_reward * 1.0 / 32
        diff_uncle_reward = utils.unit_convert_from_ether(diff_uncle_reward)
        new_block['reward'] = old_block['reward'] - undo_fee + apply_fee + diff_uncle_reward
        new_block['txs_num'] = len(new_block['transactions'])
        del new_block['transactions']

        # update miner account
        operation = {
            "$set" : {"address" : new_block["miner"], "miner":1},
        }
        self.db_proxy.update(FLAGS.accounts, {"address":new_block["miner"]}, operation, upsert = True)
        
        uncle_miners =  self.process_uncle(new_block)
        accounts.extend(uncle_miners)

        accounts.append(new_block["miner"])
        accounts.append(old_block["miner"])

        # set balance
        if self.sync_balance : self.set_balance(accounts, self.blk_number)
        
        self.db_proxy.update(FLAGS.blocks, {"hash": new_block['hash']}, {"$set":new_block}, block_height = self.blk_number, upsert = True)

    def process_uncle(self, block):
        miners = []

        if isinstance(block['number'], str) or isinstance(block['number'], unicode):
            current_height = utils.convert_to_int(block['number'])
            block['number'] = current_height
        else:
            current_height = block['number']
        
        for _, uncle in enumerate(block['uncles']):
            buncle = self.rpc_cli.call(constant.METHOD_GET_UNCLE_BY_BLOCK_HASH_AND_INDEX, block["hash"], _)
            if buncle:
                miners.append(buncle["miner"])
                uncle_reward = (utils.convert_to_int(buncle['number']) - current_height + 8) * 1.0 * FLAGS.default_reward / 8
                uncle_reward = utils.unit_convert_from_ether(uncle_reward)
                
                operation = {
                    "$set" : {"address" : buncle["miner"], "miner":1},
                }
                self.db_proxy.update(FLAGS.accounts, {"address":buncle["miner"]}, operation, upsert = True)

                buncle['mainNumber'] = current_height
                buncle['reward'] = uncle_reward
                self.db_proxy.update(FLAGS.uncles, {"hash":buncle["hash"]}, {"$set":buncle}, block_height = current_height, upsert = True)
        return miners

    def process_tx(self, tx, timestamp):
        # merge tx receipt data
        receipt = self.rpc_cli.call(constant.METHOD_GET_TX_RECEIPT, tx['hash'])
        receipt_useful_field = ['cumulativeGasUsed', 'contractAddress', 'gasUsed', 'logs']
        for k, v in receipt.items():
            if k in receipt_useful_field:
                tx[k] = v
        tx['timestamp'] = timestamp
        
        # apply tx
        try:
            operation = {
                "$set" : {"address" : tx["from"]},
            }
            self.db_proxy.update(FLAGS.accounts, {"address":tx["from"]}, operation, upsert = True)

            
            if tx['contractAddress']:
                # contract creation transaction
                code = self.rpc_cli.call(constant.METHOD_GET_CODE, tx['contractAddress'], "latest")
                operation = {
                    "$set" : {"address" : tx["contractAddress"], "contract" : 1, "code" : code},
                }
                # update contract table
                self.db_proxy.update(FLAGS.accounts, {"address":tx["contractAddress"]}, operation, upsert = True)

            else:
                operation = {
                    "$set" : {"address" : tx["to"]},
                }
                self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation, upsert = True)

            # always insert the tx after this tx has been process
            
            gas_cost = utils.convert_to_int(tx['gasUsed']) * utils.convert_to_int(tx["gasPrice"])
            tx['fee'] = utils.unit_convert(gas_cost)
            self.db_proxy.update(FLAGS.txs, {"hash" : tx["hash"]}, {"$set":tx}, block_height = self.blk_number, upsert = True)
            return tx['fee']

        except Exception, e:
            self.logger.error(e)
            return 0

    def revert(self, block, tx_list):
        # delete origin block
        self.db_proxy.delete(FLAGS.blocks, {"hash":block["hash"]}, block_height = self.blk_number, multi = False)
        # revert txs
        for tx in tx_list:
            self.revert_tx(tx, self.blk_number)
        
    def revert_uncle(self, block):
        miners = []
        current_height = block['number']
        for _, uncle in enumerate(block['uncles']):
            buncle = self.db_proxy.get(FLAGS.uncles, {"mainNumber":current_height, "hash": uncle}, block_height = current_height, multi = False)
            if buncle:
                miners.append(buncle["miner"])
                self.db_proxy.delete(FLAGS.uncles, {"mainNumber":current_height, "hash":uncle}, block_height = current_height)
        return miners
    
    def revert_tx(self, tx, block_height):
        try:        
            if tx['contractAddress']:
                # contract creation transaction
                self.db_proxy.delete(FLAGS.accounts, {"address": tx["contractAddress"]}, block_height = block_height)

            self.db_proxy.delete(FLAGS.txs, {"hash" : tx["hash"]}, block_height = block_height)

        except Exception, e:
            self.logger.error(e)
            return False

    def _sync_balance(self, net_last_block):
        accounts = self.db_proxy.get(FLAGS.accounts, None, multi = True, projection = {"address":1})
        accounts = [acct["address"] for acct in accounts]
        self.set_balance(accounts, net_last_block)

    def set_balance(self, accounts, block_number):
        if type(accounts) is list: accounts = list(set(accounts))
        else: accounts = list(accounts)
        
        while len(accounts) > 0:
            try:
                if accounts[0] is None: continue
                balance = self.rpc_cli.call(constant.METHOD_GET_BALANCE, accounts[0], block_number)
                if balance:    
                    operation = {
                        "$set" : {"balance" : balance, "address":accounts[0]}
                    }
                    self.db_proxy.update(FLAGS.accounts, {"address": accounts[0]}, operation, upsert = True)
                else:
                    self.logger.info("request %s balance failed!" % accounts[0])
                accounts.pop(0)
            except Exception, e:
                self.logger.info(e)
                accounts.append(accounts[0])
        
        self.db_proxy.update(FLAGS.meta, {"sync_record":"ethereum"}, {"$set": {"account_status_flag":block_number} }, multi = False, upsert = True)
           
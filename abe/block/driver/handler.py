from abe.db import base
from abe import logger
from abe import constant
from pyethapp.rpc_client import JSONRPCClient
from abe import flags
import time
from abe import utils
FLAGS = flags.FLAGS

class BlockHandler(object):
    def __init__(self, rpc_cli, logger, db_proxy):
        self.rpc_cli = rpc_cli
        self.logger = logger
        self.db_proxy = db_proxy
        super(BlockHandler, self).__init__()

    def execute(self, blockinfo, repeat_check, fork_check):
        if isinstance(blockinfo, int):
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
                        success = self.process_block(blk, repeat_check)
                        return True
                else:
                    # no necessary to check whether is a fork block
                    success = self.process_block(blk, repeat_check)
                return success
            else:
                self.logger.info("block %s not exist in blockchain, discard" % blk["number"])
                return False
        except Exception, e:
            self.logger.error(e)
            return False
    
    def process_genesis(self, block):
        ''' add genesis block to db '''
        txs = block['transactions']
        timestamp = block['timestamp']
        for tx in txs:
            tx['blockNumber'] = "0x0"
            tx['timestamp'] = timestamp
            value = utils.convert_to_int(tx['value'])
            balance = utils.unit_convert(value)
            operation = {
                    "$addToSet" : {"tx_in": tx["hash"]},
                    "$set" : {"address" : tx["to"]},
                    "$inc" : {"balance" : balance}
            }
            self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation, upsert = True, multi = False, block_height = 0)
            self.db_proxy.update(FLAGS.txs, {"hash":tx["hash"]}, {"$set":tx}, upsert = True, multi = False, block_height = 0)

        del block['transactions']
        self.db_proxy.insert(FLAGS.blocks, block, block_height = 0)
        return True

    def process_block(self, block, repeat_check):
        txs = block['transactions']
        timestamp = block['timestamp']
        
        total_fee = 0
        for tx in txs:
            res = self.process_tx(tx, timestamp, repeat_check)
            if res:
                total_fee = total_fee + res
        block['number'] = utils.convert_to_int(block['number'])

        del block['transactions']
        basic_reward = FLAGS.default_reward + len(block['uncles']) * FLAGS.default_reward * 1.0 / 32
        basic_reward = utils.unit_convert_from_ether(basic_reward)
        block['reward'] = total_fee + basic_reward

        if repeat_check and self.db_proxy.get(FLAGS.accounts, 
            {"address":block["miner"], "mine" : {"$elemMatch": {"$eq" : block["hash"] }}}, block_height = self.blk_number):
            self.logger.info("block: %s has been add to miner account %s" % (block['hash'], block['miner']))
        else:
            operation = {
                "$addToSet" : {"mine": block["hash"]},
                "$set" : {"address" : block["miner"]},
                "$inc" : {"balance" : block['reward']}
            }
            self.db_proxy.update(FLAGS.accounts, {"address":block["miner"]}, operation, block_height = self.blk_number, upsert = True)
        self.process_uncle(block, repeat_check)
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

        # apply_txs
        apply_txs = []
        for _,tx in enumerate(new_hashes):
            if tx not in old_hashes:
                apply_txs.append(new_block['transactions'][_])

        # revert
        self.revert(old_block, undo_txs)
        self.revert_uncle(old_block)

        # process new block
        apply_fee = 0
        for tx in apply_txs:
            fee = self.process_tx(tx, new_block['timestamp'], False)
            apply_fee = apply_fee + fee

        # save new block info
        new_block["number"] = utils.convert_to_int(new_block['number'])
        diff_uncle_reward = (len(new_block['uncles']) - len(old_block['uncles'])) * FLAGS.default_reward * 1.0 / 32
        diff_uncle_reward = utils.unit_convert_from_ether(diff_uncle_reward)
        new_block['reward'] = old_block['reward'] - undo_fee + apply_fee + diff_uncle_reward
        
        del new_block['transactions']

        # update miner account
        operation = {
            "$addToSet" : {"mine": new_block["hash"]},
            "$set" : {"address" : new_block["miner"]},
            "$inc" : {"balance" : new_block['reward']}
        }
        self.db_proxy.update(FLAGS.accounts, {"address":new_block["miner"]}, operation, block_height = self.blk_number, upsert = True)
        self.process_uncle(new_block, True)
        self.db_proxy.update(FLAGS.blocks, {"hash": new_block['hash']}, {"$set":new_block}, block_height = self.blk_number, upsert = True)

    def process_uncle(self, block, repeat_check):
        if isinstance(block['number'], str) or isinstance(block['number'], unicode):
            current_height = utils.convert_to_int(block['number'])
            block['number'] = current_height
        else:
            current_height = block['number']
        
        for _, uncle in enumerate(block['uncles']):
            buncle = self.rpc_cli.call(constant.METHOD_GET_UNCLE_BY_BLOCK_HASH_AND_INDEX, block["hash"], _)
            if buncle:
                uncle_reward = (utils.convert_to_int(buncle['number']) - current_height + 8) * 1.0 * FLAGS.default_reward / 8
                uncle_reward = utils.unit_convert_from_ether(uncle_reward)
                if repeat_check and self.db_proxy.get(FLAGS.accounts, 
                    {"address":buncle["miner"], "uncles" : {"$elemMatch": {"$eq" : buncle["hash"] }}}, block_height = current_height):
                    self.logger.info("uncle: %s has been process at height %s" % (buncle['hash'], buncle['number']))
                else:
                    operation = {
                        "$set" : {"address" : buncle["miner"]},
                        "$inc" : {"balance" : uncle_reward},
                        "$addToSet" : {"uncles" : uncle}
                    }
                    self.db_proxy.update(FLAGS.accounts, {"address":buncle["miner"]}, operation, block_height = current_height, upsert = True)

                buncle['mainNumber'] = current_height
                buncle['reward'] = uncle_reward
                self.db_proxy.update(FLAGS.uncles, {"hash":buncle["hash"]}, {"$set":buncle}, block_height = current_height, upsert = True)

    def process_tx(self, tx, timestamp, repeat_check):
        # merge tx receipt data
        receipt = self.rpc_cli.call(constant.METHOD_GET_TX_RECEIPT, tx['hash'])
        receipt_useful_field = ['cumulativeGasUsed', 'contractAddress', 'gasUsed', 'logs']
        for k, v in receipt.items():
            if k in receipt_useful_field:
                tx[k] = v
        tx['timestamp'] = timestamp
        repeat = False

        # apply tx
        try:
            # for the block, those tx may been applied since last execution
            # which need repeat-check
            if repeat_check and self.db_proxy.get(FLAGS.accounts, 
                {"address":tx["from"], "tx_out" : {"$elemMatch": {"$eq" : tx["hash"] }}}, block_height = self.blk_number):
                self.logger.info("tx: %s has been add to account %s" % (tx['hash'], tx['from']))
                repeat = True
            else:
                # transaction fee
                gas_cost = utils.convert_to_int(tx['gasUsed']) * utils.convert_to_int(tx["gasPrice"])
                # transfer amount
                value = utils.convert_to_int(tx['value'])
                total = utils.unit_convert(gas_cost + value)
                operation1 = {
                    "$addToSet" : {"tx_out": tx["hash"]},
                    "$set" : {"address" : tx["from"]},
                    "$inc" : {"balance" : -1 * total}
                }
                self.db_proxy.update(FLAGS.accounts, {"address":tx["from"]}, operation1, block_height = self.blk_number, upsert = True)

            if repeat_check and self.db_proxy.get(FLAGS.accounts, 
                {"address":tx["to"], "tx_in" : {"$elemMatch": { "$eq": tx["hash"] }}}, block_height = self.blk_number):
                self.logger.info("tx: %s has been add to account %s" % (tx['hash'], tx['to']))
                repeat = True
            else:
                if tx['contractAddress']:
                    # contract creation transaction
                    code = self.rpc_cli.call(constant.METHOD_GET_CODE, tx['contractAddress'], "latest")
                    tx["contractAddress"] = tx["contractAddress"]
                    value = utils.convert_to_int(tx['value'])
                    balance = utils.unit_convert(value)
                    operation2 = {
                        "$set" : {"address" : tx["contractAddress"], "is_contract" : 1, "code" : code},
                        "$addToSet" : {"tx_in": tx["hash"]},
                        "$inc" : {"balance" : balance}
                    }
                    self.db_proxy.update(FLAGS.accounts, {"address":tx["contractAddress"]}, operation2, block_height = self.blk_number, upsert = True)

                else:
                    value = utils.convert_to_int(tx['value'])
                    balance = utils.unit_convert(value)
                    operation2 = {
                        "$addToSet" : {"tx_in": tx["hash"]},
                        "$set" : {"address" : tx["to"]},
                        "$inc" : {"balance" : balance}
                    }
                    self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation2, block_height = self.blk_number, upsert = True)

            # always insert the tx after this tx has been process
            if repeat:
                gas_cost = utils.convert_to_int(tx['gasUsed']) * utils.convert_to_int(tx["gasPrice"])
            tx['fee'] = utils.unit_convert(gas_cost)
            self.db_proxy.update(FLAGS.txs, {"hash" : tx["hash"]}, {"$set":tx}, block_height = self.blk_number, upsert = True)
            return tx['fee']

        except Exception, e:
            self.logger.error(e)
            return 0

    def revert(self, block, tx_list):
        operation = {
            "$inc" : {"balance" : block['reward'] * -1},
            "$pull" : {"mine" : block['hash']},
        }
        self.db_proxy.update(FLAGS.accounts, {"address":block["miner"]}, operation, block_height = self.blk_number)
        # delete origin block
        self.db_proxy.delete(FLAGS.blocks, {"hash":block["hash"]}, block_height = self.blk_number, multi = False)
        # revert txs
        for tx in tx_list:
            self.revert_tx(tx, self.blk_number)
        
    def revert_uncle(self, block):
        current_height = block['number']
        for _, uncle in enumerate(block['uncles']):
            buncle = self.db_proxy.get(FLAGS.uncles, {"mainNumber":current_height, "hash": uncle}, block_height = current_height, multi = False)
            if buncle:
                operation = {
                    "$set" : {"address" : buncle["miner"]},
                    "$inc" : {"balance" : -1 * buncle["reward"]},
                    "$pull" : {"uncles" : buncle['hash']}
                }
                self.db_proxy.update(FLAGS.accounts, {"address":buncle["miner"]}, operation, block_height = current_height, upsert = True)
                self.db_proxy.delete(FLAGS.uncles, {"mainNumber":current_height, "hash":uncle}, block_height = current_height)

    
    def revert_tx(self, tx, block_height):
        try:        
            fee = tx['fee']
            value = utils.convert_to_int(tx['value'])
            total = utils.unit_convert(value) + fee

            operation1 = {
                "$pull" : {"tx_out": tx["hash"]},
                "$inc" : {"balance" : total}
            }
            self.db_proxy.update(FLAGS.accounts, {"address":tx["from"]}, operation1, block_height = block_height)

            if tx['contractAddress']:
                # contract creation transaction
                self.db_proxy.delete(FLAGS.accounts, {"address": tx["contractAddress"]}, block_height = block_height)

            else: 
                balance = utils.unit_convert(value)
                operation2 = {
                    "$pull" : {"tx_in": tx["hash"]},
                    "$inc" : {"balance" : -1 * balance}
                }
                self.db_proxy.update(FLAGS.accounts, {"address":tx["to"]}, operation2, block_height = block_height)

            self.db_proxy.delete(FLAGS.txs, {"hash" : tx["hash"]}, block_height = block_height)

        except Exception, e:
            self.logger.error(e)
            return False



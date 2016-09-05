from ethereum._solidity import get_solidity, compile_file
from ethereum import abi
import hashlib

def compile_contract(contract_path):
    contract_compiled = compile_file(contract_path)
    return contract_compiled

def remove_binary_data(compile_contracts):
    for contract, value in compile_contracts.items():
        if value.has_key('bin'):
            value.pop('bin')
    return compile_contracts

def method_id(method_name, method_params):
    return abi.method_id(method_name, method_params)

def event_id(event_name, event_params):
    return abi.event_id(event_name, event_params)

def decode_abi(types, data):
    return abi.decode_abi(types, data)
    
def hash_log(log):
    hash = '0x' + hashlib.new("sha224", str(log).encode("hex")).hexdigest()
    return hash
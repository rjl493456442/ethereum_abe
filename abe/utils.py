import os
import sys
from abe import flags
import re
import hashlib
FLAGS = flags.FLAGS

def import_class(import_str):
    """Returns a class from a string including module and class"""
    mod_str, _sep, class_str = import_str.rpartition('.')
    try:
        __import__(mod_str)
        return getattr(sys.modules[mod_str], class_str)
    except Exception, e:
        print e

def import_object(import_str, *args, **kwargs):
    """Returns an object including a module or module and class"""
    if isinstance(import_str, str):
        try:
            __import__(import_str)
            return sys.modules[import_str]
        except ImportError:
            cls = import_class(import_str)
            return cls()
    elif isinstance(import_str, list):
        modules = []
        for s in import_str:
            try:
                __import__(s)
                modules.append(sys.modules[s])
            except ImportError:
                cls = import_class(s)
                modules.append(cls())
        return modules

def signal_handler(signum, frame):
    print "Got ctrl + c"
    sys.exit(1)

def convert_to_int(hex_str):
    ''' convert hex str to int'''
    return int(hex_str, 16) 

def unit_convert_from_ether(num):
    ''' convert value from ether unit to specific unit''' 
    precision = FLAGS.precision
    if precision == 'wei':
        radix = 1 * (10 ** 18)
        radix = 1 * (10 ** 18)
    elif precision == 'Kwei':
        radix = 1 * (10 ** 15)
    elif precision == 'Mwei':
        radix = 1 * (10 ** 12)
    elif precision == 'Gwei':
        radix = 1 * (10 ** 9)
    elif precision == 'szabo':
        radix = 1 * (10 ** 6)
    elif precision == 'finney':
        radix = 1 * (10 ** 3)
    else:
        # save in ether 
        radix = 1
    return num * radix

def unit_convert(num):
    ''' convert value from wei unit to specific unit'''
    
    precision = FLAGS.precision
    if precision == 'wei':
        radix = 1
    elif precision == 'Kwei':
        radix = 1 * (10 ** 3)
    elif precision == 'Mwei':
        radix = 1 * (10 ** 6)
    elif precision == 'Gwei':
        radix = 1 * (10 ** 9)
    elif precision == 'szabo':
        radix = 1 * (10 ** 12)
    elif precision == 'finney':
        radix = 1 * (10 ** 15)
    else:
        # save in ether 
        radix = 1 * (10 ** 18)
    return num * 1.0 / radix

def regular_extract(line):
    '''
        ## internal tx
        <field name> - <offset> - <description>
        log related     | 0 |       date time code line and etc
        type            | 1 |       log entry type: (1)internaltx (2) error status
        blocknumber     | 2 |       blocknumber
        blockhash       | 3 |       blockhash
        txhash          | 4 |       txhash
        from            | 5 |       sender address
        to              | 6 |       receive address
        value           | 7 |       transfer value
        timestamp       | 8 |       timestamp
        depth           | 9 |       call depth
        pc              | 10|       pc
        callType        | 11|       invocation type: (1) call (2) callcode (3) create (4) suicide
        error           | 12|       error msg while execution failed

        ## error status
        <field name> - <offset> - <description>
        log related     | 0 |       date time code line and etc
        type            | 1 |       log entry type: (1)internaltx (2) error status
        blocknumber     | 2 |       blocknumber
        blockHash       | 3 |       blockHash
        txhash          | 4 |       txhash
        depth           | 5 |       call depth
        status          | 6 |       error msg while execution failed

        ## block
        <field name> - <offset> - <description>
        log related     | 0 |       date time code line and etc
        type            | 1 |       block type: (1)reorg block (2) side chain block
        blocknumber     | 2 |       blocknumber
        blockHash       | 3 |       blockHash

    '''
    hash = '0x' + hashlib.new("sha224", line.encode("hex")).hexdigest()
    fields = line.split(' ')
    
    if fields[3] == "INTERNALTX":
        info = {
            "hash" : hash,
            "type" : 0,
            "blocknumber" : int(fields[4][8:-2]),
            "blockhash" : '0x' + fields[5][11:-2],
            "txhash" : '0x' + fields[6][8:-2],
            "from" : '0x' + fields[7][6:-2],
            "to" : '0x' + fields[8][4:-2],
            "value" : fields[9][7:-2],
            "timestamp" : int(fields[10][6:-2]),
            "depth" : int(fields[11][7:-2]),
            "pc": int(fields[12][4:-2]),
            "calltype": fields[13][6:-2],
            "error" : line[line.find("ERROR")+7:-2],
        }
    elif fields[3] == "EXERESULT":
        info = {
            "hash" : hash,
            "type" : 1,
            "blocknumber" : int(fields[4][8:-2]),
            "blockhash" : '0x' + fields[5][11:-2],
            "txhash" : '0x' + fields[6][8:-2],
            "depth" : int(fields[7][7:-2]),
            "status" : line[line.find("STATUS")+8:-2],
        }
    elif fields[3] == "BLOCK":
        info = {
            "hash" : hash,
            "type" : 2,
            "blocktype" : fields[4][6:-2],
            "blockhash" : fields[5][6:-2],
            "blocknumber" : fields[6][8:-2],
        }
    else:
        info = None

    return info



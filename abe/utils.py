import os
import sys
from abe import flags
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

    


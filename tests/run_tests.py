import unittest
import os
from os import  sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

def get_test_modules():
    modules = []
    discovery_paths = [
        (None, '.'),
    ]
    
    for modpath, dirpath in discovery_paths:
        for f in os.listdir(dirpath):
            if 'tests_' in f and '.pyc' not in f:
                name, pfix = f.split('.')
                tests, obj = name.split('_')
                mod = __import__(name)
                
                cls = getattr(mod, obj[0].upper()+obj[1:]+'Test') 
                modules.append(cls)
    return set(modules)


if __name__ == "__main__":
    suites = []
    test_mods = get_test_modules()
    
    for  mod in test_mods:
        suites.append(unittest.TestLoader().loadTestsFromTestCase(mod))  
    suite = unittest.TestSuite(suites)  
    unittest.TextTestRunner(verbosity=2).run(suite)  

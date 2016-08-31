import unittest
import os
from os import  sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from abe import flags
FLAGS = flags.FLAGS

def get_test_modules():
    argv = FLAGS(sys.argv)
    if FLAGS.test_all is False and len(FLAGS.test_case) == 0:
        print 'Usage: %s  command [option]\n%s' % (sys.argv[0], FLAGS)

    modules = []
    discovery_paths = [
        (None, '.'),
    ]
    
    for modpath, dirpath in discovery_paths:
        for f in os.listdir(dirpath):
            if 'tests_' in f and '.pyc' not in f:
                name, pfix = f.split('.')
                tests, obj = name.split('_')
                
                if FLAGS.test_all is False and obj not in FLAGS.test_case: continue

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

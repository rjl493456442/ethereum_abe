from abe import flags, utils
import utils as _utils
FLAGS = flags.FLAGS


class TokenAPI(object):
    def __init__(self, token_driver = None, *args, **kwargs):
        if not token_driver:
            token_driver = FLAGS.token_driver
        driver_list = utils.import_object(token_driver)
        self.driver = {}
        for index, name in enumerate(token_driver):
            self.driver[name.split('.')[-3]] = driver_list[index]

    def synchronize(self, token_name):
        self.driver[token_name].synchronize()


    
from abe.utils import signal_handler
import signal

def signal_watcher(fn):
    def wrapper(*args, **kwargs):
        signal.signal(signal.SIGINT, signal_handler)
        fn(*args, **kwargs)
    return wrapper

def mongo_res_handler(f):
    """ remove field can not unmarshal """
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        if isinstance(res, list):
            # list of query result
            for item in res:
                if item and item.has_key("_id"):
                    del item["_id"]
        else:
            # single query result
            if res and res.has_key("_id"):
                del res["_id"]
        return res
    return wrapper
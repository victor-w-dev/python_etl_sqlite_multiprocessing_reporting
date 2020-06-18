import time
import inspect

def time_decorator(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        if (te - ts)>=1: print(f'{inspect.getmodule(method)} {method.__name__}\n{(te - ts):2.4f} seconds')
        elif (te-ts)>=0.001: print(f'{inspect.getmodule(method)} {method.__name__}\n{(te - ts)*10**3 :2.4f} ms')
        else: print(f'{inspect.getmodule(method)} {method.__name__}\n{(te - ts)/10**6 :2.4f} µs')
        print("-"*100)
        return result
    return timed

from functools import wraps
from time import time
import numpy as np


def timing(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time()
        result = f(*args, **kwargs)
        end = time()
        timer = np.round(end - start, 2)
        print(f'Elapsed time: {timer} seconds')
        return result

    return wrapper

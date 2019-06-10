"""
I encountered these problems using joblib:
	1. Possible bug that I reported: https://github.com/joblib/joblib/issues/517
	2. Interface not natural for my application, have to write awkward workarounds
	3. Checking function code feature which I don't want and which causes bugs with multiprocessing
	4. As input to hash function, it uses pickle.dump, which can give different results for identical dictionaries.
	Presumably this is due tox them having different internal state.
Decided to write own.

Took a while to figure out right way to make hash.
Attempt n: hash of custom dill Pickler dumps which converts dicts to sorted list. Sorting worked, but still got weird
issues with identical simple objects like small lists giving nonidentical hash digests. Probably due to memoization.

Log levels:
    2. Several messages per eval_with_cache.
"""
import dill
import hashlib
import logging
import os
import types
from collections import OrderedDict
from collections.abc import Sequence, Mapping

logger = logging.getLogger(__name__)

CACHE_MODES = 'normal', 'read', 'overwrite', 'bypass'


class RecursiveMapError(Exception):
    pass


def sort_dict(d):
    """Sort dictionary by key producing ordered dictionary."""
    return OrderedDict([(k, d[k]) for k in sorted(d.keys())])


# Works but not used
def recursive_map(obj, func, top_level=True):
    if isinstance(obj, Sequence) and not isinstance(obj, str):
        return type(obj)([recursive_map(item, func, False) for item in obj])
    elif isinstance(obj, Mapping):
        return type(obj)([(key, recursive_map(value, func, False)) for key, value in obj.items()])
    elif not top_level:
        return func(obj)
    else:
        raise RecursiveMapError('Don''t know how to map type %s.', type(obj))


# # Works but not used.
# def strip(obj):
#     try:
#         method=obj.strip
#     except AttributeError:
#         pass
#     else:
#         return method()
#     if isinstance(obj,np.ndarray):
#         return obj
#     try:
#         return recursive_map(obj,strip)
#     except RecursiveMapError:
#         pass
#     return obj

# def get_cache_repr(obj):
#     try:
#         method=obj.get_cache_repr
#     except AttributeError:
#         pass
#     else:
#         return method()
#     # For numpy arrays use repr
#     if isinstance(obj,np.ndarray):
#         return repr(obj)
#     # Try treating obj as mapping
#     try:
#         items=obj.items()
#     except AttributeError:
#         pass
#     else:
#         obj=copy.copy(obj)
#         for key,value in items:
#             obj[key]=get_cache_repr(value)
#         return repr(obj)
#     # Try treating as iterable
#     try:
#         iter_=iter(obj)
#     except TypeError:
#         pass
#     else:
#         obj=type(obj)(get_cache_repr(item) for item in iter_)
#         return repr(obj)
#     return repr(obj)
#     #raise ValueError('Don''t know how to generate cache representation for %s.',obj)

def calc_hash_digest(obj):
    """Calculate hash digest of an object.

    The object is converted to string representation and then fed to the MD5 hashing algorithm."""
    # Get string representation
    string = repr(obj).encode()
    # Create hash digest
    hash = hashlib.md5()
    hash.update(string)
    digest = hash.hexdigest()
    return digest


def compare_hash_digests(a, b):
    if calc_hash_digest(a) == calc_hash_digest(b):
        return True, None, None
    try:
        gsa = a.__getstate__
        gsb = b.__getstate__
    except AttributeError:
        pass
    else:
        a = gsa()
        b = gsb()
    assert calc_hash_digest(a) != calc_hash_digest(
        b), "IDs of objects weren't identical but those of their states were: %s and %s."%(a, b)
    try:
        # Assume it's a dict
        # TODO use isinstance collections.Mapping etc
        if a.keys() != b.keys():
            return False, (), (a.keys(), b.keys())
    except AttributeError:
        pass
    else:
        for key, value in a.items():
            equal, indices, values = compare_hash_digests(value, b[key])
            if not equal:
                return equal, (key,) + indices, values
        # Odd - hashes of a & b were unequal, those of dict contents were not
        return False, ('dict?',), (a, b)
    if not isinstance(a, str):
        # Assume its a sequence
        try:
            if len(a) != len(b):
                return False, (), (len(a), len(b))
        except TypeError:
            pass
        else:
            for num, (ea, eb) in enumerate(zip(a, b)):
                equal, indices, values = compare_hash_digests(ea, eb)
                if not equal:
                    return equal, (num,) + indices, values
            # Odd - hashes of a & b were unequal, but those of sequence contents were not
            return False, ('sequence?',), (a, b)
    return False, (), (a, b)


class CacheNotFoundError(Exception):
    pass


def eval_with_cache(path, function, args=(), kwargs={}, mode=None):
    if mode is None:
        if path is None:
            mode = 'bypass'
        else:
            mode = 'normal'
    assert mode in CACHE_MODES

    def eval_and_dump():
        result = function(*args, **kwargs)
        logger.log(2, '%s evaluated successfully.', function.__name__)
        dir = os.path.dirname(path)
        if len(dir) > 0:
            os.makedirs(dir, exist_ok=True)
        with open(path, 'wb') as file:
            dill.dump(result, file)
        logger.log(2, '%s result dumped at %s.', function.__name__, path)
        return result

    logger.log(2, 'Evaluating %s with cache mode %s.', function.__name__, mode)
    if mode == 'bypass':
        result = function(*args, **kwargs)
        logger.log(2, '%s evaluated successfully.', function.__name__)
        return result
    elif mode == 'read':
        try:
            with open(path, 'rb') as file:
                result = dill.load(file)
            logger.log(2, 'Read result of %s from %s.', function.__name__, path)
            return result
        except FileNotFoundError as e:
            raise CacheNotFoundError('Cached result of %s not found: %s'%(function.__name__, path)) from e
    elif mode == 'normal':
        try:
            with open(path, 'rb') as file:
                logger.log(2, 'Reading result of %s from %s.', function.__name__, path)
                result = dill.load(file)
                logger.log(1, '%d bytes read.', file.tell())
            return result
        except FileNotFoundError:
            pass
        logger.log(2, '%s not found. Evaluating %s ...', path, function.__name__)
        return eval_and_dump()
    else:
        return eval_and_dump()


def eval_inplace_with_cache(path, bound_method, args=(), kwargs={}, mode='normal', kwargs_changed_only=False):
    self = bound_method.__self__

    def function(*args, **kwargs):
        if kwargs_changed_only:
            kwargs_before = self.get_kwargs()
            bound_method(*args, **kwargs)
            kwargs_after = self.get_kwargs()
            kwargs_changed = {key: value for key, value in kwargs_after.items() if
                              key not in kwargs_before or value is not kwargs_before[key]}
            return kwargs_changed
        else:
            bound_method(*args, **kwargs)
            return self

    result = eval_with_cache(path, function, args, kwargs, mode)
    if kwargs_changed_only:
        # kwargs=self.get_kwargs()
        # kwargs.update(result)
        # #self_after=type(self)(**kwargs)
        # self.__init__(**kwargs)
        # print(result.keys())
        # del result['source_dipole']
        for key, value in result.items():
            setattr(self, key, value)  # pass#HACK
    else:
        self_after = result
        self.__dict__ = self_after.__dict__.copy()


def eval_with_cache_id(dir, id, function, args=(), kwargs={}, mode='normal'):
    # Generate path from dir and id converted into hash digest
    digest = calc_hash_digest(id)
    path = os.path.join(dir, digest + '.pkl')
    eval_with_cache(path, function, args, kwargs, mode)


def cached_method(inplace):
    def decorator(method):
        def decorated(self, *args, cache_path=None, cache_mode='bypass', kwargs_changed_only=False, **kwargs):
            bound_method = types.MethodType(method, self)
            if inplace:
                if cache_mode == 'bypass':
                    bound_method(*args, **kwargs)
                else:
                    eval_inplace_with_cache(cache_path, bound_method, args, kwargs, cache_mode, kwargs_changed_only)
            else:
                assert kwargs_changed_only == False
                if cache_mode == 'bypass':
                    return bound_method(*args, **kwargs)
                else:
                    return eval_with_cache(cache_path, bound_method, args, kwargs, cache_mode)

        return decorated

    return decorator

class Cache:
    def lookup(self, key, function):
        raise NotImplementedError()
    
class NullCache(Cache):
    def lookup(self, key, function):
        return function()
    
class HashFileCache(Cache):
    def __init__(self, folder, mode='normal'):
        self.folder = folder
        self.mode = mode

    def lookup(self, key, function):
        digest = calc_hash_digest(key)
        path = os.path.join(self.folder, digest + '.pkl')
        return eval_with_cache(path, function, mode=self.mode)


class HashDictCache(Cache):
    def __init__(self):
        self.cache = {}
        self.hits = 0
        self.misses = 0

    def lookup(self, key, function):
        digest = calc_hash_digest(key)
        try:
            result = self.cache[digest]
            self.hits += 1
        except KeyError:
            # Evaluating the function in the except clause leads to confusing error message when the function
            # throws an exception.
            result = None
            self.misses += 1

        if result is None:
            result = self.cache[digest] = function()

        return result

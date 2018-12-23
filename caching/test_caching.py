import pytest,tempfile,logging,os
from collections import OrderedDict
import caching
logging.basicConfig()
logging.getLogger('cache').setLevel(1)

def test_recursive_map():
    func=lambda x:2*x
    assert caching.recursive_map((2,3),func)==(4,6)
    assert caching.recursive_map([2,3],func)==[4,6]
    assert caching.recursive_map([1,(2,3)],func)==[2,(4,6)]
    assert caching.recursive_map([1,(2,3,[4,5])],func)==[2,(4,6,[8,10])]
    assert caching.recursive_map({'a':1,'b':2},func)=={'a':2,'b':4}
    assert caching.recursive_map({'a':(1,2)},func)=={'a':(2,4)}
    assert caching.recursive_map({'a':({'b':2},3)},func)=={'a':({'b':4},6)}
    assert caching.recursive_map(OrderedDict((('a',1),('b',2))),func)==OrderedDict((('a',2),('b',4)))
    assert caching.recursive_map((1,OrderedDict((('a',1),('b',2)))),func)==(2,OrderedDict((('a',2),('b',4))))

# def test_strip():
#     assert caching.strip(1)==1
#     assert caching.strip((1,1))==(1,1)
#     assert caching.strip((1,{'a':2}))==(1,{'a':2})
#
#     class Strippable:
#         def __init__(self,redundant=1):
#             self.redundant=1
#
#         def strip(self):
#             return Strippable(None)
#
#         def __eq__(self,other):
#             return self.redundant==other.redundant
#     assert caching.strip(Strippable())==Strippable(None)
#     assert caching.strip((1,Strippable()))==(1,Strippable(None))
#     assert caching.strip((1,{'a':Strippable()}))==(1,{'a':Strippable(None)})

# def test_get_cache_repr():
#     for o in (5,(5,3),[5,3],{'a':4,'b':(3,2)}):
#         assert cache.get_cache_repr(o)==repr(o)

# def test_dumps():
#     assert cache.dumps({'x':5})!=cache.dumps({'x':6})
#     assert cache.dumps({'x':5,'y':{'y':1}})!=cache.dumps({'x':5,'y':{'y':2}})
#     assert cache.dumps((0,{'x':5}))!=cache.dumps((0,{'x':6}))
#     assert cache.dumps((0,{'x':[[[5]]]}))!=cache.dumps((0,{'x':[[[6]]]}))

def test_compare_hash_ids():
    a=(0,'a',[1,'b',{2:'c'}])
    b=(0,'a',[1,'b',{2:'d'}])
    assert caching.compare_hash_digests(a,b)==(False,(2,2,2),('c','d'))

# obselete, but may resurrect
# def test_cache():
#     num_calls=0
#     def function(x):
#         nonlocal num_calls
#         num_calls+=1
#         return x
#     with tempfile.TemporaryDirectory() as dir:
#         # Test bypass
#         assert cache.eval_with_cache_id(dir,0,function,[0],'bypass')==0 # 1
#         # Test read fail
#         with pytest.raises(cache.CacheNotFoundError) as e:
#             cache.eval_with_cache_id(dir,0,function,'read')
#         assert all(s in str(e.value) for s in (dir,'0','function'))
#         # Test normal without cached result
#         assert cache.eval_with_cache_id(dir,0,function,[0],'normal')==0 #2
#         # Test read with cache result and different input, which should be ignored
#         assert cache.eval_with_cache_id(dir,0,function,[1],'read')==0
#         # Test normal with cached result and different input, which should be ignored
#         assert cache.eval_with_cache_id(dir,0,function,[1],'normal')==0
#         # Test overwrite, specifying different input
#         assert cache.eval_with_cache_id(dir,0,function,[1],'overwrite')==1 #3
#         # Test different id
#         assert cache.eval_with_cache_id(dir,2,function,[2],'normal')==2 # 4
#         assert num_calls==4

# def test_dict_id():
#     num_calls=0
#
#     def function(x):
#         nonlocal num_calls
#         num_calls+=1
#         return x
#
#     with tempfile.TemporaryDirectory() as dir:
#         assert cache.eval_with_cache_id(dir,{'x':5},function,[5],'normal')==5
#         assert cache.eval_with_cache_id(dir,{'x':6},function,[6],'normal')==6

num_calls_do=0
class Doer:
    def __init__(self,x=None):
        self.x=x

    def get_kwargs(self):
        return dict(x=self.x)

    def do(self):
        self.x=1
        global num_calls_do
        num_calls_do+=1

def test_eval_inplace_with_cache():
    global num_calls_do
    num_calls_do=0
    with tempfile.TemporaryDirectory() as dir:
        path=os.path.join(dir,'doer.pkl')
        with pytest.raises(caching.CacheNotFoundError):
            doer=Doer()
            caching.eval_inplace_with_cache(path,doer.do,mode='read')
        assert doer.x is None
        doer=Doer()
        caching.eval_inplace_with_cache(path,doer.do)
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer()
        caching.eval_inplace_with_cache(path,doer.do,mode='read')
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer()
        caching.eval_inplace_with_cache(path,doer.do,mode='overwrite')
        assert doer.x==1
        assert num_calls_do==2

        caching.eval_inplace_with_cache(path,doer.do,mode='normal')
        assert doer.x==1
        assert num_calls_do==2

def test_eval_inplace_with_cache_changed_only():
    global num_calls_do
    num_calls_do=0
    with tempfile.TemporaryDirectory() as dir:
        path=os.path.join(dir,'doer.pkl')
        with pytest.raises(caching.CacheNotFoundError):
            doer=Doer()
            caching.eval_inplace_with_cache(path,doer.do,mode='read',kwargs_changed_only=True)
        assert doer.x is None
        doer=Doer()
        caching.eval_inplace_with_cache(path,doer.do,kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer()
        caching.eval_inplace_with_cache(path,doer.do,mode='read',kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer()
        caching.eval_inplace_with_cache(path,doer.do,mode='overwrite',kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==2

        caching.eval_inplace_with_cache(path,doer.do,mode='normal',kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==2

class Doer2:
    def __init__(self,x=None):
        self.x=x

    def get_kwargs(self):
        return dict(x=self.x)

    @caching.cached_method(inplace=True)
    def do(self):
        self.x=1
        global num_calls_do
        num_calls_do+=1

def test_cached_method_inplace():
    global num_calls_do
    num_calls_do=0
    with tempfile.TemporaryDirectory() as dir:
        path=os.path.join(dir,'doer.pkl')
        with pytest.raises(caching.CacheNotFoundError):
            doer=Doer2()
            doer.do(cache_path=path,cache_mode='read')
        assert doer.x is None
        doer=Doer2()
        doer.do(cache_path=path,cache_mode='normal')
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer2()
        doer.do(cache_path=path,cache_mode='read')
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer2()
        doer.do(cache_path=path,cache_mode='overwrite')
        assert doer.x==1
        assert num_calls_do==2
        doer=Doer2()
        doer.do(cache_mode='bypass')
        assert doer.x==1
        assert num_calls_do==3

def test_cached_method_inplace_changed_only():
    global num_calls_do
    num_calls_do=0
    with tempfile.TemporaryDirectory() as dir:
        path=os.path.join(dir,'doer.pkl')
        with pytest.raises(caching.CacheNotFoundError):
            doer=Doer2()
            doer.do(cache_path=path,cache_mode='read',kwargs_changed_only=True)
        assert doer.x is None
        doer=Doer2()
        doer.do(cache_path=path,cache_mode='normal',kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer2()
        doer.do(cache_path=path,cache_mode='read',kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==1
        doer=Doer2()
        doer.do(cache_path=path,cache_mode='overwrite',kwargs_changed_only=True)
        assert doer.x==1
        assert num_calls_do==2
        doer=Doer2()
        doer.do(cache_mode='bypass')
        assert doer.x==1
        assert num_calls_do==3

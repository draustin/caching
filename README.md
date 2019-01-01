# caching
Flexible caching/memoization of Python function and method results

# Background
I first tried [joblib](https://joblib.readthedocs.io/en/latest/) but encountered these problems in my application, which involved large scale parallel calculations using multiprocessing.
1. Possible bug that I reported: https://github.com/joblib/joblib/issues/517
2. Interface not natural for my application, have to write awkward workarounds
3. Checking function code feature which I don't want and which causes bugs with multiprocessing
4. As input to hash function, it uses pickle.dump, which can give different results for identical dictionaries.

So I decided to write my own caching module.


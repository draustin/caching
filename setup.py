from setuptools import setup, find_packages

# I attempted to separate test and install dependencies but couldn't figure it out (in 10 minutes). Keeping them
# toegether for now - pytest is lightweight.
setup(name="caching", version=0.1, description="Flexible caching of Python function and method results", author='Dane Austin',
      author_email='dane_austin@fastmail.com.au', url='https://github.com/draustin/caching', license='BSD',
      packages=find_packages(), install_requires=['pytest', 'dill'], python_requires='>=3.4')

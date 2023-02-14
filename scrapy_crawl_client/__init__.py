import os

crawlprocpy_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'crawlproc.py')
assert os.path.exists(crawlprocpy_path)
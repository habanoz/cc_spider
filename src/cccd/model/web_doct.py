from collections import namedtuple

WebDocT = namedtuple("WebDocT", ['id', 'url', 'filename','offset','length'])
WebDocResultT = namedtuple('WebDocResultT',['id', 'url','raw'])
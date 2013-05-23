#!/usr/bin/env python2

from inspect import stack as inspect_stack
from logging import getLogger
from rawes import Elastic
from subprocess import check_output


__author__ = 'ahammond'

# This is where the .deb places elasticsearch stuff by default.
ELASTICSEARCH_INDEX_DIR = '/var/lib/elasticsearch'


class ElasticSearch():
    def __init__(self, host='localhost', port=9200):
        self.host = host
        self.port = port
        self._es = None
        self._stats = None
        self._indices = None

    @property
    def es(self):
        l = getLogger("{}.{}".format(self.__class__.__name__, inspect_stack()[1][3]))
        if self._es is None:
            uri = '{0}:{1}'.format(self.host, self.port)
            l.debug('connecting to: %r', uri)
            self._es = Elastic(uri)
        return self._es

    @property
    def stats(self):
        l = getLogger("{}.{}".format(self.__class__.__name__, inspect_stack()[1][3]))
        if self._stats is None:
            self._stats = self.es.get('_stats')
            l.debug('stats: %r', self._stats)
        return self._stats

    @property
    def indices(self):
        l = getLogger("{}.{}".format(self.__class__.__name__, inspect_stack()[1][3]))
        if self._indices is None:
            self._indices = [x for x in self.stats.get(u'indices', {}).keys() if x.startswith(u'logstash')]
            self._indices.sort()
            l.debug('indices: %r', self._indices)
        return self._indices

    def delete_oldest_index(self):
        l = getLogger("{}.{}".format(self.__class__.__name__, inspect_stack()[1][3]))
        oldest_index = self.indices.pop(0)
        l.debug('deleting %r', oldest_index)
        return self.es.delete(oldest_index)


def current_usage(path):
    l = getLogger('{0}'.format(inspect_stack()[1][3]))
    filesystem, blocks, blocks_used, blocks_avail, percentage, mountpoint = \
        check_output(['/bin/df', path]).split('\n')[1].split()
    usage = int(percentage[:-1])
    l.debug("usage: %r", usage)
    return usage


def drop_below_threshold(port, path, threshold):
    es = ElasticSearch(port=port)
    while current_usage(path) > threshold:
        es.delete_oldest_index()


if '__main__' == __name__:
    from argparse import ArgumentParser
    from logging import basicConfig

    parser = ArgumentParser(description='''Retention handler for logstash elasticsearch data.


    ''')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('-q', '--quiet', action='count')

    parser.add_argument('-p', '--port', default=9200)
    parser.add_argument('-d', '--dir_name', default=ELASTICSEARCH_INDEX_DIR)
    parser.add_argument('-t', '--threshold', default=70)

    arguments = parser.parse_args()

    # The magic numbers come from the logging code.
    log_level = 30 + 10 * ((arguments.quiet or 0) - (arguments.verbose or 0))
    basicConfig(level=log_level)

    drop_below_threshold(arguments.port, arguments.dir_name, arguments.threshold)

#coding=utf-8

import re, os, urllib


class Downloader(object):

    base_url = r'http://www.17500.cn/getData/'

    def __init__(self):
        pass

    def _url(self, kind):
        all_kinds = ['3d', 'ssq', '7lc', 'p3', 'p5', 'dlt', '7xc']
        if kind not in all_kinds:
            raise AssertionError('kind must in [{0}], but {1} is given.'.format(','.join(all_kinds), kind))
        return self.base_url+kind+'.TXT'

    def download(self, kind):
        content = urllib.urlopen(self._url(kind)).read()
        print content



if __name__ == '__main__':
    dl = Downloader()
    dl.download('dlt')
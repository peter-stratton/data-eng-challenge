from collections import namedtuple

from nhldata.nhl.v1.api import NHLApi as NHLApiV1
from nhldata.nhl.v1.crawler import Crawler as CrawlerV1

NHLAdapter = namedtuple('NHLAdapter', ['api', 'crawler'])


class AdapterFactory:
    def __init__(self):
        self.api_versions = {
            'v1': NHLAdapter(NHLApiV1, CrawlerV1)
        }

    def all_versions(self):
        return self.api_versions.keys()

    def adapter_for_version(self, version: str):
        if version not in self.all_versions():
            raise ValueError('Version %s is unsupported, please choose from [%s]' % (version, self.all_versions()))
        return self.api_versions[version]

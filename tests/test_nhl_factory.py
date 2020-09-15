import pytest

from nhldata.nhl import AdapterFactory
from nhldata.nhl.v1.api import NHLApi as NHLApiV1
from nhldata.nhl.v1.crawler import Crawler as CrawlerV1


def test_adapter_factory_returns_available_versions():
    factory = AdapterFactory()

    result = factory.all_versions()

    assert len(result) == 1
    assert 'v1' in result


def test_adapter_for_version_supported_version():
    factory = AdapterFactory()

    result = factory.adapter_for_version('v1')
    api = result.api()
    crawler = result.crawler(None, None)

    assert isinstance(api, NHLApiV1)
    assert isinstance(crawler, CrawlerV1)


def test_adapter_for_version_unsupported_version():
    factory = AdapterFactory()
    with pytest.raises(ValueError):
        _ = factory.adapter_for_version('v2')

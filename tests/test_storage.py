from unittest.mock import Mock

from nhldata.storage import Storage, StorageKey


def test_storage_key_returns_key():
    key = StorageKey(
        game_year='2020',
        game_month='01',
        game_day='01',
        game_id='foo'
    )
    result = key.key()
    assert result == '2020/01/01/foo.csv'


def test_storage_store_game():
    s3_mock = Mock()
    storage = Storage('testbucket', 'jobbucket', s3_mock)
    key = StorageKey('a', 'b', 'c', 'd')

    result = storage.store_game(key, 'foo bar baz')

    assert result is True
    s3_mock.put_object.assert_called_with(Bucket='testbucket', Key='a/b/c/d.csv', Body='foo bar baz')


def test_storage_store_job():
    s3_mock = Mock()
    storage = Storage('testbucket', 'jobbucket', s3_mock)

    result = storage.store_job('1/2/3/4.csv', 'foo bar baz')

    assert result is True
    s3_mock.put_object.assert_called_with(Bucket='jobbucket', Key='1/2/3/4.csv', Body='foo bar baz')

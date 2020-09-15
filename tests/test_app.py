from unittest.mock import patch

from click.testing import CliRunner

from nhldata import __version__
from nhldata.app import main, splash


def test_splash_with_debug_on(capsys):
    splash(True)
    captured = capsys.readouterr()

    assert __version__ in captured.out
    assert 'log level is DEBUG and higher' in captured.out


def test_splash_with_debug_off(capsys):
    splash(False)
    captured = capsys.readouterr()

    assert __version__ in captured.out
    assert 'log level is INFO and higher' in captured.out


@patch('nhldata.app.API_FACTORY')
def test_cli_main_with_debug_on(mock_factory, monkeypatch, data_bucket, job_bucket):
    monkeypatch.setenv("DEST_BUCKET", 'testdatabucket')
    monkeypatch.setenv("JOB_BUCKET", 'testjobbucket')

    runner = CliRunner()
    result = runner.invoke(main, ['--debug', 'games'])

    assert result.exit_code == 0
    assert __version__ in result.output
    assert 'log level is DEBUG and higher' in result.output


@patch('nhldata.app.API_FACTORY')
def test_cli_main_with_debug_off(mock_factory, monkeypatch, data_bucket, job_bucket):
    monkeypatch.setenv("DEST_BUCKET", 'testdatabucket')
    monkeypatch.setenv("JOB_BUCKET", 'testjobbucket')

    runner = CliRunner()
    result = runner.invoke(main, ['games'])

    assert result.exit_code == 0
    assert __version__ in result.output
    assert 'log level is INFO and higher' in result.output


@patch('nhldata.app.API_FACTORY.adapter_for_version')
def test_cli_main_with_exception(mock_factory, monkeypatch, data_bucket, job_bucket):
    mock_factory.side_effect = Exception("IT IS BROKEN!")
    monkeypatch.setenv("DEST_BUCKET", 'testdatabucket')
    monkeypatch.setenv("JOB_BUCKET", 'testjobbucket')

    runner = CliRunner()
    result = runner.invoke(main, ['games'])

    assert result.exit_code == 0
    assert __version__ in result.output
    assert 'log level is INFO and higher' in result.output

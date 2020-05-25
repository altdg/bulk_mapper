import re
import subprocess
from os import getenv, path, listdir


def test_cli_tool_on_live_api():
    """ Checks whether the tool may be invoked using CLI and the resulting CSV file is generated """

    api_key = getenv('ADG_API_KEY')
    if not api_key:
        raise ValueError('ADG_API_KEY env var not set')

    pth = path.dirname(path.abspath(__file__))

    subprocess.run(['rm', path.join(pth, 'sample-merchants-*.csv')])

    result = subprocess.run([
        'python', path.join(pth, 'altdg', 'api.py'),
        '-e', 'merchants',
        '-k', api_key,
        path.join(pth, 'sample-merchants.txt'),
    ])
    result.check_returncode()

    assert any(re.match(r'sample-merchants-.*\.csv', name) for name in listdir(pth)), \
        'CSV file not generated in target folder'

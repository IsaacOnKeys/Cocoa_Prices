"""Check a packaged function's resolved dependencies and import without calling sources."""

import argparse
import importlib
import importlib.metadata
import json
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runtime', type=Path)
    parser.add_argument('entrypoint')
    args = parser.parse_args()
    assert sys.version_info[:2] == (3, 11), 'Python 3.11 required'
    pins = [line for line in (args.runtime / 'constraints-python311.txt').read_text().splitlines()
            if line and not line.startswith('#')]
    for pin in pins:
        name, version = pin.split('==')
        assert importlib.metadata.version(name) == version, name + ' differs from constraints'
    sys.path.insert(0, str(args.runtime.resolve()))
    module = importlib.import_module('main')
    assert callable(getattr(module, args.entrypoint))
    print(json.dumps({'python': sys.version.split()[0], 'resolved_packages': len(pins),
                      'entrypoint': args.entrypoint, 'imported': True, 'invoked': False}))


if __name__ == '__main__':
    main()

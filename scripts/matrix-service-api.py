#!/usr/bin/env python

"""
Entry point for starting a local test DCP Matrix Service API server.
"""

import os
import sys
import logging
import argparse

from chalice.cli import CLIFactory

if __name__ == '__main__':
    pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
    sys.path.insert(0, pkg_root)  # noqa

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--port", type=int, default=5000)
parser.add_argument("--no-debug", dest="debug", action="store_false",
                    help="Disable Chalice/Connexion/Flask debug mode")
parser.add_argument("--project-dir", help=argparse.SUPPRESS,
                    default=os.path.join(os.path.dirname(__file__), "..", "chalice"))
parser.add_argument("--log-level",
                    help=str([logging.getLevelName(i) for i in range(0, 60, 10)]),
                    choices={logging.getLevelName(i) for i in range(0, 60, 10)},
                    default=logging.DEBUG)
args = parser.parse_args()

logging.basicConfig(level=args.log_level, stream=sys.stderr)

factory = CLIFactory(project_dir=args.project_dir, debug=args.debug)
# The following code snippet is basically stolen from chalice/cli/__init__py:run_local_server
config = factory.create_config_obj(chalice_stage_name=os.environ['DEPLOYMENT_STAGE'])
app_obj = factory.load_chalice_app()
logging.basicConfig(stream=sys.stdout)
server = factory.create_local_server(app_obj, config, "localhost", args.port)
server.serve_forever()

#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import sys

from destination_h2ogpte import DestinationH2OGPTE


def run() -> None:
    DestinationH2OGPTE().run(sys.argv[1:])


if __name__ == "__main__":
    run()
#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
# Original code by https://github.com/mn-mikke7
#


import sys

from destination_h2ogpte import DestinationH2OGPTE

if __name__ == "__main__":
    DestinationH2OGPTE().run(sys.argv[1:])

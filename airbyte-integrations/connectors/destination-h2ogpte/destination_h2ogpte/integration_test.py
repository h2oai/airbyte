#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import json
import logging
from datetime import datetime

from airbyte_cdk.models import DestinationSyncMode, Status, Type
from airbyte_cdk.models import AirbyteMessage, AirbyteRecordMessage, AirbyteStateMessage
from .destination import DestinationH2OGPTE

def get_config():
    config = """
    {
        "address": "https://playground.h2ogpte.h2o.ai",
        "api_key": "TODO",
        "collection_id": "f99e3241-6bf1-4afa-a79f-6be733a3d57d"
    }
    """
    return json.loads(config)

def get_record(stream, record):
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream=stream,
            data=record,
            emitted_at=int(datetime.now().timestamp()) * 1000,
        ),
    )

def get_state(data):
    return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=data))


def test_check_valid_config():
    config = get_config()
    outcome = DestinationH2OGPTE().check(logging.getLogger("airbyte"), config)
    assert outcome.status == Status.SUCCEEDED

def test_write():
    config = get_config()
    catalog = None
    first_state_message = get_state({"state": "1"})
    first_record_chunk = [get_record("mystream", {"message": f"Dogs are number {i}"}) for i in range(5)]

    destination = DestinationH2OGPTE()
    list(destination.write(config, catalog, [*first_record_chunk, first_state_message]))

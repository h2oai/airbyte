#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, Mapping
import hashlib
import orjson
import logging
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models.airbyte_protocol_serializers import custom_type_resolver
from airbyte_cdk.exception_handler import init_uncaught_exception_handler
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteLogMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    Type,
    Level,
    AirbyteStateStats
)
from typing import Any, Dict, Iterable, List, Mapping, cast
from h2ogpte import H2OGPTE
from collections import defaultdict
import io
import tempfile
import os
from serpyco_rs import Serializer
from typing_extensions import override
from dataclasses import dataclass

@dataclass
class PatchedAirbyteStateMessage(AirbyteStateMessage):
    """Declare the `id` attribute that platform sends."""

    id: int | None = None
    """Injected by the platform."""


@dataclass
class PatchedAirbyteMessage(AirbyteMessage):
    """Keep all defaults but override the type used in `state`."""

    state: PatchedAirbyteStateMessage | None = None
    """Override class for the state message only."""


PatchedAirbyteMessageSerializer = Serializer(
    PatchedAirbyteMessage,
    omit_none=True,
    custom_type_resolver=custom_type_resolver,
)
"""Redeclared SerDes class using the patched dataclass."""

logger = logging.getLogger("airbyte")

class PatchedDestination(Destination):
    @override
    def run(self, args: list[str]) -> None:
        """Overridden from CDK base class in order to use the patched SerDes class."""
        init_uncaught_exception_handler(logger)
        parsed_args = self.parse_args(args)
        output_messages = self.run_cmd(parsed_args)
        for message in output_messages:
            print(
                orjson.dumps(
                    PatchedAirbyteMessageSerializer.dump(
                        cast(PatchedAirbyteMessage, message),
                    )
                ).decode()
            )

    @override
    def _parse_input_stream(self, input_stream: io.TextIOWrapper) -> Iterable[AirbyteMessage]:
        """Reads from stdin, converting to Airbyte messages.

        Includes overrides that should be in the CDK but we need to test it in the wild first.

        Rationale:
            The platform injects `id` but our serializer classes don't support
            `additionalProperties`.
        """
        for line in input_stream:
            try:
                yield PatchedAirbyteMessageSerializer.load(orjson.loads(line))
            except orjson.JSONDecodeError:
                logger.info(f"ignoring input which can't be deserialized as Airbyte Message: {line}")


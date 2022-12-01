"""Adnavem tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_adnavem.streams import (
    PurchaseOrderMasterStream,
    PurchaseOrderDocumentStream,
    ShipmentActiveContainerStream,
    ShipmentDetailStream
)

STREAM_TYPES = [
    PurchaseOrderMasterStream,
    PurchaseOrderDocumentStream,
    ShipmentActiveContainerStream,
    ShipmentDetailStream
]

class TapAdnavem(Tap):
    """Adnavem tap class."""
    name = "tap-adnavem"

    records_jsonpath = "$.data[*]"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "party_id",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "role",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

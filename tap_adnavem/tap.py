"""Adnavem tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers


import logging
from singer_sdk.helpers._state import write_stream_state

# TODO: Import your custom stream types here:
from tap_adnavem.streams import (
    AdnavemStream,
    PurchaseOrderMasterStream,
    PurchaseOrderDocumentStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    PurchaseOrderMasterStream,
    PurchaseOrderDocumentStream,
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

    def load_state(self, state: dict) -> None:
        """Merge or initialize stream state with the provided state dictionary input.
        Override this method to perform validation and backwards-compatibility patches
        on self.state. If overriding, we recommend first running
        `super().load_state(state)` to ensure compatibility with the SDK.
        Args:
            state: Initialize the tap's state with this value.
        Raises:
            ValueError: If the tap's own state is None, meaning it has not been
                initialized.
        """
        if self.state is None:
            raise ValueError("Cannot write to uninitialized state dictionary.")

        #logging.info(f'ELIZA DBG LOAD STATE {state.get("bookmarks", {}).items()}')
        for stream_name, stream_state in state.get("bookmarks", {}).items():
            # key: i
            # val: {'context': {'number': number}}
            for key, val in stream_state.items():
                #logging.info(f'ELIZA DBG LOAD STATE {key, val}')
#                for partitioned_key, partitioned_val in val.get("context").items():
#                    logging.info(f'ELIZA DBG LOAD STATE {partitioned_key, partitioned_val}')
                write_stream_state(
                    self.state,
                    stream_name,
                    key,
                    val,
                )

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

"""Stream type classes for tap-adnavem."""
from datetime import datetime

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_adnavem.client import AdnavemStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

from singer_sdk.helpers._state import get_writeable_state_dict
import logging

class PurchaseOrderMasterStream(AdnavemStream):
    """Stream to get purchase order master models."""
    name = "purchase_order"
    path = "/purchasing/purchaseOrder/master"

    primary_keys = ["id"]
    replication_key = "date"

    state_partitioning_keys = ["number"]

    schema_filepath = SCHEMAS_DIR / "purchase_orders.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.replication_key:
            params["partyId"] = self.config.get("party_id")
            params["role"] = self.config.get("role")
        return params

    def get_child_context(
        self, record: dict, context: Optional[dict]
    ) -> Dict[str, Any]:
        """Return a context dictionary for child streams."""
        return {
            "number": record["number"]
        }

    def post_process(
        self, row: dict, context: Optional[dict]
    ) -> Dict[str, Any]:
        """As needed, append or transform raw data to match expected structure."""
        row["extraction_date"] = datetime.now()
        row["date"] = datetime.strptime(row["date"], "%Y-%m-%d")

        logging.info(f'DEBUG MASTER post_process, {self.get_starting_timestamp(context)}')
        logging.info(f'DEBUG MASTER post_process, {context}')

        return row

class PurchaseOrderDocumentStream(AdnavemStream):
    """Stream to get purchase order documents."""
    name = "purchase_documents"
    path = "/purchasing/purchaseOrderDocument"
    schema_filepath = SCHEMAS_DIR / "purchase_order_documents.json"

    # TODO: only id
    primary_keys = ["id"]
    replication_key = "date"

    # Streams should be invoked once per parent:
    parent_stream_type = PurchaseOrderMasterStream
    # Assume epics don't have `updated_at` incremented when issues are changed:
    ignore_parent_replication_keys = True
    state_partitioning_keys = ["number"]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["partyId"] = self.config.get("party_id")
        params["number"] = context["number"]

        return params

    def post_process(
        self, row: dict, context: Optional[dict]
    ) -> Dict[str, Any]:
        """As needed, append or transform raw data to match expected structure."""
        row["extraction_date"] = datetime.now()
        row["date"] = datetime.strptime(row["date"], "%Y-%m-%d")

        logging.info(f'DEBUG DOCUMENT post_process, {self.get_starting_timestamp(context)}')
        logging.info(f'DEBUG DOCUMENT post_process, {context}')

        return row

    def _get_state_partition_context(self, context):
        """Override state handling if Stream.state_partitioning_keys is specified.
        Args:
            context: Stream partition or context dictionary.
        Returns:
            TODO
        """
        if context is None:
            return None

        if self.state_partitioning_keys is None:
            return context

        #logging.info("GET STATE PARTITION CONTEXT")
        #logging.info({k: v for k, v in context.items() if k in self.state_partitioning_keys})
        return {k: v for k, v in context.items() if k in self.state_partitioning_keys}

    def get_context_state(self, context) -> dict:
        """Return a writable state dict for the given context.
        Gives a partitioned context state if applicable; else returns stream state.
        A blank state will be created in none exists.
        This method is internal to the SDK and should not need to be overridden.
        Developers may access this property but this is not recommended except in
        advanced use cases. Instead, developers should access the latest stream
        replication key values using
        :meth:`~singer_sdk.Stream.get_starting_timestamp()` for timestamp keys, or
        :meth:`~singer_sdk.Stream.get_starting_replication_key_value()` for
        non-timestamp keys.
        Partition level may be overridden by
        :attr:`~singer_sdk.Stream.state_partitioning_keys` if set.
        Args:
            context: Stream partition or context dictionary.
        Returns:
            A partitioned context state if applicable; else returns stream state.
            A blank state will be created in none exists.
        """
        state_partition_context = self._get_state_partition_context(context)
        logging.info(f"STATE_PARTITION_CONTEXT {state_partition_context}")
        if state_partition_context:
            return get_writeable_state_dict(
                self.tap_state,
                self.name,
                state_partition_context=state_partition_context,
            )
        return self.stream_state

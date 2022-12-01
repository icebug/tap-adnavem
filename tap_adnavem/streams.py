"""Stream type classes for tap-adnavem."""
from datetime import datetime

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_adnavem.client import AdnavemStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class PurchaseOrderMasterStream(AdnavemStream):
    """Stream to get purchase order master models."""
    name = "purchase_order"
    path = "/purchasing/purchaseOrder/master"

    primary_keys = ["id"]
    replication_key = "extraction_date"

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
        return row

class PurchaseOrderDocumentStream(AdnavemStream):
    """Stream to get purchase order documents."""
    name = "purchase_documents"
    path = "/purchasing/purchaseOrderDocument"
    schema_filepath = SCHEMAS_DIR / "purchase_order_documents.json"

    primary_keys = ["id"]
    replication_key = "extraction_date"

    # Streams should be invoked once per parent:
    parent_stream_type = PurchaseOrderMasterStream
    # Assume epics don't have `updated_at` incremented when issues are changed:
    ignore_parent_replication_keys = True

    # Don't use state partitioning
    state_partitioning_keys = []

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
        return row
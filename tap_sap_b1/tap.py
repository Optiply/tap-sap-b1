"""SAP Business One tap."""

from datetime import datetime, timezone

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.exceptions import FatalAPIError
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_sap_b1.client import shift_back
from tap_sap_b1.streams import (
    InventoryTransferRequestsStream,
    ProductsStream,
    PurchaseDeliveryNotesStream,
    PurchaseOrdersStream,
    ReservePurchaseInvoicesStream,
    SalesOrdersStream,
    SuppliersStream,
)

STREAM_TYPES = (
    ProductsStream,
    SuppliersStream,
    PurchaseOrdersStream,
    ReservePurchaseInvoicesStream,
    PurchaseDeliveryNotesStream,
    SalesOrdersStream,
    InventoryTransferRequestsStream,
)


class TapSAPB1(Tap):
    """Read-only Singer tap for SAP Business One Service Layer."""

    name = "tap-sap-b1"
    alerting_level = AlertingLevel.WARNING
    exception_alerting_level_map = {InvalidCredentialsError: AlertingLevel.NONE}

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            required=True,
            description="Service Layer root, for example https://host:50000/b1s/v1",
        ),
        th.Property("company_db", th.StringType, required=True),
        th.Property("username", th.StringType, required=True),
        th.Property("password", th.StringType, required=True),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2000-01-01T00:00:00Z",
            description="Initial cursor for incremental streams",
        ),
        th.Property(
            "verify_ssl",
            th.BooleanType,
            default=True,
            description="Verify the SAP TLS certificate",
        ),
        th.Property(
            "ca_bundle",
            th.StringType,
            description="Optional CA bundle path; takes precedence over verify_ssl",
        ),
        th.Property(
            "request_timeout",
            th.IntegerType,
            default=300,
            description="Per-request timeout in seconds",
        ),
        th.Property("page_size", th.IntegerType),
        th.Property("page_sizes", th.ObjectType()),
        th.Property("include_warehouse_stock", th.BooleanType, default=True),
        th.Property(
            "replication_keys",
            th.ObjectType(),
            description=(
                'Per-stream replication key, e.g. {"products": "U_last_changed"}. '
                "Set a stream to null for a full-table sync."
            ),
        ),
        th.Property(
            "lookback_windows",
            th.ObjectType(),
            description=(
                'Per-stream overlap re-scanned every run, e.g. {"sales_orders": '
                '"1mo"}. Accepts m, h, d, w, mo, y.'
            ),
        ),
    ).to_dict()

    @staticmethod
    def _is_secret_config(config_key: str) -> bool:
        """Prevent all SAP login fields from being written to logs."""
        return config_key in {"company_db", "username", "password"}

    def _validate_stream_settings(self) -> None:
        """Fail fast on per-stream settings that name a stream that is not there.

        These settings are keyed by stream name, so a typo is otherwise silent:
        the setting is simply never applied, and the stream keeps its default.
        """
        known = {stream_type.name for stream_type in STREAM_TYPES}
        for setting in ("page_sizes", "lookback_windows", "replication_keys"):
            configured = self.config.get(setting)
            if configured is None:
                continue
            if not isinstance(configured, dict):
                raise FatalAPIError(f"{setting} must be an object keyed by stream name")
            unknown = sorted(set(configured) - known)
            if unknown:
                raise FatalAPIError(
                    f"{setting} refers to unknown streams: {', '.join(unknown)}. "
                    f"Known streams: {', '.join(sorted(known))}"
                )
        windows = self.config.get("lookback_windows") or {}
        for window in windows.values():
            if window:
                # Raises with the offending value rather than failing mid-sync.
                shift_back(datetime.now(timezone.utc), window)

    def discover_streams(self) -> list[Stream]:
        """Return the SAP streams represented by the Alumio integration."""
        self._validate_stream_settings()
        return [stream_type(tap=self) for stream_type in STREAM_TYPES]

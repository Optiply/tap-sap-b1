"""SAP Business One tap."""

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_sap_b1.streams import (
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
            default=30,
            description="Per-request timeout in seconds",
        ),
    ).to_dict()

    @staticmethod
    def _is_secret_config(config_key: str) -> bool:
        """Prevent all SAP login fields from being written to logs."""
        return config_key in {"company_db", "username", "password"}

    def discover_streams(self) -> list[Stream]:
        """Return the SAP streams represented by the Alumio integration."""
        return [stream_type(tap=self) for stream_type in STREAM_TYPES]

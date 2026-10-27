"""Credential-free tests for URL handling, paging and query shape."""

from __future__ import annotations

import json

import jsonschema
import pytest
import requests
from hotglue_singer_sdk.exceptions import FatalAPIError
from requests.cookies import RequestsCookieJar

from tap_sap_b1.client import SAPSessionAuthenticator
from tap_sap_b1.streams import (
    InventoryTransferRequestsStream,
    ProductsStream,
    PurchaseDeliveryNotesStream,
    PurchaseOrdersStream,
    ReservePurchaseInvoicesStream,
    SalesOrdersStream,
    SuppliersStream,
)
from tap_sap_b1.tap import STREAM_TYPES, TapSAPB1

CONFIG = {
    "api_url": "https://sap.example.test:50000/b1s/v1",
    "company_db": "TEST",
    "username": "test-user",
    "password": "test-password",
    "start_date": "2024-01-01T00:00:00Z",
}


def stream(stream_type=ProductsStream, **overrides):
    """Build one stream with the sample config plus overrides."""
    return stream_type(TapSAPB1(config=CONFIG | overrides, validate_config=False))


def response(status: int, payload: dict | None = None) -> requests.Response:
    """Build a real requests response without making a request."""
    result = requests.Response()
    result.status_code = status
    result.cookies = RequestsCookieJar()
    result._content = b"" if payload is None else json.dumps(payload).encode()
    return result


class FakeSession:
    """Return queued responses and record the requests that asked for them."""

    def __init__(self, *responses: requests.Response):
        self.responses = list(responses)
        self.calls: list[tuple] = []

    def post(self, url, **kwargs) -> requests.Response:
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


# --------------------------------------------------------------- api_url


@pytest.mark.parametrize(
    "api_url,expected",
    [
        ("https://host:50000/b1s/v1", "https://host:50000/b1s/v1"),
        ("https://host:50000/b1s/v2", "https://host:50000/b1s/v2"),
        ("https://host:50000/b1s/v1/", "https://host:50000/b1s/v1"),
        # A bare host used to make every request 404 against /Login.
        ("https://host:50000", "https://host:50000/b1s/v1"),
        ("https://host:50000/", "https://host:50000/b1s/v1"),
    ],
)
def test_url_base_fills_in_a_missing_service_layer_path(api_url, expected):
    assert stream(api_url=api_url).url_base == expected


def test_url_base_still_rejects_a_non_absolute_api_url():
    with pytest.raises(FatalAPIError, match="absolute"):
        _ = stream(api_url="$SAP_URL").url_base


def test_login_404_names_the_url_and_the_fix(monkeypatch):
    products = stream()
    session = FakeSession(response(404, {"error": {"code": -1, "message": "not found"}}))
    monkeypatch.setattr(type(products), "requests_session", property(lambda self: session))

    with pytest.raises(FatalAPIError) as excinfo:
        SAPSessionAuthenticator(products)

    message = str(excinfo.value)
    assert "https://sap.example.test:50000/b1s/v1/Login" in message
    assert "api_url" in message
    assert CONFIG["password"] not in message


# -------------------------------------------------------------- page size


def test_page_size_defaults_are_modest_enough_to_build_in_time():
    assert stream().page_size == 500
    assert stream(SalesOrdersStream).page_size == 500
    assert stream(SuppliersStream).page_size == 500


def test_page_size_is_configurable_globally_and_per_stream():
    assert stream(page_size=100).page_size == 100
    assert stream(page_size=100, page_sizes={"products": 25}).page_size == 25
    # a per-stream entry for another stream must not leak
    assert stream(SuppliersStream, page_sizes={"products": 25}).page_size == 500


@pytest.mark.parametrize("stream_type", STREAM_TYPES)
def test_no_stream_shadows_the_page_size_property(stream_type):
    """A class-level page_size would silently ignore the config."""
    assert isinstance(getattr(type(stream(stream_type)), "page_size", None), property)
    assert stream(stream_type, page_size=77).page_size == 77


def test_page_size_drives_both_top_and_the_prefer_header():
    products = stream(page_size=200)

    assert products.get_url_params(None, None)["$top"] == 200
    assert products.http_headers["Prefer"] == "odata.maxpagesize=200"


@pytest.mark.parametrize("value", [0, -1, "many", None.__class__])
def test_invalid_page_size_fails_loudly(value):
    with pytest.raises(FatalAPIError, match="page size"):
        _ = stream(page_size=value).page_size


# ------------------------------------------------------------ query shape


def test_orderby_uses_the_primary_key_only():
    params = stream().get_url_params(None, None)

    # U_last_changed is an unindexed UDF: sorting on it scans the whole table.
    assert params["$orderby"] == "ItemCode"
    assert params["$filter"].startswith("UpdateDate ge ")


def test_warehouse_stock_can_be_switched_off():
    with_stock = stream().get_url_params(None, None)["$select"]
    without_stock = stream(include_warehouse_stock=False).get_url_params(None, None)["$select"]

    assert "ItemWarehouseInfoCollection" in with_stock
    assert "ItemWarehouseInfoCollection" not in without_stock
    # everything else is untouched
    assert without_stock.split(",") == [
        field for field in with_stock.split(",") if field != "ItemWarehouseInfoCollection"
    ]


def test_paging_still_advances_with_the_configured_page_size():
    products = stream(page_size=2)
    full_page = response(200, {"value": [{"ItemCode": "A"}, {"ItemCode": "B"}]})
    last_page = response(200, {"value": [{"ItemCode": "C"}]})

    assert products.get_next_page_token(full_page, None) == 2
    assert products.get_next_page_token(last_page, 2) is None
    assert products.get_url_params(None, 2)["$skip"] == 2


def test_request_timeout_default_allows_a_slow_service_layer():
    assert stream().timeout == 300
    assert stream(request_timeout=30).timeout == 30


# ------------------------------------------------- incremental documents


def test_closed_document_streams_are_incremental_on_update_date():
    """A full scan of every closed document is unbounded; UpdateDate is not."""
    for stream_type in (SalesOrdersStream, PurchaseDeliveryNotesStream):
        assert stream(stream_type).replication_key == "UpdateDate"


def test_open_document_streams_stay_full_table():
    """Open POs and reserve invoices are snapshots: absence means closed."""
    for stream_type in (PurchaseOrdersStream, ReservePurchaseInvoicesStream):
        assert stream(stream_type).replication_key is None
        assert "$filter" in stream(stream_type).get_url_params(None, None)


def test_products_replicates_on_sap_update_date_not_the_udf():
    products = stream()
    params = products.get_url_params(None, None)

    assert products.replication_key == "UpdateDate"
    assert params["$filter"].startswith("UpdateDate ge ")
    # the UDF is still delivered, so a shop can switch back to it
    assert "U_last_changed" in params["$select"]


def test_replication_key_is_configurable_per_stream():
    as_udf = stream(replication_keys={"products": "U_last_changed"})
    assert as_udf.replication_key == "U_last_changed"
    assert as_udf.get_url_params(None, None)["$filter"].startswith("U_last_changed ge ")

    full_table = stream(replication_keys={"products": None})
    assert full_table.replication_key is None
    assert "$filter" not in full_table.get_url_params(None, None)


def test_a_configured_replication_key_reaches_select_and_schema():
    """SAP only returns selected fields, and the SDK can only bookmark schema ones."""
    custom = stream(replication_keys={"products": "U_custom_stamp"})

    assert "U_custom_stamp" in custom.get_url_params(None, None)["$select"]
    assert "U_custom_stamp" in custom.schema["properties"]


def test_lookback_widens_the_filter_and_is_configurable():
    # start_date 2024-01-01, products default lookback 1d
    assert stream().get_url_params(None, None)["$filter"] == "UpdateDate ge '2023-12-31'"
    assert stream(lookback_windows={"products": "1mo"}).get_url_params(None, None)[
        "$filter"
    ] == "UpdateDate ge '2023-12-01'"
    assert stream(lookback_windows={"products": "0d"}).get_url_params(None, None)[
        "$filter"
    ] == "UpdateDate ge '2024-01-01'"


@pytest.mark.parametrize("window", ["soon", "7 parsecs"])
def test_an_unparseable_lookback_fails_loudly(window):
    with pytest.raises(FatalAPIError, match="lookback"):
        stream(lookback_windows={"products": window}).get_url_params(None, None)


def test_cancelled_documents_are_delivered_with_their_flag():
    """Cancelling closes a document, so they arrive via bost_Close either way."""
    for stream_type in (SalesOrdersStream, PurchaseDeliveryNotesStream):
        subject = stream(stream_type)
        params = subject.get_url_params(None, None)
        assert "Cancelled" in params["$select"]
        assert "Cancelled" in subject.schema["properties"]
        # they are not filtered out: the consumer decides what to do with them
        assert "Cancelled eq" not in params["$filter"]


def test_bookmark_and_lookback_drive_the_filter(tmp_path):
    """With state, the filter starts at the bookmark minus the lookback."""
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "bookmarks": {
                    name: {
                        "replication_key": "UpdateDate",
                        "replication_key_value": "2026-10-20T00:00:00Z",
                    }
                    for name in ("products", "sales_orders")
                }
            }
        )
    )
    tap = TapSAPB1(config=CONFIG, state=str(state_file), validate_config=False)

    for stream_type, expected in (
        (ProductsStream, "UpdateDate ge '2026-10-19'"),  # 1 day overlap
        (SalesOrdersStream, "UpdateDate ge '2026-10-13'"),  # 7 day overlap
    ):
        subject = stream_type(tap)
        # sync() seeds the starting replication value before the first request
        subject._write_starting_replication_value(None)
        assert subject.get_url_params(None, None)["$filter"].endswith(expected)


# ---------------------------------------------------------- resilience


def test_odata_annotations_are_stripped_from_records():
    """SAP tags every record; the SDK would warn and drop them on each one."""
    subject = stream()
    row = {
        "odata.etag": 'W/"1"',
        "@odata.etag": 'W/"1"',
        "ItemCode": "9136",
        "ItemPrices": [{"odata.type": "#Price", "PriceList": 1, "Price": 10}],
        "ItemWarehouseInfoCollection": [{"@odata.id": "x", "InStock": 5}],
    }

    assert subject.post_process(row, None) == {
        "ItemCode": "9136",
        "ItemPrices": [{"PriceList": 1, "Price": 10}],
        "ItemWarehouseInfoCollection": [{"InStock": 5}],
    }


def test_a_timeout_becomes_an_actionable_retriable_error(monkeypatch):
    """A timed-out query keeps running in SAP; the message says which knob to turn."""
    from hotglue_singer_sdk.exceptions import RetriableAPIError

    subject = stream(request_timeout=30, page_sizes={"products": 2500})

    class TimingOutSession:
        def send(self, *args, **kwargs):
            raise requests.Timeout("read timed out")

    monkeypatch.setattr(
        type(subject), "requests_session", property(lambda self: TimingOutSession())
    )
    request = requests.Request("GET", "https://sap.example.test:50000/b1s/v1/Items")

    with pytest.raises(RetriableAPIError) as excinfo:
        subject._request(request.prepare(), None)

    message = str(excinfo.value)
    assert "30s" in message and "2500" in message
    assert 'page_sizes["products"]' in message and "request_timeout" in message


def test_retries_wait_long_enough_for_sap_to_recover():
    """The SDK default retries a heavy query within a second of it timing out."""
    subject = stream()
    waits = [next(subject.backoff_wait_generator()) for _ in range(3)]

    assert min(waits) >= 5
    assert subject.backoff_max_tries() == 4


# ------------------------------------------------------ config validation


@pytest.mark.parametrize(
    "setting", ["page_sizes", "lookback_windows", "replication_keys"]
)
def test_a_typo_in_a_per_stream_setting_is_rejected(setting):
    """These are keyed by stream name, so a typo would otherwise do nothing."""
    tap = TapSAPB1(
        config=CONFIG | {setting: {"product": 100}}, validate_config=False
    )

    with pytest.raises(FatalAPIError, match="unknown streams: product"):
        tap.discover_streams()


def test_an_invalid_lookback_window_is_rejected_at_discovery():
    tap = TapSAPB1(
        config=CONFIG | {"lookback_windows": {"products": "ages"}},
        validate_config=False,
    )

    with pytest.raises(FatalAPIError, match="lookback"):
        tap.discover_streams()


def test_valid_per_stream_settings_pass_validation():
    tap = TapSAPB1(
        config=CONFIG
        | {
            "page_sizes": {"products": 200},
            "lookback_windows": {"sales_orders": "1mo"},
            "replication_keys": {"products": "U_last_changed"},
        },
        validate_config=False,
    )

    assert len(tap.discover_streams()) == len(STREAM_TYPES)


# ------------------------------------------- inventory transfer requests


def test_inventory_transfer_requests_reads_open_requests_with_their_lines():
    """Alumio queried these per SKU; one paged pass covers every SKU."""
    subject = stream(InventoryTransferRequestsStream)
    params = subject.get_url_params(None, None)

    assert subject.path == "/InventoryTransferRequests"
    assert params["$filter"] == "DocumentStatus eq 'bost_Open'"
    assert "StockTransferLines" in params["$select"]
    assert params["$orderby"] == "DocEntry"
    # a snapshot of what is open, like purchase_orders
    assert subject.replication_key is None


def test_inventory_transfer_request_schema_accepts_a_realistic_record():
    subject = stream(InventoryTransferRequestsStream)
    record = {
        "DocEntry": 412,
        "DocNum": 1412,
        "DocDate": "2026-10-20T00:00:00Z",
        "DocDueDate": "2026-10-27",
        "DocumentStatus": "bost_Open",
        "Cancelled": "tNO",
        "UpdateDate": "2026-10-26T00:00:00Z",
        "StockTransferLines": [
            {
                "LineNum": 0,
                "ItemCode": "9136",
                "ItemDescription": "Doos 300x200x150",
                "Quantity": 25.0,
                "WarehouseCode": "01",
                "FromWarehouseCode": "02",
            }
        ],
    }

    jsonschema.validate(record, subject.schema)


def test_sap_error_detail_reaches_the_message():
    """A bare "400 Bad Request" hides which field SAP rejected."""
    subject = stream()
    rejected = response(
        400,
        {
            "error": {
                "code": -1004,
                "message": {"lang": "en-us", "value": "Invalid field name 'DocDueDate'"},
            }
        },
    )

    with pytest.raises(FatalAPIError, match="Invalid field name 'DocDueDate'"):
        subject.validate_response(rejected)


@pytest.mark.parametrize("body", ['"Service Unavailable"', "[1, 2]", "not-json"])
def test_an_unusual_error_body_does_not_crash_the_handler(body):
    subject = stream()
    raw = requests.Response()
    raw.status_code = 500
    raw._content = body.encode()

    # retriable, not an AttributeError from parsing the body
    with pytest.raises(Exception, match="500"):
        subject.validate_response(raw)


def test_inventory_transfer_requests_only_asks_for_proven_fields():
    """OWTQ is not a marketing document; unknown fields 400 the whole request."""
    params = stream(InventoryTransferRequestsStream).get_url_params(None, None)
    requested = set(params["$select"].split(","))

    assert requested == {
        "DocEntry",
        "DocNum",
        "DocDate",
        "DocumentStatus",
        "StockTransferLines",
    }

# tap-sap-b1

Read-only Singer tap for SAP Business One Service Layer, built with `hotglue_singer_sdk`.

## Streams

| Stream | Service Layer endpoint | Primary key | Replication |
| --- | --- | --- | --- |
| `products` | `/Items` | `ItemCode` | `UpdateDate` (+1d overlap) |
| `suppliers` | `/BusinessPartners` | `CardCode` | `UpdateDate` (+1d overlap) |
| `purchase_orders` | `/PurchaseOrders` | `DocEntry` | full table, open only |
| `reserve_purchase_invoices` | `/PurchaseInvoices` | `DocEntry` | full table, open reserve invoices only |
| `purchase_delivery_notes` | `/PurchaseDeliveryNotes` | `DocEntry` | `UpdateDate` (+7d overlap), completed only |
| `sales_orders` | `/Orders` | `DocEntry` | `UpdateDate` (+7d overlap), completed only |
| `inventory_transfer_requests` | `/InventoryTransferRequests` | `DocEntry` | full table, open only |

Closed documents replicate on `UpdateDate`, which SAP moves when a document is closed or cancelled - `DocDate` does not, which is why these streams used to read the full closed history on every run. Each run also re-reads a lookback window before the bookmark as a safety margin.

Cancelled documents are **not** filtered out: cancelling closes a document, so they arrive through the `bost_Close` filter carrying `Cancelled: 'tYES'`, and the consumer decides whether to retract them.

`inventory_transfer_requests` carries `StockTransferLines`: stock requested for transfer between warehouses but not yet moved. The Alumio integration added these quantities to a product's stock level, querying them one SKU at a time through `QueryService_PostQuery` because a GET cannot filter on a line-level field. This stream reads every open request once instead, and the consumer sums the lines per `ItemCode`.

`products` includes `ItemPreferredVendors` and `ItemWarehouseInfoCollection`, covering supplier-product links and stock by warehouse. Document streams retain `DocumentLines` so lines stay joined to their parent and product identifiers.

## Configuration

```json
{
  "api_url": "https://sap.example:50000/b1s/v1",
  "company_db": "COMPANY_DB",
  "username": "USERNAME",
  "password": "PASSWORD",
  "start_date": "2024-01-01T00:00:00Z",
  "verify_ssl": true,
  "request_timeout": 300,
  "page_size": 500,
  "page_sizes": { "products": 200 },
  "include_warehouse_stock": true
}
```

`api_url`, `company_db`, `username`, and `password` are required. Keep real config under `.secrets/` (gitignored). Set `ca_bundle` for a private CA. Use `verify_ssl: false` only for an explicitly approved development system.

`api_url` should be the Service Layer root, including the version segment (`/b1s/v1` or `/b1s/v2`). A bare host is accepted and `/b1s/v1` is assumed, with a warning — pin the version yourself when the server offers both.

### Tuning throughput

| Setting | Default | What it does |
| --- | --- | --- |
| `request_timeout` | `300` | Seconds before a request is abandoned. SAP can take minutes to build a large page, and a timed-out request keeps running server side while the tap retries it. |
| `page_size` | `500` | Records per request for every stream (sent as `$top` and `Prefer: odata.maxpagesize`). |
| `page_sizes` | – | Per-stream override, e.g. `{"products": 200}`. |
| `include_warehouse_stock` | `true` | Request `ItemWarehouseInfoCollection` on `products`. Switch it off when only totals are needed. |
| `replication_keys` | – | Per-stream replication key, e.g. `{"products": "U_last_changed"}`. Set a stream to `null` for a full-table sync. |
| `lookback_windows` | see above | Per-stream overlap re-read every run, e.g. `{"sales_orders": "1mo"}`. Units: `m`, `h`, `d`, `w`, `mo`, `y`. |

Items carry three child collections (`ItemPrices`, `ItemPreferredVendors`, `ItemWarehouseInfoCollection`), and SAP serializes a page in one go: cost grows with the page size, not with the total number of records. If `products` times out, lower `page_sizes.products` before raising `request_timeout`, and switch `include_warehouse_stock` off if the warehouse data is not used downstream.

## Run

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/tap-sap-b1 --config .secrets/config.json --discover > catalog.json
.venv/bin/pytest
```

For a bounded sync, select one stream in the catalog and use a recent `start_date`.

## Behavior

- Logs in through `POST /Login`; credentials are marked secret and never logged.
- Strips OData annotations (`odata.etag` and friends) from records and nested objects.
- Rejects per-stream settings naming an unknown stream, and unparseable lookback windows, at discovery rather than mid-sync.
- Waits 5s, 10s, 20s between retries: a timed-out query is still running inside SAP, so retrying immediately stacks copies of it. A timeout names the page size and timeout in the error.
- Sends the returned `B1SESSION` and optional `ROUTEID` cookies; refreshes once after a data-request 401.
- Prefers `@odata.nextLink`/`odata.nextLink`, then falls back to stable `$orderby`, `$top`, and `$skip` pagination.
- Retries login only for connection failures, HTTP 429, and HTTP 5xx, with finite waits.
- Maps HTTP 401/403 to `InvalidCredentialsError`; permanent HTTP 4xx responses are fatal.
- Uses inclusive incremental lower bounds (`ge`) to preserve records tied on a date cursor.
- Reads completed documents as full tables because `DocDate` does not track later status changes.
- Does not write to SAP, create SQL queries, deploy, or schedule jobs.

## Evidence

- SAP Business One Service Layer API Reference: <https://help.sap.com/doc/056f69366b5345a386bb8149f1700c19/10.0/en-US/Service%20Layer%20API%20Reference.html>
- SAP Business One Service Layer guide: <https://help.sap.com/doc/fc2f5477516c404c8bf9ad1315a17238/10.0/en-US/Working_with_SAP_Business_One_Service_Layer.pdf>

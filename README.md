# tap-sap-b1

Read-only Singer tap for SAP Business One Service Layer, built with `hotglue_singer_sdk`.

The stream scope mirrors the active Paco Verpakkingen Alumio integration exported on 2026-08-25.

## Streams

| Stream | Service Layer endpoint | Primary key | Replication |
| --- | --- | --- | --- |
| `products` | `/Items` | `ItemCode` | `U_last_changed` |
| `suppliers` | `/BusinessPartners` | `CardCode` | `UpdateDate` |
| `purchase_orders` | `/PurchaseOrders` | `DocEntry` | full table, open only |
| `reserve_purchase_invoices` | `/PurchaseInvoices` | `DocEntry` | full table, open reserve invoices only |
| `purchase_delivery_notes` | `/PurchaseDeliveryNotes` | `DocEntry` | full table, completed only |
| `sales_orders` | `/Orders` | `DocEntry` | full table, completed only |

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
  "request_timeout": 30
}
```

`api_url`, `company_db`, `username`, and `password` are required. Keep real config under `.secrets/` (gitignored). Set `ca_bundle` for a private CA. Use `verify_ssl: false` only for an explicitly approved development system.

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
- Source integration: `PacoVerpakkingen_SAPBusinessOne.ndjson` (local operator-approved path; credentials are not copied into this repository).

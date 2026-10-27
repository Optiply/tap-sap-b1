"""Streams mirrored from the active Paco Verpakkingen Alumio integration."""

from hotglue_singer_sdk import typing as th

from tap_sap_b1.client import SAPB1Stream

_item_price = th.ObjectType(
    th.Property("PriceList", th.IntegerType),
    th.Property("Price", th.NumberType),
    th.Property("Currency", th.StringType),
    th.Property("BasePriceList", th.IntegerType),
    th.Property("Factor", th.NumberType),
)

_preferred_vendor = th.ObjectType(
    th.Property("BPCode", th.StringType),
)

_warehouse_stock = th.ObjectType(
    th.Property("WarehouseCode", th.StringType),
    th.Property("InStock", th.NumberType),
    th.Property("Committed", th.NumberType),
    th.Property("Ordered", th.NumberType),
    th.Property("MinStock", th.NumberType),
    th.Property("MaxStock", th.NumberType),
    th.Property("Locked", th.StringType),
)

_document_line = th.ObjectType(
    th.Property("LineNum", th.IntegerType),
    th.Property("ItemCode", th.StringType),
    th.Property("ItemDescription", th.StringType),
    th.Property("Quantity", th.NumberType),
    th.Property("OpenQuantity", th.NumberType),
    th.Property("Price", th.NumberType),
    th.Property("LineTotal", th.NumberType),
    th.Property("WarehouseCode", th.StringType),
    th.Property("ShipDate", th.StringType),
)

_document_schema = th.PropertiesList(
    th.Property("DocEntry", th.IntegerType),
    th.Property("DocNum", th.IntegerType),
    th.Property("DocDate", th.DateTimeType),
    th.Property("DocTime", th.StringType),
    th.Property("DocDueDate", th.StringType),
    th.Property("DocTotal", th.NumberType),
    th.Property("VatSum", th.NumberType),
    th.Property("CardCode", th.StringType),
    th.Property("DocumentStatus", th.StringType),
    th.Property("Cancelled", th.StringType),
    th.Property("UpdateDate", th.DateTimeType),
    th.Property("U_OPT_PO_HeaderID", th.StringType),
    th.Property("DocumentLines", th.ArrayType(_document_line)),
).to_dict()

_DOCUMENT_FIELDS = (
    "DocEntry",
    "DocNum",
    "DocDate",
    "DocTime",
    "DocDueDate",
    "DocTotal",
    "VatSum",
    "DocumentStatus",
    # Cancelling a document closes it, so cancelled documents arrive through
    # the bost_Close filter. Without this flag the consumer cannot tell them
    # apart from genuinely completed ones.
    "Cancelled",
    "UpdateDate",
    "DocumentLines",
    "CardCode",
    "U_OPT_PO_HeaderID",
)


class ProductsStream(SAPB1Stream):
    """Products, supplier-product links, and warehouse stock from Items."""

    name = "products"
    path = "/Items"
    #: SAP's own last-modified date, not a UDF: it exists on every item and is
    #: maintained by SAP itself. U_last_changed is still selected, so a shop
    #: that prefers it can switch with replication_keys config.
    default_replication_key = "UpdateDate"
    default_lookback = "1d"
    default_primary_keys = ("ItemCode",)
    #: Items carry three child collections, so pages are expensive to build.
    default_page_size = 500
    #: Per-warehouse stock: the heaviest part of an item page by far. Optiply
    #: only needs the totals, so it can be switched off per connection.
    warehouse_stock_field = "ItemWarehouseInfoCollection"
    all_select_fields = (
        "ItemCode",
        "PurchaseItem",
        "QuantityOnStock",
        "QuantityOrderedByCustomers",
        "ItemPrices",
        "ItemName",
        "MinInventory",
        "Valid",
        "U_OPT_Ignored",
        "U_OPT_Status",
        "U_OPT_Backorder",
        "U_OPT_Related",
        "ItemPreferredVendors",
        "ItemWarehouseInfoCollection",
        "LeadTime",
        "ToleranceDays",
        "PurchaseItemsPerUnit",
        "DesiredInventory",
        "PurchaseUnitWeight",
        "U_ItemName",
        "SupplierCatalogNo",
        "Mainsupplier",
        "PurchaseUnitVolume",
        "U_last_changed",
        "UpdateDate",
        "UpdateTime",
    )

    @property
    def select_fields(self) -> tuple[str, ...]:
        """Return the item fields to request, honouring include_warehouse_stock."""
        if self.config.get("include_warehouse_stock", True):
            return self.all_select_fields
        return tuple(
            field for field in self.all_select_fields if field != self.warehouse_stock_field
        )

    default_schema = th.PropertiesList(
        th.Property("ItemCode", th.StringType),
        th.Property("PurchaseItem", th.StringType),
        th.Property("QuantityOnStock", th.NumberType),
        th.Property("QuantityOrderedByCustomers", th.NumberType),
        th.Property("ItemPrices", th.ArrayType(_item_price)),
        th.Property("ItemName", th.StringType),
        th.Property("MinInventory", th.NumberType),
        th.Property("Valid", th.StringType),
        th.Property("U_OPT_Ignored", th.StringType),
        th.Property("U_OPT_Status", th.StringType),
        th.Property("U_OPT_Backorder", th.StringType),
        th.Property("U_OPT_Related", th.StringType),
        th.Property("ItemPreferredVendors", th.ArrayType(_preferred_vendor)),
        th.Property("ItemWarehouseInfoCollection", th.ArrayType(_warehouse_stock)),
        th.Property("LeadTime", th.IntegerType),
        th.Property("ToleranceDays", th.IntegerType),
        th.Property("PurchaseItemsPerUnit", th.NumberType),
        th.Property("DesiredInventory", th.NumberType),
        th.Property("PurchaseUnitWeight", th.NumberType),
        th.Property("U_ItemName", th.StringType),
        th.Property("SupplierCatalogNo", th.StringType),
        th.Property("Mainsupplier", th.StringType),
        th.Property("PurchaseUnitVolume", th.NumberType),
        th.Property("U_last_changed", th.DateTimeType),
        th.Property("UpdateDate", th.DateTimeType),
        th.Property("UpdateTime", th.StringType),
    ).to_dict()


class SuppliersStream(SAPB1Stream):
    """Supplier business partners."""

    name = "suppliers"
    path = "/BusinessPartners"
    default_replication_key = "UpdateDate"
    default_lookback = "1d"
    default_primary_keys = ("CardCode",)
    default_page_size = 500
    fixed_filter = "CardType eq 'cSupplier'"
    select_fields = (
        "CardName",
        "CardCode",
        "EmailAddress",
        "UpdateDate",
        "UpdateTime",
        "U_OPT_IgnoredSupplier",
        "U_OPT_FixedCosts",
        "U_LBS_MinimaalOrderbedrag",
    )
    default_schema = th.PropertiesList(
        th.Property("CardName", th.StringType),
        th.Property("CardCode", th.StringType),
        th.Property("EmailAddress", th.StringType),
        th.Property("UpdateDate", th.DateTimeType),
        th.Property("UpdateTime", th.StringType),
        th.Property("U_OPT_IgnoredSupplier", th.StringType),
        th.Property("U_OPT_FixedCosts", th.NumberType),
        th.Property("U_LBS_MinimaalOrderbedrag", th.NumberType),
    ).to_dict()


class PurchaseOrdersStream(SAPB1Stream):
    """Open purchase orders."""

    name = "purchase_orders"
    path = "/PurchaseOrders"
    fixed_filter = "DocumentStatus eq 'bost_Open'"
    default_primary_keys = ("DocEntry",)
    select_fields = _DOCUMENT_FIELDS
    default_schema = _document_schema


class ReservePurchaseInvoicesStream(SAPB1Stream):
    """Open reserve purchase invoices."""

    name = "reserve_purchase_invoices"
    path = "/PurchaseInvoices"
    fixed_filter = "ReserveInvoice eq 'tYES' and InventoryStatus eq 'bost_Open'"
    default_primary_keys = ("DocEntry",)
    select_fields = _DOCUMENT_FIELDS
    default_schema = _document_schema


class PurchaseDeliveryNotesStream(SAPB1Stream):
    """Completed purchase delivery notes and their lines."""

    name = "purchase_delivery_notes"
    path = "/PurchaseDeliveryNotes"
    #: UpdateDate moves when a document is closed or cancelled, which DocDate
    #: does not. A full scan of every closed note is otherwise unbounded.
    default_replication_key = "UpdateDate"
    default_lookback = "7d"
    default_primary_keys = ("DocEntry",)
    fixed_filter = "DocumentStatus eq 'bost_Close'"
    select_fields = (
        "DocEntry",
        "DocDate",
        "DocumentStatus",
        "Cancelled",
        "UpdateDate",
        "DocumentLines",
    )
    default_schema = _document_schema


class SalesOrdersStream(SAPB1Stream):
    """Completed sales orders and their lines."""

    name = "sales_orders"
    path = "/Orders"
    #: See PurchaseDeliveryNotesStream: UpdateDate tracks closure, DocDate does
    #: not. Cancelled orders arrive too, flagged, so a consumer can retract
    #: them downstream.
    default_replication_key = "UpdateDate"
    default_lookback = "7d"
    default_primary_keys = ("DocEntry",)
    #: Orders carry DocumentLines, so keep pages modest.
    default_page_size = 500
    fixed_filter = "DocumentStatus eq 'bost_Close'"
    select_fields = _DOCUMENT_FIELDS
    default_schema = _document_schema

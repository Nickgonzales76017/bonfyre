from bonfyre_erpnext import __version__ as app_version

app_name = "bonfyre_erpnext"
app_title = "Bonfyre ERPNext"
app_publisher = "Bonfyre"
app_description = "Bridges ERPNext documents into the Bonfyre native estate (CMS, runtime, machine/entity graph)."
app_email = "engineering@bonfyre.internal"
app_license = "MIT"

doc_events = {
    "Sales Order": {
        "on_submit": "bonfyre_erpnext.events.sales_order.on_submit",
        "on_cancel": "bonfyre_erpnext.events.sales_order.on_cancel",
    },
    "Payment Entry": {
        "on_submit": "bonfyre_erpnext.events.payment_entry.on_submit",
    },
    "Purchase Invoice": {
        "on_submit": "bonfyre_erpnext.events.purchase_invoice.on_submit",
    },
}

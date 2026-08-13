from bonfyre_erpnext.services import cms_bridge, machine_bridge


def on_submit(doc, method=None):
    cms_bridge.mirror_document(doc, entry_status="submitted")
    if doc.customer:
        machine_bridge.record_party_observation(
            entity_id=f"customer:{doc.customer}",
            display=doc.customer_name or doc.customer,
            modal="sales_order",
            value=doc.name,
            source="bonfyre_erpnext.sales_order",
        )


def on_cancel(doc, method=None):
    cms_bridge.mirror_document(doc, entry_status="cancelled")

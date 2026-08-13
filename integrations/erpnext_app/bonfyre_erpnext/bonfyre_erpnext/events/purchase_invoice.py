from bonfyre_erpnext.services import cms_bridge, machine_bridge


def on_submit(doc, method=None):
    cms_bridge.mirror_document(doc, entry_status="submitted")
    if doc.supplier:
        machine_bridge.record_party_observation(
            entity_id=f"supplier:{doc.supplier}",
            display=doc.supplier_name or doc.supplier,
            modal="purchase_invoice",
            value=doc.name,
            source="bonfyre_erpnext.purchase_invoice",
        )

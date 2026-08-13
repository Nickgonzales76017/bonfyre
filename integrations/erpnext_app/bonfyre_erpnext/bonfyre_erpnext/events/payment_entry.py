from bonfyre_erpnext.services import cms_bridge


def on_submit(doc, method=None):
    cms_bridge.mirror_document(doc, entry_status="submitted")

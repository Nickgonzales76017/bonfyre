import frappe

from bonfyre_erpnext import __version__
from bonfyre_erpnext.services import cms_bridge


@frappe.whitelist()
def ping():
    """Liveness check -- confirms the app is installed and loadable."""
    return {"ok": True, "app": "bonfyre_erpnext", "version": __version__}


@frappe.whitelist()
def recent_documents(limit: int = 20):
    """Recently mirrored ERPNext documents from the Bonfyre CMS erp_document table."""
    return cms_bridge.recent_documents(limit=int(limit))

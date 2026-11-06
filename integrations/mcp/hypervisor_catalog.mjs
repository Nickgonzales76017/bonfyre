// Catalog preserves the installed 33 operation names and argument schemas.
// Writes previously mislabeled read-only are corrected at the shared boundary.
// The schemas are data in hypervisor_catalog.json; the import keeps it in the
// pinned deployment closure.
import tools from './hypervisor_catalog.json' with { type: 'json' };
export const HYPERVISOR_TOOLS = Object.freeze(tools);

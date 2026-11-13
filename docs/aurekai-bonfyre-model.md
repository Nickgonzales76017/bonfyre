# Institutional integration example: Aurekai uses Bonfÿre

**This is not a combined company-and-engine identity document.**

- **[Bonfÿre](../README.md)** is an independent software engine and developer platform. Its native source, runtime, contracts and tests are authoritative here.
- **Aurekai** is an independently operated business. Its brands, customers, partnerships, commercial models and sites belong to its actual operating institution or named downstream client. [Aurekai business site](https://aurekai.work/) is the public business entrance, **not** the engine documentation.
- The [software/business ownership boundary](OWNERSHIP-BOUNDARY.md) defines what is an engine asset, an institutional deployment, a business source, a grant and a distinct release.
- The [typed engine architecture](architecture.md) and [native compute/presentation guide](native-data-compute-and-presentation.md) cover reusable technology without assigning Aurekai ownership of all users and data.

The previous mixed Aurekai/Bonfÿre strategy essay has been retained only for historical context at [the pre-separation archive](archive/aurekai-bonfyre-model-20261113.md). **It is superseded**, should not be linked as a source of legal/technical identity or release authority, and must not drive generic engine code.

Aurekai's separately hosted legacy Akai runtime distribution is a **compatibility and migration question**. It is not proof the company and engine are synonymous, and no existing npm, PyPI or CLI alias has been removed by these documentation changes.

## Exact deployment contract

~~~mermaid
flowchart LR
  ENGINE["Bonfÿre engine release"] --> INTERFACE["Versioned API / binary / optional app adapter"]
  INTERFACE --> TENANT["Aurekai owned tenant install"]
  TENANT --> SITE["Aurekai-specific public sites and account apps"]
  TENANT --> GRANTS["Aurekai-held source, provider, publishing and effect grants"]
  GRANTS --> EFFECT["Business result and separate release receipt"]
~~~

The Bonfÿre build **must not implicitly grant** the Aurekai tenant access to accounts. A deployed engine command is **not** an Aurekai public release. An Aurekai customer account is **not** Bonfÿre project data.

The [boundary checker](../scripts/check-product-boundary.py) now rejects Aurekai-specific identifiers in core engine and library source. Separating historical higher-level WebAddress/CMS defaults, moving business-owned source packs to their true repository, testing tenant grants and proving the four public site cutovers remain live implementation work.

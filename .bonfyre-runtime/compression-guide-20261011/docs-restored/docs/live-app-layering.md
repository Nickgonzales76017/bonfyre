# Live App Layering

Bonfyre should show both:

- the deterministic floor
- the orchestrated layer

without confusing people about what is core Bonfyre and what is the optional policy layer.

## Default Pattern

Use one app repo with an in-app compare surface when:

- the floor and layered versions share the same artifact family
- the UI shell is mostly the same
- the point is to show incremental uplift, not a different product

This should be the default for most `pages-*` apps.

## Split Repo Pattern

Create a sibling repo like `pages-foo-orchestrated` only when:

- the layered UX becomes a different product surface
- the intake, workflow, or review model is materially different
- the layered app needs different Pages positioning or onboarding
- the compare view inside one app would make the primary product harder to understand

That means split repos are the exception, not the default.

## Naming

Recommended:

- base app: `pages-release-radio`
- layered sibling only if needed: `pages-release-radio-orchestrated`

Avoid:

- duplicating the whole fleet immediately
- splitting before the compare pattern is proven in the flagship apps

## Rollout

1. Pilot the compare surface inside the flagship apps.
2. Use shipped demo artifacts to show `floor` vs `layered`.
3. Split into sibling repos only where the layered flow becomes product-distinct.

## First Apps

Best candidates for in-app compare first:

- `pages-release-radio`
- `pages-podcast-plant`
- `pages-town-box`
- `pages-oss-cockpit`

Best candidates for future sibling repos if the orchestrated flow keeps diverging:

- `pages-town-box-orchestrated`
- `pages-oss-cockpit-orchestrated`


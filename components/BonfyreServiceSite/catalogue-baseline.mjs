// Build-time source projection. Browser chunks and intake use the same baseline.
import { parseFragment } from 'parse5';
import { createHash } from 'node:crypto';
import { money, priceCents } from './catalogue-model.mjs';
import { emptyPreparation } from './catalogue-preparation.mjs';

export function descriptionText(html) {
  const omitted = new Set(['script', 'style', 'noscript', 'iframe', 'object', 'template']);
  const blocks = new Set(['p', 'li', 'div', 'h1', 'h2', 'h3']);
  const text = node => {
    if (omitted.has(node.tagName)) return '';
    if (node.nodeName === '#text') return node.value;
    if (node.tagName === 'br') return '\n';
    return (node.childNodes ?? []).map(text).join('') + (blocks.has(node.tagName) ? '\n' : '');
  };
  return text(parseFragment(html || '')).replace(/\n[\t ]+/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
}

export function prepareCatalogueRow(product) {
  const description_text = descriptionText(product.description_html);
  const fields = { title: product.title, description: description_text, tags: product.tags.join(', '), ...emptyPreparation() };
  for (const variant of product.variants) {
    fields['sku:' + variant.id] = variant.sku ?? '';
    fields['price:' + variant.id] = money(priceCents(variant.price));
  }
  const before_sha256 = Object.fromEntries(Object.entries(fields).map(([field, value]) => {
    if (typeof value !== 'string') throw Error('Catalogue source field is not text.');
    return [field, createHash('sha256').update(value).digest('hex')];
  }));
  return { ...product, description_text, before_sha256 };
}

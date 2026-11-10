import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { descriptionText, prepareCatalogueRow } from '../catalogue-baseline.mjs';
import { baseline } from '../catalogue-model.mjs';

test('retained HTML becomes inert decoded text with consistent paragraph boundaries', () => {
  const html = '<p>Caf&eacute; &amp; tea &#x1f375;<br>Next line</p><div>Second <b>paragraph</b></div>' +
    '<script>throw Error("execute")</script><style>secret</style><noscript>hidden</noscript><iframe>hidden</iframe><object>hidden</object><template>hidden</template>';
  expect(descriptionText(html)).toBe('Café & tea 🍵\nNext line\nSecond paragraph');
  expect(descriptionText('<p>First<p>Second<!--comment-->')).toBe('First\nSecond');
  expect(descriptionText('')).toBe('');
});

test('browser baseline and source digests share decoded text and normalized prices', () => {
  const product = { title: 'Mug', description_html: '<p>Caf&eacute;</p>', tags: ['tea', 'café'], variants: [{ id: '9', sku: '', price: '19.5' }] };
  const prepared = prepareCatalogueRow(product), digest = value => createHash('sha256').update(value).digest('hex');
  expect(baseline(prepared, 'different browser parsing').description).toBe('Café');
  expect(prepared.before_sha256).toMatchObject({ title: digest('Mug'), description: digest('Café'), tags: digest('tea, café'), 'sku:9': digest(''), 'price:9': digest('19.50') });
  for (const field of ['seo_title','seo_description','image_filename','image_alt','collections','questions']) expect(prepared.before_sha256['prep:'+field]).toBe(digest(''));
  expect(product).not.toHaveProperty('description_text');
});

#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────
// scrape-channels.js — Puppeteer-based YouTube channel scraper
// Discovers video URLs from a creator's channel /videos page.
//
// Usage:
//   node scrape-channels.js --handle @AlexHormozi --count 3 --output urls.txt
//   node scrape-channels.js --batch creators.json --count 2 --output-dir ./output
//
// The URLs are then fed into bonfyre-ingest for download + processing.
// ──────────────────────────────────────────────────────────────
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// A kept URL list is a cache hit only when it is what this run would have
// written: every line a watch URL, at least as many as asked for, for the same
// channel, and the bytes the record beside it describes. An empty or cut-off
// file left by an interrupted run is not a result.
const WATCH = /^https:\/\/www\.youtube\.com\/watch\?v=[\w-]{6,}/;
const digest = text => crypto.createHash('sha256').update(text).digest('hex');
function keptUrls(outFile, handle, count) {
  let text;
  try { text = fs.readFileSync(outFile, 'utf8'); } catch (_) { return null; }
  const urls = text.split('\n').filter(Boolean);
  if (!urls.length || !urls.every(url => WATCH.test(url))) return null;
  let record = null;
  try { record = JSON.parse(fs.readFileSync(outFile + '.json', 'utf8')); } catch (_) {}
  // A list written before records existed is kept if it is whole and long enough.
  if (!record) return urls.length >= count ? urls : null;
  if (record.handle !== handle || record.sha256 !== digest(text)) return null;
  // A channel with fewer videos than asked for was scraped fully once already.
  return urls.length >= count || record.requested >= count ? urls : null;
}
function keepUrls(outFile, handle, count, urls) {
  const text = urls.join('\n') + '\n';
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  // Written beside the target and renamed, so a reader never sees half a list.
  fs.writeFileSync(outFile + '.tmp', text);
  fs.renameSync(outFile + '.tmp', outFile);
  fs.writeFileSync(outFile + '.json', JSON.stringify({ handle, requested: count, urls: urls.length, sha256: digest(text), scraped_at: new Date().toISOString() }) + '\n');
}

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { count: 3, handle: '', batch: '', output: '', outputDir: '' };
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--handle':    opts.handle = args[++i]; break;
      case '--count':     opts.count = parseInt(args[++i], 10); break;
      case '--batch':     opts.batch = args[++i]; break;
      case '--output':    opts.output = args[++i]; break;
      case '--output-dir': opts.outputDir = args[++i]; break;
    }
  }
  return opts;
}

async function scrapeChannelVideos(browser, handle, count) {
  // The browser says what it is. A pinned identity from an old release claims
  // a browser this is not and ages into one no site still serves.
  const page = await browser.newPage();

  const url = `https://www.youtube.com/${handle}/videos`;
  console.error(`  Scraping ${url} for ${count} video(s)...`);

  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

    // Any link to a video, rather than an element id the site is free to
    // rename: the ids this used to wait for are gone and it found nothing,
    // silently. A video is its id; the same one linked twice is one.
    await page.waitForSelector('a[href^="/watch?v="]', { timeout: 15000 }).catch(() => {});

    // Scroll to load more videos if needed
    for (let scroll = 0; scroll < Math.ceil(count / 30); scroll++) {
      await page.evaluate(() => window.scrollBy(0, 2000));
      await new Promise(r => setTimeout(r, 1500));
    }

    const videoUrls = await page.evaluate((maxCount) => {
      const seen = new Set(), urls = [];
      for (const link of document.querySelectorAll('a[href^="/watch?v="]')) {
        const id = (link.getAttribute('href').match(/^\/watch\?v=([\w-]{6,})/) || [])[1];
        if (!id || seen.has(id)) continue;
        seen.add(id);
        if (urls.length < maxCount) urls.push('https://www.youtube.com/watch?v=' + id);
      }
      return urls;
    }, count);

    return videoUrls;
  } catch (err) {
    console.error(`  WARN: Failed to scrape ${handle}: ${err.message}`);
    return [];
  } finally {
    await page.close();
  }
}

async function main() {
  const opts = parseArgs();

  // The sandbox stays on: this loads pages this estate does not control. A
  // host that cannot run a sandboxed browser says so and stops rather than
  // running one without it.
  if (process.argv.includes('--check-kept')) {
    const at = process.argv.indexOf('--check-kept');
    const urls = keptUrls(process.argv[at + 1], opts.handle, opts.count);
    console.log(urls ? `kept ${urls.length}` : 'not kept');
    process.exit(urls ? 0 : 1);
  }
  // Loaded only when a browser is actually needed, so checking what is kept
  // does not require one to be installed.
  const puppeteer = require('puppeteer');
  const browser = await puppeteer.launch({ headless: 'new' });

  try {
    if (opts.batch) {
      // Batch mode: process all creators from JSON
      const data = JSON.parse(fs.readFileSync(opts.batch, 'utf8'));
      const baseDir = opts.outputDir || './output';

      for (const group of data.groups) {
        for (const creator of group.creators) {
          const slug = creator.name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
          const outFile = path.join(baseDir, slug, '.video_urls');

          const kept = keptUrls(outFile, creator.yt, opts.count);
          if (kept) {
            console.error(`  ${creator.name}: kept ${kept.length} URL(s) (${outFile})`);
            continue;
          }

          const urls = await scrapeChannelVideos(browser, creator.yt, opts.count);

          if (urls.length > 0) {
            keepUrls(outFile, creator.yt, opts.count, urls);
            console.error(`  ${creator.name}: ${urls.length} URL(s) → ${outFile}`);
          } else {
            console.error(`  ${creator.name}: no videos found`);
          }

          // Small delay to avoid rate limiting
          await new Promise(r => setTimeout(r, 2000));
        }
      }
    } else if (opts.handle) {
      // Single-creator mode
      const urls = await scrapeChannelVideos(browser, opts.handle, opts.count);

      if (opts.output && urls.length) keepUrls(opts.output, opts.handle, opts.count, urls);
      // Finding nothing is a failure to say out loud, not an empty success.
      if (!urls.length) { console.error(`  ${opts.handle}: no videos found`); process.exitCode = 3; }
      // Print to stdout for piping
      urls.forEach(u => console.log(u));
    } else {
      console.error('Usage: node scrape-channels.js --handle @Creator --count N');
      console.error('       node scrape-channels.js --batch creators.json --count N --output-dir ./output');
      process.exit(1);
    }
  } finally {
    await browser.close();
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});

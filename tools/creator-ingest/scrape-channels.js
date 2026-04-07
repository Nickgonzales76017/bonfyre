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
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

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
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

  const url = `https://www.youtube.com/${handle}/videos`;
  console.error(`  Scraping ${url} for ${count} video(s)...`);

  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

    // Wait for video thumbnails to appear
    await page.waitForSelector('a#video-title-link, a#video-title', { timeout: 15000 }).catch(() => {});

    // Scroll to load more videos if needed
    for (let scroll = 0; scroll < Math.ceil(count / 30); scroll++) {
      await page.evaluate(() => window.scrollBy(0, 2000));
      await new Promise(r => setTimeout(r, 1500));
    }

    // Extract video URLs from the page
    const videoUrls = await page.evaluate((maxCount) => {
      const links = document.querySelectorAll('a#video-title-link, a#video-title');
      const urls = [];
      for (const link of links) {
        const href = link.getAttribute('href');
        if (href && href.startsWith('/watch?v=') && urls.length < maxCount) {
          urls.push('https://www.youtube.com' + href);
        }
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

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu']
  });

  try {
    if (opts.batch) {
      // Batch mode: process all creators from JSON
      const data = JSON.parse(fs.readFileSync(opts.batch, 'utf8'));
      const baseDir = opts.outputDir || './output';

      for (const group of data.groups) {
        for (const creator of group.creators) {
          const slug = creator.name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
          const outFile = path.join(baseDir, slug, '.video_urls');

          // Skip if cache exists
          if (fs.existsSync(outFile)) {
            console.error(`  ${creator.name}: cached (${outFile})`);
            continue;
          }

          const urls = await scrapeChannelVideos(browser, creator.yt, opts.count);

          if (urls.length > 0) {
            fs.mkdirSync(path.dirname(outFile), { recursive: true });
            fs.writeFileSync(outFile, urls.join('\n') + '\n');
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

      if (opts.output) {
        fs.mkdirSync(path.dirname(opts.output), { recursive: true });
        fs.writeFileSync(opts.output, urls.join('\n') + '\n');
      }
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

# CMS Guide — bonfyre-cms

`bonfyre-cms` is a 299 KB content management system that replaces Strapi. It provides a REST API, dynamic schemas, token authentication, and Lambda Tensors compression — all in a single static binary.

## Quick start

```bash
bonfyre-cms serve --port 8800
```

That's it. The CMS is running on port 8800 with a SQLite database.

## Commands

| Command | Description |
|---|---|
| `serve` | Start the HTTP server |
| `bench` | Run in-process benchmarks |
| `status` | Print system info |
| `--help` | Show usage |

## Flags

| Flag | Default | Description |
|---|---|---|
| `--port PORT` | 8800 | HTTP listen port |
| `--data DIR` | `~/.local/share/bonfyre/` | Data directory |
| `--db FILE` | `cms.db` (in data dir) | SQLite database |

## Schema management

Schemas are created dynamically via the API:

```bash
# Create a schema
curl -X POST http://localhost:8800/api/schemas \
  -H "Content-Type: application/json" \
  -d '{"name": "posts", "fields": [{"name": "title", "type": "string"}, {"name": "body", "type": "text"}]}'

# Create content
curl -X POST http://localhost:8800/api/posts \
  -H "Content-Type: application/json" \
  -d '{"title": "Hello", "body": "First post"}'

# List content
curl http://localhost:8800/api/posts

# Get one
curl http://localhost:8800/api/posts/1
```

## Lambda Tensors integration

When a content type accumulates multiple records, BonfyreCMS automatically groups them into families and applies Lambda Tensors compression.

This means:
- Storage shrinks as similar records accumulate
- Individual fields can be read without decompressing the whole family
- Queries and projections operate on compressed data

## Authentication

BonfyreCMS integrates with `bonfyre-gate` for API key validation:

```bash
# Start with gate integration
bonfyre-cms serve --port 8800 --gate-key YOUR_KEY
```

API requests must include:
```
Authorization: Bearer YOUR_KEY
```

## Migration from Strapi

1. Export your Strapi schemas as JSON
2. Create the same schemas via the BonfyreCMS API
3. Export content from Strapi's REST API
4. Import into BonfyreCMS

The REST API surface is intentionally similar to Strapi's `/api/:collection` pattern.

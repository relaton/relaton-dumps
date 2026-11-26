# relaton-dumps

Relaton bulk data plane — versioned, open snapshots of the Relaton Index on
`data.relaton.org`.

Epic: relaton/relaton-api-js#175 (Week 2). Implementation charter lives in
this repo's issue tracker.

## What this service does

- **NDJSON snapshots** per flavor: full records, versioned URLs, immutable
- **Manifest + checksums** per snapshot (sha256 per file + dataset manifest)
- **Provenance metadata**: source repos, index generation sha, export date
- **CC-BY licensing statement** on the metadata

OpenCitations-style full dumps: researchers, model trainers, and mirror
operators consume bulk instead of hammering the API.

## Cloudflare deployment

- Worker: `data-relaton-org` (`wrangler.jsonc`)
- R2 bucket: `relaton-dumps` (`wrangler r2 bucket create relaton-dumps` once)
- Route: `data.relaton.org` (custom domain, enable once zone confirmed)
- GitHub secrets: `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`
- Generation: export from the api-relaton-org D1 dataset, triggered on
  workflow_dispatch / schedule — budget-shaped, reads only

## Laws (binding)

- Snapshots are immutable and versioned: never mutate a published snapshot.
- No daily full-corpus D1 sync — exports are budgeted reads; regeneration is
  manifest-sha-gated.
- Wire shapes derive from relaton-models; no hand-rolled serialization.

## Development

```sh
npm install
npm run dev      # local workerd
npm test         # vitest
npm run deploy   # requires CF secrets
```

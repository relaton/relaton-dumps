// relaton-dumps#1: versioned NDJSON snapshots per flavor, sha256-manifested.
//
// Budgeted reads: one flavor per invocation, key-ordered cursor walk over
// the API dataset's documents_v2 (LIMIT pages), each record one NDJSON
// line. The generation id is derived from the flavor's row count and the
// max indexed_at — two exports of an unchanged dataset derive the same
// id, and the second one finds its objects already in R2 and skips.
// Snapshots are immutable: a published generation is never rewritten.
export interface Generation {
  flavor: string;
  generation: string;
  records: number;
  latest: string | null;
}

export interface SnapshotManifest {
  flavor: string;
  generation: string;
  records: number;
  sha256: string;
  bytes: number;
  created: string;
  license: string;
  provenance: {
    source_repo: string;
    generation_inputs: { count: number; latest_indexed_at: string | null };
    export_date: string;
    tool: string;
  };
  url: string;
}

export interface ExportResult {
  flavor: string;
  generation: string;
  records: number;
  sha256: string;
  bytes: number;
  skipped: boolean;
}

const PAGE = 500;
// R2 multipart parts must be >= 5 MiB (except the last); flush at ~8 MiB.
const PART_BYTES = 8 * 1024 * 1024;
const LICENSE = "CC-BY-4.0";
import { Sha256 } from "./sha256";
export const TOOL = "relaton-dumps/0.1.0";

export async function sha256Hex(data: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", data as BufferSource);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// The generation id: sha256(count + latest indexed_at), 16 hex chars —
// stable for an unchanged dataset, different for any ingest wave.
export async function generationOf(db: D1Database, flavor: string): Promise<Generation> {
  const row = await db
    .prepare("SELECT COUNT(*) AS n, MAX(indexed_at) AS latest FROM documents_v2 WHERE flavor = ?1")
    .bind(flavor)
    .first<{ n: number; latest: string | null }>();
  const inputs = { count: row?.n ?? 0, latest_indexed_at: row?.latest ?? null };
  const gen = await sha256Hex(new TextEncoder().encode(JSON.stringify(inputs)));
  return { flavor, generation: gen.slice(0, 16), records: inputs.count, latest: inputs.latest_indexed_at };
}

export function snapshotKey(flavor: string, generation: string): string {
  return `snapshots/${flavor}/${generation}/records.ndjson`;
}

export function manifestKey(flavor: string): string {
  return `snapshots/${flavor}/manifest.json`;
}

// Walk the flavor's records and write the snapshot as one R2 multipart
// object, digesting the exact bytes as they go (the manifest sha256 is
// over the NDJSON file, not the records).
export async function exportFlavor(
  db: D1Database,
  bucket: R2Bucket,
  flavor: string,
  opts: { baseUrl: string },
): Promise<ExportResult> {
  const gen = await generationOf(db, flavor);
  const key = snapshotKey(flavor, gen.generation);
  const existing = await bucket.head(key);
  if (existing) {
    const manifest = await bucket.get(manifestKey(flavor));
    const m = manifest ? ((await manifest.json()) as SnapshotManifest) : null;
    return {
      flavor,
      generation: gen.generation,
      records: m?.records ?? gen.records,
      sha256: m?.sha256 ?? "",
      bytes: m?.bytes ?? existing.size,
      skipped: true,
    };
  }
  if (gen.records === 0) throw new Error(`flavor ${flavor} has no records in the dataset`);

  // Incremental SHA-256 over the streamed bytes.
  let digest: Sha256;
  let bytes = 0;
  let records = 0;
  let cursor = "";
  let partLines: string[] = [];
  let partBytes = 0;
  const parts: R2UploadedPart[] = [];
  const upload = await bucket.createMultipartUpload(key, {
    httpMetadata: { contentType: "application/x-ndjson" },
    customMetadata: {
      flavor,
      generation: gen.generation,
      license: LICENSE,
      "export-tool": TOOL,
    },
  });

  const flushPart = async () => {
    if (partLines.length === 0) return;
    const body = new TextEncoder().encode(partLines.join(""));
    digest.update(body);
    bytes += body.byteLength;
    parts.push(await upload.uploadPart(parts.length + 1, body as BufferSource));
    partLines = [];
    partBytes = 0;
  };

  try {
    digest = new Sha256();
    // The cursor walks documents_v2 in key order; LIMIT pages keep every
    // read bounded (relaton-dumps#1: budget-shaped reads, no full sync).
    for (;;) {
      const stmt = cursor
        ? db.prepare("SELECT key, flavor, file, data, indexed_at FROM documents_v2 WHERE flavor = ?1 AND key > ?2 ORDER BY key LIMIT 500").bind(flavor, cursor)
        : db.prepare("SELECT key, flavor, file, data, indexed_at FROM documents_v2 WHERE flavor = ?1 ORDER BY key LIMIT 500").bind(flavor);
      const { results } = await stmt.all<{ key: string; flavor: string; file: string; data: string; indexed_at: string }>();
      if (!results || results.length === 0) break;
      for (const r of results) {
        let record: unknown;
        try {
          record = JSON.parse(r.data);
        } catch {
          record = { key: r.key, unparseable: true };
        }
        const line = JSON.stringify({ key: r.key, flavor: r.flavor, file: r.file, indexed_at: r.indexed_at, record }) + "\n";
        partLines.push(line);
        partBytes += line.length;
        records += 1;
        if (partBytes >= PART_BYTES) await flushPart();
      }
      cursor = results[results.length - 1]!.key;
      // D1 throttles bursts ("overloaded within a 10 second window") —
      // pace the page reads so big flavors stay inside the window.
      await new Promise((r) => setTimeout(r, 250));
    }
    await flushPart();
    const sha = digest.hex();
    await upload.complete(parts);

    const manifest: SnapshotManifest = {
      flavor,
      generation: gen.generation,
      records,
      sha256: sha,
      bytes,
      created: new Date().toISOString().slice(0, 10),
      license: LICENSE,
      provenance: {
        source_repo: `https://github.com/relaton/relaton-data-${flavor}`,
        generation_inputs: { count: gen.records, latest_indexed_at: gen.latest },
        export_date: new Date().toISOString(),
        tool: TOOL,
      },
      url: `${opts.baseUrl}/${key}`,
    };
    await bucket.put(manifestKey(flavor), JSON.stringify(manifest, null, 2), {
      httpMetadata: { contentType: "application/json" },
    });
    return { flavor, generation: gen.generation, records, sha256: sha, bytes, skipped: false };
  } catch (e) {
    await upload.abort();
    throw e;
  }
}



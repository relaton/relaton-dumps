// relaton-dumps#1: versioned, SHARDED NDJSON snapshots per flavor.
//
// A flavor above the D1 burst-window scale cannot export in one request
// (iso/200k records trips "overloaded within a 10 second window" even
// paced — proven 2026-11-28), so the export is cursor-resumable: each
// `POST /admin/export/{flavor}` invocation reads a bounded slice and
// writes ONE shard object (single PUT, own sha256), returning the next
// cursor; the zero-row invocation finalizes the manifest. The fleet
// workflow loops on next_cursor until done.
//
// Immutability: a shard object is written once and never rewritten; a
// generation's manifest exists only after its last shard, so consumers
// can never see a partial generation as complete.
export interface Generation {
  flavor: string;
  generation: string;
  records: number;
  latest: string | null;
}

export interface ShardInfo {
  key: string;
  records: number;
  bytes: number;
  sha256: string;
}

export interface SnapshotManifest {
  flavor: string;
  generation: string;
  records: number;
  bytes: number;
  integrity: "per-shard";
  shards: ShardInfo[];
  created: string;
  license: string;
  provenance: {
    source_repo: string;
    generation_inputs: { count: number; latest_indexed_at: string | null };
    export_date: string;
    tool: string;
  };
}

export interface SliceResult {
  flavor: string;
  generation: string;
  shard: ShardInfo | null;
  next_cursor: string | null;
  records: number;
  done: boolean;
  skipped: boolean;
}

const PAGE = 500;
const SLICE_RECORDS = 10_000;
const LICENSE = "CC-BY-4.0";
export const TOOL = "relaton-dumps/0.2.0";

export async function sha256Hex(data: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", data as BufferSource);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function manifestKeyOf(flavor: string, generation: string): string {
  return `snapshots/${flavor}/${generation}/manifest.json`;
}

// The stable flavor → generation pointer the public routes resolve
// through; written at finalize.
export async function writePointer(bucket: R2Bucket, flavor: string, generation: string): Promise<void> {
  await bucket.put(
    `snapshots/${flavor}/latest.json`,
    JSON.stringify({ flavor, generation }),
    { httpMetadata: { contentType: "application/json" } },
  );
}

export async function pointerOf(bucket: R2Bucket, flavor: string): Promise<string | null> {
  const obj = await bucket.get(`snapshots/${flavor}/latest.json`);
  if (!obj) return null;
  const p = (await obj.json()) as { generation?: string };
  return p.generation ?? null;
}

function shardPrefix(flavor: string, generation: string): string {
  return `snapshots/${flavor}/${generation}/shard-`;
}

function shardKey(flavor: string, generation: string, no: number): string {
  return `${shardPrefix(flavor, generation)}${String(no).padStart(4, "0")}.jsonl`;
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

export async function manifestOf(bucket: R2Bucket, flavor: string, generation: string): Promise<SnapshotManifest | null> {
  const obj = await bucket.get(manifestKeyOf(flavor, generation));
  return obj ? ((await obj.json()) as SnapshotManifest) : null;
}

// One bounded slice of the flavor's export: read up to SLICE_RECORDS
// records from the cursor, PUT them as one shard, answer the next cursor.
export async function exportSlice(
  db: D1Database,
  bucket: R2Bucket,
  flavor: string,
  opts: { cursor?: string | null; maxRecords?: number },
): Promise<SliceResult> {
  const gen = await generationOf(db, flavor);
  if (gen.records === 0) throw new Error(`flavor ${flavor} has no records in the dataset`);

  if (await manifestOf(bucket, flavor, gen.generation)) {
    const manifest = await manifestOf(bucket, flavor, gen.generation);
    return {
      flavor,
      generation: gen.generation,
      shard: null,
      next_cursor: null,
      records: manifest?.records ?? 0,
      done: true,
      skipped: true,
    };
  }

  const existing = await bucket.list({ prefix: shardPrefix(flavor, gen.generation) });
  const shardNo = existing.objects.length;
  let lastKey: string | null = opts.cursor ?? null;
  const max = opts.maxRecords ?? SLICE_RECORDS;

  const lines: string[] = [];
  let records = 0;
  for (;;) {
    const stmt = lastKey
      ? db.prepare("SELECT key, flavor, file, data, indexed_at FROM documents_v2 WHERE flavor = ?1 AND key > ?2 ORDER BY key LIMIT 500").bind(flavor, lastKey)
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
      lines.push(JSON.stringify({ key: r.key, flavor: r.flavor, file: r.file, indexed_at: r.indexed_at, record }) + "\n");
      records += 1;
      if (records >= max) break;
    }
    lastKey = results[results.length - 1]!.key;
    // D1 throttles bursts ("overloaded within a 10 second window") —
    // pace the page reads so slices stay inside the window.
    await new Promise((r) => setTimeout(r, 100));
    if (records >= max) break;
  }

  const key = shardKey(flavor, gen.generation, shardNo);
  const body = new TextEncoder().encode(lines.join(""));
  const sha = await sha256Hex(body);
  await bucket.put(key, body as BufferSource, {
    httpMetadata: { contentType: "application/x-ndjson" },
    customMetadata: {
      flavor,
      generation: gen.generation,
      license: LICENSE,
      "export-tool": TOOL,
      records: String(records),
      sha256: sha,
    },
  });

  const more = records >= max;
  return {
    flavor,
    generation: gen.generation,
    shard: { key, records, bytes: body.byteLength, sha256: sha },
    next_cursor: more ? lastKey : null,
    records,
    done: !more,
    skipped: false,
  };
}

// Finalize: compose the manifest from the shards' custom metadata. Each
// shard verifies individually (integrity: per-shard); WebCrypto digests
// cannot resume across isolates, so there is deliberately no whole-file
// sha256.
export async function finalizeForGen(
  bucket: R2Bucket,
  flavor: string,
  gen: Generation,
  opts: { baseUrl: string },
): Promise<SnapshotManifest> {
  const listed = await bucket.list({ prefix: shardPrefix(flavor, gen.generation), include: ["customMetadata"] });
  const shards: ShardInfo[] = [];
  let records = 0;
  let bytes = 0;
  for (const obj of listed.objects) {
    const shardRecords = Number(obj.customMetadata?.records ?? 0);
    const shaMd = obj.customMetadata?.sha256 ?? "";
    if (!shardRecords || !shaMd) throw new Error(`shard ${obj.key} lacks its checksum metadata`);
    shards.push({ key: obj.key, records: shardRecords, bytes: obj.size, sha256: shaMd });
    records += shardRecords;
    bytes += obj.size;
  }
  const manifest: SnapshotManifest = {
    flavor,
    generation: gen.generation,
    records,
    bytes,
    integrity: "per-shard",
    shards,
    created: new Date().toISOString().slice(0, 10),
    license: LICENSE,
    provenance: {
      source_repo: `https://github.com/relaton/relaton-data-${flavor}`,
      generation_inputs: { count: gen.records, latest_indexed_at: gen.latest },
      export_date: new Date().toISOString(),
      tool: TOOL,
    },
  };
  await bucket.put(manifestKeyOf(flavor, gen.generation), JSON.stringify(manifest, null, 2), {
    httpMetadata: { contentType: "application/json" },
  });
  await writePointer(bucket, flavor, gen.generation);
  void opts;
  return manifest;
}

// The stable flavor → generation pointer the public routes resolve
// through; written at finalize.


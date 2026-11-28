import { beforeAll, describe, expect, it } from "vitest";
import { SELF, env } from "cloudflare:test";

// relaton-dumps#1: versioned SHARDED NDJSON snapshots. The D1 schema
// mirrors the API dataset's tables this worker reads (documents_v2 for
// the records, documents_search for the flavor list).

const SCHEMA = `
CREATE TABLE documents_v2 (
  key TEXT PRIMARY KEY,
  flavor TEXT NOT NULL,
  file TEXT NOT NULL,
  data TEXT NOT NULL,
  indexed_at TEXT NOT NULL,
  UNIQUE (flavor, file)
);
CREATE TABLE documents_search (
  key TEXT PRIMARY KEY,
  flavor TEXT NOT NULL,
  docid TEXT NOT NULL,
  year INTEGER,
  norm TEXT,
  undated_norm TEXT,
  allparts_norm TEXT,
  publisher TEXT,
  number TEXT
);
CREATE INDEX idx_ds_flavor ON documents_search (flavor);
`;

const RECORD = (key: string, flavor: string, title: string) =>
  JSON.stringify({ key, flavor, docidentifier: [{ content: key, type: flavor.toUpperCase(), primary: true }], title: [{ content: title, type: "main" }] });

const { DB } = env as unknown as { DB: D1Database };

async function seed() {
  const base: [string, string, string][] = [
    ["RFC 1", "rfc", "Host Software"],
    ["RFC 8446", "rfc", "TLS 1.3"],
    ["RFC 9000", "rfc", "QUIC"],
  ];
  const rows = base.map(([key, flavor, title]) => [key, flavor, `${flavor}-${key}.yaml`, RECORD(key, flavor, title), "2026-11-28T00:00:00Z"] as [string, string, string, string, string]);
  for (const [key, flavor, file, data, at] of rows) {
    await DB.prepare("INSERT INTO documents_v2 (key, flavor, file, data, indexed_at) VALUES (?1, ?2, ?3, ?4, ?5)")
      .bind(key, flavor, file, data, at)
      .run();
    await DB.prepare("INSERT INTO documents_search (key, flavor, docid, norm) VALUES (?1, ?2, ?3, ?4)")
      .bind(key, flavor, key, key.toLowerCase())
      .run();
  }
}

beforeAll(async () => {
  for (const stmt of SCHEMA.split(";").map((s) => s.trim()).filter(Boolean)) {
    await DB.prepare(stmt).run();
  }
  await seed();
});

function auth(): Record<string, string> {
  return { authorization: "Bearer test-admin", "content-type": "application/json" };
}

describe("GET /healthz", () => {
  it("reports ok", async () => {
    const res = await SELF.fetch("https://data.relaton.org/healthz");
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok", service: "relaton-dumps" });
  });
});

describe("POST /admin/export/:flavor (sharded, cursor-resumable)", () => {
  it("requires the admin bearer token", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/rfc", { method: "POST" });
    expect(res.status).toBe(403);
  });

  it("exports in bounded slices: cursor loop, per-shard checksums, manifest", async () => {
    // max_records=2 forces 3 records into 2 slices.
    const s1 = await SELF.fetch("https://data.relaton.org/admin/export/rfc", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({ max_records: 2 }),
    });
    if (s1.status !== 200) expect(`s1 ### ${s1.status} ${await s1.text()}`).toBe("");
    const first = (await s1.json()) as { done: boolean; next_cursor: string | null; records: number; shard: { key: string; sha256: string } | null };
    expect(first.done).toBe(false);
    expect(first.records).toBe(2);
    expect(first.next_cursor).toBeTruthy();
    expect(first.shard?.sha256).toMatch(/^[0-9a-f]{64}$/);

    const s2 = await SELF.fetch("https://data.relaton.org/admin/export/rfc", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({ cursor: first.next_cursor, max_records: 2 }),
    });
    if (s2.status !== 200) expect(`s2 ### ${s2.status} ${await s2.text()}`).toBe("");
    const second = (await s2.json()) as { done: boolean; records: number; manifest?: { records: number; shards: { records: number; sha256: string }[]; integrity: string; license: string; provenance: { source_repo: string; tool: string } } };
    expect(second.done).toBe(true);
    expect(second.records).toBe(1);

    // The manifest composes the shards: per-shard integrity, provenance.
    expect(second.manifest).toBeTruthy();
    const manifest = second.manifest!;
    expect(manifest.integrity).toBe("per-shard");
    expect(manifest.license).toBe("CC-BY-4.0");
    expect(manifest.records).toBe(3);
    expect(manifest.shards).toHaveLength(2);
    expect(manifest.shards[0]!.sha256).toBe(first.shard!.sha256);
    expect(manifest.provenance.source_repo).toBe("https://github.com/relaton/relaton-data-rfc");
    expect(manifest.provenance.tool).toContain("relaton-dumps");
  });

  it("serves shards immutable and resolves the flavor pointer", async () => {
    const man = await SELF.fetch("https://data.relaton.org/v1/rfc/manifest.json");
    expect(man.status).toBe(200);
    const manifest = (await man.json()) as { generation: string; shards: { key: string }[] };

    const latest = await SELF.fetch("https://data.relaton.org/v1/rfc/latest");
    expect(latest.status).toBe(200);
    const pointers = (await latest.json()) as { generation: string; shards: { url: string; sha256: string }[] };
    expect(pointers.generation).toBe(manifest.generation);
    expect(pointers.shards).toHaveLength(2);

    const shardUrl = new URL(pointers.shards[0]!.url);
    const shard = await SELF.fetch(`https://data.relaton.org${shardUrl.pathname}`);
    expect(shard.status).toBe(200);
    expect(shard.headers.get("cache-control")).toContain("immutable");
    expect(shard.headers.get("content-type")).toBe("application/x-ndjson");
    const lines = (await shard.text()).trim().split("\n");
    expect(lines.length).toBe(2);
    const first = JSON.parse(lines[0]!) as { key: string; record: { title: unknown[] } };
    expect(first.key).toBe("RFC 1");
    expect(first.record.title[0]).toMatchObject({ content: "Host Software" });
  });

  it("skips regeneration of an unchanged generation", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/rfc", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { skipped: boolean; done: boolean };
    expect(body.skipped).toBe(true);
    expect(body.done).toBe(true);
  });

  it("rejects an empty flavor with a clear error", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/nosuch", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(500);
    expect(((await res.json()) as { error: string }).error).toContain("no records");
  });
});

describe("GET /manifest.json", () => {
  it("lists snapshots and pending flavors", async () => {
    const res = await SELF.fetch("https://data.relaton.org/manifest.json");
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      license: string;
      snapshots: { flavor: string; shards: number }[];
      pending: string[];
    };
    expect(body.license).toBe("CC-BY-4.0");
    expect(body.snapshots.map((s) => s.flavor)).toEqual(["rfc"]);
    expect(body.snapshots[0]!.shards).toBe(2);
    expect(body.pending).toEqual([]);
  });
});

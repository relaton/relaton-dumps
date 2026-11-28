import { beforeAll, describe, expect, it } from "vitest";
import { SELF, env } from "cloudflare:test";

// The pool's ProvidedEnv doesn't know the binding names; the worker's own
// Env does. (Same cast pattern the api-js suite uses for its secrets.)
const { DB } = env as unknown as { DB: D1Database };

// relaton-dumps#1: versioned NDJSON snapshots. The D1 schema mirrors the
// API dataset's tables this worker reads (documents_v2 for the records,
// documents_search for the flavor list) — the same CREATE statements,
// trimmed to the columns the dump reads.

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
  return { authorization: "Bearer test-admin" };
}

describe("GET /healthz", () => {
  it("reports ok", async () => {
    const res = await SELF.fetch("https://data.relaton.org/healthz");
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok", service: "relaton-dumps" });
  });
});

describe("POST /admin/export/:flavor", () => {
  it("requires the admin bearer token", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/rfc", { method: "POST" });
    expect(res.status).toBe(403);
  });

  it("writes a versioned NDJSON snapshot with manifest and provenance", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/rfc", {
      method: "POST",
      headers: auth(),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { generation: string; records: number; sha256: string; skipped: boolean };
    expect(body.skipped).toBe(false);
    expect(body.records).toBe(3);
    expect(body.sha256).toMatch(/^[0-9a-f]{64}$/);

    // The manifest carries provenance + licensing.
    const man = await SELF.fetch(`https://data.relaton.org/v1/rfc/manifest.json`);
    expect(man.status).toBe(200);
    const manifest = (await man.json()) as {
      license: string;
      sha256: string;
      provenance: { source_repo: string; export_date: string; tool: string };
    };
    expect(manifest.license).toBe("CC-BY-4.0");
    expect(manifest.sha256).toBe(body.sha256);
    expect(manifest.provenance.source_repo).toBe("https://github.com/relaton/relaton-data-rfc");
    expect(manifest.provenance.tool).toContain("relaton-dumps");
    expect(manifest.provenance.export_date).toBeTruthy();
  });

  it("serves the snapshot immutable with a latest redirect", async () => {
    const gen = await SELF.fetch("https://data.relaton.org/admin/generation/rfc", { headers: auth() });
    const { generation } = (await gen.json()) as { generation: string };

    const latest = await SELF.fetch("https://data.relaton.org/v1/rfc/latest", { redirect: "manual" });
    expect(latest.status).toBe(302);
    expect(latest.headers.get("location")).toContain(`/snapshots/rfc/${generation}/records.ndjson`);

    const snap = await SELF.fetch(`https://data.relaton.org/snapshots/rfc/${generation}/records.ndjson`);
    expect(snap.status).toBe(200);
    expect(snap.headers.get("cache-control")).toContain("immutable");
    const lines = (await snap.text()).trim().split("\n");
    expect(lines).toHaveLength(3);
    const first = JSON.parse(lines[0]!) as { key: string; record: { title: unknown[] } };
    expect(first.key).toBe("RFC 1");
    expect(first.record.title[0]).toMatchObject({ content: "Host Software" });
  });

  it("skips regeneration of an unchanged generation", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/rfc", {
      method: "POST",
      headers: auth(),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { skipped: boolean; generation: string };
    expect(body.skipped).toBe(true);
  });

  it("rejects an empty flavor with a clear error", async () => {
    const res = await SELF.fetch("https://data.relaton.org/admin/export/nosuch", {
      method: "POST",
      headers: auth(),
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
      snapshots: { flavor: string }[];
      pending: string[];
    };
    expect(body.license).toBe("CC-BY-4.0");
    expect(body.snapshots.map((s) => s.flavor)).toEqual(["rfc"]);
    expect(body.pending).toEqual([]);
  });
});

import { exportFlavor, generationOf, manifestKey, snapshotKey, TOOL, type SnapshotManifest } from "./export";
import type { Env } from "./env";

const SERVICE = "relaton-dumps";

function authorized(request: Request, env: Env): boolean {
  const token = (request.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "");
  return token !== "" && token === env.ADMIN_TOKEN;
}

function baseUrl(request: Request): string {
  const url = new URL(request.url);
  return `${url.protocol}//${url.host}`;
}

export default {
  async fetch(request, env, _ctx): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (pathname === "/healthz") {
      return Response.json({ status: "ok", service: SERVICE });
    }

    // The flavor list from the indexed search table (DISTINCT on an
    // indexed column); the records themselves come from documents_v2.
    const flavorList = async (): Promise<string[]> => {
      const { results } = await env.DB.prepare("SELECT DISTINCT flavor FROM documents_search ORDER BY flavor").all<{ flavor: string }>();
      return (results ?? []).map((r) => r.flavor);
    };

    if (pathname === "/" || pathname === "/manifest.json") {
      const flavors = await flavorList();
      const entries = await Promise.all(
        flavors.map(async (f) => {
          const obj = await env.BUCKET.get(manifestKey(f));
          return obj ? ((await obj.json()) as SnapshotManifest) : null;
        }),
      );
      const snapshots = entries.filter((e): e is SnapshotManifest => e !== null);
      return Response.json({
        service: SERVICE,
        tool: TOOL,
        license: "CC-BY-4.0",
        flavors: flavors.length,
        snapshots: snapshots.map((m) => ({
          flavor: m.flavor,
          generation: m.generation,
          records: m.records,
          sha256: m.sha256,
          bytes: m.bytes,
          created: m.created,
          license: m.license,
          manifest_url: `${baseUrl(request)}/v1/${m.flavor}/manifest.json`,
          snapshot_url: `${baseUrl(request)}/snapshots/${m.flavor}/${m.generation}/records.ndjson`,
          latest_url: `${baseUrl(request)}/v1/${m.flavor}/latest`,
        })),
        pending: flavors.filter((f) => !snapshots.some((s) => s.flavor === f)),
      });
    }

    const flavorMatch = pathname.match(/^\/v1\/([a-z0-9_-]+)\/(latest|manifest\.json)$/);
    if (flavorMatch) {
      const [flavor, what] = [flavorMatch[1]!, flavorMatch[2]!];
      const obj = await env.BUCKET.get(manifestKey(flavor));
      if (!obj) return Response.json({ error: `no snapshot for ${flavor}` }, { status: 404 });
      const manifest = (await obj.json()) as SnapshotManifest;
      if (what === "manifest.json") return Response.json(manifest);
      return Response.redirect(`${baseUrl(request)}/${snapshotKey(flavor, manifest.generation)}`, 302);
    }

    const snapshotMatch = pathname.match(/^\/snapshots\/([a-z0-9_-]+)\/([0-9a-f]{16})\/records\.ndjson$/);
    if (snapshotMatch) {
      const [flavor, generation] = [snapshotMatch[1]!, snapshotMatch[2]!];
      const obj = await env.BUCKET.get(snapshotKey(flavor, generation));
      if (!obj) return new Response("not found\n", { status: 404 });
      // Snapshots are immutable: published generations cache forever.
      return new Response(obj.body, {
        headers: {
          "content-type": "application/x-ndjson",
          "cache-control": "public, max-age=31536000, immutable",
          etag: `"${generation}"`,
        },
      });
    }

    const exportMatch = pathname.match(/^\/admin\/export\/([a-z0-9_-]+)$/);
    if (exportMatch && request.method === "POST") {
      if (!authorized(request, env)) return new Response("Forbidden\n", { status: 403 });
      const flavor = exportMatch[1]!;
      try {
        const result = await exportFlavor(env.DB, env.BUCKET, flavor, { baseUrl: baseUrl(request) });
        return Response.json(result);
      } catch (e) {
        return Response.json({ error: e instanceof Error ? e.message : String(e) }, { status: 500 });
      }
    }

    const genMatch = pathname.match(/^\/admin\/generation\/([a-z0-9_-]+)$/);
    if (genMatch && request.method === "GET") {
      if (!authorized(request, env)) return new Response("Forbidden\n", { status: 403 });
      return Response.json(await generationOf(env.DB, genMatch[1]!));
    }

    return new Response("not found\n", { status: 404 });
  },
} satisfies ExportedHandler<Env>;

import worker from "../src/index";

describe("health", () => {
  it("reports ok", async () => {
    const res = await worker.fetch(new Request("https://data.relaton.org/healthz"));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok", service: "relaton-dumps" });
  });
});

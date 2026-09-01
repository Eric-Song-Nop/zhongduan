import { describe, expect, it } from "vitest";
import e0Worker, {
  installE0MiniflareMultipartEtagShim,
  isE0LoopbackRequest,
} from "../src/worker/e0-local";

describe("E0 local Worker entry", () => {
  it("accepts only loopback request hosts", () => {
    expect(isE0LoopbackRequest(new Request("http://127.0.0.1/"))).toBe(true);
    expect(isE0LoopbackRequest(new Request("http://localhost/"))).toBe(true);
    expect(isE0LoopbackRequest(new Request("http://[::1]/"))).toBe(true);
    expect(isE0LoopbackRequest(new Request("https://terminal.example.test/"))).toBe(false);
  });

  it("fails closed before a non-loopback request reaches production dispatch", async () => {
    const response = await e0Worker.fetch(
      new Request("https://terminal.example.test/api/v1/sessions") as never,
      {} as never,
    );
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "not-found" });
  });

  it("scopes the Miniflare multipart ETag shim to one E0 coordinator instance", () => {
    const prototype = {
      snapshotPartEtagMatches: () => false,
    };
    const coordinator = Object.create(prototype) as object;
    const unrelated = Object.create(prototype) as {
      snapshotPartEtagMatches(): boolean;
    };

    installE0MiniflareMultipartEtagShim(coordinator);

    expect(
      Reflect.get(coordinator, "snapshotPartEtagMatches").call(coordinator, "opaque", "md5"),
    ).toBe(true);
    expect(unrelated.snapshotPartEtagMatches()).toBe(false);
    expect(Object.hasOwn(prototype, "snapshotPartEtagMatches")).toBe(true);
  });
});

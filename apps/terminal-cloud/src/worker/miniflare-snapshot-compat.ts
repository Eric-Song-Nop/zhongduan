const MINIFLARE_MULTIPART_PART_ETAG = /^[A-Za-z0-9_-]{171}$/u;

export function matchesMiniflareMultipartPartEtag(etag: string): boolean {
  return MINIFLARE_MULTIPART_PART_ETAG.test(etag);
}

export function installMiniflareMultipartEtagCompatibility(owner: object): void {
  const coordinator = Reflect.get(owner, "snapshotUploads") as unknown;
  if (
    typeof coordinator !== "object" ||
    coordinator === null ||
    typeof Reflect.get(coordinator, "snapshotPartEtagMatches") !== "function"
  ) {
    throw new TypeError("snapshot upload coordinator compatibility seam is unavailable");
  }
  if (
    !Reflect.set(coordinator, "snapshotPartEtagMatches", (partEtag: string) =>
      matchesMiniflareMultipartPartEtag(partEtag),
    )
  ) {
    throw new TypeError("snapshot upload coordinator compatibility seam is not writable");
  }
}

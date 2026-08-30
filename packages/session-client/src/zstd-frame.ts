import { Decompress } from "fzstd";

const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd] as const;
const MAX_ZSTD_BLOCK_BYTES = 128 * 1024;

function requireBytes(bytes: Uint8Array, offset: number, length: number): void {
  if (offset + length > bytes.byteLength) {
    throw new Error("zstd frame is truncated");
  }
}

function readLittleEndian(bytes: Uint8Array, offset: number, length: number): bigint {
  requireBytes(bytes, offset, length);
  let value = 0n;
  for (let index = 0; index < length; index += 1) {
    value |= BigInt(bytes[offset + index] ?? 0) << BigInt(index * 8);
  }
  return value;
}

function contentSizeFieldBytes(flag: number, singleSegment: boolean): number {
  if (flag === 0) return singleSegment ? 1 : 0;
  if (flag === 1) return 2;
  return flag === 2 ? 4 : 8;
}

function dictionaryIdFieldBytes(flag: number): number {
  return flag === 3 ? 4 : flag;
}

/** Validate one complete standard frame before fzstd can allocate from its header. */
function validateZstdFrameEnvelope(
  bytes: Uint8Array,
  expectedLength: number,
  maximumLength: number,
): void {
  requireBytes(bytes, 0, 5);
  if (ZSTD_MAGIC.some((byte, index) => bytes[index] !== byte)) {
    throw new Error("snapshot is not a standard zstd frame");
  }

  let offset = 4;
  const descriptor = bytes[offset] ?? 0;
  offset += 1;
  if ((descriptor & 0x08) !== 0) {
    throw new Error("zstd frame descriptor uses its reserved bit");
  }

  const contentSizeFlag = descriptor >>> 6;
  const singleSegment = (descriptor & 0x20) !== 0;
  const hasChecksum = (descriptor & 0x04) !== 0;
  const dictionaryIdBytes = dictionaryIdFieldBytes(descriptor & 0x03);

  let windowSize: number | undefined;
  if (!singleSegment) {
    requireBytes(bytes, offset, 1);
    const windowDescriptor = bytes[offset] ?? 0;
    offset += 1;
    const windowBase = 2 ** (10 + (windowDescriptor >>> 3));
    windowSize = windowBase + (windowBase / 8) * (windowDescriptor & 0x07);
    if (windowSize > maximumLength) {
      throw new Error("zstd frame window exceeds configured limit");
    }
  }

  const dictionaryId = readLittleEndian(bytes, offset, dictionaryIdBytes);
  offset += dictionaryIdBytes;
  if (dictionaryId !== 0n) {
    throw new Error("zstd dictionaries are not supported");
  }

  const contentSizeBytes = contentSizeFieldBytes(contentSizeFlag, singleSegment);
  let contentSize: bigint | undefined;
  if (contentSizeBytes !== 0) {
    contentSize = readLittleEndian(bytes, offset, contentSizeBytes);
    offset += contentSizeBytes;
    if (contentSizeBytes === 2) contentSize += 256n;
    if (contentSize > BigInt(maximumLength)) {
      throw new Error("zstd frame content size exceeds configured limit");
    }
    if (contentSize !== BigInt(expectedLength)) {
      throw new Error("zstd frame content size does not match restore source");
    }
  }

  if (singleSegment) {
    if (contentSize === undefined) {
      throw new Error("single-segment zstd frame has no content size");
    }
    windowSize = Number(contentSize);
  }
  if (windowSize === undefined) {
    throw new Error("zstd frame has no bounded window");
  }
  const maximumBlockSize = Math.min(windowSize, MAX_ZSTD_BLOCK_BYTES);

  let lastBlock = false;
  while (!lastBlock) {
    const header = Number(readLittleEndian(bytes, offset, 3));
    offset += 3;
    lastBlock = (header & 0x01) !== 0;
    const blockType = (header >>> 1) & 0x03;
    const blockSize = header >>> 3;
    if (blockType === 3) {
      throw new Error("zstd frame uses a reserved block type");
    }
    if (blockSize > maximumBlockSize) {
      throw new Error("zstd frame block exceeds its bounded window");
    }
    const encodedBlockSize = blockType === 1 ? 1 : blockSize;
    requireBytes(bytes, offset, encodedBlockSize);
    offset += encodedBlockSize;
  }

  if (hasChecksum) {
    requireBytes(bytes, offset, 4);
    offset += 4;
  }
  if (offset !== bytes.byteLength) {
    throw new Error("zstd frame has trailing or concatenated data");
  }
}

export function decompressBoundedZstdFrame(
  bytes: Uint8Array,
  expectedLength: number,
  maximumLength: number,
): Uint8Array {
  if (
    !Number.isSafeInteger(expectedLength) ||
    expectedLength < 0 ||
    expectedLength > maximumLength
  ) {
    throw new Error("zstd expected length exceeds configured limit");
  }
  validateZstdFrameEnvelope(bytes, expectedLength, maximumLength);

  const output = new Uint8Array(expectedLength);
  let outputOffset = 0;
  let finished = false;
  const decompressor = new Decompress((chunk, final) => {
    if (finished) throw new Error("zstd decoder emitted data after completion");
    if (
      chunk.byteLength > expectedLength - outputOffset ||
      chunk.byteLength > maximumLength - outputOffset
    ) {
      throw new Error("snapshot decompressed length exceeds configured limit");
    }
    output.set(chunk, outputOffset);
    outputOffset += chunk.byteLength;
    finished = final === true;
  });
  decompressor.push(bytes, true);

  if (!finished) throw new Error("zstd decoder did not finish the frame");
  if (outputOffset !== expectedLength) {
    throw new Error("snapshot decompressed length does not match restore source");
  }
  return output;
}

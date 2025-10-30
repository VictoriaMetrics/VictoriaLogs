export function isUint8Array(value: unknown): value is Uint8Array {
  return value instanceof Uint8Array;
}

export function encodeRFC5987(value: string): string {
  // RFC5987: additionally encode ! ' ( ) *
  return encodeURIComponent(value).replace(
    /[!'()*]/g,
    c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

export const errors = {
  INVALID: [
    "Failed to seek: invalid position.",
    "InvalidStateError"
  ],
  GONE: [
    "A requested file or directory could not be found at the time the operation was processed.",
    "NotFoundError"
  ],
  MISMATCH: [
    "The supplied path exists but is not an entry of the requested type.",
    "TypeMismatchError"
  ],
  MOD_ERR: [
    "The object cannot be modified in this way.",
    "InvalidModificationError"
  ],
  SYNTAX: (m: string) => [
    `Failed to execute 'write' on 'UnderlyingSinkBase': invalid parameters. ${m}`,
    "SyntaxError"
  ],
  SECURITY: [
    "Access to the requested file was blocked for security reasons or due to excessive access attempts.",
    "SecurityError"
  ],
  DISALLOWED: [
    "The request is not allowed by the user agent or platform in the current context.",
    "NotAllowedError"
  ]
};

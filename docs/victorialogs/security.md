---
weight: 11
menu:
  docs:
    identifier: victorialogs-security
    parent: victorialogs
    weight: 11
title: Security
tags:
  - logs
---

VictoriaLogs implements multiple security layers to help protect data confidentiality and reduce unauthorized access.

It supports [Basic Auth](https://docs.victoriametrics.com/victorialogs/security/#basic-auth)
and [mTLS](https://docs.victoriametrics.com/victorialogs/security/#mtls) for authentication and access control.
Traffic between clients and servers can be encrypted using [TLS/SSL](https://docs.victoriametrics.com/victorialogs/security/#tlsssl).

Recommended practices:

* Deploy all VictoriaLogs components inside a private network, avoid direct Internet exposure.
* Use vmauth as an authorization proxy and load balancer in front of vlinsert and vlselect for authentication and authorization of data ingestion and querying requests.
  See [Security and Load Balancing](https://docs.victoriametrics.com/victorialogs/security-and-lb/) for configuration details.
  For TLS termination proxy setup, see [TLS termination proxy docs](https://docs.victoriametrics.com/victoriametrics/vmauth/#tls-termination-proxy).
* Protect components exposed to external clients with Basic Auth + TLS or mTLS.
* Disable read API on vlinsert with the `-select.disable` command-line flag.
* Disable write API on vlselect with the `-insert.disable` command-line flag.

## Basic Auth

It is recommended to enable Basic Auth when VictoriaLogs is deployed in untrusted or unsecured networks.
For a full overview of security best practices, see the recommendations listed above.

Basic Auth validates the `Authorization: Basic <base64(user:password)>` HTTP header.
If the username and password match the values configured in the auth layer (for example, VictoriaLogs or an upstream authorization proxy), the request is allowed.

Basic Auth encodes credentials using Base64 encoding, but does not encrypt the transmitted data.
Always use Basic Auth exclusively over [secure TLS/SSL connections](https://docs.victoriametrics.com/victorialogs/security/#tlsssl).

Production Security Recommendations:

- TLS encryption: never use Basic Auth over unencrypted HTTP connections.
- Consider mTLS: implement mutual TLS for certificate-based authentication instead of password-based auth.
- Regular credential rotation: change passwords periodically and use strong, unique passwords.

### Quick start

To quickly start VictoriaLogs with Basic Auth, run the following command:

```sh
./victoria-logs -httpAuth.username vlstorage -httpAuth.password secret
```

This will start the VictoriaLogs server with Basic authentication on the default port (`9428`).

To test the configuration, you can use `curl`:

```sh
curl -u vlstorage:secret http://localhost:9428/metrics
```

This HTTP example is intended for local testing on `localhost`.

For cluster mode, you may configure both client authentication (users connecting to your service)
and internal authentication (communication between VictoriaLogs components), depending on your network trust boundary and security requirements:

```sh
./victoria-logs -httpListenAddr :9429 \
    -httpAuth.username vlinsert -httpAuth.password top-secret \
    -storageNode localhost:9428 \
    -storageNode.username vlstorage -storageNode.password secret
```

This configuration:
- Starts a VictoriaLogs server on port `9429` that functions as both vlinsert and vlselect.
- Accepts requests to this node only from user `vlinsert` with password `top-secret`.
- Connects to vlstorage at `localhost:9428` using credentials `vlstorage:secret`.

**Security Warning:** Passing passwords directly in command-line arguments is not recommended for production,
as they may appear in process listings (e.g., `ps aux`)
or debug endpoints (e.g., `http://victoria-logs:9428/debug/pprof/cmdline?debug=1`).

Use one of the safer methods below:

* [Providing password from a file](https://docs.victoriametrics.com/victorialogs/security/#providing-password-from-a-file).
* [Providing password via environment variables](https://docs.victoriametrics.com/victorialogs/security/#providing-password-via-environment-variables).
* [Providing password via HTTP/HTTPS](https://docs.victoriametrics.com/victorialogs/security/#providing-password-via-httphttps).

After configuring Basic Auth, update your clients accordingly, such as
vmauth (see [these docs](https://docs.victoriametrics.com/victorialogs/security-and-lb/))
and VictoriaMetrics components (see [these docs](https://docs.victoriametrics.com/victoriametrics/sd_configs/#http-api-client-options)).

### Providing password from a file

To read passwords from a file instead of command-line arguments, use the `file://` prefix:

```sh
./victoria-logs -httpAuth.username vlstorage -httpAuth.password file:///absolute/path/to/file
# or relative path
./victoria-logs -httpAuth.username vlstorage -httpAuth.password file://./path/to/file
```

The value from the file is periodically reloaded, which allows changing the password without restarting the application.

**Note:** the `-httpAuth.username` command-line flag does not support reading values from a file.

For cluster deployments, you can also use dedicated file-based flags for storage node authentication:

```sh
./victoria-logs -httpListenAddr :9429 \
    -httpAuth.username vlinsert -httpAuth.password file://./path/to/file \
    -storageNode localhost:9428 \
    -storageNode.usernameFile /path/to/username/file -storageNode.passwordFile /path/to/password/file
```

Example with vlagent connecting to vlinsert:

```sh
./vlagent -httpListenAddr :9430 \
    -httpAuth.username vlagent -httpAuth.password file://./path/to/file \
    -remoteWrite.url http://localhost:9429/insert/native \
    -remoteWrite.basicAuth.username vlinsert -remoteWrite.basicAuth.passwordFile ./path/to/file
```

To test that vlagent is properly configured with Basic Auth, you can send a test log entry
(use the same password value as configured in `./path/to/file` for `-httpAuth.password`):

```sh
curl -u "vlagent:$(cat ./path/to/file)" http://localhost:9430/insert/jsonline -H 'Content-Type: application/json' \
    -d '{"_msg":"Hello, VictoriaLogs!"}'
```

### Providing password via environment variables

To use environment variables for configuration, enable the feature with `-envflag.enable`.
Optionally set a custom prefix with `-envflag.prefix`:

```sh
export VL_httpAuth_username=vlstorage
export VL_httpAuth_password=secret

./victoria-logs -envflag.enable -envflag.prefix VL_
```

Flag names are converted to environment variables by replacing dots with underscores.
For example, `-httpAuth.password` becomes `VL_httpAuth_password`.

Environment variables are read only at startup. You must restart VictoriaLogs to pick up changes.

Cluster configuration example:

```sh
export VL_httpAuth_username=vlinsert
export VL_httpAuth_password=top-secret
export VL_storageNode_username=vlstorage
export VL_storageNode_password=secret

./victoria-logs -envflag.enable -envflag.prefix VL_ \
    -httpListenAddr :9429 -storageNode localhost:9428
```

vlagent configuration example:

```sh
export VL_httpAuth_username=vlagent
export VL_httpAuth_password=top-top-secret
export VL_remoteWrite_basicAuth_username=vlinsert
export VL_remoteWrite_basicAuth_password=top-secret

./vlagent -envflag.enable -envflag.prefix VL_ \
    -httpListenAddr :9430 \
    -remoteWrite.url http://localhost:9429/insert/native
```

To test that vlagent is properly configured with Basic Auth, you can send a test log entry:

```sh
curl -u vlagent:top-top-secret http://localhost:9430/insert/jsonline -H 'Content-Type: application/json' \
    -d '{"_msg":"Hello, VictoriaLogs!"}'
```

See also all supported command-line flags
for VictoriaLogs (see [these docs](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags))
and vlagent (see [these docs](https://docs.victoriametrics.com/victorialogs/vlagent/#advanced-usage)).

### Providing password via HTTP/HTTPS

VictoriaLogs can retrieve passwords from HTTP/HTTPS endpoints for dynamic password management:

```sh
./victoria-logs -httpAuth.username foo \
    -httpAuth.password 'https://example.com/victoria-logs/password?example-arg=example-value'
```

The value from the URL is periodically reloaded, which allows changing the password without restarting the application.

**Note:** the `-httpAuth.username` command-line flag does not support reading values via HTTP/HTTPS.

If the HTTP request fails, VictoriaLogs continues using the last successfully retrieved password.
Errors are logged for troubleshooting.
If no password has ever been successfully retrieved and `-httpAuth.username` is set, requests protected by Basic Auth will fail with authentication errors.

**Note**: Ensure the URL endpoint is fast and reliable, because password retrieval from a slow or unstable endpoint can increase request latency during authentication checks.

## System endpoints

When Basic Auth or mTLS is enabled, the following system endpoints inherit the same authentication requirements by default.
These defaults can be overridden per endpoint with dedicated `*AuthKey` flags.

- [`/metrics`](https://docs.victoriametrics.com/victorialogs/metrics/) - monitoring endpoint for VictoriaMetrics, vmagent, and Prometheus.
  Override authentication using the `-metricsAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- `/flags` - debugging endpoint that shows all active command-line flags.
  Override authentication using the `-flagsAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
  **Note:** Passwords are hidden in the output for security reasons.
- [`/debug/pprof/*`](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/#profiling) - profiling endpoints for performance analysis (advanced users only).
  Override authentication using the `-pprofAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/log_new_streams`](https://docs.victoriametrics.com/victorialogs/#logging-new-streams) - enables logging new log streams during data ingestion.
  Override authentication using the `-logNewStreamsAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/force_flush`](https://docs.victoriametrics.com/victorialogs/#forced-flush) - forces immediate flushing of in-memory data to disk.
  Override authentication using the `-forceFlushAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/force_merge`](https://docs.victoriametrics.com/victorialogs/#forced-merge) - triggers manual data compaction and merge operations.
  Override authentication using the `-forceMergeAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/partition/*`](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle) - manages partition lifecycle operations including creation and deletion.
  Override authentication using the `-partitionManageAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).

Example of accessing the `/metrics` endpoint using an authentication key
(works when `-metricsAuthKey` is configured):

```sh
curl 'http://victoria-logs:9428/metrics?authKey=monitoring-secret-key'
```

## TLS/SSL

All VictoriaLogs components support TLS encryption for secure communication.
TLS is disabled by default but can be enabled on both server and client sides.

### Enabling TLS on the server

To enable TLS encryption, provide the server certificate and private key:

```sh
./victoria-logs -tls -tlsCertFile /path/to/victoria-logs.pem -tlsKeyFile /path/to/victoria-logs-key.pem
```

This starts VictoriaLogs with HTTPS encryption on the default port (9428).

### Connecting vlselect and vlinsert to vlstorage with TLS

To connect VictoriaLogs components to a TLS-enabled vlstorage server:

```sh
./victoria-logs -httpListenAddr :9429 \
    -storageNode localhost:9428 -storageNode.tls
```

By default, VictoriaLogs verifies server certificates using the system's trusted certificate store.
If using self-signed certificates, you have two options:
1. Install your CA certificate in the system's trusted certificate store.
1. Specify the CA certificate path manually using `-storageNode.tlsCAFile`.

For testing purposes only, you can disable certificate verification with `-storageNode.tlsInsecureSkipVerify`.

**Security Warning:** Disabling certificate verification eliminates
protection against man-in-the-middle attacks. Never use this in production.

### Connecting vlagent to VictoriaLogs with TLS

To send data over TLS, simply change the URL scheme from `http` to `https`:

```sh
./vlagent -httpListenAddr :9430 \
    -remoteWrite.url https://localhost:9428/insert/native
```

By default, vlagent verifies server certificates using the system's trusted certificate store.
If using self-signed certificates, you have two options:
1. Install your CA certificate in the system's trusted certificate store.
1. Specify the CA certificate path manually using `-remoteWrite.tlsCAFile`.

For testing purposes only, you can disable certificate verification with `-remoteWrite.tlsInsecureSkipVerify`.

**Security Warning:** Disabling certificate verification eliminates
protection against man-in-the-middle attacks. Never use this in production.

### Automatic issuing of TLS certificates

All the [VictoriaLogs Enterprise](https://docs.victoriametrics.com/victoriametrics/enterprise/) components support automatic issuing of TLS certificates
for public HTTPS server running at `-httpListenAddr` via [Let's Encrypt service](https://letsencrypt.org/).
The following command-line flags must be set in order to enable automatic issuing of TLS certificates:

- `-httpListenAddr` must be set for listening TCP port `443`. For example, `-httpListenAddr=:443`.
  This port must be accessible by the [Let's Encrypt service](https://letsencrypt.org/).
- `-tls` must be set in order to accept HTTPS requests at `-httpListenAddr`.
  Note that `-tlsCertFile` and `-tlsKeyFile` aren't needed when automatic TLS certificate issuing is enabled.
- `-tlsAutocertHosts` must be set to comma-separated list of hosts, which can be reached via `-httpListenAddr`.
  TLS certificates are automatically issued for these hosts.
- `-tlsAutocertEmail` must be set to contact email for the issued TLS certificates.
- `-tlsAutocertCacheDir` may be set to the directory path for persisting the issued TLS certificates between VictoriaLogs restarts.
  If this flag isn't set, then TLS certificates are re-issued on every restart.

Example of starting VictoriaLogs with automatic TLS certificate issuing:

```sh
./victoria-logs -httpListenAddr=:443 \
    -tls \
    -tlsAutocertHosts=victorialogs.example.com,logs.example.com \
    -tlsAutocertEmail=admin@example.com \
    -tlsAutocertCacheDir=/path/to/tls-cache \
    -licenseFile /path/to/license
```

#### The `no viable challenge type found` error

Let's Encrypt validates domain ownership by performing TLS-ALPN-01 challenges to verify you control the specified domains.
The Let's Encrypt service must be able to reach your VictoriaLogs instance over the public internet on port 443 to complete this validation.

Common troubleshooting steps:
1. Ensure your server is reachable from the internet on port 443.
1. Confirm that your domain names resolve to your server's public IP address.
1. Use external tools to verify HTTPS connectivity to port 443.

Example connectivity verification:

```sh
# Test domain resolution
nslookup victorialogs.example.com

# Test HTTPS connectivity from an external network (port 443)
curl -I https://victorialogs.example.com/
```

This functionality can be evaluated for free according to [these docs](https://docs.victoriametrics.com/victoriametrics/enterprise/).

## Individual Configuration

When connecting to multiple clients (storage nodes or remote write endpoints),
each connection can have its own configuration.

Configuration Rules:
- Single flag value: applied to all clients.
- Multiple flag values: each client gets its own value in declaration order.
- If multiple values are provided but fewer than the number of clients, remaining clients use the flag default/empty value unless explicitly set.

### Example: Different TLS settings for storage nodes

Enable TLS for one storage node while keeping another unencrypted:

```sh
./victoria-logs -storageNode vlstorage-1:9428 -storageNode.tls \
    -storageNode vlstorage-2:9428 -storageNode.tls=false
```

### Example: Shared username with different passwords

Use the same username for all nodes but different passwords:

```sh
./victoria-logs -storageNode.username vlselect \
    -storageNode vlstorage-1:9428 -storageNode.passwordFile /path/to/password-vlstorage-1.txt \
    -storageNode vlstorage-2:9428 -storageNode.passwordFile /path/to/password-vlstorage-2.txt
```

### Example: Mixed authentication methods

Some clients use Bearer tokens while others use Basic Auth:

```sh
./victoria-logs \
    -storageNode vlstorage-1:9428 -storageNode.bearerTokenFile /path/to/bearer.txt -storageNode.passwordFile="" -storageNode.username="" \
    -storageNode vlstorage-2:9428 -storageNode.username vlstorage -storageNode.passwordFile /path/to/password.txt -storageNode.bearerTokenFile=""
```

**Note:** When mixing authentication methods, explicitly set unused parameters to empty strings (`""`) to avoid configuration conflicts.

The same configuration logic applies to all `-remoteWrite.*` and `-storageNode.*` flags.

For the complete list of available flags, refer to [these docs](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).

## mTLS

> This feature requires [Enterprise binaries](https://docs.victoriametrics.com/victoriametrics/enterprise/) for components that use mTLS (for example, VictoriaLogs and/or vlagent).

Mutual TLS (mTLS) provides the highest level of security
by requiring both client and server to present valid certificates for authentication.
Unlike standard TLS where only the server authenticates itself, mTLS enables bidirectional authentication.

Production Security Recommendations:

- Implement proper certificate rotation and expiration monitoring
- Store private keys securely with appropriate file permissions (e.g., 600)
- Implement certificate revocation procedures for compromised certificates
- Keep certificates updated before expiration to avoid service disruptions

### Enabling mTLS on the server

mTLS requires both standard TLS configuration and additional mutual authentication settings:

```sh
./victoria-logs -tls \
    -tlsCertFile ./victoria-logs.pem \
    -tlsKeyFile ./victoria-logs-key.pem \
    -mtls \
    -licenseFile /path/to/license
```

By default, VictoriaLogs verifies client certificates using the system's trusted certificate store.
If using certificates signed by a private CA not present in the system trust store, you have two options:
1. Install your CA certificate in the system's trusted certificate store.
1. Specify the CA certificate path manually using `-mtlsCAFile`.

### Connecting vlselect and vlinsert to vlstorage with mTLS

Client components need both server verification and client authentication certificates:

```sh
./victoria-logs -storageNode vlstorage:9428 \
    -storageNode.tls \
    -storageNode.tlsCAFile /path/to/server-ca.pem \
    -storageNode.tlsCertFile /path/to/client-cert.pem \
    -storageNode.tlsKeyFile /path/to/client-key.pem
```

- `-storageNode.tlsCAFile` - CA certificate to verify the server's identity.
- `-storageNode.tlsCertFile` - client certificate for client authentication.
- `-storageNode.tlsKeyFile` - private key corresponding to the client certificate.

### Connecting vlagent to VictoriaLogs with mTLS

vlagent mTLS configuration follows the same pattern:

```sh
./vlagent -remoteWrite.url https://vlinsert:9428/insert/native \
    -remoteWrite.tlsCAFile /path/to/server-ca.pem \
    -remoteWrite.tlsCertFile /path/to/client-cert.pem \
    -remoteWrite.tlsKeyFile /path/to/client-key.pem
```

### mTLS with different client certificates

For multiple storage nodes, each client can use its own unique certificate:

```sh
./victoria-logs -storageNode.tls \
    -storageNode.tlsCAFile /path/to/server-ca.pem \
    -storageNode vlstorage-1:9428 -storageNode.tlsCertFile /path/to/client1-cert.pem -storageNode.tlsKeyFile /path/to/client1-key.pem \
    -storageNode vlstorage-2:9428 -storageNode.tlsCertFile /path/to/client2-cert.pem -storageNode.tlsKeyFile /path/to/client2-key.pem
```

This approach allows for granular access control and easier certificate management in complex deployments.

### Certificate reloading

VictoriaLogs automatically re-reads TLS certificate files (server certificates, client certificates, and CA certificates)
without requiring server or client restarts.
Certificate and TLS config values are cached for up to 1 second and then refreshed for new handshakes/requests,
which enables seamless certificate rotation in production environments.

The certificate reloading feature works for:
- Server certificates (`-tlsCertFile` and `-tlsKeyFile`).
- Client certificates (`-storageNode.tlsCertFile`, `-storageNode.tlsKeyFile`, `-remoteWrite.tlsCertFile`, `-remoteWrite.tlsKeyFile`).
- CA certificates (`-mtlsCAFile`, `-storageNode.tlsCAFile`, `-remoteWrite.tlsCAFile`).

To update certificates:
1. Replace the certificate files on disk with new versions.
1. VictoriaLogs will automatically detect and load the updated certificates.
1. New connections will use the updated certificates immediately.
1. Existing connections will continue using the old certificates until they reconnect.

**Note:** Ensure new certificates are valid and properly signed before replacing the old ones,
as invalid certificates will cause authentication failures for new connections.

### Certificate generation example

Create a Certificate Authority (CA):

```sh
# Generate CA private key
openssl genrsa -out ca-key.pem 4096

# Generate CA certificate (valid for 1 year)
openssl req -new -x509 -days 365 -key ca-key.pem -out ca-cert.pem -subj "/CN=VictoriaLogs-CA"
```

Generate server certificate:

```sh
# Generate server private key
openssl genrsa -out server-key.pem 4096

# Create certificate signing request
openssl req -new -key server-key.pem -out server.csr -subj "/CN=localhost"

# Sign server certificate with CA
openssl x509 -req -days 365 -in server.csr -CA ca-cert.pem -CAkey ca-key.pem -out server-cert.pem -CAcreateserial
```

> [!NOTE]
> This minimal example is intended for `localhost` testing.
> For production (or IP/real hostname access), generate the server certificate with `subjectAltName` (SAN).

Generate client certificate:

```sh
# Generate client private key
openssl genrsa -out client-key.pem 4096

# Create client certificate
openssl req -new -key client-key.pem -out client.csr -subj "/CN=victoria-logs-client"

# Sign client certificate with CA
openssl x509 -req -days 365 -in client.csr -CA ca-cert.pem -CAkey ca-key.pem -out client-cert.pem -CAcreateserial
```

Certificate Files Summary:
- `ca-cert.pem` - Used for both server and client certificate verification
- `server-cert.pem` + `server-key.pem` - Server certificate and key
- `client-cert.pem` + `client-key.pem` - Client certificate and key

Start VictoriaLogs with these certificates:

```sh
./victoria-logs -tls \
    -tlsCertFile ./server-cert.pem \
    -tlsKeyFile ./server-key.pem \
    -mtls \
    -mtlsCAFile ./ca-cert.pem \
    -licenseFile /path/to/license
```

Test the connection using `curl`:

```sh
curl --cacert ./ca-cert.pem --key ./client-key.pem --cert ./client-cert.pem https://localhost:9428/metrics
```

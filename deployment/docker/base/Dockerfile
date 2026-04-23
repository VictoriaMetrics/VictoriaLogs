# See https://medium.com/on-docker/use-multi-stage-builds-to-inject-ca-certs-ad1e8f01de1b
ARG certs_image=non-existing
ARG root_image=non-existing
FROM $certs_image AS certs

FROM $root_image

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

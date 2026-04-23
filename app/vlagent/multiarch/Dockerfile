# See https://medium.com/on-docker/use-multi-stage-builds-to-inject-ca-certs-ad1e8f01de1b
ARG certs_image=non-existing
ARG root_image=non-existing
# Use $BUILDPLATFORM since ca-certificates are platform-independent,
# and distroless does not support all target platforms (e.g. i386).
FROM --platform=$BUILDPLATFORM $certs_image AS certs

FROM $root_image
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
EXPOSE 9429
ENTRYPOINT ["/vlagent-prod"]
ARG TARGETARCH
COPY vlagent-linux-${TARGETARCH}-prod ./vlagent-prod

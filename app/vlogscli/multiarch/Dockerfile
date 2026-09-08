ARG root_image=non-existing

FROM $root_image
ENTRYPOINT ["/vlogscli-prod"]
ARG TARGETARCH
COPY vlogscli-linux-${TARGETARCH}-prod ./vlogscli-prod

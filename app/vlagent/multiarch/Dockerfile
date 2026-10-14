ARG root_image=non-existing

FROM $root_image
EXPOSE 9429
ENTRYPOINT ["/vlagent-prod"]
ARG TARGETARCH
COPY vlagent-linux-${TARGETARCH}-prod ./vlagent-prod

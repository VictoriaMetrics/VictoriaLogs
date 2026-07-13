ARG root_image=non-existing

FROM $root_image
EXPOSE 9428
ENTRYPOINT ["/victoria-logs-prod"]
ARG TARGETARCH
COPY victoria-logs-linux-${TARGETARCH}-prod ./victoria-logs-prod

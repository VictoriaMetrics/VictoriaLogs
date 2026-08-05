ARG root_image=non-existing

FROM $root_image
EXPOSE 9428
ENTRYPOINT ["/victoria-logs-prod"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=12 CMD ["/victoria-logs-prod", "-healthcheck", "-envflag.enable"]
ARG TARGETARCH
COPY victoria-logs-linux-${TARGETARCH}-prod ./victoria-logs-prod

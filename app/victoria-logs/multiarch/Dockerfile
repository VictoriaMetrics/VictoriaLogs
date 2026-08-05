ARG root_image=non-existing

FROM $root_image
EXPOSE 9428
ENTRYPOINT ["/victoria-logs-prod"]
HEALTHCHECK CMD ["/victoria-logs-prod", "-healthcheck", "-envflag.enable"]
ARG TARGETARCH
COPY victoria-logs-linux-${TARGETARCH}-prod ./victoria-logs-prod

FROM gcr.io/distroless/static-debian13
EXPOSE 9429
ENTRYPOINT ["/vlagent-prod"]
ARG TARGETARCH
COPY vlagent-linux-${TARGETARCH}-prod ./vlagent-prod

FROM debian:forky

RUN \
  apt update && \
  apt install -y \
    systemd \
    systemd-journal-remote && \
  sed -i 's/# URL=/URL=http:\/\/vlagent:9429\/insert\/journald\nHeader=AccountID: 10\nHeader=ProjectID: 10/g' /etc/systemd/journal-upload.conf && \
  systemctl enable systemd-journal-upload.service

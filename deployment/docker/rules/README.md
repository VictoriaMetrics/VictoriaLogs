# Alerting rules for VictoriaLogs components

This folder contains alerting rules for VictoriaLogs components.

## Best practice

`annotations.summary` and `annotations.description` should use literal style, preferably `|-` for easier synchronization across Helm templates.

Good example:

```yaml
          summary: |-
            Instance {{ $labels.instance }} (job={{ $labels.job }}) will run out of disk space soon
          description: |-
            Disk utilisation on instance {{ $labels.instance }} is more than 80%.
```

Not recommended:

```yaml
          summary: "Instance \"{{ $labels.instance }}\" (job=\"{{ $labels.job }}\") will run out of disk space soon"
          description: "Disk utilisation on instance \"{{ $labels.instance }}\" is more than 80%."
```

Run `make format-rules` in root folder to format `annotations.summary` and `annotations.description` fields. `yq` is required for formatting. It can be installed via the following command:

```sh
go install github.com/mikefarah/yq/v4@latest
```

Visit [this page](https://github.com/mikefarah/yq) for more information about the installation.

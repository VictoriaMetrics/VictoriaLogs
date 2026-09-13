# vmui

Web UI for VictoriaLogs

* [Static build](#static-build)
* [Updating vmui embedded into VictoriaLogs](#updating-vmui-embedded-into-victorialogs)

----

### Static build

Run the following command from the root of VictoriaLogs repository for building `vmui` static contents:

```
make vmui-build
```

The built static contents is put into `app/vmui/packages/vmui/` directory.

### Updating vmui embedded into VictoriaLogs

Run the following command from the root of VictoriaLogs repository for updating `vmui` embedded into VictoriaLogs:

```
make vmui-update
```

This command should update `vmui` static files at `app/vlselect/vmui` directory. Commit changes to these files if needed.

Then build VictoriaLogs with the following command:

```
make victoria-logs
```

Then run the built binary with the following command:

```
bin/victoria-logs
```

Then navigate to `http://localhost:9428/vmui/`. See [these docs](https://docs.victoriametrics.com/victorialogs/querying/#web-ui) for more details.

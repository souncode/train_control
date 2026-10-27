# train_control

## HTTP server

Run `start_waitress.cmd` to start the web UI on `http://127.0.0.1:820`.

The launcher runs Waitress directly without TLS:

```cmd
start_waitress.cmd
```

`start_waitress.cmd` listens directly on `0.0.0.0:820`, so other devices on the LAN can use `http://SERVER_IP:820`.

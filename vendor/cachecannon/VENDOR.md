# Vendored Cachecannon (owned copy)

Source: https://github.com/cachecannon/cachecannon.git
Pinned commit: 341254afe3d7596588cafdc230832dc57b14c5d0

This is a self-contained copy. Offbench builds and invokes this tree directly; it
does not clone cachecannon, patch a checkout, or read `../rig/cachecannon_auth.patch`
at deploy time.

## In-source auth support

RESP `AUTH [<user>] <password>` on every new connection, controlled by two optional
`[target]` config fields:

- `auth_user`
- `auth_password`

Integrated directly into source (no runtime patch):

- `src/config/mod.rs`   auth_user / auth_password on `Target`
- `src/worker.rs`       sends AUTH before PING on each RESP connection
- `src/valkey_lab.rs`   Target initializer
- `src/runner.rs`       Target initializer (tls test)

`.git` and `target/` are intentionally excluded.

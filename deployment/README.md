# Neo4j Deployment (Podman Quadlet)

Deploy Neo4j as a systemd-managed rootless container on Fedora. Quadlet files are declarative container definitions that Podman translates into native systemd units — Neo4j starts on boot, restarts on failure, and integrates with `systemctl` and `journalctl`.

## Prerequisites

- Fedora 42+ (tested on 43)
- Podman 5+ (`sudo dnf install podman`)
- User lingering enabled (the install script handles this)

## Server Setup (admin account)

The service user runs Neo4j rootless and doesn't need `wheel` or any special groups. However, an admin with sudo access must perform two one-time setup steps:

```bash
# 1. Enable lingering so the service user's systemd units run without an active login
sudo loginctl enable-linger <service-user>

# 2. Open firewall ports (Fedora uses firewalld by default)
sudo firewall-cmd --permanent --add-port=7474/tcp   # Neo4j Browser (HTTP)
sudo firewall-cmd --permanent --add-port=7687/tcp   # Bolt protocol
sudo firewall-cmd --reload
```

After this, all remaining steps are done as the service user.

## Quick Start

```bash
# Install and start Neo4j (as service user)
bash deployment/scripts/install.sh

# (Optional) Import an existing database dump
bash deployment/scripts/import-dump.sh backups/neo4j.dump

# Verify
systemctl --user status neo4j
```

Neo4j will be available at:
- **Browser:** http://localhost:7474
- **Bolt:** bolt://localhost:7687
- **Credentials:** `neo4j` / `neo4j_dev`

## What Gets Installed

The install script copies four Quadlet files to `~/.config/containers/systemd/`:

| File | Purpose |
|------|---------|
| `neo4j.container` | Container definition (image, ports, env vars) |
| `neo4j-data.volume` | Persistent graph data |
| `neo4j-logs.volume` | Neo4j logs |
| `neo4j-import.volume` | APOC file import staging |

Podman's Quadlet generator converts these into systemd units on `daemon-reload`. The resulting service is called `neo4j.service`.

## Importing a Database Dump

The import script loads a `neo4j-admin` dump file (like `backups/neo4j.dump`) into the running instance. Neo4j Community Edition requires the database to be stopped during load, so the script handles stop/load/restart automatically.

```bash
# Default: loads backups/neo4j.dump
bash deployment/scripts/import-dump.sh

# Custom dump file
bash deployment/scripts/import-dump.sh /path/to/my-backup.dump
```

## Common Operations

```bash
# Service management
systemctl --user status neo4j
systemctl --user stop neo4j
systemctl --user start neo4j
systemctl --user restart neo4j

# Logs (live tail)
journalctl --user -u neo4j -f

# Logs (last 50 lines)
journalctl --user -u neo4j -n 50 --no-pager

# Interactive Cypher shell
podman exec -it neo4j-etl cypher-shell -u neo4j -p neo4j_dev

# Container shell
podman exec -it neo4j-etl bash
```

## Connecting From the ETL Pipeline

The default `.env` settings work without changes:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4j_dev
```

Verify connectivity:

```bash
python src/scripts/init_db.py --stats
```

## Migrating From podman-compose

If you were previously using `docker/podman-compose.yml`:

1. Stop the compose container: `podman stop neo4j-etl && podman rm neo4j-etl`
2. The compose setup used volumes named `docker_neo4j-data` etc. The Quadlet uses `systemd-neo4j-data`. To migrate data, either:
   - Import from a dump (recommended): `bash deployment/scripts/import-dump.sh backups/neo4j.dump`
   - Or copy the volume: `podman volume create systemd-neo4j-data && podman run --rm -v docker_neo4j-data:/src:ro -v systemd-neo4j-data:/dst alpine cp -a /src/. /dst/`
3. Install the Quadlet: `bash deployment/scripts/install.sh`

## Uninstall

```bash
# Stop and disable the service
systemctl --user stop neo4j.service
systemctl --user disable neo4j.service

# Remove Quadlet files
rm ~/.config/containers/systemd/neo4j.container
rm ~/.config/containers/systemd/neo4j-data.volume
rm ~/.config/containers/systemd/neo4j-logs.volume
rm ~/.config/containers/systemd/neo4j-import.volume
systemctl --user daemon-reload

# (Optional) Remove data volumes
podman volume rm systemd-neo4j-data systemd-neo4j-logs systemd-neo4j-import
```

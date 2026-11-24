# Graph stores for the experiment harness

Neo4j and Apache Jena Fuseki as the two stores the agents write into. This is the development setup. It is separate from [`../local/`](../local/podman-compose.yml): other container names, ports and volumes, so both can exist on one machine.

```bash
podman-compose -f deploy/harness/podman-compose.yml up -d     # the first run builds the Fuseki image
podman ps --format '{{.Names}} {{.Status}}'                   # both report (healthy) when ready
podman-compose -f deploy/harness/podman-compose.yml down      # stop; add -v to delete the data
```

| Store | Container | Version | Address | Login |
|---|---|---|---|---|
| Neo4j | `harness-neo4j` | 5.26 Community, with APOC | Bolt `bolt://localhost:17687`, Browser `http://localhost:17474` | `neo4j` / `harness_dev` |
| Fuseki | `harness-fuseki` | 6.2.0 | `http://localhost:13030`, logged dataset `/cell` | `admin` / `harness_dev` |

Both listen on localhost only. The passwords are development passwords; for the runs they come from outside this repository.

Neo4j runs on the host network (bound to localhost) with the APOC commit hook enabled (`apoc.trigger.enabled`). The database tool in [`../../experiments/harness/dbtool/`](../../experiments/harness/dbtool/README.md) installs a hook through which the store hands every transaction's changes to the tool before it commits. While such a hook is installed, a write that does not come through the tool is undone; stopping the tool with `remove_hooks_on_stop` removes it again.

## Neo4j: one database

The Community edition holds one user database per instance. That is enough for development. The runs need one database per cell, so either the Enterprise edition (several databases and one account per database on one server) or one instance per cell.

```bash
# apply one property-graph schema (constraints and indexes)
python src/scripts/init_db.py --uri bolt://localhost:17687 --password harness_dev \
    --schema --schema-file schemas/treatments/S4-curated/pg/schema.cql

# empty the database and drop every constraint and index
podman exec harness-neo4j cypher-shell -u neo4j -p harness_dev \
    "MATCH (n) DETACH DELETE n; CALL apoc.schema.assert({}, {}) YIELD label RETURN count(*);"
```

## Fuseki: one logged dataset per cell

The database tool needs to know what a statement changed, and Fuseki has no hook before the commit. So a cell's dataset is a **logged dataset**: Fuseki appends every change to a file, and the tool reads that file ([`experiments/harness/dbtool/`](../../experiments/harness/dbtool/README.md), section "How a write is checked (Fuseki)"). Such a dataset cannot be created over HTTP (Fuseki 6.2.0 refuses it); it comes from a configuration file at the start: [`fuseki/cell.ttl`](fuseki/cell.ttl).

| | In the container | In this development setup |
|---|---|---|
| Dataset | `cell`: `/cell/sparql` (read), `/cell/update` (write), `/cell/patch` (the tool's undo), `/cell/data` (the whole graph, read only) | `http://localhost:13030/cell` |
| Data | `/fuseki/databases/cell` | volume `fuseki-data` |
| Change log | `/fuseki/logs/cell.rdfp` | [`fuseki/log/cell.rdfp`](fuseki/log/) beside this file (not tracked) |

The container's user is mapped to the user who starts it (`userns_mode: keep-id`), so the log is an ordinary file of that user and the tool on this machine reads it. **This replaces the old way of working** (a dataset `dev` created over HTTP): datasets can still be created and deleted over HTTP and an existing `dev` is still served, but they are not logged and the tool cannot use them.

```bash
F=http://localhost:13030; A="-u admin:harness_dev"

curl $A "$F/cell/sparql" --data-urlencode 'query=SELECT (COUNT(*) AS ?n) { ?s ?p ?o }'
curl $A "$F/cell/data" -H 'Accept: application/n-triples'               # the whole graph

# empty the cell: data, change log and the tool's mark belong together
podman-compose -f deploy/harness/podman-compose.yml stop fuseki
podman volume rm harness_fuseki-data && rm -f deploy/harness/fuseki/log/cell.rdfp     # and <records>/<cell>.settled of the tool
podman-compose -f deploy/harness/podman-compose.yml up -d fuseki
```

A write straight to `/cell/update` while the tool is in use is a write the tool did not check: the tool takes it out again and reports it. An RDF schema file is not loaded into a cell's dataset: the dataset holds the cell's data only.

The logins are in [`fuseki/shiro.ini`](fuseki/shiro.ini). There is one account for everything, and the agent never gets it.

### One cell in a run

Every cell gets its own Fuseki with the same configuration file, so the dataset is called `cell` everywhere and the tool tells cells apart by their address. What one cell needs:

```yaml
  fuseki-<cell>:
    image: localhost/fuseki:6.2.0
    command: ["--shiro", "/fuseki/shiro.ini", "--ping", "--compact", "--config", "/fuseki/cell.ttl"]
    volumes:
      - ../harness/fuseki/cell.ttl:/fuseki/cell.ttl:ro
      - ./shiro.ini:/fuseki/shiro.ini:ro            # a login of the run, not the development one
      - fuseki-<cell>-data:/fuseki/databases        # the data
      - fuseki-<cell>-log:/fuseki/logs              # the change log
  dbtool:
    volumes:
      - fuseki-<cell>-log:/logs/<cell>:ro           # the same volume, read only
```

and in the tool's configuration:

```json
"<cell>": {"arm": "rdf", "endpoint": "http://fuseki-<cell>:3030/cell", "user": "admin", "password_env": "...",
           "change_log": "/logs/<cell>/cell.rdfp", "realisation": "S4-curated/rdf", "tool_key_env": "..."}
```

- **The tool reads the log as a file, through a shared volume.** That is the simple way when store and tool run on one machine, as in a compose bundle: nothing to build, and reading is exact to the byte. Over HTTP the log is not offered by Fuseki itself; it would need a small file server beside every store. On two machines the volume has to be a shared file system, or the tool runs beside its stores.
- Both folders exist in the image and belong to its user, so the two named volumes are writable as they are (tried with Podman; no `:U` and no `userns_mode` needed, and the tool's container reads the log whatever user it runs as).
- The tool's own mark of how far the log is dealt with lies beside its records (`<log_dir>/<cell>.settled`). Keep the records folder on a volume too: without the mark a restarted tool takes everything in the log as kept.
- No port of a store has to be published: only the tool talks to it.
- Memory: `JAVA_OPTIONS=-Xmx1g` was enough for the tests, 200,000 triples included; the tool itself holds its list of the graph in memory (about 300 bytes per triple).

## Properties on a relationship in RDF

Fuseki 6.2.0 follows RDF 1.2. Tested on 14 November 2026; the two ways of writing differ:

| Written as | The relationship itself is stored | The property is stored |
|---|---|---|
| `:p :HAS_COMPONENT :c {\| :qty 2 \|} .` | yes | yes |
| `<< :p :HAS_COMPONENT :c >> :qty 2 .` | **no** | yes |

So a write has to use the first form, or state the relationship as well. Both can be read back with `<< ?p :HAS_COMPONENT ?c >> :qty ?q`.

In the store the first form is three triples: the relationship, a blank node that points at it (`_:r rdf:reifies <<( :p :HAS_COMPONENT :c )>>`), and the value on that blank node (`_:r :qty 2`). The database tool reads them back together as one relationship with the property `qty`.

## Numbers in Fuseki

The store (TDB2) keeps whole numbers, decimals, floating-point numbers, truth values and moments in time by value, and gives them back in its own spelling. Tested on 22 November 2026 with 37 spellings: `05` and `+7` come back as `5` and `7`, `2.50` as `2.5` (`1.0` stays), `1.5e3` as `1500.0e0`, `"1"^^xsd:boolean` as `true`, `"…T10:00:00.500Z"^^xsd:dateTime` as `…T10:00:00.5Z`, `"0.123456789012"^^xsd:float` as `0.12345679`. Unchanged: text, text with a language, dates, durations, numbers too large or too precise to pack, and every value that is not a valid form of its datatype. So in the RDF arm a number written as a number does not keep its spelling; written as text it does.

## Not in this setup yet

- Neo4j Enterprise with one database and one account per cell.
- Images pinned by digest.
- A snapshot or export of a store.
- Access from the agent cluster: the stores listen on localhost, so whatever talks to them has to run on the same machine.
- An account per cell in Fuseki: there is one login for everything. It matters less than on Neo4j, because only the tool holds it.

**A store from before 22 November 2026.** The Fuseki container now runs as the user who starts it, so that the database tool can read the change log. Volumes created under the earlier definition belong to another user and Fuseki stops with "Fuseki base is not writable: /fuseki/run". Hand them over once, with the container stopped:

```bash
for v in harness_fuseki-run harness_fuseki-data; do podman unshare chown -R 0:0 "$(podman volume inspect $v -f '{{.Mountpoint}}')"; done
```

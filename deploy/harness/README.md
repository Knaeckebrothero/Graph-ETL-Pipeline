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
| Fuseki | `harness-fuseki` | 6.2.0 | `http://localhost:13030` | `admin` / `harness_dev` |

Both listen on localhost only. The passwords are development passwords; for the runs they come from outside this repository.

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

## Fuseki: one dataset per cell

Fuseki starts without a dataset. Datasets are created and deleted over HTTP and survive a restart. Every request needs the login, except `/$/ping`. The dataset `dev` exists after the first setup on this machine; on a fresh volume, create it with the first command.

```bash
F=http://localhost:13030; A="-u admin:harness_dev"

curl $A -X POST   "$F/\$/datasets" --data "dbName=dev&dbType=tdb2"     # create
curl $A           "$F/\$/datasets"                                      # list
curl $A -X DELETE "$F/\$/datasets/dev"                                  # delete, data included

curl $A "$F/dev/sparql" --data-urlencode 'query=SELECT (COUNT(*) AS ?n) { ?s ?p ?o }'
curl $A "$F/dev/update" --data-urlencode 'update=INSERT DATA { <urn:a> <urn:p> 1 }'
```

A dataset has its query endpoint at `/<name>/sparql`, its update endpoint at `/<name>/update` and the Graph Store Protocol at `/<name>/data`. An RDF schema file is not loaded into a cell's dataset: the dataset holds the cell's data only.

The logins are in [`fuseki/shiro.ini`](fuseki/shiro.ini). There is one account for everything. An account per dataset would be a line per dataset in that file and a restart.

## Properties on a relationship in RDF

Fuseki 6.2.0 follows RDF 1.2. Tested on 14 November 2026; the two ways of writing differ:

| Written as | The relationship itself is stored | The property is stored |
|---|---|---|
| `:p :HAS_COMPONENT :c {\| :qty 2 \|} .` | yes | yes |
| `<< :p :HAS_COMPONENT :c >> :qty 2 .` | **no** | yes |

So a write has to use the first form, or state the relationship as well. Both can be read back with `<< ?p :HAS_COMPONENT ?c >> :qty ?q`.

## Not in this setup yet

- Neo4j Enterprise with one database and one account per cell.
- Images pinned by digest.
- A snapshot or export of a store.
- Access from the agent cluster: the stores listen on localhost, so whatever talks to them has to run on the same machine.

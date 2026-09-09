# N Elasticsearch clusters, for remote-reindex testing

Independent single-node clusters, as many as you ask for, each able to
remote-reindex from any of the others. Test scaffolding: no security, small
heaps, throwaway data.

## Use it

```powershell
cd OntologyProcessor\docker\elastic-clusters
.\clusters.ps1 up 3      # start three, wait until they are actually serving
.\clusters.ps1 urls      # print their URLs
.\clusters.ps1 test      # prove remote reindex works between two of them
.\clusters.ps1 down      # remove them (-Volumes drops the data too)
```

`up` prints something like:

```
Cluster Url                    Internal         Status
------- ---                    --------         ------
es-1    http://localhost:32768 http://es-1:9200 running(1)
es-2    http://localhost:32769 http://es-2:9200 running(1)
es-3    http://localhost:32770 http://es-3:9200 running(1)
```

Two columns because there are two ways in, and mixing them up is the usual
first failure:

- **Url** — from Windows: curl, Kibana, your Spark job on the host.
- **Internal** — from *inside* another cluster. This is what a remote reindex
  source must be, because the pull happens container-to-container over the
  shared Docker network, not through the published host port.

`up N` is idempotent: running `up 5` after `up 3` leaves the first three
running and adds two.

## How "how many" works

There is no N in the compose file. `docker-compose.yml` describes **one**
cluster, and `clusters.ps1` starts it once per cluster as its own compose
project — `es-1`, `es-2`, … Compose then gives each one its own network,
volume and container names for free, so nothing needs to be parameterised
except the cluster name.

This is why `docker compose --scale` isn't used: it scales one *service*, not
a group, and scaled replicas can't hold distinct published ports. Generating
one big compose file with N copies also works, but leaves you maintaining a
generator plus a generated file that drifts from it.

Because the clusters are discovered (`docker compose ls`) rather than assumed,
`urls`, `status`, `down` and `test` don't need to be told how many exist.

## Remote reindex

Three things make it work, and each is a separate way to get stuck:

1. **It runs over HTTP 9200**, not the transport port. That's the difference
   from cross-cluster search, and it's why the clusters only need to share an
   ordinary Docker network.
2. **The destination whitelists the source.** `reindex.remote.whitelist` is
   set to `es-*:9200` so any cluster here can pull from any other. It is a
   **static** setting — changing it means restarting the cluster, not calling
   an API.
3. **Address the source by its internal name.** `http://es-1:9200`, never
   `http://localhost:32768` — the destination container resolves that name on
   the shared network.

By hand:

```powershell
$body = '{"source":{"remote":{"host":"http://es-1:9200"},"index":"people"},
          "dest":{"index":"people_copy"}}'
Invoke-RestMethod -Method Post -ContentType 'application/json' -Body $body `
  -Uri "http://localhost:32769/_reindex?refresh=true"     # es-2's Url
```

## Ports

Host ports are ephemeral by default (`0:9200`), and `clusters.ps1` asks Docker
what got assigned. Fixed offsets across N clusters break the moment something
else is listening on one of them. Set `ES_PORT` in `.env` if you need a
predictable port — but note it applies to every cluster, so pin it only when
running a single one.

## Resources

Each node runs a 512m heap and needs roughly 1.5 GB of RAM in practice. That,
not anything in this setup, is what caps N — five clusters is about 7 GB.
Lower `ES_HEAP` in `.env` (256m is enough for trivial tests) or run fewer.

## Settings worth knowing

| Setting | Why |
| --- | --- |
| `xpack.security.enabled=false` | With security on, ES 8 mints a CA per cluster, every URL becomes https, and each remote reindex needs the source's certificate. |
| `discovery.type=single-node` | One node per cluster; skips the bootstrap checks. |
| `cluster.routing.allocation.disk.threshold_enabled=false` | Stops a full host disk silently flipping indices to read-only. |
| `ES_VERSION` in `.env` | Pinned to 8.15.3. Your other projects here use 7.17.13 / 7.13.1 — match whatever your `elasticsearch-hadoop` connector supports. |

## One sharp edge

Compose attaches the service-name alias `es` to every container on the shared
network, so all N clusters answer to `es` there and DNS round-robins between
them. Always use the unique alias (`es-1`, `es-2`, …); the compose file adds
those explicitly for exactly this reason.

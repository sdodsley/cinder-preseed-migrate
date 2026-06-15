# EverPure — Ladder A PoC

Proxy-assisted live volume migration for OpenStack Cinder, validated against a
**stock** cloud with **zero core changes**.

This proof of concept demonstrates that an external proxy can discover migration
intent, pre-seed a target volume on a different backend, and keep it in sync with
the source — all using existing Cinder APIs and os-brick, without any new
microversions, DB tables, or driver changes. Cutover uses the stock host-assisted
`os-migrate_volume` path, so the fence window is **not** shortened. That is the
deliberate scope of "Ladder A": de-risk everything *except* the fence window, and
measure precisely how much a delta-only cutover (Ladder B) would save.

> **Status:** experimental proof of concept. Not for production. Intended to
> produce timing data and a limitations report.

---

## How it works

```
                           ┌──────────────────────────────────────┐
                           │              proxy host              │
                           │   (initiator on both fabrics)        │
                           │                                      │
   source backend          │   1. discover intent (metadata)      │   target backend
   ┌────────────┐          │   2. snapshot source                 │   ┌────────────┐
   │  Ceph/RBD  │◀── ref ─┤   3. clone snapshot → readable vol   │   │ FlashArray │
   │  (source   │  clone   │   4. attach both, bulk copy ─────────┼──▶│ (pre-seed  │
   │   volume)  │          │   5. resync: diff + apply delta      │   │  target)   │
   └────────────┘          │   6. (Ladder A) stock cinder migrate │   └────────────┘
                           └──────────────────────────────────────┘
```

1. **Declare intent.** An operator stamps `proxy_migration:` keys onto the source
   volume's metadata. The volume stays `available` and fully usable.
2. **Discover.** The proxy lists volumes (admin, all tenants) and picks up any
   declaring `proxy_migration:status = preseeding`.
3. **Pre-seed.** The proxy creates a target volume on the destination backend
   (owned by the Cinder internal tenant so it doesn't charge the source tenant's
   quota), snapshots the source, materializes the snapshot as a readable volume,
   then attaches both via the Cinder attachments API + os-brick and bulk-copies
   source → target.
4. **Resync.** A second snapshot is diffed against the reference and only the
   changed extents are applied to the target. Repeatable to keep the delta small.
5. **Cutover.** In Ladder A this calls stock `cinder migrate`, which performs a
   full host-assisted copy and discards the pre-seeded target. The resync delta is
   measured to project the fence window a delta-only cutover would achieve.
6. **Cleanup.** A janitor reclaims every proxy-created resource by its intent tag.

---

## Requirements

- An OpenStack cloud with Cinder (volume API **v3.44+** for the attachments API),
  two backends (a source and a target), and a volume type for each.
- `cinder_internal_tenant_project_id` and `cinder_internal_tenant_user_id` set in
  `cinder.conf [DEFAULT]`. The internal tenant's volume and gigabyte quotas must be
  raised above the number of in-flight intermediates (the default project quota is
  only 10 volumes).
- A **proxy host** that is a working storage initiator on **both** fabrics — e.g. a
  Ceph keyring/conf for an RBD source, and an iSCSI initiator with data-network
  reach to a FlashArray target. `os-brick connect_volume` fails otherwise.
- Python 3.8+ on the proxy host, run as **root** (os-brick needs privileged calls)
  or with a configured root helper.

### Python dependencies

```bash
pip install python-cinderclient keystoneauth1 os-brick
```

---

## Configuration

Authentication uses the standard OpenStack environment variables. Source your
admin RC file:

```bash
source admin-openrc.sh        # sets OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, etc.
```

The internal-tenant client reuses the admin **user** but re-scopes its token to the
internal project. Provide that project id via flag or environment:

```bash
export POC_INTERNAL_PROJECT_ID=<cinder_internal_tenant_project_id>
export POC_TARGET_VOLUME_TYPE=<target-volume-type>   # optional; see note below
```

A couple of knobs live at the top of `poc_demo.py`:

| Constant | Default | Purpose |
|---|---|---|
| `CINDER_API_VERSION` | `3.44` | Volume API microversion (attachments API) |
| `BLOCK_SIZE` | `8 MiB` | Copy/diff block size |
| `ROOT_HELPER` | `sudo cinder-rootwrap …` | os-brick privileged command helper |
| `POLL_INTERVAL` | `5 s` | Migration status poll interval |

---

## Usage

### Discover intent (read-only)

Lists volumes declaring preseeding intent. Needs only admin credentials — no
internal tenant, no source volume. Good first check against a new environment.

```bash
python poc_demo.py --discover
```

### Declare intent (operator action)

Stamp the source volume so the proxy will pick it up:

```bash
cinder metadata <source-vol-uuid> set \
    proxy_migration:target_backend="<host@backend#pool>" \
    proxy_migration:status="preseeding"
```

### Dry run

Print the plan without making any API calls:

```bash
python poc_demo.py --dry-run \
    --source-vol <source-vol-uuid> \
    --target-backend <host@backend#pool>
```

### Full run

Pre-seed, run one resync cycle, print a timing summary, then prompt before the
stock cutover. The janitor always runs at the end:

```bash
python poc_demo.py \
    --source-vol <source-vol-uuid> \
    --target-backend <host@backend#pool>
```

### Abort / clean up

Reclaim every resource tagged with a given intent id, without cutover:

```bash
python poc_demo.py --abort --intent-id <intent-id>
```

### Flags

| Flag | Description |
|---|---|
| `--source-vol` | Source volume UUID |
| `--target-backend` | `host@backend#pool` of the target pool |
| `--internal-project-id` | Internal tenant project id (or `POC_INTERNAL_PROJECT_ID`) |
| `--intent-id` | Reuse an existing intent id (default: generated) |
| `--discover` | List preseeding volumes and exit (read-only) |
| `--dry-run` | Print the plan; make no API calls |
| `--abort` | Clean up resources for `--intent-id`; no cutover |

---

## Intent metadata schema

Intent is carried in regular volume metadata under the `proxy_migration:`
namespace. (Cinder exposes no public API for arbitrary admin-metadata keys, so
regular metadata is used; note that these keys are tenant-visible and mutable.)

| Key | Written by | Value |
|---|---|---|
| `proxy_migration:target_backend` | Operator | `host@backend#pool` of the target pool |
| `proxy_migration:status` | Proxy | `preseeding` / `cutover_ready` / `error` |
| `proxy_migration:reference_snapshot_id` | Proxy | Cinder snapshot UUID of the baseline |
| `proxy_migration:target_volume_id` | Proxy | Cinder UUID of the pre-seeded target |
| `proxy_migration:preseed_progress` | Proxy | 0–100 integer |
| `proxy_migration:intent_id` | Proxy | Short id used for orphan tagging |
| `proxy_migration:error_reason` | Proxy | Human-readable string, nullable |

---

## Known limitations

These are inherent to the Ladder A scope and are what the PoC is designed to
quantify:

- **Discovery has to scan every volume.** There is no way to ask Cinder for "just
  the volumes being migrated," so the proxy lists all volumes in the cloud and
  filters them itself. That gets slower as the fleet grows.
- **The fence window is not shortened.** Cutover uses stock `os-migrate_volume`,
  which re-copies from the live source and discards the pre-seeded target. The
  pre-seed and resync pipeline produces no cutover benefit in Ladder A — it exists
  to exercise the code paths and to measure delta sizes.
- **The reference snapshot charges the source tenant.** Snapshots follow volume
  ownership; there is no public `use_quota=False` snapshot path.
- **Diff reads the full volume twice.** Without an array-level diff API, computing
  changed extents reads both snapshot clones end to end, which does not scale to
  large or frequently-resynced volumes.
- **In-use volumes are out of scope.** For attached volumes Cinder delegates the
  copy to Nova; a pre-seeded target does not shorten that path.

---

## Roadmap

Ladder A is Phase 0. The measured limitations above motivate a small set of Cinder
core hooks (Ladder B and beyond): on-proxy migration policy, a pluggable mover that
performs a delta-only cutover against the pre-seeded target, an indexed intent
table with crash recovery, low-latency notifications, a snapshot-diff API, and a
service-only quota-neutral snapshot path. Ladder B is where the fence window
actually shrinks.

---

## Repository layout

```
poc_demo.py    end-to-end demo: discover / pre-seed / resync / cutover / cleanup
README.md      this file
```

---

## License

Apache License 2.0.

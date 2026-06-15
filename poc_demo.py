#!/usr/bin/env python3
"""
poc_demo.py — EverPure Ladder A (Phase 0) proxy-assisted migration PoC.

Validates discovery, intent management, quota-hiding, and block access against a
STOCK Cinder cloud with ZERO core changes. Cutover uses the stock host-assisted
``os-migrate_volume`` path, so the fence window is NOT shortened — that is the
deliberate limitation of Ladder A. The pre-seed + resync pipeline here yields no
cutover benefit; it exists only to (a) exercise the code paths Ladder B will
reuse and (b) MEASURE delta sizes so the Ladder B fence window can be projected.

--------------------------------------------------------------------------------
Design notes:

  1. Reference data is read via create-volume-from-snapshot, not by attaching a
     snapshot. Cinder's attachment_create is volume-only; snapshot attach is a
     driver-internal capability with no public REST action. On RBD/FlashArray the
     materialized clone is COW/thin, so this adds bookkeeping but ~zero data
     movement on the source backend.

  2. Proxy-owned intermediates are created with a Keystone token scoped to the
     internal tenant; ownership follows the token's project, so these volumes do
     not charge the source tenant's quota. The internal tenant is itself subject
     to quota — raise its volume/gigabyte quota first.

  3. Every proxy-created resource (target volume, reference clones, snapshots) is
     tagged with proxy_migration:intent_id, and the janitor reclaims all of them.

  4. Intent lives in regular volume metadata under the proxy_migration: namespace.
     Cinder exposes no public API to set arbitrary admin_metadata keys, so regular
     metadata is used. Trade-off: the keys are tenant-visible and tenant-mutable.

--------------------------------------------------------------------------------
Environment prerequisites:

  * The proxy host must be a working initiator on BOTH fabrics: a Ceph keyring/conf
    for the RBD source, and iSCSI initiator + data-network reach to the FlashArray
    target. os-brick connect_volume will fail otherwise.
  * Run as root (or configure ROOT_HELPER below) — os-brick needs privileged calls.
  * The internal tenant's volume + gigabytes quota must be raised above the number
    of in-flight intermediates (default project quota is only 10 volumes).

Admin auth comes from the standard OS_* environment variables (source your admin
RC file). The internal-tenant client reuses the admin USER but re-scopes the token
to --internal-project-id.
"""

import argparse
import os
import socket
import sys
import time
import uuid

# --- Cinder / Keystone / os-brick imports ------------------------------------
from keystoneauth1 import loading as ks_loading
from keystoneauth1 import session as ks_session
from keystoneauth1.identity import v3 as ks_v3
from cinderclient import client as cinder_client

# os-brick is imported lazily inside the attach helpers so that --dry-run works
# on a host without os-brick / root.

# --- Constants ---------------------------------------------------------------
CINDER_API_VERSION = "3.44"          # >= 3.44 for the attachments API
BLOCK_SIZE = 8 * 1024 * 1024         # 8 MiB copy block
ROOT_HELPER = "sudo cinder-rootwrap /etc/cinder/rootwrap.conf"
POLL_INTERVAL = 5                    # seconds, for migration status polling

NS = "proxy_migration"
K_TARGET_BACKEND = f"{NS}:target_backend"
K_STATUS = f"{NS}:status"
K_REF_SNAP = f"{NS}:reference_snapshot_id"
K_TARGET_VOL = f"{NS}:target_volume_id"
K_PROGRESS = f"{NS}:preseed_progress"
K_INTENT = f"{NS}:intent_id"
K_ERROR = f"{NS}:error_reason"

STATUS_PRESEEDING = "preseeding"
STATUS_CUTOVER_READY = "cutover_ready"
STATUS_ERROR = "error"


def log(msg):
    print(f"[poc] {msg}", flush=True)


# =============================================================================
# Auth
# =============================================================================
def _admin_auth_from_env():
    """Build a v3 password auth from OS_* env vars (admin RC file)."""
    return ks_v3.Password(
        auth_url=os.environ["OS_AUTH_URL"],
        username=os.environ["OS_USERNAME"],
        password=os.environ["OS_PASSWORD"],
        project_name=os.environ.get("OS_PROJECT_NAME"),
        project_id=os.environ.get("OS_PROJECT_ID"),
        user_domain_name=os.environ.get("OS_USER_DOMAIN_NAME", "Default"),
        project_domain_name=os.environ.get("OS_PROJECT_DOMAIN_NAME", "Default"),
    )


def get_admin_client():
    """Admin-scoped Cinder client: discovery, migrate, all_tenants listing."""
    sess = ks_session.Session(auth=_admin_auth_from_env())
    return cinder_client.Client(CINDER_API_VERSION, session=sess)


def get_internal_tenant_client(internal_project_id):
    """
    Cinder client whose token is scoped to the internal tenant. Volumes created
    through this client are owned by the internal tenant and therefore do not
    charge the source tenant's quota. Ownership follows the token's project.
    """
    auth = ks_v3.Password(
        auth_url=os.environ["OS_AUTH_URL"],
        username=os.environ["OS_USERNAME"],
        password=os.environ["OS_PASSWORD"],
        project_id=internal_project_id,            # re-scope to the internal tenant
        user_domain_name=os.environ.get("OS_USER_DOMAIN_NAME", "Default"),
    )
    sess = ks_session.Session(auth=auth)
    return cinder_client.Client(CINDER_API_VERSION, session=sess)


# =============================================================================
# Intent metadata helpers (regular metadata, proxy_migration: namespace)
# =============================================================================
def set_intent(cinder, vol_id, **kwargs):
    """Merge keys into the source volume's metadata. Values are stringified."""
    meta = {k: str(v) for k, v in kwargs.items() if v is not None}
    cinder.volumes.set_metadata(vol_id, meta)


def get_meta(obj):
    return getattr(obj, "metadata", None) or {}


def discover_intents(cinder):
    """
    Return [(volume_uuid, target_backend)] for volumes declaring preseeding intent.

    Note: this is O(all volumes) — there is no server-side index on metadata, so
    we list everything and filter client-side.
    """
    out = []
    for v in cinder.volumes.list(search_opts={"all_tenants": 1}):
        m = get_meta(v)
        if m.get(K_STATUS) == STATUS_PRESEEDING:
            out.append((v.id, m.get(K_TARGET_BACKEND)))
    return out


# =============================================================================
# Block access: attach a VOLUME (never a snapshot) and connect via os-brick
# =============================================================================
def _connector_properties():
    from os_brick.initiator import connector as brick
    my_ip = socket.gethostbyname(socket.gethostname())
    return brick.get_connector_properties(
        ROOT_HELPER, my_ip, multipath=False, enforce_multipath=False
    )


def attach_volume(cinder, volume_id):
    """
    Attach a Cinder VOLUME to this proxy host via the attachments API + os-brick.
    Returns (attachment_id, connection_info, device_info) for later detach.
    """
    from os_brick.initiator import connector as brick

    # Reserve, then update with our connector to get connection_info.
    attach = cinder.attachments.create(connector={}, volume_id=volume_id)
    props = _connector_properties()
    attach = cinder.attachments.update(attach["id"], connector=props)
    conn_info = attach.connection_info

    conn = brick.InitiatorConnector.factory(
        conn_info["driver_volume_type"], ROOT_HELPER, use_multipath=False
    )
    device_info = conn.connect_volume(conn_info["data"])
    log(f"  attached {volume_id} -> {device_info['path']}")
    return attach["id"] if isinstance(attach, dict) else attach.id, conn_info, device_info


def detach_volume(cinder, attachment_id, conn_info, device_info):
    from os_brick.initiator import connector as brick
    conn = brick.InitiatorConnector.factory(
        conn_info["driver_volume_type"], ROOT_HELPER, use_multipath=False
    )
    conn.disconnect_volume(conn_info["data"], device_info)
    cinder.attachments.delete(attachment_id)


# =============================================================================
# Snapshot -> readable volume
# =============================================================================
def clone_snapshot_to_volume(internal_cinder, snapshot, intent_id, label):
    """
    Materialize a snapshot as a readable volume (COW/thin clone on the source
    backend). Owned by the internal tenant; tagged with the intent_id so the
    janitor reclaims it.
    """
    vol = internal_cinder.volumes.create(
        size=snapshot.size,
        snapshot_id=snapshot.id,
        name=f"proxy-migration-{label}-{intent_id}",
        metadata={K_INTENT: intent_id},
    )
    _wait_status(internal_cinder, vol.id, "available")
    return internal_cinder.volumes.get(vol.id)


# =============================================================================
# Copy / diff primitives
# =============================================================================
def bulk_copy(src_path, dst_path, size_bytes, progress_cb=None):
    copied = 0
    with open(src_path, "rb") as src, open(dst_path, "wb") as dst:
        while copied < size_bytes:
            data = src.read(min(BLOCK_SIZE, size_bytes - copied))
            if not data:
                break
            dst.write(data)
            copied += len(data)
            if progress_cb:
                progress_cb(int(100 * copied / size_bytes))
        dst.flush()
        os.fsync(dst.fileno())
    return copied


def compute_diff(base_path, target_path, size_bytes):
    """Return [(offset, length)] of changed regions. Reads the full volume twice
    (once per snapshot clone)."""
    changed, offset = [], 0
    with open(base_path, "rb") as base, open(target_path, "rb") as tgt:
        while offset < size_bytes:
            n = min(BLOCK_SIZE, size_bytes - offset)
            b, t = base.read(n), tgt.read(n)
            if b != t:
                changed.append((offset, len(b)))
            offset += n
    return changed


def apply_delta(src_path, dst_path, extents):
    with open(src_path, "rb") as src, open(dst_path, "r+b") as dst:
        for offset, length in extents:
            src.seek(offset)
            dst.seek(offset)
            dst.write(src.read(length))
        dst.flush()
        os.fsync(dst.fileno())


# =============================================================================
# Cinder waiters
# =============================================================================
def _wait_status(cinder, vol_id, target, timeout=600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = cinder.volumes.get(vol_id)
        if v.status == target:
            return v
        if v.status in ("error", "error_deleting"):
            raise RuntimeError(f"volume {vol_id} entered {v.status}")
        time.sleep(2)
    raise TimeoutError(f"volume {vol_id} did not reach {target}")


def _wait_snapshot(cinder, snap_id, target="available", timeout=600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = cinder.volume_snapshots.get(snap_id)
        if s.status == target:
            return s
        if s.status == "error":
            raise RuntimeError(f"snapshot {snap_id} entered error")
        time.sleep(2)
    raise TimeoutError(f"snapshot {snap_id} did not reach {target}")


# =============================================================================
# Orphan janitor (reclaims volumes, clones, and snapshots by intent_id)
# =============================================================================
def cleanup_orphans(admin_cinder, intent_id):
    deleted = []
    for v in admin_cinder.volumes.list(search_opts={"all_tenants": 1}):
        if get_meta(v).get(K_INTENT) == intent_id:
            try:
                admin_cinder.volumes.delete(v.id, cascade=True)
                deleted.append(("volume", v.id))
            except Exception as e:           # noqa: BLE001 - best-effort cleanup
                log(f"  WARN could not delete volume {v.id}: {e}")
    for s in admin_cinder.volume_snapshots.list(search_opts={"all_tenants": 1}):
        if get_meta(s).get(K_INTENT) == intent_id:
            try:
                admin_cinder.volume_snapshots.delete(s.id)
                deleted.append(("snapshot", s.id))
            except Exception as e:           # noqa: BLE001
                log(f"  WARN could not delete snapshot {s.id}: {e}")
    for kind, rid in deleted:
        log(f"  cleaned up {kind} {rid}")
    return deleted


# =============================================================================
# Orchestration
# =============================================================================
def run_preseed(admin, internal, source_vol, target_backend, intent_id, dry):
    """Declare intent, create the target volume and reference snapshot, then
    bulk-copy the reference into the target."""
    log(f"declaring intent on {source_vol.id} -> {target_backend}")
    if dry:
        log("  [dry-run] would set intent metadata + create target + ref snapshot")
        return None, None

    set_intent(admin, source_vol.id,
               **{K_TARGET_BACKEND: target_backend,
                  K_STATUS: STATUS_PRESEEDING,
                  K_INTENT: intent_id})

    # Target volume on the FlashArray backend, owned by the internal tenant.
    log("creating pre-seed target volume (internal tenant)")
    target = internal.volumes.create(
        size=source_vol.size,
        volume_type=_type_for_backend(internal, target_backend),
        name=f"proxy-migration-target-{intent_id}",
        metadata={K_INTENT: intent_id},
    )
    _wait_status(internal, target.id, "available")
    set_intent(admin, source_vol.id, **{K_TARGET_VOL: target.id})

    # Reference snapshot of the source. Snapshots follow volume ownership, so this
    # is owned by the source tenant and charges its quota.
    log("creating reference snapshot")
    ref_snap = admin.volume_snapshots.create(
        volume_id=source_vol.id,
        name=f"proxy-migration-ref-{intent_id}",
        force=True,
        metadata={K_INTENT: intent_id},
    )
    _wait_snapshot(admin, ref_snap.id)
    set_intent(admin, source_vol.id, **{K_REF_SNAP: ref_snap.id})

    # Materialize the snapshot as a readable volume and bulk copy.
    ref_vol = clone_snapshot_to_volume(internal, ref_snap, intent_id, "refclone")
    sa = ta = None
    t0 = time.monotonic()
    try:
        sa = attach_volume(internal, ref_vol.id)
        ta = attach_volume(internal, target.id)
        size = source_vol.size * 1024 ** 3
        copied = bulk_copy(
            sa[2]["path"], ta[2]["path"], size,
            progress_cb=lambda p: set_intent(admin, source_vol.id, **{K_PROGRESS: p}),
        )
    finally:
        if sa:
            detach_volume(internal, *sa)
        if ta:
            detach_volume(internal, *ta)
    dt = time.monotonic() - t0
    log(f"pre-seed copy: {copied/1e6:.0f} MB in {dt:.1f}s "
        f"({copied/1e6/dt:.1f} MB/s)")
    # Reference clone has served its purpose; reclaim it.
    internal.volumes.delete(ref_vol.id)
    return target, ref_snap


def run_resync(admin, internal, source_vol, target, ref_snap, intent_id):
    """
    Snapshot the source again, diff against the reference, and apply the delta to
    the target. In Ladder A this is measurement only — the stock cutover re-copies
    from the live source, so the applied delta is discarded. Changed-bytes are
    recorded to project the Ladder B fence window.
    """
    log("creating resync snapshot")
    resync_snap = admin.volume_snapshots.create(
        volume_id=source_vol.id,
        name=f"proxy-migration-resync-{intent_id}",
        force=True,
        metadata={K_INTENT: intent_id},
    )
    _wait_snapshot(admin, resync_snap.id)

    base_vol = clone_snapshot_to_volume(internal, ref_snap, intent_id, "base")
    new_vol = clone_snapshot_to_volume(internal, resync_snap, intent_id, "resync")

    ba = na = ta = None
    t0 = time.monotonic()
    try:
        ba = attach_volume(internal, base_vol.id)
        na = attach_volume(internal, new_vol.id)
        size = source_vol.size * 1024 ** 3
        extents = compute_diff(ba[2]["path"], na[2]["path"], size)
        changed = sum(l for _, l in extents)
        log(f"resync diff: {len(extents)} extents, {changed/1e6:.1f} MB changed")
        ta = attach_volume(internal, target.id)
        apply_delta(na[2]["path"], ta[2]["path"], extents)
    finally:
        for a in (ba, na, ta):
            if a:
                detach_volume(internal, *a)
    dt = time.monotonic() - t0
    log(f"resync cycle: {dt:.1f}s")

    # Promote: write the new reference pointer before deleting the old snapshot,
    # so a crash mid-promote leaves a recoverable state.
    set_intent(admin, source_vol.id, **{K_REF_SNAP: resync_snap.id})
    admin.volume_snapshots.delete(ref_snap.id)
    internal.volumes.delete(base_vol.id)
    internal.volumes.delete(new_vol.id)
    return resync_snap, changed, dt


def run_cutover(admin, source_vol, target, target_backend, dry):
    """Stock host-assisted migrate. Cinder creates its own destination and copies
    everything, so the pre-seeded target is discarded and the full fence window is
    present. This is the Ladder A baseline."""
    set_intent(admin, source_vol.id,
               **{K_TARGET_VOL: target.id, K_STATUS: STATUS_CUTOVER_READY})
    if dry:
        log(f"  [dry-run] would: cinder migrate {source_vol.id} {target_backend}")
        return None
    log(f"triggering stock migrate -> {target_backend}")
    t0 = time.monotonic()
    admin.volumes.migrate_volume(source_vol.id, target_backend,
                                 force_host_copy=False, lock_volume=False)
    while True:
        v = admin.volumes.get(source_vol.id)
        if v.migration_status == "success":
            break
        if v.migration_status == "error":
            raise RuntimeError("migration failed")
        time.sleep(POLL_INTERVAL)
    dt = time.monotonic() - t0
    log(f"cutover (FULL fence window): {dt:.1f}s")
    return dt


def _type_for_backend(cinder, backend):
    """Resolve a volume type whose extra_specs target the given backend host.
    PoC stub: relies on a type named after the backend, or override via env."""
    override = os.environ.get("POC_TARGET_VOLUME_TYPE")
    if override:
        return override
    # host@backend#pool -> backend
    return backend.split("@")[1].split("#")[0]


# =============================================================================
# main
# =============================================================================
def main(argv=None):
    p = argparse.ArgumentParser(description="EverPure Ladder A PoC demo")
    p.add_argument("--source-vol", help="source volume UUID")
    p.add_argument("--target-backend", help="host@backend#pool of the FlashArray pool")
    p.add_argument("--internal-project-id",
                   default=os.environ.get("POC_INTERNAL_PROJECT_ID"),
                   help="cinder_internal_tenant_project_id from cinder.conf")
    p.add_argument("--intent-id", default=None,
                   help="reuse an existing intent id (default: generate)")
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan; make no API calls")
    p.add_argument("--abort", action="store_true",
                   help="clean up all proxy resources for --intent-id; no cutover")
    p.add_argument("--discover", action="store_true",
                   help="list volumes declaring preseeding intent and exit "
                        "(read-only; admin creds only)")
    args = p.parse_args(argv)

    intent_id = args.intent_id or uuid.uuid4().hex[:12]

    if args.dry_run:
        log("DRY RUN — no API calls will be made")
        log(f"intent_id={intent_id}")
        log(f"plan: stamp intent on {args.source_vol}; create internal-tenant target "
            f"on {args.target_backend}; ref snapshot; clone->attach->bulk copy; "
            f"detach; one resync cycle; stock migrate; janitor cleanup")
        return 0

    admin = get_admin_client()

    if args.discover:
        intents = discover_intents(admin)
        if not intents:
            log("no volumes declaring preseeding intent")
            return 0
        log(f"{len(intents)} volume(s) declaring preseeding intent:")
        for vol_id, backend in intents:
            log(f"  {vol_id} -> {backend or '(no target_backend set)'}")
        return 0

    if args.abort:
        log(f"ABORT — cleaning up intent {intent_id}")
        cleanup_orphans(admin, intent_id)
        return 0

    if not args.internal_project_id:
        p.error("--internal-project-id (or POC_INTERNAL_PROJECT_ID) is required")
    if not (args.source_vol and args.target_backend):
        p.error("--source-vol and --target-backend are required")

    internal = get_internal_tenant_client(args.internal_project_id)
    source_vol = admin.volumes.get(args.source_vol)

    try:
        target, ref_snap = run_preseed(
            admin, internal, source_vol, args.target_backend, intent_id, dry=False)
        resync_snap, changed, resync_dt = run_resync(
            admin, internal, source_vol, target, ref_snap, intent_id)

        log("--- pre-seed complete -------------------------------------------")
        log(f"  intent_id        : {intent_id}")
        log(f"  target volume    : {target.id} (internal tenant, will be discarded)")
        log(f"  last delta       : {changed/1e6:.1f} MB in {resync_dt:.1f}s")
        log(f"  PROJECTED Ladder B fence window ~= delta-apply time above")
        log(f"  Ladder A cutover : full host-assisted copy (measured next)")
        log("-----------------------------------------------------------------")

        reply = input("Proceed with stock cutover (cinder migrate)? [y/N] ")
        if reply.strip().lower() == "y":
            run_cutover(admin, source_vol, target, args.target_backend, dry=False)
            # Verify the volume moved (UUID is preserved by core migrate).
            v = admin.volumes.get(source_vol.id)
            host = getattr(v, "os-vol-host-attr:host", "?")
            log(f"post-cutover host: {host} (uuid preserved: {v.id})")
        else:
            log("cutover skipped")
    finally:
        log("running janitor")
        cleanup_orphans(admin, intent_id)

    return 0


if __name__ == "__main__":
    sys.exit(main())

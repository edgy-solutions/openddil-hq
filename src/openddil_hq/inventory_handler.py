# =============================================================================
# OpenDDIL HQ — Inventory Processor (Restate Virtual Object)
# =============================================================================
# Durable event handler consuming ItemAllocatedEvent from Redpanda.
# Uses the official restate-sdk for Python.
#
# This demonstrates OpenDDIL's polyglot architecture:
#   - C# Edge clients push Protobuf bytes to Redpanda (Phase 4)
#   - This Python handler deserializes and processes them durably
#
# Durable execution guarantees:
#   - ctx.run() wraps every side effect (DB call) in Restate's journal
#   - On replay after a crash, journaled operations are SKIPPED
#   - The Virtual Object key (item_id) ensures sequential processing per item
# =============================================================================

import json
import logging
import uuid

import asyncpg
import restate
from google.protobuf.any_pb2 import Any as ProtoAny

# Generated from openddil-contracts: make python
from openddil.events.v1.cloud_event_pb2 import CloudEvent
from openddil.inventory.v1.inventory_events_pb2 import ItemAllocatedEvent

logger = logging.getLogger("openddil.hq.inventory")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL = "postgresql://openddil:openddil@localhost:5432/openddil"

# ---------------------------------------------------------------------------
# Restate Virtual Object: InventoryProcessor
# ---------------------------------------------------------------------------
# Keyed by item_id. Restate guarantees that only ONE handler invocation runs
# at a time per key. This means concurrent allocations for the same item are
# queued and processed sequentially — no locking or optimistic concurrency
# needed, even with multiple Edge nodes flushing simultaneously.
# ---------------------------------------------------------------------------
inventory_processor = restate.VirtualObject("InventoryProcessor")


@inventory_processor.handler("handleItemAllocated")
async def handle_item_allocated(
    ctx: restate.ObjectContext,
    cloud_event_bytes: bytes,
) -> None:
    """
    Process an ItemAllocatedEvent arriving from the Edge via Redpanda.

    Steps:
      1. Deserialize the CloudEvent envelope (raw Protobuf bytes from Edge C#/Python SDK)
      2. Unpack the ItemAllocatedEvent domain payload
      3. ctx.run: Query Postgres for current available_count
      4. Calculate new count
      5. ctx.run: Either UPDATE inventory (valid) or INSERT audit warning (insufficient)

    Every database call is wrapped in ctx.run() so Restate journals the result.
    If this handler crashes mid-flight, Restate replays it but SKIPS the
    already-journaled DB operations — achieving exactly-once processing.
    """
    item_id = ctx.key()  # Virtual Object key = item_id

    # ----- Step 1: Deserialize the CloudEvent envelope -----
    cloud_event = CloudEvent()
    cloud_event.ParseFromString(cloud_event_bytes)

    # ----- Step 2: Unpack the domain event -----
    evt = ItemAllocatedEvent()
    cloud_event.data.Unpack(evt)

    logger.info(
        "[HQ] Processing allocation: item=%s qty=%d edge=%s user=%s",
        item_id,
        evt.quantity,
        cloud_event.edge_node_id,
        evt.user_id,
    )

    # ----- Step 3: Query current inventory (durable side effect) -----
    async def query_current_count() -> int:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            row = await conn.fetchrow(
                "SELECT available_count FROM inventory_items WHERE id = $1",
                uuid.UUID(item_id),
            )
            return row["available_count"] if row else -1
        finally:
            await conn.close()

    current_count: int = await ctx.run("query_inventory", query_current_count)

    # ----- Step 3a: Item not found -----
    if current_count == -1:
        logger.warning("[HQ] Item %s not found. Rejecting allocation.", item_id)

        async def audit_not_found() -> None:
            conn = await asyncpg.connect(DATABASE_URL)
            try:
                await conn.execute(
                    """
                    INSERT INTO audit_log
                        (entity_type, entity_id, action, payload, actor)
                    VALUES ($1, $2::uuid, $3, $4::jsonb, $5)
                    """,
                    "inventory_item",
                    item_id,
                    "ALLOCATION_REJECTED_NOT_FOUND",
                    json.dumps({
                        "event_id": evt.event_id,
                        "quantity": evt.quantity,
                        "edge_node": cloud_event.edge_node_id,
                    }),
                    evt.user_id,
                )
            finally:
                await conn.close()

        await ctx.run("audit_not_found", audit_not_found)
        return

    # ----- Step 4: Calculate new count -----
    new_count = current_count - evt.quantity

    # ----- Step 5a: Insufficient stock — compensating action -----
    if new_count < 0:
        logger.warning(
            "[HQ] INSUFFICIENT STOCK: item=%s requested=%d available=%d "
            "(edge snapshot was %d)",
            item_id,
            evt.quantity,
            current_count,
            evt.original_count_at_action,
        )

        async def audit_insufficient_stock() -> None:
            conn = await asyncpg.connect(DATABASE_URL)
            try:
                await conn.execute(
                    """
                    INSERT INTO audit_log
                        (entity_type, entity_id, action, payload, actor)
                    VALUES ($1, $2::uuid, $3, $4::jsonb, $5)
                    """,
                    "inventory_item",
                    item_id,
                    "ALLOCATION_REJECTED_INSUFFICIENT",
                    json.dumps({
                        "event_id": evt.event_id,
                        "requested": evt.quantity,
                        "available": current_count,
                        "edge_snapshot": evt.original_count_at_action,
                        "edge_node": cloud_event.edge_node_id,
                    }),
                    evt.user_id,
                )
            finally:
                await conn.close()

        await ctx.run("audit_insufficient", audit_insufficient_stock)
        return

    # ----- Step 5b: Valid allocation — update inventory + audit -----
    logger.info(
        "[HQ] ALLOCATING: item=%s qty=%d available=%d → %d",
        item_id,
        evt.quantity,
        current_count,
        new_count,
    )

    async def apply_allocation() -> None:
        conn = await asyncpg.connect(DATABASE_URL)
        try:
            async with conn.transaction():
                # Deduct from available, add to allocated
                await conn.execute(
                    """
                    UPDATE inventory_items
                    SET available_count = available_count - $1,
                        allocated_count = allocated_count + $1,
                        updated_at = now()
                    WHERE id = $2
                    """,
                    evt.quantity,
                    uuid.UUID(item_id),
                )

                # Record the successful allocation in the audit trail
                await conn.execute(
                    """
                    INSERT INTO audit_log
                        (entity_type, entity_id, action, payload, actor)
                    VALUES ($1, $2::uuid, $3, $4::jsonb, $5)
                    """,
                    "inventory_item",
                    item_id,
                    "ALLOCATED",
                    json.dumps({
                        "event_id": evt.event_id,
                        "quantity": evt.quantity,
                        "previous_available": current_count,
                        "new_available": new_count,
                        "edge_snapshot": evt.original_count_at_action,
                        "edge_node": cloud_event.edge_node_id,
                    }),
                    evt.user_id,
                )
        finally:
            await conn.close()

    await ctx.run("apply_allocation", apply_allocation)


# ---------------------------------------------------------------------------
# Restate App
# ---------------------------------------------------------------------------
app = restate.app([inventory_processor])

# ---------------------------------------------------------------------------
# Serving & Registration
# ---------------------------------------------------------------------------
# Start the handler service:
#   pip install restate-sdk asyncpg protobuf hypercorn
#   python -m hypercorn openddil_hq.inventory_handler:app --bind 0.0.0.0:9080
#
# Register with the Restate server:
#   curl -X POST http://localhost:9070/deployments \
#     -H 'content-type: application/json' \
#     -d '{"uri": "http://host.docker.internal:9080"}'
#
# Restate will discover the InventoryProcessor Virtual Object and begin
# routing events from Redpanda to the handleItemAllocated handler.
# ---------------------------------------------------------------------------

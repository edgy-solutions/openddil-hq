# AGENTS.md — OpenDDIL HQ

Guidelines and safety constraints for AI agents working in this repository.

## Repository Scope

This repo contains **Python Restate Virtual Object handlers** (using the official `restate-sdk`) that consume events from Redpanda and apply them to PostgreSQL with exactly-once durable execution.

This is the **only HQ processor repo** — there is no C# equivalent (Restate has no official .NET SDK). The polyglot architecture works because events are serialized as Protobufs, making them language-agnostic.

## What You CAN Do

- **Add new Virtual Object handlers** for new bounded contexts (e.g., `ShipmentProcessor`).
- **Add new handler methods** to existing Virtual Objects.
- **Modify DB queries** within `ctx.run()` blocks.
- **Run `hypercorn`** to serve and test handlers locally.
- **Update documentation** (README, llms.txt, .cursorrules, this file).

## What You MUST NOT Do

- ❌ **Never call asyncpg outside `ctx.run()`** — breaks durable execution. On replay, non-journaled DB calls would re-execute, causing duplicates or corruption.
- ❌ **Never modify CloudEvent deserialization** — envelope is defined in openddil-contracts.
- ❌ **Never skip audit_log writes** — every outcome (success, rejection, not-found) must be audited.
- ❌ **Never process multiple entity types in one Virtual Object** — each type gets its own VO.
- ❌ **Never assume the handler runs only once** — Restate may replay. That's why `ctx.run()` exists.
- ❌ **Never pass a coroutine to `ctx.run()`** — pass the async function reference, not `await fn()`.

## Adding a New Event Handler

1. Ensure the Protobuf message exists in `openddil-contracts`.
2. Create a new Virtual Object: `my_processor = restate.VirtualObject("MyProcessor")`
3. Key it by the entity's primary ID (accessed via `ctx.key()`).
4. Follow the pattern: deserialize → `ctx.run`: query → decide → `ctx.run`: apply/audit.
5. Register the new VO: `app = restate.app([inventory_processor, my_processor])`
6. Re-register the deployment with Restate Admin API.
7. Update README, llms.txt.

## Documentation Maintenance

After ANY change, update:
1. `README.md` — Handler list, quick-start, architecture diagram.
2. `llms.txt` — Key files, dependencies, handler descriptions.
3. `.cursorrules` — Only if new conventions are introduced.
4. This file — Only if new safety constraints apply.

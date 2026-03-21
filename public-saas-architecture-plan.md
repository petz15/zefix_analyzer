# Public SaaS Architecture Plan for Zefix Analyzer

## Document Metadata
- version: 1.0
- date: 2026-03-18
- audience: engineering, product, operations
- purpose: roadmap to evolve internal monolith into public SaaS
- status: draft

## Executive Summary
This plan describes how to evolve the current single-service FastAPI application into a production-grade public SaaS platform with user management, role-based access control, tenant isolation, subscriptions, and operational reliability.

Recommended strategy:
- Start with a modular monolith.
- Add strong boundaries between modules now.
- Extract services only where scale or team velocity requires it.

## Current State Snapshot
- Stack: FastAPI + Jinja templates + PostgreSQL + Alembic + background jobs.
- App shape: mostly monolithic (UI routes, API routes, jobs in one deployable).
- User model exists but end-to-end authn/authz is not fully enforced on all routes.
- Audit log exists but user attribution is often null.
- No tenant isolation model yet.
- No billing/subscription domain yet.

## Target State
- Secure public web app with hardened auth, sessions, and authorization.
- Multi-tenant SaaS data model.
- Subscription billing with plan entitlements and usage limits.
- Separated web and worker runtime units.
- Observability, backups, incident response, and compliance baseline.

## Guiding Principles
1. Security first: never expose public endpoints before authn/authz and CSRF hardening.
2. Tenant safety over speed: enforce tenant scoping in all queries.
3. Backward-safe migrations: incremental, reversible schema changes.
4. Idempotent job processing: safe retries and cancellation.
5. Instrument before scale: metrics and tracing before aggressive optimization.

## Proposed Modular Boundaries
- identity
  - users, authentication, sessions, password reset, RBAC
- tenants
  - organizations/workspaces, memberships, invitations
- leads
  - companies, notes, scoring, filters, exports
- ingestion
  - zefix collect, enrichment, geocoding, classify, cluster
- jobs
  - queue, worker orchestration, job events, retries
- billing
  - plans, subscriptions, invoices, entitlements, quotas
- platform
  - settings, feature flags, audit, observability, security middleware

## Phase Plan

### Phase 1: Security and Production Baseline (mandatory first)
Goals:
- make the app safe for internet exposure

Work:
- add secure session cookie settings (Secure, HttpOnly, SameSite=Lax/Strict as needed)
- add CSRF protection to all form posts
- add security headers: HSTS, CSP, X-Content-Type-Options, Referrer-Policy
- enforce trusted host and proxy handling for HTTPS reverse proxy
- add centralized error handling and structured request logging
- remove insecure default configuration values in production mode
- add basic rate limiting for auth and expensive routes

Exit criteria:
- all mutating endpoints require CSRF and authenticated session
- security header checks pass on key routes
- no default secrets/credentials allowed in production

### Phase 2: Authentication and Authorization
Goals:
- robust account system and route-level authorization

Work:
- implement login/logout/session rotation
- add password reset flow (email token)
- add RBAC roles: owner, admin, member, readonly
- enforce RBAC in UI and API write operations
- write authenticated user id into audit logs for all changes

Exit criteria:
- every write endpoint protected by auth and permission checks
- unauthorized access tests pass
- audit logs show actor identity for updates

### Phase 3: Tenant Isolation (single DB, row-level app scoping)
Goals:
- multi-tenant foundation without immediate shard complexity

Work:
- add organization and membership tables
- add tenant_id to domain tables (companies, notes, jobs, settings, audit)
- scope all CRUD queries by tenant
- add migration/backfill to move existing data to a default tenant
- add cross-tenant access test suite

Exit criteria:
- no cross-tenant read/write path exists
- exports and jobs are tenant-scoped
- admin operations have explicit safeguards and audit trail

### Phase 4: Subscriptions and Entitlements
Goals:
- monetize safely with plan limits and lifecycle states

Work:
- integrate Stripe checkout and customer portal
- implement webhook processor with signature verification and retries
- add subscription states: trialing, active, past_due, canceled
- implement entitlements and quotas (seats, monthly runs, API usage)
- gate expensive jobs and enrichment features by plan

Exit criteria:
- plan changes update access in near real time
- over-quota requests return clear product messaging
- billing webhook reliability and idempotency tests pass

### Phase 5: Worker and Runtime Separation
Goals:
- improve reliability and scale for long-running tasks

Work:
- split web and worker process deployment units
- keep DB-backed jobs initially, add queue abstraction
- add retry policies, dead-letter state, idempotency keys
- add concurrency controls and per-tenant fairness limits

Exit criteria:
- worker restarts do not corrupt or duplicate jobs
- deploys do not block UI availability
- queue depth and failure metrics visible

### Phase 6: API and Product Hardening
Goals:
- stable external and internal product surface

Work:
- version API routes
- define consistent pagination, filtering, and error schema
- add account pages: profile, org settings, plan, invoices
- add admin support tooling (safe impersonation with audit)

Exit criteria:
- API contract tests pass
- customer account lifecycle is self-serve for common actions

### Phase 7: Reliability, Compliance, and Operations
Goals:
- operational maturity for public SaaS

Work:
- define SLOs and alerts (latency, error rate, queue lag, webhook failures)
- add tracing and metrics dashboards
- backup and restore drills with documented RTO/RPO
- data retention, deletion, and export workflows
- privacy policy, terms, incident response playbook

Exit criteria:
- on-call runbooks tested
- successful restore drill completed
- compliance baseline documented

## Data Model Additions (High Level)
- organizations
- memberships
- invitations
- roles/permissions mapping
- subscriptions
- plans
- entitlements
- usage_counters
- billing_events
- auth_sessions
- password_reset_tokens

## Cross-Cutting Technical Requirements
- idempotency for webhooks and async jobs
- optimistic locking or safe update patterns on mutable records
- transactional boundaries around billing and entitlement updates
- standardized domain events for major state transitions
- feature flags for staged rollout

## Security Checklist
- password hashing policy and rotation strategy
- session invalidation on password change
- brute-force protection on login and reset endpoints
- input validation and output encoding
- dependency vulnerability scanning in CI
- secret management via environment/secret store only
- least-privilege DB and service credentials

## Testing Strategy
- unit tests for domain logic (auth, billing, quotas, tenant scoping)
- integration tests for route protection and cross-tenant isolation
- end-to-end tests for signup, login, checkout, and plan change
- load tests for dashboard and job throughput
- chaos/resilience tests for worker crashes and restarts

## Suggested 30-60-90 Day Rollout

### Day 0-30
- complete Phase 1
- start Phase 2 core login/session/RBAC
- add baseline observability and alerts

### Day 31-60
- complete Phase 2
- implement Phase 3 tenant schema + query scoping
- backfill existing data and run tenant isolation tests

### Day 61-90
- implement Phase 4 billing and entitlements
- split worker runtime (Phase 5 core)
- run production readiness and restore drills

## Risks and Mitigations
- risk: tenant leakage through unscoped query
  - mitigation: enforce tenant context in shared query helpers and test gates
- risk: billing webhook race conditions
  - mitigation: idempotency keys + event version checks + retries
- risk: long jobs harming interactive UX
  - mitigation: worker separation, queue backpressure, per-tenant quotas
- risk: migration regressions
  - mitigation: staged migrations, shadow verification, rollback plan

## Minimal First Public Launch Scope (if time-constrained)
- required:
  - Phase 1 fully complete
  - Phase 2 auth + RBAC complete
  - Phase 3 tenant isolation complete
  - basic Stripe subscription and plan gate for at least one paid feature
- can defer:
  - advanced analytics dashboards
  - enterprise SSO (SAML/SCIM)
  - deep microservice decomposition

## Backlog Format for LLM Ingestion
Use this template per task:

- id: ARCH-001
- title: Add secure session middleware
- phase: 1
- priority: P0
- owner: backend
- estimate: 2d
- dependencies: []
- acceptance_criteria:
  - session cookie has secure attributes in production
  - session rotates on login
  - logout invalidates server-side session
- test_cases:
  - unauthenticated request to protected endpoint returns 401/redirect
  - authenticated request succeeds

## Feature Improvements Implemented (current monolith)

These features were added to the internal monolith prior to the SaaS migration and
should be preserved/ported during tenant scoping.

### Claude classification cost optimisation
- Prompt caching: system prompt sent with `cache_control: ephemeral` — cached reads cost ~10% of normal input price.
- Multi-company messages: N companies packed into one API call with JSON-array response (`companies_per_message`, default 10).
- Anthropic Message Batches API path: all requests submitted in one batch call, 50% discount, polled until done.
- Sort order: companies processed in descending `zefix_score` + ascending distance-to-origin so the highest-value companies are classified first when a limit is applied.
- Combined saving: ~10–15× cheaper per 1k companies vs original per-request approach.

### Purpose boilerplate stripping
- `boilerplate_patterns` DB table stores regex patterns (with description, example, match count, active flag).
- `strip_purpose_boilerplate(text, patterns)` splits purpose text into sentences and drops any matching a stored regex before the text is sent to Claude — reduces input tokens for boilerplate-heavy registrations.
- Analysis script: `scripts/analyze_boilerplate.py` — counts sentence frequency across all purpose texts, prints top candidates, and supports `--insert` for interactive review and DB insertion. Accepts `--db-url` to run outside the container.
- Patterns managed via Settings UI (list, add, toggle active, delete) without code changes.

### Google scoring improvements
- Address/ZIP/street matching added to location scoring (max +45 pts from municipality + canton + ZIP + street).
- Directory domains (moneyhouse, local.ch, etc.) hard-return score 0 instead of applying a penalty.
- `social_media_only` flag set when the best Google result is a social media domain; passed to Claude context.

### Negative scoring
- `scoring_exclude_clusters` and `scoring_exclude_keywords` settings with configurable point deductions.

## Decision Log
- choose modular monolith first to reduce migration risk
- choose single database tenant scoping before sharding
- choose Stripe for initial subscription implementation
- defer service extraction until measured bottlenecks appear

## Next Actionable Work Items
1. Implement production config validation and security middleware hardening.
2. Wire route guards and role checks on all mutating UI/API endpoints.
3. Introduce organization and tenant_id schema migration plan.
4. Define Stripe product-plan-entitlement mapping.
5. Split web and worker processes in deployment manifests.

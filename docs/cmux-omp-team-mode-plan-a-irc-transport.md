# Plan A — omp cross-process irc transport (team bridge core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let two omp processes exchange `irc` messages through a lead-hosted unix-socket broker, with remote peers visible in each side's `AgentRegistry`, testable headless (no cmux).

**Architecture:** Each omp process keeps its existing in-process `IrcBus` + `AgentRegistry`. A new `TeamBridge` connects processes over a line-delimited-JSON unix socket (the same pattern as `tools/browser/cmux/socket-client.ts`): the lead runs a broker (server), each child runs a connector (client). A remote agent is an `AgentRef` with `remote: true` and `session: null`; `IrcBus.send` routes those through a pluggable remote router (the bridge) instead of `session.deliverIrcMessage`. The receiving process delivers the frame into its own local bus (the `/collab` "republish on the peer's local bus" pattern). No encryption/auth: same-machine unix socket only.

**Tech Stack:** TypeScript, Bun, `node:net` unix sockets, arktype (existing), `bun test`.

**Scope note:** This is Plan A of the `cmux omp` team-mode feature (spec: `docs/cmux-omp-team-mode.md`). It delivers the transport core as standalone, tested software. Follow-on plans: B = team orchestrator command + cmux-mediated tab spawn + worktree lifecycle; C = registry/sidebar + notifications wiring; D = the `cmux omp` Swift launch arm in the cmux repo. Do not start B-D from this plan.

**Non-goals here:** spawning tabs, cmux at all, worktree creation, the orchestrator/team CLI command, sidebar. Pure transport + bus/registry integration, exercised with a fake session.

---

### Task 1: Mark remote agents in the registry

**Files:**
- Modify: `packages/coding-agent/src/registry/agent-registry.ts` (`AgentRef` ~33-46, `RegisterInput` ~55-63, `register` ~83-99)
- Test: `packages/coding-agent/src/registry/agent-registry.test.ts` (create if absent)

- [ ] **Step 1: Write the failing test**

```ts
import { describe, expect, it } from "bun:test";
import { AgentRegistry } from "./agent-registry";

describe("AgentRegistry remote refs", () => {
  it("marks remote peers and keeps them visible", () => {
    const r = new AgentRegistry();
    r.register({ id: "Main", displayName: "Main", kind: "main", session: null });
    r.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: null, remote: true, status: "idle" });
    expect(r.get("ChildA")?.remote).toBe(true);
    // remote peers (session null) still show in the peer roster
    expect(r.listVisibleTo("Main").map(x => x.id)).toContain("ChildA");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/coding-agent && bun test src/registry/agent-registry.test.ts`
Expected: FAIL (`remote` is not a property of `AgentRef`).

- [ ] **Step 3: Add the `remote` field**

In `agent-registry.ts`, add to the `AgentRef` interface (after `activity?: string;`):

```ts
	/** True when this ref is a peer in another process, reached via the team bridge (session is always null). */
	remote?: boolean;
```

Add to `RegisterInput` (after `status?: AgentStatus;`):

```ts
	remote?: boolean;
```

In `register`, set it on the constructed ref (after `sessionFile: input.sessionFile ?? null,`):

```ts
			remote: input.remote ?? false,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/coding-agent && bun test src/registry/agent-registry.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/coding-agent/src/registry/agent-registry.ts packages/coding-agent/src/registry/agent-registry.test.ts
git commit -m "feat(irc): mark remote agent refs in the registry"
```

---

### Task 2: Line-delimited JSON unix-socket transport

**Files:**
- Create: `packages/coding-agent/src/irc/transport.ts`
- Test: `packages/coding-agent/src/irc/transport.test.ts`

Frame protocol (one JSON object per `\n`-terminated line):
- `{ t: "hello", agents: PeerAgent[] }` — connector -> broker on connect (announce local agents)
- `{ t: "roster", agents: PeerAgent[] }` — broker -> connector (current remote roster for that peer)
- `{ t: "irc", reqId: string, msg: IrcMessage }` — either direction: deliver this message to a local agent on the receiver
- `{ t: "receipt", reqId: string, receipt: IrcDeliveryReceipt }` — response to an `irc` frame

where `PeerAgent = { id: string; displayName: string; kind: "main" | "sub" }`.

- [ ] **Step 1: Write the failing test**

```ts
import { describe, expect, it } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JsonSocketServer, connectJsonSocket } from "./transport";

describe("json socket transport", () => {
  it("round-trips line-delimited frames over a unix socket", async () => {
    const sockPath = join(mkdtempSync(join(tmpdir(), "omp-irc-")), "s.sock");
    const got: unknown[] = [];
    const server = new JsonSocketServer();
    server.onConnection(conn => conn.onFrame(f => { got.push(f); conn.send({ t: "ack" }); }));
    await server.listen(sockPath);

    const client = await connectJsonSocket(sockPath);
    const acks: unknown[] = [];
    client.onFrame(f => acks.push(f));
    client.send({ t: "hello", agents: [] });
    await Bun.sleep(50);

    expect(got).toEqual([{ t: "hello", agents: [] }]);
    expect(acks).toEqual([{ t: "ack" }]);
    client.close();
    await server.close();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/coding-agent && bun test src/irc/transport.test.ts`
Expected: FAIL (module `./transport` not found).

- [ ] **Step 3: Implement the transport**

```ts
import * as net from "node:net";

export interface JsonConn {
  send(frame: unknown): void;
  onFrame(cb: (frame: unknown) => void): void;
  onClose(cb: () => void): void;
  close(): void;
}

function wrap(socket: net.Socket): JsonConn {
  let buf = "";
  const frameCbs: ((f: unknown) => void)[] = [];
  const closeCbs: (() => void)[] = [];
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    buf += chunk;
    let nl: number;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (!line) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        continue; // ignore malformed lines
      }
      for (const cb of frameCbs) cb(parsed);
    }
  });
  socket.on("close", () => { for (const cb of closeCbs) cb(); });
  socket.on("error", () => { /* surfaced via close */ });
  return {
    send: frame => { socket.write(`${JSON.stringify(frame)}\n`); },
    onFrame: cb => { frameCbs.push(cb); },
    onClose: cb => { closeCbs.push(cb); },
    close: () => socket.destroy(),
  };
}

export class JsonSocketServer {
  #server: net.Server | undefined;
  #onConn: ((conn: JsonConn) => void)[] = [];

  onConnection(cb: (conn: JsonConn) => void): void {
    this.#onConn.push(cb);
  }

  listen(socketPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const server = net.createServer(socket => {
        const conn = wrap(socket);
        for (const cb of this.#onConn) cb(conn);
      });
      server.on("error", reject);
      server.listen(socketPath, () => {
        server.off("error", reject);
        this.#server = server;
        resolve();
      });
    });
  }

  close(): Promise<void> {
    return new Promise(resolve => {
      if (!this.#server) return resolve();
      this.#server.close(() => resolve());
    });
  }
}

export function connectJsonSocket(socketPath: string, timeoutMs = 10_000): Promise<JsonConn> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    const timer = setTimeout(() => { socket.destroy(); reject(new Error("irc socket connect timeout")); }, timeoutMs);
    socket.once("connect", () => { clearTimeout(timer); resolve(wrap(socket)); });
    socket.once("error", err => { clearTimeout(timer); reject(err); });
  });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/coding-agent && bun test src/irc/transport.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/coding-agent/src/irc/transport.ts packages/coding-agent/src/irc/transport.test.ts
git commit -m "feat(irc): line-delimited json unix-socket transport"
```

---

### Task 3: Pluggable remote router in IrcBus

**Files:**
- Modify: `packages/coding-agent/src/irc/bus.ts` (`IrcBus` class; `send` ~95-155)
- Test: `packages/coding-agent/src/irc/bus-remote.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
import { describe, expect, it } from "bun:test";
import { AgentRegistry } from "../registry/agent-registry";
import { IrcBus } from "./bus";

describe("IrcBus remote routing", () => {
  it("routes sends to remote refs through the remote router", async () => {
    const reg = new AgentRegistry();
    reg.register({ id: "Main", displayName: "Main", kind: "main", session: null });
    reg.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: null, remote: true, status: "idle" });
    const bus = new IrcBus(reg);
    const seen: string[] = [];
    bus.setRemoteRouter({
      deliver: async msg => { seen.push(`${msg.from}->${msg.to}:${msg.body}`); return { to: msg.to, outcome: "injected" }; },
    });

    const receipt = await bus.send({ from: "Main", to: "ChildA", body: "hi" });
    expect(receipt.outcome).toBe("injected");
    expect(seen).toEqual(["Main->ChildA:hi"]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/coding-agent && bun test src/irc/bus-remote.test.ts`
Expected: FAIL (`setRemoteRouter` is not a function).

- [ ] **Step 3: Add the remote router hook**

In `bus.ts`, add the interface near the top (after the `IrcDeliveryReceipt` interface):

```ts
export interface IrcRemoteRouter {
	/** Deliver a message addressed to a remote (other-process) peer. */
	deliver(message: IrcMessage, opts?: { expectsReply?: boolean }): Promise<IrcDeliveryReceipt>;
}
```

Add a private field + setter to the `IrcBus` class (next to `#mailboxes`):

```ts
	#remoteRouter: IrcRemoteRouter | undefined;

	setRemoteRouter(router: IrcRemoteRouter | undefined): void {
		this.#remoteRouter = router;
	}
```

In `send`, insert the remote branch immediately after the advisor-kind check block (after the `if (ref.kind === "advisor") { ... }` block, before the `let revived = false;` line):

```ts
		if (ref.remote) {
			if (!this.#remoteRouter) {
				return { to: message.to, outcome: "failed", error: `No team bridge for remote agent "${message.to}".` };
			}
			return this.#remoteRouter.deliver(message, opts);
		}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd packages/coding-agent && bun test src/irc/bus-remote.test.ts`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/coding-agent/src/irc/bus.ts packages/coding-agent/src/irc/bus-remote.test.ts
git commit -m "feat(irc): pluggable remote router on IrcBus.send"
```

---

### Task 4: TeamBridge (broker + connector)

**Files:**
- Create: `packages/coding-agent/src/irc/bridge.ts`
- Test: covered by Task 5's integration test.

The bridge wires a process's `IrcBus` + `AgentRegistry` to the socket. Broker (lead): accept connectors, register their announced agents as remote refs, route the lead bus's remote sends to the owning connector, relay child<->child, broadcast roster. Connector (child): announce local agents, set the child bus's remote router to forward to the broker, register the roster as remote refs, deliver inbound frames into the local bus.

- [ ] **Step 1: Write the implementation**

```ts
import { randomUUID } from "node:crypto";
import type { AgentRegistry } from "../registry/agent-registry";
import type { IrcBus, IrcDeliveryReceipt, IrcMessage, IrcRemoteRouter } from "./bus";
import { type JsonConn, JsonSocketServer, connectJsonSocket } from "./transport";

export interface PeerAgent {
	id: string;
	displayName: string;
	kind: "main" | "sub";
}

interface IrcFrame {
	t: "irc";
	reqId: string;
	msg: IrcMessage;
}

const REQUEST_TIMEOUT_MS = 30_000;

/** Correlate outbound irc frames to their receipt frames over one conn. */
function makeReceiptWaiter() {
	const pending = new Map<string, (r: IrcDeliveryReceipt) => void>();
	return {
		track(reqId: string): Promise<IrcDeliveryReceipt> {
			return new Promise(resolve => {
				const timer = setTimeout(() => {
					pending.delete(reqId);
					resolve({ to: "", outcome: "failed", error: "team bridge receipt timeout" });
				}, REQUEST_TIMEOUT_MS);
				timer.unref?.();
				pending.set(reqId, r => { clearTimeout(timer); resolve(r); });
			});
		},
		settle(reqId: string, receipt: IrcDeliveryReceipt): void {
			pending.get(reqId)?.(receipt);
			pending.delete(reqId);
		},
	};
}

/** Deliver an inbound frame to a LOCAL agent via this process's bus; reply a receipt. */
async function deliverInbound(bus: IrcBus, conn: JsonConn, frame: IrcFrame): Promise<void> {
	let receipt: IrcDeliveryReceipt;
	try {
		receipt = await bus.send({ from: frame.msg.from, to: frame.msg.to, body: frame.msg.body, replyTo: frame.msg.replyTo });
	} catch (error) {
		receipt = { to: frame.msg.to, outcome: "failed", error: error instanceof Error ? error.message : String(error) };
	}
	conn.send({ t: "receipt", reqId: frame.reqId, receipt });
}

/** Lead-side broker. */
export class TeamBroker {
	#server = new JsonSocketServer();
	readonly #connOf = new Map<string, JsonConn>(); // agentId -> owning connector
	readonly #agentsOf = new Map<JsonConn, PeerAgent[]>();
	readonly #waiter = makeReceiptWaiter();

	constructor(
		private readonly bus: IrcBus,
		private readonly registry: AgentRegistry,
	) {}

	async listen(socketPath: string): Promise<void> {
		this.#server.onConnection(conn => this.#onConn(conn));
		await this.#server.listen(socketPath);
		const router: IrcRemoteRouter = { deliver: msg => this.#routeTo(this.#connOf.get(msg.to), msg) };
		this.bus.setRemoteRouter(router);
	}

	#onConn(conn: JsonConn): void {
		conn.onFrame(raw => {
			const frame = raw as { t?: string };
			if (frame.t === "hello") this.#onHello(conn, (raw as { agents: PeerAgent[] }).agents);
			else if (frame.t === "irc") void this.#onIrc(conn, raw as IrcFrame);
			else if (frame.t === "receipt") {
				const r = raw as { reqId: string; receipt: IrcDeliveryReceipt };
				this.#waiter.settle(r.reqId, r.receipt);
			}
		});
		conn.onClose(() => this.#onClose(conn));
	}

	#onHello(conn: JsonConn, agents: PeerAgent[]): void {
		this.#agentsOf.set(conn, agents);
		for (const a of agents) {
			this.#connOf.set(a.id, conn);
			this.registry.register({ id: a.id, displayName: a.displayName, kind: a.kind, session: null, remote: true, status: "idle" });
		}
		this.#broadcastRoster();
	}

	async #onIrc(from: JsonConn, frame: IrcFrame): Promise<void> {
		const targetConn = this.#connOf.get(frame.msg.to);
		if (!targetConn) {
			// Target is a local lead agent (e.g. Main): deliver here.
			await deliverInbound(this.bus, from, frame);
			return;
		}
		// child -> child relay: forward and pipe the receipt back to the origin.
		const receipt = await this.#routeTo(targetConn, frame.msg);
		from.send({ t: "receipt", reqId: frame.reqId, receipt });
	}

	#routeTo(conn: JsonConn | undefined, msg: IrcMessage): Promise<IrcDeliveryReceipt> {
		if (!conn) return Promise.resolve({ to: msg.to, outcome: "failed", error: `No connector for "${msg.to}".` });
		const reqId = randomUUID();
		const p = this.#waiter.track(reqId);
		conn.send({ t: "irc", reqId, msg });
		return p;
	}

	#onClose(conn: JsonConn): void {
		const agents = this.#agentsOf.get(conn) ?? [];
		for (const a of agents) {
			this.#connOf.delete(a.id);
			this.registry.setStatus(a.id, "aborted");
			this.registry.unregister(a.id);
		}
		this.#agentsOf.delete(conn);
		this.#broadcastRoster();
	}

	#broadcastRoster(): void {
		const all = [...this.#agentsOf.entries()];
		for (const [conn, own] of all) {
			const ownIds = new Set(own.map(a => a.id));
			const others = all.flatMap(([, a]) => a).filter(a => !ownIds.has(a.id));
			conn.send({ t: "roster", agents: others });
		}
	}

	async close(): Promise<void> {
		this.bus.setRemoteRouter(undefined);
		await this.#server.close();
	}
}

/** Child-side connector. */
export class TeamConnector {
	#conn: JsonConn | undefined;
	readonly #waiter = makeReceiptWaiter();

	constructor(
		private readonly bus: IrcBus,
		private readonly registry: AgentRegistry,
		private readonly localAgents: PeerAgent[],
	) {}

	async connect(socketPath: string): Promise<void> {
		const conn = await connectJsonSocket(socketPath);
		this.#conn = conn;
		conn.onFrame(raw => {
			const frame = raw as { t?: string };
			if (frame.t === "roster") this.#onRoster((raw as { agents: PeerAgent[] }).agents);
			else if (frame.t === "irc") void deliverInbound(this.bus, conn, raw as IrcFrame);
			else if (frame.t === "receipt") {
				const r = raw as { reqId: string; receipt: IrcDeliveryReceipt };
				this.#waiter.settle(r.reqId, r.receipt);
			}
		});
		conn.send({ t: "hello", agents: this.localAgents });
		const router: IrcRemoteRouter = { deliver: msg => this.#sendToBroker(msg) };
		this.bus.setRemoteRouter(router);
	}

	#sendToBroker(msg: IrcMessage): Promise<IrcDeliveryReceipt> {
		if (!this.#conn) return Promise.resolve({ to: msg.to, outcome: "failed", error: "team bridge not connected" });
		const reqId = randomUUID();
		const p = this.#waiter.track(reqId);
		this.#conn.send({ t: "irc", reqId, msg });
		return p;
	}

	#onRoster(agents: PeerAgent[]): void {
		for (const a of agents) {
			if (this.registry.get(a.id)) continue;
			this.registry.register({ id: a.id, displayName: a.displayName, kind: a.kind, session: null, remote: true, status: "idle" });
		}
	}

	close(): void {
		this.bus.setRemoteRouter(undefined);
		this.#conn?.close();
	}
}
```

- [ ] **Step 2: Commit (no standalone test; exercised in Task 5)**

```bash
git add packages/coding-agent/src/irc/bridge.ts
git commit -m "feat(irc): team bridge broker + connector"
```

---

### Task 5: Headless integration test — two nodes irc each other

**Files:**
- Create: `packages/coding-agent/src/irc/bridge.test.ts`

This wires a broker node (lead, with a fake `Main` session) and a connector node (child, with a fake `ChildA` session) over a real temp unix socket, in one test process, and asserts bidirectional delivery + roster sync.

- [ ] **Step 1: Write the test**

```ts
import { describe, expect, it } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AgentRegistry } from "../registry/agent-registry";
import { IrcBus, type IrcMessage } from "./bus";
import { TeamBroker, TeamConnector } from "./bridge";

// Minimal fake AgentSession: records delivered messages, reports "injected".
function fakeSession(sink: IrcMessage[]) {
  return {
    deliverIrcMessage: async (msg: IrcMessage) => { sink.push(msg); return "injected" as const; },
  } as unknown as Parameters<AgentRegistry["register"]>[0]["session"];
}

describe("team bridge end to end", () => {
  it("delivers irc lead<->child across the socket", async () => {
    const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-")), "irc.sock");

    // Lead node
    const leadReg = new AgentRegistry();
    const leadBus = new IrcBus(leadReg);
    const leadInbox: IrcMessage[] = [];
    leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession(leadInbox) });
    const broker = new TeamBroker(leadBus, leadReg);
    await broker.listen(sockPath);

    // Child node
    const childReg = new AgentRegistry();
    const childBus = new IrcBus(childReg);
    const childInbox: IrcMessage[] = [];
    childReg.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: fakeSession(childInbox) });
    const connector = new TeamConnector(childBus, childReg, [{ id: "ChildA", displayName: "ChildA", kind: "sub" }]);
    await connector.connect(sockPath);
    await Bun.sleep(50); // let hello + roster settle

    // Roster sync: lead sees ChildA as a remote peer; child sees Main.
    expect(leadReg.get("ChildA")?.remote).toBe(true);
    expect(childReg.get("Main")?.remote).toBe(true);

    // lead -> child
    const r1 = await leadBus.send({ from: "Main", to: "ChildA", body: "do the thing" });
    expect(r1.outcome).toBe("injected");
    expect(childInbox.map(m => m.body)).toEqual(["do the thing"]);

    // child -> lead
    const r2 = await childBus.send({ from: "ChildA", to: "Main", body: "diverged: did X instead" });
    expect(r2.outcome).toBe("injected");
    expect(leadInbox.map(m => m.body)).toEqual(["diverged: did X instead"]);

    connector.close();
    await broker.close();
  });
});
```

- [ ] **Step 2: Run the test**

Run: `cd packages/coding-agent && bun test src/irc/bridge.test.ts`
Expected: PASS. If timing flakes, raise the `Bun.sleep` to 100ms (handshake is async).

- [ ] **Step 3: Run the full irc suite to confirm no regressions**

Run: `cd packages/coding-agent && bun test src/irc/ src/registry/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add packages/coding-agent/src/irc/bridge.test.ts
git commit -m "test(irc): headless team-bridge end-to-end irc round-trip"
```

---

## Self-Review

**Spec coverage (vs `docs/cmux-omp-team-mode.md` layer 1 "cross-process irc transport"):** the bridge makes `IrcBus`/`AgentRegistry` peer-aware over a unix socket (Tasks 1, 3, 4); remote-routing plugs into `IrcBus.send` exactly where `session.deliverIrcMessage` is today (Task 3, grounded in `bus.ts:134-142`); the headless no-cmux test path from the spec's testing section is Task 5. Layers 2-4 (spawn, sidebar, cmux arm) are explicitly out of scope and deferred to Plans B-D.

**Placeholder scan:** none — every step has full code, exact paths, exact `bun test` commands.

**Type consistency:** `PeerAgent`, `IrcFrame`, `IrcRemoteRouter`, `IrcDeliveryReceipt`, `IrcMessage` are used identically across Tasks 2-5; `IrcRemoteRouter.deliver` signature matches its consumer in `bus.ts` (Task 3) and both producers (`TeamBroker`/`TeamConnector`, Task 4); `register({... remote, status ...})` matches the `RegisterInput` extended in Task 1.

**Known follow-ups (not gaps in this plan):** receipts collapse `woken`/`revived` to whatever the receiver returns (fine — the bridge is transport, semantics stay on the receiving bus); `wait`-based replies across processes work because the receiver's local `bus.send` resolves a local waiter or the session; child<->child relay is via the broker (Task 4 `#onIrc`).

**Review fixes applied (socket lifecycle hardening):** `TeamBroker.close()` now closes live connector sockets before awaiting the server (else `net.Server.close()` hangs); `TeamConnector` installs an `onClose` that clears the remote router and unregisters the remote refs it added (else a dropped socket leaks idle refs that hang the receipt timeout); `JsonSocketServer.listen` keeps a durable `error` handler after listen; `#onHello` skips ids a non-remote local ref already owns (no clobbering `Main`); `#broadcastRoster` advertises only live (`running`/`idle`) lead agents. Still deferred: dynamic roster membership (subagents added/removed after connect) is deferred to Plan B via `registry.onChange` rebroadcast.

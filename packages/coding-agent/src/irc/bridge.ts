import { randomUUID } from "node:crypto";
import type { AgentRegistry, AgentStatus } from "../registry/agent-registry";
import type { IrcBus, IrcDeliveryReceipt, IrcMessage, IrcRemoteRouter } from "./bus";
import { connectJsonSocket, type JsonConn, JsonSocketServer } from "./transport";

export interface PeerAgent {
	id: string;
	displayName: string;
	kind: "main" | "sub";
	status: AgentStatus;
	activity?: string;
}

/** Identity of a local agent this process announces; its live status/activity is read from the registry at announce time. */
type LocalAgent = Pick<PeerAgent, "id" | "displayName" | "kind">;

interface HelloFrame {
	t: "hello";
	agents: PeerAgent[];
}
interface RosterFrame {
	t: "roster";
	agents: PeerAgent[];
}
interface IrcFrame {
	t: "irc";
	reqId: string;
	msg: IrcMessage;
}
interface ReceiptFrame {
	t: "receipt";
	reqId: string;
	receipt: IrcDeliveryReceipt;
}
type WireFrame = HelloFrame | RosterFrame | IrcFrame | ReceiptFrame;

const REQUEST_TIMEOUT_MS = 30_000;

/**
 * Narrow an inbound socket payload to a known frame; unrecognized shapes are
 * dropped. Same-machine IPC, so a lightweight discriminant + field-presence
 * guard is enough — the inner payloads are trusted producers on this socket.
 */
function asFrame(raw: unknown): WireFrame | undefined {
	if (typeof raw !== "object" || raw === null || !("t" in raw)) return undefined;
	const t = raw.t;
	if (t === "hello" && "agents" in raw) return { t, agents: raw.agents as PeerAgent[] };
	if (t === "roster" && "agents" in raw) return { t, agents: raw.agents as PeerAgent[] };
	if (t === "irc" && "reqId" in raw && "msg" in raw) {
		return { t, reqId: raw.reqId as string, msg: raw.msg as IrcMessage };
	}
	if (t === "receipt" && "reqId" in raw && "receipt" in raw) {
		return { t, reqId: raw.reqId as string, receipt: raw.receipt as IrcDeliveryReceipt };
	}
	return undefined;
}

/**
 * Correlate outbound irc frames to their receipt frames. Each pending request
 * remembers its owning conn + target so an owning conn's close can fail-fast
 * its in-flight receipts (peer dropped before acking) instead of stalling the
 * full receipt timeout.
 */
function makeReceiptWaiter() {
	interface Pending {
		owner: JsonConn | undefined;
		to: string;
		settle: (r: IrcDeliveryReceipt) => void;
	}
	const pending = new Map<string, Pending>();
	const fail = (entry: Pending, error: string) => entry.settle({ to: entry.to, outcome: "failed", error });
	return {
		track(reqId: string, owner?: JsonConn, to = ""): Promise<IrcDeliveryReceipt> {
			const { promise, resolve } = Promise.withResolvers<IrcDeliveryReceipt>();
			const timer = setTimeout(() => {
				pending.delete(reqId);
				resolve({ to, outcome: "failed", error: "team bridge receipt timeout" });
			}, REQUEST_TIMEOUT_MS);
			timer.unref?.();
			pending.set(reqId, {
				owner,
				to,
				settle: r => {
					clearTimeout(timer);
					resolve(r);
				},
			});
			return promise;
		},
		settle(reqId: string, receipt: IrcDeliveryReceipt): void {
			const entry = pending.get(reqId);
			if (!entry) return;
			pending.delete(reqId);
			entry.settle(receipt);
		},
		/** Fail every in-flight receipt owned by `owner` (its conn just closed). */
		failOwner(owner: JsonConn, error: string): void {
			for (const [reqId, entry] of pending) {
				if (entry.owner !== owner) continue;
				pending.delete(reqId);
				fail(entry, error);
			}
		},
		/** Fail every in-flight receipt (this side's only conn dropped). */
		failAll(error: string): void {
			for (const [reqId, entry] of pending) {
				pending.delete(reqId);
				fail(entry, error);
			}
		},
	};
}

/** Deliver an inbound frame to a LOCAL agent via this process's bus; reply a receipt. */
async function deliverInbound(bus: IrcBus, conn: JsonConn, frame: IrcFrame): Promise<void> {
	let receipt: IrcDeliveryReceipt;
	// `expectsReply` is intentionally not propagated cross-process: the
	// synchronous in-batch auto-reply it gates cannot span processes. A remote
	// recipient replies via a real turn over the bridge, never an inline reply.
	try {
		receipt = await bus.send({
			from: frame.msg.from,
			to: frame.msg.to,
			body: frame.msg.body,
			replyTo: frame.msg.replyTo,
		});
	} catch (error) {
		receipt = { to: frame.msg.to, outcome: "failed", error: error instanceof Error ? error.message : String(error) };
	}
	conn.send({ t: "receipt", reqId: frame.reqId, receipt });
}

/**
 * Register or update a remote peer ref from an announced {@link PeerAgent}.
 * A real local ref this process owns is never clobbered (a remote stub with
 * session:null would break local delivery). Returns true when a NEW ref was
 * registered, so callers can track which ids they own for later withdrawal.
 */
function upsertRemoteRef(registry: AgentRegistry, peer: PeerAgent): boolean {
	const existing = registry.get(peer.id);
	if (existing) {
		if (!existing.remote) return false;
		registry.setStatus(peer.id, peer.status);
		if (peer.activity !== undefined) registry.setActivity(peer.id, peer.activity);
		return false;
	}
	registry.register({
		id: peer.id,
		displayName: peer.displayName,
		kind: peer.kind,
		session: null,
		remote: true,
		status: peer.status,
	});
	if (peer.activity !== undefined) registry.setActivity(peer.id, peer.activity);
	return true;
}

/** Lead-side broker. */
export class TeamBroker {
	#server = new JsonSocketServer();
	readonly #connOf = new Map<string, JsonConn>(); // agentId -> owning connector
	readonly #agentsOf = new Map<JsonConn, PeerAgent[]>();
	readonly #waiter = makeReceiptWaiter();
	#unsubscribeRegistry: (() => void) | undefined;
	#rosterScheduled = false;

	constructor(
		private readonly bus: IrcBus,
		private readonly registry: AgentRegistry,
	) {}

	async listen(socketPath: string): Promise<void> {
		this.#server.onConnection(conn => this.#onConn(conn));
		await this.#server.listen(socketPath);
		const router: IrcRemoteRouter = { deliver: msg => this.#routeTo(this.#connOf.get(msg.to), msg) };
		this.bus.setRemoteRouter(router);
		// Dynamic membership: re-announce the roster when agents come or go after
		// the initial handshake (later-spawned subagents, agents that finish).
		// Debounced to one broadcast per microtask so a burst of register/status
		// changes collapses into a single roster frame.
		this.#unsubscribeRegistry = this.registry.onChange(() => this.#scheduleRosterBroadcast());
	}

	#scheduleRosterBroadcast(): void {
		if (this.#rosterScheduled) return;
		this.#rosterScheduled = true;
		queueMicrotask(() => {
			this.#rosterScheduled = false;
			this.#broadcastRoster();
		});
	}

	#onConn(conn: JsonConn): void {
		conn.onFrame(raw => {
			const frame = asFrame(raw);
			if (!frame) return;
			if (frame.t === "hello") this.#onHello(conn, frame.agents);
			else if (frame.t === "irc") void this.#onIrc(conn, frame);
			else if (frame.t === "receipt") this.#waiter.settle(frame.reqId, frame.receipt);
		});
		conn.onClose(() => this.#onClose(conn));
	}

	#onHello(conn: JsonConn, agents: PeerAgent[]): void {
		this.#agentsOf.set(conn, agents);
		for (const a of agents) {
			// Don't let an announced id clobber a real local ref the lead owns
			// (e.g. Main): a remote stub with session:null would break local
			// delivery. upsertRemoteRef repeats this guard for the connector side.
			const existing = this.registry.get(a.id);
			if (existing && !existing.remote) continue;
			this.#connOf.set(a.id, conn);
			upsertRemoteRef(this.registry, a);
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
		const p = this.#waiter.track(reqId, conn, msg.to);
		conn.send({ t: "irc", reqId, msg });
		return p;
	}

	/** True while `id` has announced over a still-open conn (used to reap a placeholder whose child never connected). */
	isConnected(id: string): boolean {
		return this.#connOf.has(id);
	}

	#onClose(conn: JsonConn): void {
		// Settle any irc the lead routed to this conn before it acked: the peer
		// dropped mid-flight, so fail now instead of stalling the receipt timeout.
		this.#waiter.failOwner(conn, "peer disconnected");
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
		// The lead's own in-process agents (e.g. Main) are not announced by any
		// connector, so include them in every connector's roster too — otherwise
		// a child never learns about lead-local peers.
		const leadAgents: PeerAgent[] = this.registry
			.list()
			.filter(ref => !ref.remote && ref.kind !== "advisor" && (ref.status === "running" || ref.status === "idle"))
			.map(ref => ({
				id: ref.id,
				displayName: ref.displayName,
				kind: ref.kind === "main" ? "main" : "sub",
				status: ref.status,
				activity: ref.activity,
			}));
		const connectorAgents = all.flatMap(([, a]) => a);
		for (const [conn, own] of all) {
			const ownIds = new Set(own.map(a => a.id));
			const others = [...connectorAgents, ...leadAgents].filter(a => !ownIds.has(a.id));
			conn.send({ t: "roster", agents: others });
		}
	}

	async close(): Promise<void> {
		this.#unsubscribeRegistry?.();
		this.#unsubscribeRegistry = undefined;
		this.bus.setRemoteRouter(undefined);
		// net.Server.close() waits for live connections to end, so close the
		// connector sockets first or this would hang while peers stay connected.
		for (const conn of this.#agentsOf.keys()) conn.close();
		// Drop membership so a roster microtask queued just before close (by a
		// final onChange) broadcasts over an empty set instead of writing to the
		// now-destroyed sockets.
		this.#agentsOf.clear();
		this.#connOf.clear();
		await this.#server.close();
	}
}

/** Child-side connector. */
export class TeamConnector {
	#conn: JsonConn | undefined;
	readonly #waiter = makeReceiptWaiter();
	readonly #remoteIds = new Set<string>();
	#unsubscribeRegistry: (() => void) | undefined;
	#helloScheduled = false;

	constructor(
		private readonly bus: IrcBus,
		private readonly registry: AgentRegistry,
		private readonly localAgents: readonly LocalAgent[],
	) {}

	async connect(socketPath: string): Promise<void> {
		const conn = await connectJsonSocket(socketPath);
		this.#conn = conn;
		conn.onFrame(raw => {
			const frame = asFrame(raw);
			if (!frame) return;
			if (frame.t === "roster") this.#onRoster(frame.agents);
			else if (frame.t === "irc") void deliverInbound(this.bus, conn, frame);
			else if (frame.t === "receipt") this.#waiter.settle(frame.reqId, frame.receipt);
		});
		conn.onClose(() => {
			// Socket dropped: tear down the remote router and withdraw the remote
			// refs we registered, else later sends hang the receipt timeout
			// against a dead conn.
			this.bus.setRemoteRouter(undefined);
			this.#conn = undefined;
			this.#unsubscribeRegistry?.();
			this.#unsubscribeRegistry = undefined;
			for (const id of this.#remoteIds) this.registry.unregister(id);
			this.#remoteIds.clear();
			// In-flight sends will never be acked over the dead conn; fail them
			// now rather than stall each one for the full receipt timeout.
			this.#waiter.failAll("team bridge connection closed");
		});
		this.#sendHello();
		// Re-announce when one of THIS connector's local agents changes status
		// (running<->idle) so the lead's roster tracks the child live. Debounced
		// to one hello per microtask, mirroring the broker's roster rebroadcast.
		// Remote-ref churn from #onRoster is ignored (those ids aren't local).
		this.#unsubscribeRegistry = this.registry.onChange(event => {
			if (this.localAgents.some(a => a.id === event.ref.id)) this.#scheduleHello();
		});
		const router: IrcRemoteRouter = { deliver: msg => this.#sendToBroker(msg) };
		this.bus.setRemoteRouter(router);
	}

	#sendToBroker(msg: IrcMessage): Promise<IrcDeliveryReceipt> {
		if (!this.#conn) return Promise.resolve({ to: msg.to, outcome: "failed", error: "team bridge not connected" });
		const reqId = randomUUID();
		const p = this.#waiter.track(reqId, this.#conn, msg.to);
		this.#conn.send({ t: "irc", reqId, msg });
		return p;
	}

	#scheduleHello(): void {
		if (this.#helloScheduled) return;
		this.#helloScheduled = true;
		queueMicrotask(() => {
			this.#helloScheduled = false;
			this.#sendHello();
		});
	}

	/** Announce local agents with their CURRENT status/activity, read live from the registry. */
	#sendHello(): void {
		this.#conn?.send({ t: "hello", agents: this.#localRoster() });
	}

	#localRoster(): PeerAgent[] {
		return this.localAgents.map(a => {
			const ref = this.registry.get(a.id);
			return {
				id: a.id,
				displayName: a.displayName,
				kind: a.kind,
				status: ref?.status ?? "running",
				activity: ref?.activity,
			};
		});
	}

	#onRoster(agents: PeerAgent[]): void {
		const next = new Set(agents.map(a => a.id));
		// Withdraw remote refs that left the latest roster (a lead-side agent
		// finished, or another child disconnected) — without this a child's roster
		// only ever grows. Only refs we registered (#remoteIds) are touched.
		for (const id of [...this.#remoteIds]) {
			if (next.has(id)) continue;
			this.registry.unregister(id);
			this.#remoteIds.delete(id);
		}
		for (const a of agents) {
			if (upsertRemoteRef(this.registry, a)) this.#remoteIds.add(a.id);
		}
	}

	close(): void {
		this.#unsubscribeRegistry?.();
		this.#unsubscribeRegistry = undefined;
		this.bus.setRemoteRouter(undefined);
		this.#conn?.close();
	}
}

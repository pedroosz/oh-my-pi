import { randomUUID } from "node:crypto";
import type { AgentRegistry } from "../registry/agent-registry";
import type { IrcBus, IrcDeliveryReceipt, IrcMessage, IrcRemoteRouter } from "./bus";
import { type JsonConn, JsonSocketServer, connectJsonSocket } from "./transport";

export interface PeerAgent {
	id: string;
	displayName: string;
	kind: "main" | "sub";
}

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

/** Correlate outbound irc frames to their receipt frames over one conn. */
function makeReceiptWaiter() {
	const pending = new Map<string, (r: IrcDeliveryReceipt) => void>();
	return {
		track(reqId: string): Promise<IrcDeliveryReceipt> {
			const { promise, resolve } = Promise.withResolvers<IrcDeliveryReceipt>();
			const timer = setTimeout(() => {
				pending.delete(reqId);
				resolve({ to: "", outcome: "failed", error: "team bridge receipt timeout" });
			}, REQUEST_TIMEOUT_MS);
			timer.unref?.();
			pending.set(reqId, r => {
				clearTimeout(timer);
				resolve(r);
			});
			return promise;
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
			// delivery. Mirrors the connector-side guard in #onRoster.
			const existing = this.registry.get(a.id);
			if (existing && !existing.remote) continue;
			this.#connOf.set(a.id, conn);
			this.registry.register({
				id: a.id,
				displayName: a.displayName,
				kind: a.kind,
				session: null,
				remote: true,
				status: "idle",
			});
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

	// v1 limitation: the roster is computed at hello/close time only. Agents
	// added or removed after connect (subagents spawned later, local agents that
	// finish) are not re-propagated to peers. Dynamic membership is deferred to
	// Plan B (registry.onChange rebroadcast).
	#broadcastRoster(): void {
		const all = [...this.#agentsOf.entries()];
		// The lead's own in-process agents (e.g. Main) are not announced by any
		// connector, so include them in every connector's roster too — otherwise
		// a child never learns about lead-local peers.
		const leadAgents: PeerAgent[] = this.registry
			.list()
			.filter(ref => !ref.remote && ref.kind !== "advisor" && (ref.status === "running" || ref.status === "idle"))
			.map(ref => ({ id: ref.id, displayName: ref.displayName, kind: ref.kind === "main" ? "main" : "sub" }));
		const connectorAgents = all.flatMap(([, a]) => a);
		for (const [conn, own] of all) {
			const ownIds = new Set(own.map(a => a.id));
			const others = [...connectorAgents, ...leadAgents].filter(a => !ownIds.has(a.id));
			conn.send({ t: "roster", agents: others });
		}
	}

	async close(): Promise<void> {
		this.bus.setRemoteRouter(undefined);
		// net.Server.close() waits for live connections to end, so close the
		// connector sockets first or this would hang while peers stay connected.
		for (const conn of this.#agentsOf.keys()) conn.close();
		await this.#server.close();
	}
}

/** Child-side connector. */
export class TeamConnector {
	#conn: JsonConn | undefined;
	readonly #waiter = makeReceiptWaiter();
	readonly #remoteIds = new Set<string>();

	constructor(
		private readonly bus: IrcBus,
		private readonly registry: AgentRegistry,
		private readonly localAgents: PeerAgent[],
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
			for (const id of this.#remoteIds) this.registry.unregister(id);
			this.#remoteIds.clear();
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
			this.registry.register({
				id: a.id,
				displayName: a.displayName,
				kind: a.kind,
				session: null,
				remote: true,
				status: "idle",
			});
			this.#remoteIds.add(a.id);
		}
	}

	close(): void {
		this.bus.setRemoteRouter(undefined);
		this.#conn?.close();
	}
}

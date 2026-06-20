import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { join } from "node:path";
import process from "node:process";
import { IrcBus, type IrcMessage } from "../irc/bus";
import { AgentRegistry } from "../registry/agent-registry";
import { getOrStartBroker, shutdownTeam, spawnTeamSubagent } from "./orchestrator";

const fixturePath = join(import.meta.dir, "__fixtures__", "join-child.ts");

// Lead-side fake session: forwards each delivered message to a sink callback,
// reports "injected". Shaped like an AgentSession for the registry.
function leadSession(onMessage: (m: IrcMessage) => void) {
	return {
		deliverIrcMessage: async (msg: IrcMessage) => {
			onMessage(msg);
			return "injected" as const;
		},
	} as unknown as Parameters<AgentRegistry["register"]>[0]["session"];
}

// Resolve once `id` is removed from the registry — awaits async socket teardown
// deterministically (rule ts-no-test-timers).
function whenRemoved(reg: AgentRegistry, id: string): Promise<void> {
	if (!reg.get(id)) return Promise.resolve();
	const { promise, resolve } = Promise.withResolvers<void>();
	const off = reg.onChange(event => {
		if (event.type === "removed" && event.ref.id === id) {
			off();
			resolve();
		}
	});
	return promise;
}

// Resolve on the NEXT "removed" event for `id`, with no current-presence
// shortcut. Armed before a spawn whose placeholder is registered then reclaimed,
// so it observes that reclamation rather than resolving on the not-yet-present id.
function whenRemovedNext(reg: AgentRegistry, id: string): Promise<void> {
	const { promise, resolve } = Promise.withResolvers<void>();
	const off = reg.onChange(event => {
		if (event.type === "removed" && event.ref.id === id) {
			off();
			resolve();
		}
	});
	return promise;
}

describe("team e2e: cross-process spawn + irc", () => {
	it("spawns children, sees them as remote, round-trips irc, drops one on exit", async () => {
		IrcBus.resetGlobalForTests();
		AgentRegistry.resetGlobalForTests();
		const bus = IrcBus.global();
		const registry = AgentRegistry.global();

		// Children send a "ready" handshake once connected, and reply "ack:…"
		// to a lead message — both observed via the lead's session sink.
		const aReady = Promise.withResolvers<void>();
		const bReady = Promise.withResolvers<void>();
		const ack = Promise.withResolvers<IrcMessage>();
		registry.register({
			id: "Main",
			displayName: "Main",
			kind: "main",
			session: leadSession(m => {
				if (m.body === "ready" && m.from === "ChildA") aReady.resolve();
				if (m.body === "ready" && m.from === "ChildB") bReady.resolve();
				if (m.body.startsWith("ack:")) ack.resolve(m);
			}),
		});

		try {
			await getOrStartBroker(bus, registry);
			await spawnTeamSubagent({
				id: "ChildA",
				assignment: "noop",
				cwd: process.cwd(),
				command: { cmd: "bun", args: [fixturePath] },
			});
			await spawnTeamSubagent({
				id: "ChildB",
				assignment: "noop",
				cwd: process.cwd(),
				command: { cmd: "bun", args: [fixturePath] },
			});

			// Both children connected (their "ready" handshakes arrived over the socket).
			await Promise.all([aReady.promise, bReady.promise]);
			expect(registry.get("ChildA")?.remote).toBe(true);
			expect(registry.get("ChildB")?.remote).toBe(true);

			// lead -> child + child -> lead: round-trip against ChildB (it stays
			// alive, so the lead's send receives its receipt before any teardown).
			const receipt = await bus.send({ from: "Main", to: "ChildB", body: "ping" });
			expect(receipt.outcome).toBe("injected");

			const reply = await ack.promise;
			expect(reply.from).toBe("ChildB");
			expect(reply.body).toBe("ack:ping");

			// Exit ChildA with a fire-and-forget message (the dying child sends no
			// receipt, so the lead must not await one); its socket drop removes the
			// ref. ChildB is untouched and stays connected.
			const childAremoved = whenRemoved(registry, "ChildA");
			void bus.send({ from: "Main", to: "ChildA", body: "exit" });
			await childAremoved;
			expect(registry.get("ChildA")).toBeUndefined();
			expect(registry.get("ChildB")?.remote).toBe(true);
		} finally {
			await shutdownTeam();
		}
	}, 30_000);
});

describe("team e2e: phantom placeholder reclamation", () => {
	// Force the plain ptree spawn path: this environment may export
	// CMUX_SOCKET_PATH, which would otherwise route spawns through real cmux
	// workspaces (untracked, so shutdownTeam could not reclaim them).
	const savedCmux = process.env.CMUX_SOCKET_PATH;
	beforeEach(() => {
		delete process.env.CMUX_SOCKET_PATH;
	});
	afterEach(() => {
		if (savedCmux === undefined) delete process.env.CMUX_SOCKET_PATH;
		else process.env.CMUX_SOCKET_PATH = savedCmux;
	});
	it("reclaims a placeholder when the child exits before connecting (plain path)", async () => {
		IrcBus.resetGlobalForTests();
		AgentRegistry.resetGlobalForTests();
		const bus = IrcBus.global();
		const registry = AgentRegistry.global();
		registry.register({ id: "Main", displayName: "Main", kind: "main", session: leadSession(() => {}) });

		try {
			await getOrStartBroker(bus, registry);
			// Passing only once the placeholder is both registered and then removed
			// (a never-registered id would leave this pending and time the test out).
			const gone = whenRemovedNext(registry, "DeadChild");
			await spawnTeamSubagent({
				id: "DeadChild",
				assignment: "noop",
				cwd: process.cwd(),
				// Exits immediately, never joining the broker: no hello, so the
				// socket-close reaper never runs and child.exited must clean up.
				command: { cmd: "bun", args: ["-e", "process.exit(1)"] },
			});
			await gone;
			expect(registry.get("DeadChild")).toBeUndefined();
		} finally {
			await shutdownTeam();
		}
	}, 30_000);

	it("reclaims a placeholder via the connect watchdog when the child never joins", async () => {
		IrcBus.resetGlobalForTests();
		AgentRegistry.resetGlobalForTests();
		const bus = IrcBus.global();
		const registry = AgentRegistry.global();
		registry.register({ id: "Main", displayName: "Main", kind: "main", session: leadSession(() => {}) });

		try {
			await getOrStartBroker(bus, registry);
			const gone = whenRemovedNext(registry, "Loner");
			await spawnTeamSubagent({
				id: "Loner",
				assignment: "noop",
				cwd: process.cwd(),
				// Stays alive but never joins the broker, so only the watchdog
				// (injected to fire next tick) can reclaim the placeholder.
				command: { cmd: "bun", args: ["-e", "setInterval(() => {}, 1000)"] },
				connectWatchdogMs: 0,
			});
			await gone;
			expect(registry.get("Loner")).toBeUndefined();
		} finally {
			await shutdownTeam();
		}
	}, 30_000);
});

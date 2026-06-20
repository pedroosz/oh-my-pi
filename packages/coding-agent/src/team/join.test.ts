import { describe, expect, it } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TeamBroker } from "../irc/bridge";
import { IrcBus, type IrcMessage } from "../irc/bus";
import { AgentRegistry } from "../registry/agent-registry";
import { joinTeamIfConfigured } from "./join";

// Minimal fake AgentSession: records delivered messages, reports "injected".
function fakeSession(sink: IrcMessage[] = []) {
	return {
		deliverIrcMessage: async (msg: IrcMessage) => {
			sink.push(msg);
			return "injected" as const;
		},
	} as unknown as Parameters<AgentRegistry["register"]>[0]["session"];
}

// Resolve once `id` is registered — a deterministic handshake keyed on the real
// registry event, so no wall-clock sleep is needed (rule ts-no-test-timers).
function whenRegistered(reg: AgentRegistry, id: string): Promise<void> {
	if (reg.get(id)) return Promise.resolve();
	const { promise, resolve } = Promise.withResolvers<void>();
	const off = reg.onChange(e => {
		if (e.type === "registered" && e.ref.id === id) {
			off();
			resolve();
		}
	});
	return promise;
}

describe("joinTeamIfConfigured", () => {
	it("connects a child to a live broker; broker roster reaches the child", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-join-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession() });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		const childReg = new AgentRegistry();
		const childBus = new IrcBus(childReg);

		const leadSeesChild = whenRegistered(leadReg, "ChildA");
		const childSeesMain = whenRegistered(childReg, "Main");
		const connector = await joinTeamIfConfigured(
			{ OMP_IRC_SOCKET: sockPath, OMP_AGENT_ID: "ChildA" },
			childBus,
			childReg,
		);
		expect(connector).toBeDefined();
		await Promise.all([leadSeesChild, childSeesMain]);

		// Lead now sees the child as a remote peer; child sees the lead's Main.
		expect(leadReg.get("ChildA")?.remote).toBe(true);
		expect(childReg.get("Main")?.remote).toBe(true);

		connector?.close();
		await broker.close();
	});

	it("returns undefined when OMP_IRC_SOCKET is unset (standalone)", async () => {
		const reg = new AgentRegistry();
		const bus = new IrcBus(reg);
		expect(await joinTeamIfConfigured({}, bus, reg)).toBeUndefined();
	});

	it("returns undefined when OMP_TEAM is set (a process is broker XOR connector)", async () => {
		const reg = new AgentRegistry();
		const bus = new IrcBus(reg);
		const connector = await joinTeamIfConfigured({ OMP_IRC_SOCKET: "/nonexistent.sock", OMP_TEAM: "1" }, bus, reg);
		expect(connector).toBeUndefined();
	});

	it("degrades to standalone (undefined) when no broker is listening", async () => {
		const reg = new AgentRegistry();
		const bus = new IrcBus(reg);
		// Path with no listener: connect attempts are refused, retries exhaust,
		// and the helper resolves undefined instead of throwing or hanging.
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-join-")), "irc.sock");
		// backoffMs:0 keeps the exhausted-retry path event-fast (no wall-clock sleep).
		const connector = await joinTeamIfConfigured({ OMP_IRC_SOCKET: sockPath }, bus, reg, { backoffMs: 0 });
		expect(connector).toBeUndefined();
	});

	it('announces a child as kind "sub" to the lead and to siblings', async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-join-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession() });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		// First child joins; the lead classifies it purely from the child's hello.
		const childAReg = new AgentRegistry();
		const childABus = new IrcBus(childAReg);
		const leadSeesA = whenRegistered(leadReg, "ChildA");
		const connA = await joinTeamIfConfigured(
			{ OMP_IRC_SOCKET: sockPath, OMP_AGENT_ID: "ChildA" },
			childABus,
			childAReg,
		);
		await leadSeesA;
		expect(leadReg.get("ChildA")?.kind).toBe("sub");

		// Second child joins; the broker's roster broadcast carries ChildA to it,
		// so a sibling must also classify ChildA as a subagent, not a co-equal main.
		const childBReg = new AgentRegistry();
		const childBBus = new IrcBus(childBReg);
		const bSeesA = whenRegistered(childBReg, "ChildA");
		const connB = await joinTeamIfConfigured(
			{ OMP_IRC_SOCKET: sockPath, OMP_AGENT_ID: "ChildB" },
			childBBus,
			childBReg,
		);
		await bSeesA;
		expect(childBReg.get("ChildA")?.kind).toBe("sub");

		connA?.close();
		connB?.close();
		await broker.close();
	});
});

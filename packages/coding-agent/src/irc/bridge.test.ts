import { describe, expect, it } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AgentRegistry } from "../registry/agent-registry";
import { TeamBroker, TeamConnector } from "./bridge";
import { IrcBus, type IrcMessage } from "./bus";

// Minimal fake AgentSession: records delivered messages, reports "injected".
function fakeSession(sink: IrcMessage[]) {
	return {
		deliverIrcMessage: async (msg: IrcMessage) => {
			sink.push(msg);
			return "injected" as const;
		},
	} as unknown as Parameters<AgentRegistry["register"]>[0]["session"];
}

// Resolve once `id` is registered — a deterministic handshake wait keyed on the
// real registry event, so no wall-clock sleep is needed (rule ts-no-test-timers).
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

// Resolve once `id` is removed from the registry — used to await async socket
// teardown deterministically (rule ts-no-test-timers).
function whenRemoved(reg: AgentRegistry, id: string): Promise<void> {
	if (!reg.get(id)) return Promise.resolve();
	const { promise, resolve } = Promise.withResolvers<void>();
	const off = reg.onChange(e => {
		if (e.type === "removed" && e.ref.id === id) {
			off();
			resolve();
		}
	});
	return promise;
}

// Resolve once `id` reaches `status` via a status_changed event (rule ts-no-test-timers).
function whenStatus(reg: AgentRegistry, id: string, status: string): Promise<void> {
	const cur = reg.get(id);
	if (cur?.status === status) return Promise.resolve();
	const { promise, resolve } = Promise.withResolvers<void>();
	const off = reg.onChange(e => {
		if (e.type === "status_changed" && e.ref.id === id && e.ref.status === status) {
			off();
			resolve();
		}
	});
	return promise;
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

		// Await the hello + roster handshake deterministically: the lead must see
		// ChildA, and the child must see Main, before either side messages.
		const leadSeesChild = whenRegistered(leadReg, "ChildA");
		const childSeesMain = whenRegistered(childReg, "Main");
		await connector.connect(sockPath);
		await Promise.all([leadSeesChild, childSeesMain]);

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

	it("broker.close() resolves while a connector is still connected (fix 1)", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession([]) });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		const childReg = new AgentRegistry();
		const childBus = new IrcBus(childReg);
		childReg.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: fakeSession([]) });
		const connector = new TeamConnector(childBus, childReg, [{ id: "ChildA", displayName: "ChildA", kind: "sub" }]);

		const leadSeesChild = whenRegistered(leadReg, "ChildA");
		await connector.connect(sockPath);
		await leadSeesChild;

		// Close WITHOUT closing the connector first. Before the fix this hung on
		// net.Server.close() waiting for the live connection; a hang now fails
		// the run by timeout. Reaching the assertions proves close() resolved.
		await broker.close();

		// And the server is truly down: a fresh connect to the same path fails.
		const lateReg = new AgentRegistry();
		const late = new TeamConnector(new IrcBus(lateReg), lateReg, []);
		await expect(late.connect(sockPath)).rejects.toThrow();
	});

	it("connector tears down remote refs + router when the socket drops (fix 2)", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession([]) });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		const childReg = new AgentRegistry();
		const childBus = new IrcBus(childReg);
		childReg.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: fakeSession([]) });
		const connector = new TeamConnector(childBus, childReg, [{ id: "ChildA", displayName: "ChildA", kind: "sub" }]);

		const childSeesMain = whenRegistered(childReg, "Main");
		await connector.connect(sockPath);
		await childSeesMain;
		expect(childReg.get("Main")?.remote).toBe(true);

		// Drop the socket from the lead side; the connector's onClose must
		// withdraw the remote ref it registered for Main.
		const childForgetsMain = whenRemoved(childReg, "Main");
		await broker.close();
		await childForgetsMain;
		expect(childReg.get("Main")).toBeUndefined();

		// Router cleared too: a send to a remote target now fails fast instead of
		// hanging the receipt timeout against the dead conn.
		const r = await childBus.send({ from: "ChildA", to: "Main", body: "anyone?" });
		expect(r.outcome).toBe("failed");
	});

	it("an announced id does not clobber a real local ref the lead owns (fix 4)", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		const leadInbox: IrcMessage[] = [];
		const mainSession = fakeSession(leadInbox);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: mainSession });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		const childReg = new AgentRegistry();
		const childBus = new IrcBus(childReg);
		// Announce a colliding "Main" plus a genuinely new "ChildB". Awaiting
		// ChildB's registration (later in the same synchronous hello loop) proves
		// the Main collision was already handled.
		const connector = new TeamConnector(childBus, childReg, [
			{ id: "Main", displayName: "Impostor", kind: "sub" },
			{ id: "ChildB", displayName: "ChildB", kind: "sub" },
		]);

		const leadSeesChildB = whenRegistered(leadReg, "ChildB");
		await connector.connect(sockPath);
		await leadSeesChildB;

		const main = leadReg.get("Main");
		expect(main?.remote).toBeFalsy();
		expect(main?.session).toBe(mainSession);

		connector.close();
		await broker.close();
	});

	it("propagates dynamic roster membership to a connected child", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession([]) });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		const childReg = new AgentRegistry();
		const childBus = new IrcBus(childReg);
		childReg.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: fakeSession([]) });
		const connector = new TeamConnector(childBus, childReg, [{ id: "ChildA", displayName: "ChildA", kind: "sub" }]);

		const childSeesMain = whenRegistered(childReg, "Main");
		await connector.connect(sockPath);
		await childSeesMain;

		// A lead-local agent registered AFTER connect must reach the child.
		const childSeesLate = whenRegistered(childReg, "LateLead");
		leadReg.register({ id: "LateLead", displayName: "LateLead", kind: "sub", session: fakeSession([]) });
		await childSeesLate;
		expect(childReg.get("LateLead")?.remote).toBe(true);

		// And removing it on the lead withdraws it from the child's roster.
		const childForgetsLate = whenRemoved(childReg, "LateLead");
		leadReg.unregister("LateLead");
		await childForgetsLate;
		expect(childReg.get("LateLead")).toBeUndefined();

		connector.close();
		await broker.close();
	});

	it("propagates status and activity across the bridge in both directions", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-team-")), "irc.sock");

		const leadReg = new AgentRegistry();
		const leadBus = new IrcBus(leadReg);
		leadReg.register({ id: "Main", displayName: "Main", kind: "main", session: fakeSession([]) });
		const broker = new TeamBroker(leadBus, leadReg);
		await broker.listen(sockPath);

		const childReg = new AgentRegistry();
		const childBus = new IrcBus(childReg);
		// Start the child idle so a later running transition is a real status change.
		childReg.register({ id: "ChildA", displayName: "ChildA", kind: "sub", session: fakeSession([]), status: "idle" });
		const connector = new TeamConnector(childBus, childReg, [{ id: "ChildA", displayName: "ChildA", kind: "sub" }]);

		const leadSeesChild = whenRegistered(leadReg, "ChildA");
		const childSeesMain = whenRegistered(childReg, "Main");
		await connector.connect(sockPath);
		await Promise.all([leadSeesChild, childSeesMain]);

		// The initial hello carried the child's CURRENT (idle) status, not a hardcoded one.
		expect(leadReg.get("ChildA")?.status).toBe("idle");

		// child -> lead: ChildA goes running and records activity; both reach the lead.
		const leadSeesRunning = whenStatus(leadReg, "ChildA", "running");
		childReg.setStatus("ChildA", "running");
		childReg.setActivity("ChildA", "doing the thing");
		await leadSeesRunning;
		expect(leadReg.get("ChildA")?.activity).toBe("doing the thing");

		// lead -> child: Main goes idle; the change reaches the child's roster ref.
		const childSeesIdle = whenStatus(childReg, "Main", "idle");
		leadReg.setStatus("Main", "idle");
		await childSeesIdle;
		expect(childReg.get("Main")?.status).toBe("idle");

		connector.close();
		await broker.close();
	});
});

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
});

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
			deliver: async msg => {
				seen.push(`${msg.from}->${msg.to}:${msg.body}`);
				return { to: msg.to, outcome: "injected" };
			},
		});

		const receipt = await bus.send({ from: "Main", to: "ChildA", body: "hi" });
		expect(receipt.outcome).toBe("injected");
		expect(seen).toEqual(["Main->ChildA:hi"]);
	});
});

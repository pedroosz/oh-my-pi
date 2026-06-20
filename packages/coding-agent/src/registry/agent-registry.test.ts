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

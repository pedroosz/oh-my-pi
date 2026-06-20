/**
 * Standalone team-child fixture for the cross-process e2e (team.e2e.test.ts).
 *
 * Run as `bun join-child.ts <assignment>` with OMP_IRC_SOCKET + OMP_AGENT_ID
 * set (as a real team child is launched). It joins the lead's broker, announces
 * itself, sends a "ready" handshake once it sees the lead, then stays alive
 * acking lead messages — giving the test deterministic signals (no timers) for
 * connect, lead->child, and child->lead. A lead "exit" message ends the process
 * so the test can observe roster removal on socket drop.
 */

import process from "node:process";

import { IrcBus, type IrcMessage } from "../../irc/bus";
import { AgentRegistry, MAIN_AGENT_ID } from "../../registry/agent-registry";
import { joinTeamIfConfigured } from "../join";

const selfId = process.env.OMP_AGENT_ID ?? MAIN_AGENT_ID;
const bus = IrcBus.global();
const registry = AgentRegistry.global();

// Minimal session shaped like an AgentSession for the registry: ack each lead
// message (so child->lead is observable), or exit on "exit". Staying alive
// until then lets the lead's send receive its receipt before the socket drops.
const session = {
	deliverIrcMessage: async (msg: IrcMessage) => {
		if (msg.body === "exit") process.exit(0);
		await bus.send({ from: selfId, to: msg.from, body: `ack:${msg.body}` });
		return "injected" as const;
	},
} as unknown as Parameters<AgentRegistry["register"]>[0]["session"];

registry.register({ id: selfId, displayName: selfId, kind: "sub", session });

// Resolve once `id` is registered locally — the roster handshake delivered it.
function whenRegistered(id: string): Promise<void> {
	if (registry.get(id)) return Promise.resolve();
	const { promise, resolve } = Promise.withResolvers<void>();
	const off = registry.onChange(event => {
		if (event.type === "registered" && event.ref.id === id) {
			off();
			resolve();
		}
	});
	return promise;
}

process.on("SIGTERM", () => process.exit(0));
process.on("SIGINT", () => process.exit(0));

const connector = await joinTeamIfConfigured(process.env, bus, registry);
if (!connector) {
	process.exit(1);
}

// The lead's broker pushes its roster (including Main) right after our hello.
// Wait for Main before sending so the send resolves the recipient.
await whenRegistered(MAIN_AGENT_ID);
await bus.send({ from: selfId, to: MAIN_AGENT_ID, body: "ready" });

// The open broker socket keeps the event loop alive; we exit on an "exit"
// message or a signal (the test kills survivors in shutdownTeam).

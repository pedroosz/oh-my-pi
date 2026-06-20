import { TeamConnector } from "../irc/bridge";
import type { IrcBus } from "../irc/bus";
import { type AgentRegistry, MAIN_AGENT_ID } from "../registry/agent-registry";

/** Connect attempts before giving up and running standalone. */
const MAX_CONNECT_ATTEMPTS = 4;
/** Backoff between attempts: the lead's broker may still be binding its socket. */
const RETRY_BACKOFF_MS = 150;

/**
 * Child-side team boot. When a lead launched this process as a team subagent it
 * exports `OMP_IRC_SOCKET` (the broker socket) and `OMP_AGENT_ID` (the id the
 * lead announced to its roster). If those are present — and this process is not
 * itself a lead (`OMP_TEAM`) — open a {@link TeamConnector} so the in-process
 * `irc` send/wait/list/broadcast reach across the process boundary.
 *
 * A process is a broker XOR a connector: there is one remote router per
 * `IrcBus`, so a lead (`OMP_TEAM`) never also joins as a child. Connect is
 * best-effort: if the broker is unreachable after a few attempts the child
 * degrades to a normal standalone omp (resolves `undefined`) rather than
 * throwing or hanging — losing team coordination is recoverable, refusing to
 * boot is not.
 */
export async function joinTeamIfConfigured(
	env: Record<string, string | undefined>,
	bus: IrcBus,
	registry: AgentRegistry,
	opts: { maxAttempts?: number; backoffMs?: number } = {},
): Promise<TeamConnector | undefined> {
	const socketPath = env.OMP_IRC_SOCKET;
	if (!socketPath || env.OMP_TEAM) return undefined;

	const id = env.OMP_AGENT_ID ?? MAIN_AGENT_ID;
	const maxAttempts = opts.maxAttempts ?? MAX_CONNECT_ATTEMPTS;
	const backoffMs = opts.backoffMs ?? RETRY_BACKOFF_MS;
	// A connector is always a lead-spawned team child (gated above on
	// OMP_IRC_SOCKET && !OMP_TEAM), so it announces itself as kind "sub" so the
	// lead and sibling rosters classify it as a subagent, not a co-equal main.
	// Its own process-private self-registration stays "main".
	const connector = new TeamConnector(bus, registry, [{ id, displayName: id, kind: "sub" }]);

	for (let attempt = 1; attempt <= maxAttempts; attempt++) {
		try {
			await connector.connect(socketPath);
			return connector;
		} catch {
			if (attempt === maxAttempts) return undefined;
			// Brief backoff: the broker may not have finished binding the socket
			// yet (the lead spawned us moments ago). Awaited only in production —
			// the connect succeeds first try once the broker is up.
			await Bun.sleep(backoffMs);
		}
	}
	return undefined;
}

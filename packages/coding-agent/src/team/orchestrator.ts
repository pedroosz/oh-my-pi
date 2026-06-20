/**
 * Lead-side team orchestration: start one broker socket for this process and
 * spawn subagents as separate omp processes that join it over the irc bridge.
 *
 * v1 is single-level: a process is a lead (runs the broker here) XOR a child
 * (joins via {@link joinTeamIfConfigured}). There is one remote router per
 * `IrcBus`, so a child is never also a lead. Recursive teams — a child that is
 * itself a lead spawning its own grandchildren — are OUT OF SCOPE for v1 and
 * deliberately not built: it would need a second router per process and a
 * roster-namespacing scheme the bridge does not have.
 */

import { randomBytes } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";

import { ptree } from "@oh-my-pi/pi-utils";

import { TeamBroker } from "../irc/bridge";
import { IrcBus } from "../irc/bus";
import { AgentRegistry } from "../registry/agent-registry";
import { resolveOmpCommand } from "../task/omp-command";
import { CmuxSocketClient } from "../tools/browser/cmux/socket-client";

interface BrokerState {
	broker: TeamBroker;
	socketPath: string;
}

/**
 * Memoized broker for this process. Stored as a promise so concurrent
 * {@link spawnTeamSubagent} calls share a single `listen()` instead of racing
 * two brokers onto two sockets. Reset on failure so a transient bind error
 * does not poison every later spawn.
 */
let brokerPromise: Promise<BrokerState> | undefined;

/** Plain-spawn children tracked for {@link shutdownTeam}. cmux workspaces are owned by cmux. */
const spawnedChildren = new Set<ptree.ChildProcess>();

/**
 * Lazily start the single lead broker for this process and return its socket
 * path. Idempotent: every call after the first returns the same socket.
 */
export async function getOrStartBroker(
	bus: IrcBus = IrcBus.global(),
	registry: AgentRegistry = AgentRegistry.global(),
): Promise<{ socketPath: string }> {
	if (!brokerPromise) {
		brokerPromise = startBroker(bus, registry).catch((err: unknown) => {
			// Don't memoize a rejected bind — let a later spawn retry.
			brokerPromise = undefined;
			throw err;
		});
	}
	const { socketPath } = await brokerPromise;
	return { socketPath };
}

async function startBroker(bus: IrcBus, registry: AgentRegistry): Promise<BrokerState> {
	const socketPath = join(tmpdir(), `omp-team-${process.pid}-${randomBytes(4).toString("hex")}.sock`);
	const broker = new TeamBroker(bus, registry);
	await broker.listen(socketPath);
	return { broker, socketPath };
}

export interface TeamChildSpawn {
	argv: string[];
	env: Record<string, string>;
	cwd: string;
}

/**
 * Pure: build the argv + env for a team child. The child reads `OMP_IRC_SOCKET`
 * + `OMP_AGENT_ID` and joins the lead's broker; `OMP_TEAM` is stripped so the
 * child runs as a connector, never a second broker (broker XOR connector).
 */
export function buildTeamChildSpawn(opts: {
	id: string;
	assignment: string;
	cwd: string;
	socketPath: string;
	baseEnv?: Record<string, string | undefined>;
	command?: { cmd: string; args: string[] };
}): TeamChildSpawn {
	const { id, assignment, cwd, socketPath, baseEnv = process.env, command } = opts;
	const resolved = command ?? resolveOmpCommand();
	const argv = [resolved.cmd, ...resolved.args, assignment];

	const env: Record<string, string> = {};
	for (const [key, value] of Object.entries(baseEnv)) {
		if (value !== undefined) env[key] = value;
	}
	env.OMP_IRC_SOCKET = socketPath;
	env.OMP_AGENT_ID = id;
	delete env.OMP_TEAM;

	return { argv, env, cwd };
}

/**
 * Spawn a subagent as a separate omp process joined to this process's broker.
 *
 * Registers a placeholder remote ref immediately so the lead roster shows the
 * child before its socket connects; the broker's hello handler upgrades it
 * (and wires routing) once the child announces itself. Under `CMUX_SOCKET_PATH`
 * the child runs in a cmux workspace; otherwise it is a plain local process.
 */
export async function spawnTeamSubagent(opts: {
	id: string;
	assignment: string;
	cwd: string;
	command?: { cmd: string; args: string[] };
}): Promise<void> {
	const bus = IrcBus.global();
	const registry = AgentRegistry.global();
	const { socketPath } = await getOrStartBroker(bus, registry);

	if (!registry.get(opts.id)) {
		registry.register({
			id: opts.id,
			displayName: opts.id,
			kind: "sub",
			session: null,
			remote: true,
			status: "running",
		});
	}

	const spawn = buildTeamChildSpawn({
		id: opts.id,
		assignment: opts.assignment,
		cwd: opts.cwd,
		socketPath,
		command: opts.command,
	});

	const cmuxSocket = process.env.CMUX_SOCKET_PATH;
	if (cmuxSocket) {
		await spawnViaCmux({
			socketPath: cmuxSocket,
			password: process.env.CMUX_SOCKET_PASSWORD || undefined,
			id: opts.id,
			cwd: spawn.cwd,
			teamEnv: { OMP_IRC_SOCKET: socketPath, OMP_AGENT_ID: opts.id },
			argv: spawn.argv,
		});
		return;
	}

	const child = ptree.spawn(spawn.argv, { cwd: spawn.cwd, env: spawn.env });
	spawnedChildren.add(child);
	void child.exited.catch(() => {}).finally(() => spawnedChildren.delete(child));
	// Drain stdout: a chatty child can otherwise wedge on a full stdout pipe
	// (ptree pipes stdout and only auto-drains stderr).
	void new Response(child.stdout).text().catch(() => {});
}

function posixQuote(value: string): string {
	return `'${value.replace(/'/g, "'\\''")}'`;
}

/** Launch a child inside a cmux workspace: create it, then type the command. */
async function spawnViaCmux(args: {
	socketPath: string;
	password: string | undefined;
	id: string;
	cwd: string;
	teamEnv: Record<string, string>;
	argv: string[];
}): Promise<void> {
	const client = new CmuxSocketClient({ socketPath: args.socketPath, password: args.password });
	try {
		await client.connect();
		const created = await client.request("workspace.create", {
			cwd: args.cwd,
			title: args.id,
			workspace_env: args.teamEnv,
			focus: false,
		});
		const workspaceId = created.workspace_id;
		if (typeof workspaceId !== "string") {
			throw new Error(`cmux workspace.create returned no workspace_id (got ${typeof workspaceId})`);
		}
		const text = `${args.argv.map(posixQuote).join(" ")}\n`;
		await client.request("surface.send_text", { workspace_id: workspaceId, text });
	} finally {
		client.close();
	}
}

/**
 * Tear down the team: kill tracked children, close the broker, and reset the
 * module memo (so a later spawn — or a test — starts a fresh broker).
 */
export async function shutdownTeam(): Promise<void> {
	for (const child of spawnedChildren) child.kill();
	spawnedChildren.clear();

	const pending = brokerPromise;
	brokerPromise = undefined;
	if (!pending) return;
	const state = await pending.catch(() => undefined);
	await state?.broker.close();
}

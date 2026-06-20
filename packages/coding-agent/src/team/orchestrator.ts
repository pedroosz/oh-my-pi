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
import { mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import process from "node:process";

import { getWorktreeDir, hashPath, ptree } from "@oh-my-pi/pi-utils";

import { TeamBroker } from "../irc/bridge";
import { IrcBus } from "../irc/bus";
import { AgentRegistry, type AgentStatus } from "../registry/agent-registry";
import { resolveOmpCommand } from "../task/omp-command";
import { CmuxSocketClient } from "../tools/browser/cmux/socket-client";
import * as git from "../utils/git";

interface BrokerState {
	broker: TeamBroker;
	socketPath: string;
	notifyUnsub: () => void;
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

/** A spawned child that never announces (boot crash) leaves a phantom remote ref; reap it after this delay if no hello arrives. */
const CONNECT_WATCHDOG_MS = 30_000;

/** Cmux client surface used for completion notifications; injectable so the lead path is unit-testable. */
export interface CmuxNotifierClient {
	connect(): Promise<void>;
	request(method: string, params: Record<string, unknown>): Promise<Record<string, unknown>>;
	close(): void;
}
export type CmuxClientFactory = (opts: { socketPath: string; password?: string }) => CmuxNotifierClient;

const defaultCmuxClientFactory: CmuxClientFactory = opts => new CmuxSocketClient(opts);

/** Statuses a finished remote subagent settles into; reaching one fires the completion notification. */
const COMPLETION_STATUSES = new Set<AgentStatus>(["idle", "aborted"]);

/**
 * Lead-side: watch the registry for remote subagents that finish (status moves
 * from a live state into idle/aborted) and surface one cmux notification each.
 * Headless (no CMUX_SOCKET_PATH) is a no-op. Tracking the previous status per
 * id dedupes the idle->aborted tail the broker emits when a finished child's
 * socket later drops, so one completion notifies exactly once.
 */
function subscribeCompletionNotifications(registry: AgentRegistry, makeClient: CmuxClientFactory): () => void {
	const socketPath = process.env.CMUX_SOCKET_PATH;
	if (!socketPath) return () => {};
	const password = process.env.CMUX_SOCKET_PASSWORD || undefined;
	const prevStatus = new Map<string, AgentStatus>();
	return registry.onChange(event => {
		const { ref } = event;
		if (!ref.remote) return;
		if (event.type === "removed") {
			prevStatus.delete(ref.id);
			return;
		}
		const before = prevStatus.get(ref.id);
		prevStatus.set(ref.id, ref.status);
		if (event.type !== "status_changed") return;
		if (before !== undefined && COMPLETION_STATUSES.has(before)) return;
		if (!COMPLETION_STATUSES.has(ref.status)) return;
		void notifyCompletion(makeClient, socketPath, password, ref.id);
	});
}

async function notifyCompletion(
	makeClient: CmuxClientFactory,
	socketPath: string,
	password: string | undefined,
	agentId: string,
): Promise<void> {
	const client = makeClient({ socketPath, password });
	try {
		await client.connect();
		await client.request("notification.create", { title: agentId, body: "finished" });
	} catch {
		// Best-effort: a missing or closed cmux socket must not break the team or
		// teardown. The per-tab sidebar still reflects status via the registry.
	} finally {
		client.close();
	}
}

/**
 * Lazily start the single lead broker for this process and return its socket
 * path. Idempotent: every call after the first returns the same socket.
 */
export async function getOrStartBroker(
	bus: IrcBus = IrcBus.global(),
	registry: AgentRegistry = AgentRegistry.global(),
	makeCmuxClient: CmuxClientFactory = defaultCmuxClientFactory,
): Promise<{ socketPath: string; broker: TeamBroker }> {
	if (!brokerPromise) {
		brokerPromise = startBroker(bus, registry, makeCmuxClient).catch((err: unknown) => {
			// Don't memoize a rejected bind — let a later spawn retry.
			brokerPromise = undefined;
			throw err;
		});
	}
	const { socketPath, broker } = await brokerPromise;
	return { socketPath, broker };
}

async function startBroker(
	bus: IrcBus,
	registry: AgentRegistry,
	makeCmuxClient: CmuxClientFactory,
): Promise<BrokerState> {
	const socketPath = join(tmpdir(), `omp-team-${process.pid}-${randomBytes(4).toString("hex")}.sock`);
	const broker = new TeamBroker(bus, registry);
	await broker.listen(socketPath);
	const notifyUnsub = subscribeCompletionNotifications(registry, makeCmuxClient);
	return { broker, socketPath, notifyUnsub };
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
	/** Opt-in: run the child in a dedicated `team/<id>` git worktree (default off). */
	worktree?: boolean;
	/** Reap the placeholder after this long if the child never connects (default {@link CONNECT_WATCHDOG_MS}); 0 reaps next tick, negative disables. Test seam. */
	connectWatchdogMs?: number;
}): Promise<void> {
	const bus = IrcBus.global();
	const registry = AgentRegistry.global();
	const { socketPath, broker } = await getOrStartBroker(bus, registry);

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

	let childCwd = opts.cwd;
	if (opts.worktree) {
		const repoRoot = (await git.repo.primaryRoot(opts.cwd)) ?? opts.cwd;
		childCwd = await ensureTeamWorktree(opts.id, repoRoot);
	}

	const spawn = buildTeamChildSpawn({
		id: opts.id,
		assignment: opts.assignment,
		cwd: childCwd,
		socketPath,
		command: opts.command,
	});

	// Reap the placeholder if the child never announces over the socket (boot
	// crash, missing model/key in non-interactive mode): with no hello the
	// broker's close handler never runs, so nothing else would clear it.
	// Unref'd so a pending watchdog can't keep this process alive.
	const watchdogMs = opts.connectWatchdogMs ?? CONNECT_WATCHDOG_MS;
	const armWatchdog = (): ReturnType<typeof setTimeout> | undefined => {
		if (watchdogMs < 0) return undefined;
		const timer = setTimeout(() => {
			if (!broker.isConnected(opts.id)) registry.unregister(opts.id);
		}, watchdogMs);
		timer.unref?.();
		return timer;
	};

	const cmuxSocket = process.env.CMUX_SOCKET_PATH;
	if (cmuxSocket) {
		// cmux owns the child's process lifecycle (it is not in spawnedChildren),
		// so the watchdog is the only path that reclaims a never-connected child.
		armWatchdog();
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

	const watchdog = armWatchdog();
	const child = ptree.spawn(spawn.argv, { cwd: spawn.cwd, env: spawn.env });
	spawnedChildren.add(child);
	void child.exited
		.catch(() => {})
		.finally(() => {
			spawnedChildren.delete(child);
			// Child died: if it never connected, the broker's socket-close reaper
			// never fires, so drop the phantom placeholder here (the connected
			// case is handled by that reaper on socket drop).
			if (!broker.isConnected(opts.id)) registry.unregister(opts.id);
			if (watchdog) clearTimeout(watchdog);
		});
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
 * Opt-in per-subagent isolation: a `team/<id>` branch checked out into a
 * dedicated worktree under the omp worktrees dir. Idempotent — reuses an
 * existing worktree for the branch. Run under the per-repo lock since worktrees
 * of one repo share `.git` metadata that git locks without a waiter.
 */
export async function ensureTeamWorktree(id: string, repoRoot: string): Promise<string> {
	const branch = `team/${id}`;
	const branchRef = `refs/heads/${branch}`;
	const worktreePath = getWorktreeDir(`team-${id}-${hashPath(repoRoot)}`);
	return git.withRepoLock(repoRoot, async () => {
		const existing = (await git.worktree.list(repoRoot)).find(entry => entry.branch === branchRef);
		if (existing) return existing.path;
		if (!(await git.ref.exists(repoRoot, branchRef))) {
			await git.branch.create(repoRoot, branch, "HEAD");
		}
		await mkdir(dirname(worktreePath), { recursive: true });
		await git.worktree.add(repoRoot, worktreePath, branch);
		return worktreePath;
	});
}

/** Remove a {@link ensureTeamWorktree} worktree and its branch. Best-effort. */
export async function cleanupTeamWorktree(id: string, repoRoot: string): Promise<void> {
	const branch = `team/${id}`;
	const worktreePath = getWorktreeDir(`team-${id}-${hashPath(repoRoot)}`);
	await git.withRepoLock(repoRoot, async () => {
		await git.worktree.tryRemove(repoRoot, worktreePath);
		await git.branch.tryDelete(repoRoot, branch);
	});
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
	state?.notifyUnsub();
	await state?.broker.close();
}

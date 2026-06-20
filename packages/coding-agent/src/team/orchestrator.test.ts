import { afterEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { ptree } from "@oh-my-pi/pi-utils";
import { IrcBus } from "../irc/bus";
import { AgentRegistry } from "../registry/agent-registry";
import {
	buildTeamChildSpawn,
	type CmuxClientFactory,
	cleanupTeamWorktree,
	ensureTeamWorktree,
	getOrStartBroker,
	shutdownTeam,
} from "./orchestrator";

describe("buildTeamChildSpawn", () => {
	it("wires the team env and ends argv with the assignment", () => {
		const spawn = buildTeamChildSpawn({
			id: "ChildA",
			assignment: "do the thing",
			cwd: "/work",
			socketPath: "/tmp/omp-team.sock",
			baseEnv: { PATH: "/bin", OMP_TEAM: "1", FOO: "bar" },
			command: { cmd: "omp", args: ["--flag"] },
		});

		expect(spawn.argv).toEqual(["omp", "--flag", "do the thing"]);
		expect(spawn.argv.at(-1)).toBe("do the thing");
		expect(spawn.cwd).toBe("/work");
		expect(spawn.env.OMP_IRC_SOCKET).toBe("/tmp/omp-team.sock");
		expect(spawn.env.OMP_AGENT_ID).toBe("ChildA");
		// OMP_TEAM stripped so the child is a connector, never a second broker.
		expect("OMP_TEAM" in spawn.env).toBe(false);
		// Unrelated env passes through untouched.
		expect(spawn.env.FOO).toBe("bar");
		expect(spawn.env.PATH).toBe("/bin");
	});

	it("defaults the command from resolveOmpCommand when none is given", () => {
		const spawn = buildTeamChildSpawn({
			id: "X",
			assignment: "assignment text",
			cwd: ".",
			socketPath: "/s.sock",
			baseEnv: {},
		});

		expect(spawn.argv.length).toBeGreaterThanOrEqual(1);
		expect(spawn.argv.at(-1)).toBe("assignment text");
		expect(spawn.env.OMP_AGENT_ID).toBe("X");
	});
});

describe("ensureTeamWorktree / cleanupTeamWorktree", () => {
	it("creates then removes a per-subagent worktree and branch", async () => {
		const repo = mkdtempSync(join(tmpdir(), "omp-team-wt-"));
		await ptree.exec(["git", "init"], { cwd: repo });
		await ptree.exec(["git", "config", "user.email", "t@t.dev"], { cwd: repo });
		await ptree.exec(["git", "config", "user.name", "t"], { cwd: repo });
		await Bun.write(join(repo, "f.txt"), "x");
		await ptree.exec(["git", "add", "."], { cwd: repo });
		await ptree.exec(["git", "commit", "-m", "init"], { cwd: repo });

		const id = `WtChild-${randomUUID().slice(0, 8)}`;
		const worktreePath = await ensureTeamWorktree(id, repo);
		expect(existsSync(worktreePath)).toBe(true);
		const before = await ptree.exec(["git", "branch", "--list", `team/${id}`], { cwd: repo });
		expect(before.stdout).toContain(`team/${id}`);

		await cleanupTeamWorktree(id, repo);
		expect(existsSync(worktreePath)).toBe(false);
		const after = await ptree.exec(["git", "branch", "--list", `team/${id}`], { cwd: repo });
		expect(after.stdout.trim()).toBe("");
	});
});

describe("lead-side completion notifications", () => {
	const FAKE_SOCKET = "/tmp/omp-cmux-fake.sock";
	const ORIGINAL_SOCKET = process.env.CMUX_SOCKET_PATH;

	afterEach(async () => {
		await shutdownTeam();
		if (ORIGINAL_SOCKET === undefined) delete process.env.CMUX_SOCKET_PATH;
		else process.env.CMUX_SOCKET_PATH = ORIGINAL_SOCKET;
	});

	// Fake cmux client + a deterministic "next notification" signal so tests
	// await the real request call instead of a timer (rule ts-no-test-timers).
	function makeNotifyHarness() {
		const reg = new AgentRegistry();
		const bus = new IrcBus(reg);
		const calls: Record<string, unknown>[] = [];
		let waiters: Array<() => void> = [];
		let factoryCount = 0;
		const factory: CmuxClientFactory = () => {
			factoryCount += 1;
			return {
				connect: async () => {},
				request: async (method, params) => {
					if (method === "notification.create") {
						calls.push(params);
						const pending = waiters;
						waiters = [];
						for (const resolve of pending) resolve();
					}
					return {};
				},
				close: () => {},
			};
		};
		const nextNotify = (): Promise<void> => {
			const { promise, resolve } = Promise.withResolvers<void>();
			waiters.push(resolve);
			return promise;
		};
		return { reg, bus, calls, factory, nextNotify, factoryCount: () => factoryCount };
	}

	it("notifies once when a remote subagent finishes, and not again on the aborted tail", async () => {
		process.env.CMUX_SOCKET_PATH = FAKE_SOCKET;
		const h = makeNotifyHarness();
		await getOrStartBroker(h.bus, h.reg, h.factory);

		const first = h.nextNotify();
		h.reg.register({
			id: "Remote1",
			displayName: "Remote1",
			kind: "sub",
			session: null,
			remote: true,
			status: "running",
		});
		h.reg.setStatus("Remote1", "idle");
		await first;
		expect(h.calls).toHaveLength(1);
		expect(h.calls[0].title).toBe("Remote1");
		expect(h.calls[0].body).toBe("finished");

		// idle->aborted is the socket-drop tail of an already-finished child; it must not re-notify.
		h.reg.setStatus("Remote1", "aborted");

		// Sentinel completion drains the notify queue past the aborted tail: a
		// duplicate Remote1 would land before Remote2, so length 2 proves "once".
		const second = h.nextNotify();
		h.reg.register({
			id: "Remote2",
			displayName: "Remote2",
			kind: "sub",
			session: null,
			remote: true,
			status: "running",
		});
		h.reg.setStatus("Remote2", "idle");
		await second;
		expect(h.calls).toHaveLength(2);
		expect(h.calls[1].title).toBe("Remote2");
	});

	it("does not notify for a non-remote (local) ref", async () => {
		process.env.CMUX_SOCKET_PATH = FAKE_SOCKET;
		const h = makeNotifyHarness();
		await getOrStartBroker(h.bus, h.reg, h.factory);

		h.reg.register({
			id: "LocalChild",
			displayName: "LocalChild",
			kind: "sub",
			session: null,
			remote: false,
			status: "running",
		});
		h.reg.setStatus("LocalChild", "idle");

		// Only the remote sentinel produces a notification; the local finish is ignored.
		const notified = h.nextNotify();
		h.reg.register({
			id: "RemoteChild",
			displayName: "RemoteChild",
			kind: "sub",
			session: null,
			remote: true,
			status: "running",
		});
		h.reg.setStatus("RemoteChild", "idle");
		await notified;
		expect(h.calls.map(c => c.title)).toEqual(["RemoteChild"]);
	});

	it("is a no-op in headless mode (no CMUX_SOCKET_PATH)", async () => {
		delete process.env.CMUX_SOCKET_PATH;
		const h = makeNotifyHarness();
		await getOrStartBroker(h.bus, h.reg, h.factory);

		h.reg.register({
			id: "Remote1",
			displayName: "Remote1",
			kind: "sub",
			session: null,
			remote: true,
			status: "running",
		});
		h.reg.setStatus("Remote1", "idle");
		// No subscription is wired headless, so the cmux client is never constructed.
		expect(h.factoryCount()).toBe(0);
	});
});

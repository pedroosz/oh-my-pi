import { describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ptree } from "@oh-my-pi/pi-utils";
import { buildTeamChildSpawn, cleanupTeamWorktree, ensureTeamWorktree } from "./orchestrator";

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

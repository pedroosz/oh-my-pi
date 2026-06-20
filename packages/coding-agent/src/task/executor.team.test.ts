import { describe, expect, it, mock } from "bun:test";
import * as orchestrator from "../team/orchestrator";
import type { ExecutorOptions } from "./executor";

// Records each spawnTeamSubagent call so we can assert the executor forwards the
// team-worktree intent (fix 1) without launching a real omp child process.
const spawnCalls: Array<{ id: string; worktree?: boolean }> = [];
const spawnSpy = mock(async (opts: { id: string; worktree?: boolean }) => {
	spawnCalls.push({ id: opts.id, worktree: opts.worktree });
});
mock.module("../team/orchestrator", () => ({ ...orchestrator, spawnTeamSubagent: spawnSpy }));

// The executor must load AFTER the mock is registered, so its `spawnTeamSubagent`
// binding resolves to the spy. A static import is hoisted above the mock
// registration, so a dynamic import is the only way to control the order here.
const { runSubprocess } = await import("./executor");

function baseOptions(id: string, teamWorktree: boolean): ExecutorOptions {
	return {
		cwd: "/tmp",
		agent: { name: "task", source: "builtin" },
		task: "do it",
		assignment: "do it",
		index: 0,
		id,
		teamWorktree,
	} as unknown as ExecutorOptions;
}

describe("runSubprocess team-mode worktree wiring (fix 1)", () => {
	it("routes the isolation intent to spawnTeamSubagent({ worktree }) under OMP_TEAM", async () => {
		const prev = process.env.OMP_TEAM;
		process.env.OMP_TEAM = "1";
		try {
			// Isolation requested -> the child is asked to run in its own worktree.
			const isolated = await runSubprocess(baseOptions("ChildIso", true));
			// Team branch taken: returns immediately with exitCode 0, no in-process
			// AgentSession and no ensureIsolation/commitToBranch (those live on the
			// non-team index path, which this branch bypasses entirely).
			expect(isolated.exitCode).toBe(0);
			expect(isolated.output).toContain("ChildIso");

			// Isolation not requested -> no worktree.
			await runSubprocess(baseOptions("ChildPlain", false));

			expect(spawnCalls).toEqual([
				{ id: "ChildIso", worktree: true },
				{ id: "ChildPlain", worktree: false },
			]);
		} finally {
			if (prev === undefined) delete process.env.OMP_TEAM;
			else process.env.OMP_TEAM = prev;
		}
	});
});

import { describe, expect, it } from "bun:test";
import { buildTeamChildSpawn } from "./orchestrator";

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

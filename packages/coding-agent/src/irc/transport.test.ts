import { describe, expect, it } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JsonSocketServer, connectJsonSocket } from "./transport";

describe("json socket transport", () => {
	it("round-trips line-delimited frames over a unix socket", async () => {
		const sockPath = join(mkdtempSync(join(tmpdir(), "omp-irc-")), "s.sock");
		const got: unknown[] = [];
		const server = new JsonSocketServer();
		server.onConnection(conn =>
			conn.onFrame(f => {
				got.push(f);
				conn.send({ t: "ack" });
			}),
		);
		await server.listen(sockPath);

		const client = await connectJsonSocket(sockPath);
		const acks: unknown[] = [];
		// The server sends "ack" only after it has pushed the inbound frame, so
		// awaiting the ack deterministically proves both directions arrived —
		// no wall-clock sleep (see rule ts-no-test-timers).
		const gotAck = Promise.withResolvers<void>();
		client.onFrame(f => {
			acks.push(f);
			gotAck.resolve();
		});
		client.send({ t: "hello", agents: [] });
		await gotAck.promise;

		expect(got).toEqual([{ t: "hello", agents: [] }]);
		expect(acks).toEqual([{ t: "ack" }]);
		client.close();
		await server.close();
	});
});

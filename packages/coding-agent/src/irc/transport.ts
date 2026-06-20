import * as net from "node:net";

export interface JsonConn {
	send(frame: unknown): void;
	onFrame(cb: (frame: unknown) => void): void;
	onClose(cb: () => void): void;
	close(): void;
}

function wrap(socket: net.Socket): JsonConn {
	let buf = "";
	const frameCbs: ((f: unknown) => void)[] = [];
	const closeCbs: (() => void)[] = [];
	socket.setEncoding("utf8");
	socket.on("data", (chunk: string) => {
		buf += chunk;
		let nl: number;
		while ((nl = buf.indexOf("\n")) >= 0) {
			const line = buf.slice(0, nl);
			buf = buf.slice(nl + 1);
			if (!line) continue;
			let parsed: unknown;
			try {
				parsed = JSON.parse(line);
			} catch {
				continue; // ignore malformed lines
			}
			for (const cb of frameCbs) cb(parsed);
		}
	});
	socket.on("close", () => {
		for (const cb of closeCbs) cb();
	});
	socket.on("error", () => {
		/* surfaced via close */
	});
	return {
		send: frame => {
			socket.write(`${JSON.stringify(frame)}\n`);
		},
		onFrame: cb => {
			frameCbs.push(cb);
		},
		onClose: cb => {
			closeCbs.push(cb);
		},
		close: () => socket.destroy(),
	};
}

export class JsonSocketServer {
	#server: net.Server | undefined;
	#onConn: ((conn: JsonConn) => void)[] = [];

	onConnection(cb: (conn: JsonConn) => void): void {
		this.#onConn.push(cb);
	}

	listen(socketPath: string): Promise<void> {
		const { promise, resolve, reject } = Promise.withResolvers<void>();
		const server = net.createServer(socket => {
			const conn = wrap(socket);
			for (const cb of this.#onConn) cb(conn);
		});
		server.on("error", reject);
		server.listen(socketPath, () => {
			server.off("error", reject);
			this.#server = server;
			resolve();
		});
		return promise;
	}

	close(): Promise<void> {
		const { promise, resolve } = Promise.withResolvers<void>();
		if (!this.#server) {
			resolve();
			return promise;
		}
		this.#server.close(() => resolve());
		return promise;
	}
}

export function connectJsonSocket(socketPath: string, timeoutMs = 10_000): Promise<JsonConn> {
	const { promise, resolve, reject } = Promise.withResolvers<JsonConn>();
	const socket = net.createConnection(socketPath);
	const timer = setTimeout(() => {
		socket.destroy();
		reject(new Error("irc socket connect timeout"));
	}, timeoutMs);
	socket.once("connect", () => {
		clearTimeout(timer);
		resolve(wrap(socket));
	});
	socket.once("error", err => {
		clearTimeout(timer);
		reject(err);
	});
	return promise;
}

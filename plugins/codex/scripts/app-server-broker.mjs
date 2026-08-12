#!/usr/bin/env node

import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import process from "node:process";

import { parseArgs } from "./lib/args.mjs";
import { BROKER_BUSY_RPC_CODE, CodexAppServerClient, resolveTurnWatchdogConfig, timeoutFromEnv } from "./lib/app-server.mjs";
import { parseBrokerEndpoint } from "./lib/broker-endpoint.mjs";

const STREAMING_METHODS = new Set(["turn/start", "review/start", "thread/compact/start"]);
const DEFAULT_STREAM_LEASE_MS = 65 * 60 * 1000;
const STREAM_LEASE_MARGIN_MS = 60 * 1000;

function resolveStreamLeaseMs() {
  const configured = timeoutFromEnv(process.env, "CODEX_COMPANION_BROKER_STREAM_LEASE_MS", DEFAULT_STREAM_LEASE_MS);
  const watchdog = resolveTurnWatchdogConfig(process.env);
  const floor = Math.max(watchdog.idleTimeoutMs, watchdog.activeItemTimeoutMs) + STREAM_LEASE_MARGIN_MS;
  if (configured < floor) {
    process.stderr.write(
      `Configured broker stream lease ${configured}ms is below the watchdog safety floor ${floor}ms. Using ${floor}ms.\n`
    );
    return floor;
  }
  return configured;
}

function buildStreamThreadIds(method, params, result) {
  const threadIds = new Set();
  if (params?.threadId) {
    threadIds.add(params.threadId);
  }
  if (method === "review/start" && result?.reviewThreadId) {
    threadIds.add(result.reviewThreadId);
  }
  return threadIds;
}

function buildJsonRpcError(code, message, data) {
  return data === undefined ? { code, message } : { code, message, data };
}

function send(socket, message) {
  if (socket.destroyed) {
    return;
  }
  socket.write(`${JSON.stringify(message)}\n`);
}

function isInterruptRequest(message) {
  return message?.method === "turn/interrupt";
}

function writePidFile(pidFile) {
  if (!pidFile) {
    return;
  }
  fs.mkdirSync(path.dirname(pidFile), { recursive: true });
  fs.writeFileSync(pidFile, `${process.pid}\n`, "utf8");
}

async function main() {
  const [subcommand, ...argv] = process.argv.slice(2);
  if (subcommand !== "serve") {
    throw new Error("Usage: node scripts/app-server-broker.mjs serve --endpoint <value> [--cwd <path>] [--pid-file <path>]");
  }

  const { options } = parseArgs(argv, {
    valueOptions: ["cwd", "pid-file", "endpoint"]
  });

  if (!options.endpoint) {
    throw new Error("Missing required --endpoint.");
  }

  const cwd = options.cwd ? path.resolve(process.cwd(), options.cwd) : process.cwd();
  const endpoint = String(options.endpoint);
  const listenTarget = parseBrokerEndpoint(endpoint);
  const pidFile = options["pid-file"] ? path.resolve(options["pid-file"]) : null;
  const leaseDurationMs = resolveStreamLeaseMs();
  writePidFile(pidFile);

  const appClient = await CodexAppServerClient.connect(cwd, { disableBroker: true });
  let activeRequestSocket = null;
  let activeStreamSocket = null;
  let activeStreamThreadIds = null;
  let activeStreamLease = null;
  let runtimeAlive = true;
  let shuttingDown = false;
  const orphanThreadIds = new Set();
  const sockets = new Set();

  function clearActiveStream() {
    if (activeStreamLease?.timer) {
      clearTimeout(activeStreamLease.timer);
    }
    activeStreamSocket = null;
    activeStreamThreadIds = null;
    activeStreamLease = null;
  }

  function refreshActiveStreamLease() {
    if (!activeStreamSocket || !activeStreamLease) {
      return;
    }
    if (activeStreamLease.timer) {
      clearTimeout(activeStreamLease.timer);
    }
    const durationMs = leaseDurationMs;
    activeStreamLease.expiresAt = Date.now() + durationMs;
    const lease = activeStreamLease;
    lease.timer = setTimeout(() => {
      if (activeStreamLease !== lease || activeStreamSocket !== lease.socket) {
        return;
      }
      const owner = lease.socket;
      clearActiveStream();
      owner.destroy();
    }, durationMs);
    lease.timer.unref?.();
  }

  function setActiveStream(socket, threadIds) {
    clearActiveStream();
    activeStreamSocket = socket;
    activeStreamThreadIds = threadIds;
    activeStreamLease = { socket, expiresAt: 0, timer: null };
    refreshActiveStreamLease();
  }

  function clearSocketOwnership(socket) {
    if (activeRequestSocket === socket) {
      activeRequestSocket = null;
    }
    if (activeStreamSocket === socket) {
      clearActiveStream();
    }
  }

  function routeNotification(message) {
    const threadId = message.params?.threadId ?? null;
    if (threadId && orphanThreadIds.has(threadId)) {
      if (message.method === "turn/completed" || message.method === "error") {
        orphanThreadIds.delete(threadId);
      }
      return;
    }
    const target = activeRequestSocket ?? activeStreamSocket;
    if (!target) {
      return;
    }
    send(target, message);
    refreshActiveStreamLease();
    if (message.method === "turn/completed" && activeStreamSocket === target) {
      if (!threadId || !activeStreamThreadIds || activeStreamThreadIds.has(threadId)) {
        clearActiveStream();
        if (activeRequestSocket === target) {
          activeRequestSocket = null;
        }
      }
    }
  }

  async function shutdown(server, options = {}) {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    runtimeAlive = false;
    clearActiveStream();
    activeRequestSocket = null;
    for (const socket of sockets) {
      socket.destroy();
    }
    if (options.closeAppClient !== false) {
      await appClient.close().catch(() => {});
    }
    await new Promise((resolve) => server.close(resolve));
    if (listenTarget.kind === "unix" && fs.existsSync(listenTarget.path)) {
      fs.unlinkSync(listenTarget.path);
    }
    if (pidFile && fs.existsSync(pidFile)) {
      fs.unlinkSync(pidFile);
    }
  }

  appClient.setNotificationHandler(routeNotification);

  const server = net.createServer((socket) => {
    if (!runtimeAlive) {
      socket.destroy();
      return;
    }
    sockets.add(socket);
    socket.setEncoding("utf8");
    let buffer = "";

    socket.on("data", async (chunk) => {
      buffer += chunk;
      let newlineIndex = buffer.indexOf("\n");
      while (newlineIndex !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        newlineIndex = buffer.indexOf("\n");

        if (!line.trim()) {
          continue;
        }

        let message;
        try {
          message = JSON.parse(line);
        } catch (error) {
          send(socket, {
            id: null,
            error: buildJsonRpcError(-32700, `Invalid JSON: ${error.message}`)
          });
          continue;
        }

        if (message.id !== undefined && message.method === "initialize") {
          if (!runtimeAlive) {
            socket.destroy();
            continue;
          }
          send(socket, {
            id: message.id,
            result: {
              userAgent: "codex-companion-broker"
            }
          });
          continue;
        }

        if (message.method === "initialized" && message.id === undefined) {
          continue;
        }

        if (message.id !== undefined && message.method === "broker/shutdown") {
          send(socket, { id: message.id, result: {} });
          await shutdown(server);
          process.exit(0);
        }

        if (message.id !== undefined && message.method === "broker/orphan-threads") {
          for (const threadId of message.params?.threadIds ?? []) {
            if (typeof threadId === "string" && threadId) {
              orphanThreadIds.add(threadId);
            }
          }
          send(socket, { id: message.id, result: {} });
          continue;
        }

        if (message.id === undefined) {
          continue;
        }

        const allowInterruptDuringActiveStream =
          isInterruptRequest(message) && activeStreamSocket && activeStreamSocket !== socket && !activeRequestSocket;

        if (
          ((activeRequestSocket && activeRequestSocket !== socket) || (activeStreamSocket && activeStreamSocket !== socket)) &&
          !allowInterruptDuringActiveStream
        ) {
          send(socket, {
            id: message.id,
            error: buildJsonRpcError(BROKER_BUSY_RPC_CODE, "Shared Codex broker is busy.")
          });
          continue;
        }

        if (allowInterruptDuringActiveStream) {
          try {
            const result = await appClient.request(message.method, message.params ?? {});
            send(socket, { id: message.id, result });
          } catch (error) {
            send(socket, {
              id: message.id,
              error: buildJsonRpcError(error.rpcCode ?? -32000, error.message)
            });
          }
          continue;
        }

        const isStreaming = STREAMING_METHODS.has(message.method);
        activeRequestSocket = socket;
        if (isStreaming) {
          setActiveStream(socket, buildStreamThreadIds(message.method, message.params ?? {}, null));
        }

        try {
          const result = await appClient.request(message.method, message.params ?? {});
          send(socket, { id: message.id, result });
          if (isStreaming && activeStreamSocket === socket) {
            activeStreamThreadIds = buildStreamThreadIds(message.method, message.params ?? {}, result);
            refreshActiveStreamLease();
          }
          if (activeRequestSocket === socket) {
            activeRequestSocket = null;
          }
        } catch (error) {
          send(socket, {
            id: message.id,
            error: buildJsonRpcError(error.rpcCode ?? -32000, error.message)
          });
          if (activeRequestSocket === socket) {
            activeRequestSocket = null;
          }
          if (activeStreamSocket === socket && isStreaming) {
            clearActiveStream();
          }
        }
      }
    });

    socket.on("close", () => {
      sockets.delete(socket);
      clearSocketOwnership(socket);
    });

    socket.on("error", () => {
      sockets.delete(socket);
      clearSocketOwnership(socket);
    });
  });

  appClient.transportClosed.catch(async () => {
    if (shuttingDown) {
      return;
    }
    runtimeAlive = false;
    if (activeStreamSocket) {
      activeStreamSocket.destroy();
    }
    await shutdown(server, { closeAppClient: false });
    process.exitCode = 1;
  });

  process.on("SIGTERM", async () => {
    await shutdown(server);
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    await shutdown(server);
    process.exit(0);
  });

  server.listen(listenTarget.path);
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});

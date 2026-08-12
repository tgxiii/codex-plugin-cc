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
const ORPHAN_QUARANTINE_TTL_MS = 30 * 60 * 1000;
const MAX_ORPHAN_QUARANTINES = 32;

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
  const orphanQuarantines = new Map();
  const sockets = new Set();

  function orphanQuarantineKey(threadId, turnId) {
    return `${threadId}\u0000${turnId}`;
  }

  function purgeExpiredOrphanQuarantines() {
    const now = Date.now();
    for (const [key, entry] of orphanQuarantines) {
      if (entry.expiresAt <= now) {
        orphanQuarantines.delete(key);
      }
    }
  }

  function addOrphanQuarantine(threadId, turnId) {
    if (!threadId || !turnId) {
      return;
    }
    purgeExpiredOrphanQuarantines();
    const key = orphanQuarantineKey(threadId, turnId);
    orphanQuarantines.delete(key);
    orphanQuarantines.set(key, { threadId, turnId, expiresAt: Date.now() + ORPHAN_QUARANTINE_TTL_MS });
    while (orphanQuarantines.size > MAX_ORPHAN_QUARANTINES) {
      orphanQuarantines.delete(orphanQuarantines.keys().next().value);
    }
  }

  function hasOrphanQuarantine(threadId, turnId) {
    purgeExpiredOrphanQuarantines();
    return Boolean(threadId && turnId && orphanQuarantines.has(orphanQuarantineKey(threadId, turnId)));
  }

  function hasQuarantinedTurn(turnId) {
    purgeExpiredOrphanQuarantines();
    return Boolean(turnId && [...orphanQuarantines.values()].some((entry) => entry.turnId === turnId));
  }

  function clearThreadOrphanQuarantines(threadId) {
    for (const [key, entry] of orphanQuarantines) {
      if (entry.threadId === threadId) {
        orphanQuarantines.delete(key);
      }
    }
  }

  function notificationThreadId(message) {
    if (message.method === "thread/started") {
      return message.params?.thread?.id ?? null;
    }
    return message.params?.threadId ?? message.params?.thread?.id ?? null;
  }

  function notificationTurnId(message) {
    return message.params?.turnId ?? message.params?.turn?.id ?? null;
  }

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
    for (const threadId of threadIds) {
      clearThreadOrphanQuarantines(threadId);
    }
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
    const threadId = notificationThreadId(message);
    const turnId = notificationTurnId(message);
    if (message.method === "thread/started") {
      const parentThreadId = message.params?.parentThreadId ?? message.params?.threadId ?? null;
      const parentTurnId = message.params?.parentTurnId ?? turnId;
      if (hasOrphanQuarantine(parentThreadId, parentTurnId) || hasQuarantinedTurn(parentTurnId)) {
        addOrphanQuarantine(threadId, parentTurnId);
        return;
      }
    }
    if (hasOrphanQuarantine(threadId, turnId)) {
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

        if (message.method === "broker/orphan-threads") {
          const ownsStream = activeRequestSocket === socket || activeStreamSocket === socket;
          if (!ownsStream) {
            send(socket, {
              id: message.id,
              error: buildJsonRpcError(BROKER_BUSY_RPC_CODE, "Only the active stream owner can quarantine orphan turns.")
            });
            continue;
          }
          for (const entry of message.params?.turns ?? []) {
            if (typeof entry?.threadId === "string" && typeof entry?.turnId === "string") {
              addOrphanQuarantine(entry.threadId, entry.turnId);
            }
          }
          send(socket, { id: message.id, result: {} });
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

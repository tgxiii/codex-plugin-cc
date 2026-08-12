/**
 * @typedef {Error & { data?: unknown, rpcCode?: number }} ProtocolError
 * @typedef {import("./app-server-protocol").AppServerMethod} AppServerMethod
 * @typedef {import("./app-server-protocol").AppServerNotification} AppServerNotification
 * @typedef {import("./app-server-protocol").AppServerNotificationHandler} AppServerNotificationHandler
 * @typedef {import("./app-server-protocol").ClientInfo} ClientInfo
 * @typedef {import("./app-server-protocol").CodexAppServerClientOptions} CodexAppServerClientOptions
 * @typedef {import("./app-server-protocol").InitializeCapabilities} InitializeCapabilities
 */
import fs from "node:fs";
import net from "node:net";
import process from "node:process";
import { spawn } from "node:child_process";
import readline from "node:readline";
import { parseBrokerEndpoint } from "./broker-endpoint.mjs";
import { ensureBrokerSession, loadBrokerSession } from "./broker-lifecycle.mjs";
import { terminateProcessTree } from "./process.mjs";

const PLUGIN_MANIFEST_URL = new URL("../../.claude-plugin/plugin.json", import.meta.url);
const PLUGIN_MANIFEST = JSON.parse(fs.readFileSync(PLUGIN_MANIFEST_URL, "utf8"));

export const BROKER_ENDPOINT_ENV = "CODEX_COMPANION_APP_SERVER_ENDPOINT";
export const BROKER_BUSY_RPC_CODE = -32001;
export const TURN_IDLE_TIMEOUT_CODE = "TURN_IDLE_TIMEOUT";
export const DEFAULT_TURN_IDLE_TIMEOUT_MS = 10 * 60 * 1000;
export const DEFAULT_TURN_ACTIVE_TIMEOUT_MS = 60 * 60 * 1000;
export const DEFAULT_TURN_INTERRUPT_DEADLINE_MS = 5_000;
export const DEFAULT_TURN_INTERRUPT_GRACE_MS = 5_000;

const CLOSE_TIMEOUT_MS = 5_000;
const REQUEST_DEADLINE_MS = new Map([
  ["turn/interrupt", DEFAULT_TURN_INTERRUPT_DEADLINE_MS]
]);

export function timeoutFromEnv(env, name, fallback) {
  const value = Number.parseInt(env?.[name] ?? "", 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

export function resolveTurnWatchdogConfig(env = process.env) {
  return {
    idleTimeoutMs: timeoutFromEnv(env, "CODEX_COMPANION_TURN_IDLE_TIMEOUT_MS", DEFAULT_TURN_IDLE_TIMEOUT_MS),
    activeItemTimeoutMs: timeoutFromEnv(env, "CODEX_COMPANION_TURN_ACTIVE_TIMEOUT_MS", DEFAULT_TURN_ACTIVE_TIMEOUT_MS),
    interruptDeadlineMs: timeoutFromEnv(env, "CODEX_COMPANION_TURN_INTERRUPT_DEADLINE_MS", DEFAULT_TURN_INTERRUPT_DEADLINE_MS),
    interruptGraceMs: timeoutFromEnv(env, "CODEX_COMPANION_TURN_INTERRUPT_GRACE_MS", DEFAULT_TURN_INTERRUPT_GRACE_MS)
  };
}

function formatTimeoutWindow(timeoutMs) {
  if (timeoutMs % 60_000 === 0) {
    const minutes = timeoutMs / 60_000;
    return `${minutes} ${minutes === 1 ? "minute" : "minutes"}`;
  }
  if (timeoutMs % 1_000 === 0) {
    const seconds = timeoutMs / 1_000;
    return `${seconds} ${seconds === 1 ? "second" : "seconds"}`;
  }
  return `${timeoutMs} milliseconds`;
}

export function buildTurnTimeoutMessage(timeoutMs, interruptAcknowledged) {
  const outcome = interruptAcknowledged
    ? "The underlying work may have completed. Inspect the worktree and rollout."
    : "The underlying turn may still be running; the working tree may still be written to. Retry /codex:cancel before continuing.";
  return `Codex turn timed out after ${formatTimeoutWindow(timeoutMs)} without app-server events. ${outcome}`;
}

/** @type {ClientInfo} */
const DEFAULT_CLIENT_INFO = {
  title: "Codex Plugin",
  name: "Claude Code",
  version: PLUGIN_MANIFEST.version ?? "0.0.0"
};

/** @type {InitializeCapabilities} */
const DEFAULT_CAPABILITIES = {
  experimentalApi: false,
  requestAttestation: false,
  optOutNotificationMethods: [
    "item/agentMessage/delta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta"
  ]
};

function buildJsonRpcError(code, message, data) {
  return data === undefined ? { code, message } : { code, message, data };
}

function createProtocolError(message, data) {
  const error = /** @type {ProtocolError} */ (new Error(message));
  error.data = data;
  if (data?.code !== undefined) {
    error.rpcCode = data.code;
  }
  return error;
}

class AppServerClientBase {
  constructor(cwd, options = {}) {
    this.cwd = cwd;
    this.options = options;
    this.pending = new Map();
    this.nextId = 1;
    this.stderr = "";
    this.closed = false;
    this.exitError = null;
    /** @type {AppServerNotificationHandler | null} */
    this.notificationHandler = null;
    this.lineBuffer = "";
    this.transport = "unknown";

    this.exitPromise = new Promise((resolve) => {
      this.resolveExit = resolve;
    });
    this.transportClosed = new Promise((_, reject) => {
      this.rejectTransportClosed = reject;
    });
    this.transportClosed.catch(() => {});
  }

  setNotificationHandler(handler) {
    this.notificationHandler = handler;
  }

  /**
   * @template {AppServerMethod} M
   * @param {M} method
   * @param {import("./app-server-protocol").AppServerRequestParams<M>} params
   * @returns {Promise<import("./app-server-protocol").AppServerResponse<M>>}
   */
  request(method, params, options = {}) {
    if (this.closed) {
      throw new Error("codex app-server client is closed.");
    }

    const id = this.nextId;
    this.nextId += 1;

    return new Promise((resolve, reject) => {
      const defaultDeadlineMs = REQUEST_DEADLINE_MS.get(method) ?? null;
      const deadlineMs = options.deadlineMs ?? (
        method === "turn/interrupt"
          ? timeoutFromEnv(this.options.env ?? process.env, "CODEX_COMPANION_TURN_INTERRUPT_DEADLINE_MS", defaultDeadlineMs)
          : defaultDeadlineMs
      );
      const deadlineTimer = deadlineMs
        ? setTimeout(() => {
            this.pending.delete(id);
            const error = /** @type {Error & { code?: string }} */ (
              new Error(`codex app-server ${method} timed out after ${deadlineMs}ms.`)
            );
            error.code = "APP_SERVER_REQUEST_TIMEOUT";
            reject(error);
          }, deadlineMs)
        : null;
      deadlineTimer?.unref?.();

      const settle = (callback) => (value) => {
        if (deadlineTimer) {
          clearTimeout(deadlineTimer);
        }
        callback(value);
      };
      this.pending.set(id, { resolve: settle(resolve), reject: settle(reject), method });
      try {
        this.sendMessage({ id, method, params });
      } catch (error) {
        this.pending.delete(id);
        settle(reject)(error);
      }
    });
  }

  notify(method, params = {}) {
    if (this.closed) {
      return;
    }
    this.sendMessage({ method, params });
  }

  handleChunk(chunk) {
    this.lineBuffer += chunk;
    let newlineIndex = this.lineBuffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = this.lineBuffer.slice(0, newlineIndex);
      this.lineBuffer = this.lineBuffer.slice(newlineIndex + 1);
      this.handleLine(line);
      newlineIndex = this.lineBuffer.indexOf("\n");
    }
  }

  handleLine(line) {
    if (this.exitResolved) {
      return;
    }
    if (!line.trim()) {
      return;
    }

    let message;
    try {
      message = JSON.parse(line);
    } catch (error) {
      this.handleExit(createProtocolError(`Failed to parse codex app-server JSONL: ${error.message}`, { line }));
      return;
    }

    if (message.id !== undefined && message.method) {
      this.handleServerRequest(message);
      return;
    }

    if (message.id !== undefined) {
      const pending = this.pending.get(message.id);
      if (!pending) {
        return;
      }
      this.pending.delete(message.id);

      if (message.error) {
        pending.reject(createProtocolError(message.error.message ?? `codex app-server ${pending.method} failed.`, message.error));
      } else {
        pending.resolve(message.result ?? {});
      }
      return;
    }

    if (message.method && this.notificationHandler) {
      this.notificationHandler(/** @type {AppServerNotification} */ (message));
    }
  }

  handleServerRequest(message) {
    this.sendMessage({
      id: message.id,
      error: buildJsonRpcError(-32601, `Unsupported server request: ${message.method}`)
    });
  }

  handleExit(error) {
    if (this.exitResolved) {
      return;
    }

    this.exitResolved = true;
    this.exitError = error ?? null;
    const closureError = this.exitError ?? new Error("codex app-server connection closed.");

    for (const pending of this.pending.values()) {
      pending.reject(closureError);
    }
    this.pending.clear();
    this.rejectTransportClosed(closureError);
    this.resolveExit(undefined);
  }

  async waitForExit(onTimeout) {
    let timer = null;
    await Promise.race([
      this.exitPromise,
      new Promise((resolve) => {
        timer = setTimeout(() => {
          onTimeout();
          resolve();
        }, CLOSE_TIMEOUT_MS);
        timer.unref?.();
      })
    ]);
    if (timer) {
      clearTimeout(timer);
    }
  }

  sendMessage(_message) {
    throw new Error("sendMessage must be implemented by subclasses.");
  }
}

class SpawnedCodexAppServerClient extends AppServerClientBase {
  constructor(cwd, options = {}) {
    super(cwd, options);
    this.transport = "direct";
  }

  async initialize() {
    this.proc = spawn("codex", ["app-server"], {
      cwd: this.cwd,
      env: this.options.env ?? process.env,
      stdio: ["pipe", "pipe", "pipe"],
      shell: process.platform === "win32" ? (process.env.SHELL || true) : false,
      windowsHide: true
    });

    this.proc.stdout.setEncoding("utf8");
    this.proc.stderr.setEncoding("utf8");

    this.proc.stderr.on("data", (chunk) => {
      this.stderr += chunk;
    });

    this.proc.on("error", (error) => {
      this.handleExit(error);
    });

    this.proc.on("exit", (code, signal) => {
      const stderr = this.stderr.trim();
      const detail =
        code === 0
          ? null
          : createProtocolError(
              `codex app-server exited unexpectedly (${signal ? `signal ${signal}` : `exit ${code}`}).${stderr ? `\n${stderr}` : ""}`
            );
      this.handleExit(detail);
    });

    this.readline = readline.createInterface({ input: this.proc.stdout });
    this.readline.on("line", (line) => {
      this.handleLine(line);
    });

    await this.request("initialize", {
      clientInfo: this.options.clientInfo ?? DEFAULT_CLIENT_INFO,
      capabilities: this.options.capabilities ?? DEFAULT_CAPABILITIES
    });
    this.notify("initialized", {});
  }

  async close() {
    if (this.closed) {
      await this.waitForExit(() => {});
      return;
    }

    this.closed = true;

    if (this.readline) {
      this.readline.close();
    }

    if (this.proc && !this.proc.killed) {
      this.proc.stdin.end();
      setTimeout(() => {
        if (this.proc && !this.proc.killed && this.proc.exitCode === null) {
          // On Windows with shell: true, the direct child is cmd.exe.
          // Use terminateProcessTree to kill the entire tree including
          // the grandchild node process.
          if (process.platform === "win32") {
            try {
              terminateProcessTree(this.proc.pid);
            } catch {
              // Best-effort cleanup inside an unref'd timer — swallow errors
              // to avoid crashing the host process during shutdown.
            }
          } else {
            this.proc.kill("SIGTERM");
          }
        }
      }, 50).unref?.();
    }

    await this.waitForExit(() => {
      if (this.proc && this.proc.exitCode === null) {
        this.proc.kill("SIGKILL");
      }
      this.handleExit(new Error("Timed out while closing codex app-server."));
    });
  }

  sendMessage(message) {
    const line = `${JSON.stringify(message)}\n`;
    const stdin = this.proc?.stdin;
    if (!stdin) {
      throw new Error("codex app-server stdin is not available.");
    }
    stdin.write(line);
  }
}

class BrokerCodexAppServerClient extends AppServerClientBase {
  constructor(cwd, options = {}) {
    super(cwd, options);
    this.transport = "broker";
    this.endpoint = options.brokerEndpoint;
  }

  async initialize() {
    await new Promise((resolve, reject) => {
      const target = parseBrokerEndpoint(this.endpoint);
      let connected = false;
      this.socket = net.createConnection({ path: target.path });
      this.socket.setEncoding("utf8");
      this.socket.on("connect", () => {
        connected = true;
        resolve();
      });
      this.socket.on("data", (chunk) => {
        this.handleChunk(chunk);
      });
      this.socket.on("error", (error) => {
        if (!this.exitResolved) {
          reject(error);
        }
        this.handleExit(error);
      });
      this.socket.on("close", () => {
        if (!connected) {
          reject(this.exitError ?? new Error("codex app-server broker connection closed before initialization."));
        }
        this.handleExit(this.exitError);
      });
    });

    await this.request("initialize", {
      clientInfo: this.options.clientInfo ?? DEFAULT_CLIENT_INFO,
      capabilities: this.options.capabilities ?? DEFAULT_CAPABILITIES
    });
    this.notify("initialized", {});
  }

  async close() {
    if (this.closed) {
      await this.waitForExit(() => {});
      return;
    }

    this.closed = true;
    if (this.socket) {
      this.socket.end();
    }
    await this.waitForExit(() => {
      this.socket?.destroy();
      this.handleExit(new Error("Timed out while closing the codex app-server broker connection."));
    });
  }

  sendMessage(message) {
    const line = `${JSON.stringify(message)}\n`;
    const socket = this.socket;
    if (!socket) {
      throw new Error("codex app-server broker connection is not connected.");
    }
    socket.write(line);
  }
}

export class CodexAppServerClient {
  static async connect(cwd, options = {}) {
    let brokerEndpoint = null;
    if (!options.disableBroker) {
      brokerEndpoint = options.brokerEndpoint ?? options.env?.[BROKER_ENDPOINT_ENV] ?? process.env[BROKER_ENDPOINT_ENV] ?? null;
      if (!brokerEndpoint && options.reuseExistingBroker) {
        brokerEndpoint = loadBrokerSession(cwd)?.endpoint ?? null;
      }
      if (!brokerEndpoint && !options.reuseExistingBroker) {
        const brokerSession = await ensureBrokerSession(cwd, { env: options.env });
        brokerEndpoint = brokerSession?.endpoint ?? null;
      }
    }
    const client = brokerEndpoint
      ? new BrokerCodexAppServerClient(cwd, { ...options, brokerEndpoint })
      : new SpawnedCodexAppServerClient(cwd, options);
    await client.initialize();
    return client;
  }
}

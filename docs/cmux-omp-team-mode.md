# cmux omp — Oh My Pi team mode (design)

- Status: Proposed (design, not yet implemented)
- Date: 2026-06-19
- Scope: two repos — `oh-my-pi` (the core: cross-process irc transport + team spawn) and `manaflow-ai/cmux` (a mechanical `cmux omp` launch arm). Work happens on forks (`pedroosz/oh-my-pi`, `pedroosz/cmux`).

## Summary

`cmux omp` launches a **lead orchestrator omp** whose subagents are not in-process sessions but **full omp processes, each in its own cmux tab**. The human can drop into any subagent tab and drive it directly; the subagent keeps talking to the lead over `irc` exactly as an in-process subagent does today, including reporting how it diverged from its original assignment. The lead orchestrates with omp's existing main/subagent/`irc` model unchanged. The feature is "omp's default multi-agent behaviour, one layer up": the only thing that changes is that a subagent is an external process surfaced as a tab, reached over a cross-process transport, instead of an in-process `AgentSession`.

## Goal / UX

- `cmux omp [prompt]` opens a cmux window and starts the lead omp (orchestrator).
- The lead decomposes work and spawns subagents; each subagent appears as its own cmux tab running a full, interactive omp.
- You can drop into any subagent tab and work in it by hand (it is a normal omp TUI, not a read-only view).
- Each subagent reports to the lead over `irc` as it works, including divergence from its assignment; the lead reconciles and can steer back over the same channel (bidirectional, because omp `irc` already is).
- You do not babysit the lead tab; it orchestrates. You intervene per tab.

## Non-goals (v1)

- No new "divergence handover" primitive. Divergence is ordinary `irc` traffic (a subagent narrating progress to its main), so it needs no new message type. Full session transfer remains available via the existing `handoff` / `branch` commands if ever wanted.
- No lead reconnect-after-exit. If the lead exits, children survive as ordinary omp tabs (you keep your work); re-attaching a relaunched lead is a later nicety.
- No change to omp's decomposition, orchestration loop, or `irc` semantics. Team mode is a transport + surfacing layer over them.

## Background: most of this already exists

### cmux side (Swift, `manaflow-ai/cmux`, `CLI/cmux.swift`)
The `omx` / `omc` / `omo` integrations are thin launch wrappers that (1) resolve the tool binary, (2) write a per-tool tmux shim dir under `~/.cmuxterm/<tool>-bin/` whose fake `tmux` execs `cmux __tmux-compat`, (3) fake a tmux env via `configureTmuxCompatEnvironment` (`CLI/cmux.swift` ~19025) incl `CMUX_SOCKET_PATH` + `CMUX_WORKSPACE_ID` / `CMUX_SURFACE_ID`, then (4) `execv` the tool. `runClaudeTeamsTmuxCompat` (~21618) translates tmux verbs into V2 JSON-RPC over the Unix socket: `split-window` -> `surface.split`, `new-window` -> `workspace.create`, `send-keys` -> `surface.send_text`, `capture-pane` -> `surface.read_text`. Per-tool launch arms: `runOMX` (~21441), `runOMC` (~21561), `runOMO` (~21290); recognized-command list (~5296); usage help (~14363+).

omp is **already half-wired** into cmux:
- agent definition `id:"omp"` (`Sources/.../TaskManagerTypes.swift`)
- launcher trust map `"pi":["omp"]` (`AgentLaunchCaptureTrust.swift`)
- a session-hooks extension already in the Xcode build (`CLI/CMUXCLI+OmpExtension.swift`) that installs `~/.omp/agent/extensions/cmux-omp-session.ts`, bridging omp `session_start` / `before_agent_start` / `agent_end` -> `cmux hooks omp …`, gated on `CMUX_SURFACE_ID`
- an `AgentHookDef "omp"` (`CLI/CMUXCLI+AgentHookDefinitions.swift:202`)
- **Missing: only the `cmux omp` launch arm** — no `runOMP` / `createOMPShimDirectory` / dispatch / usage / Go relay. The explorer's verdict: a mechanical mirror of the `omx` arm.

### omp side (TypeScript, `oh-my-pi`, `packages/coding-agent/src`)
- The lead->child control channel already exists: `--mode rpc` host (`modes/rpc/rpc-mode.ts`) + `RpcClient` (`modes/rpc/rpc-client.ts`) already spawns `bun cli --mode rpc --cwd <wt> --session-dir …` and drives it with a full JSONL contract (`modes/rpc/rpc-types.ts`). Today this is **observe-only** for subagents: `modes/rpc/rpc-subagents.ts` subscribes the `task:subagent:*` EventBus channels and surfaces snapshots, but "the client can observe subagents but cannot `irc` them (irc is in-process)."
- `irc` / `AgentRegistry` / `EventBus` are **in-process singletons**; subagents are in-process `AgentSession` objects spawned by `task/executor.ts` (`runSubprocess` is documented "Run a single agent in-process"). `task` is a tool, not a CLI subcommand; only `worktree` / `wt` is a subcommand. There is no multi-process team orchestrator today.
- `PI_SUBPROCESS_CMD` overrides the subagent spawn command (`omp` / `omp.cmd` resolution bypass) — the hook for launching a subagent into a tab.
- omp is already cmux-aware: reads `CMUX_SURFACE_ID` / `CMUX_WORKSPACE_ID` (`packages/tui/src/ttyid.ts`, `tui.ts`) and talks to the cmux socket for the browser tool (`src/tools/browser/cmux/`, `CMUX_SOCKET_PATH`).

### Prior art for cross-process coordination: `/collab` (`docs/collab.md`)
omp already moves agent state across process boundaries for live session sharing. The collab hub mirrors, over an (encrypted) relay/socket:
- `bus` frames — task-subagent lifecycle/progress EventBus traffic, republished on the peer's local bus
- `agents` frames — agent-registry snapshots feeding a peer-local registry
- guest->host `agent-cmd` — steer/kill/revive another process's subagents; `fetch-transcript` for incremental reads

This is the same shape the team-mode irc transport needs (mirror the registry + bus across processes, send commands across the boundary). The difference is topology, see below. The team transport should reuse the collab framing/codec where possible rather than inventing a parallel one.

## Architecture

```mermaid
graph TD
  subgraph cmuxarm["cmux omp launch arm (Swift, mechanical, mirror omx)"]
    Shim["tmux shim -> surface.split / workspace.create"]
  end
  Lead["lead omp (orchestrator) + irc broker (unix socket)"]
  Lead -->|spawn subagent = external omp proc via PI_SUBPROCESS_CMD| C1["omp · tab A (full omp; own subagents)"]
  Lead -->|spawn| C2["omp · tab B"]
  C1 <-->|irc over socket| Lead
  C2 <-->|irc over socket| Lead
  You["human"] -->|drive directly| C1
  C1 -. cmux-omp-session.ts hooks .-> SB["cmux sidebar: branch / status / diff"]
```

Three layers, sized very differently:

1. **omp: cross-process `irc` transport (the real work).** Make `irc` / `AgentRegistry` peer-aware so a subagent running as an external omp process registers into the lead's registry and exchanges `irc` over a unix socket, behind the existing in-process `irc` API (send / wait / list / broadcast). Local vs remote peer becomes a transport detail. Reuse the collab `agents` + `bus` mirroring and the `agent-cmd` command path; add the missing piece, which is bidirectional `irc` peer messaging across the boundary (collab today is observe + steer, not full peer chat).
2. **omp: team spawn path (thin).** Launch each subagent as an external omp **process** wired to the lead's broker via env (`OMP_IRC_SOCKET`, `OMP_AGENT_ID`, assignment). Under cmux the spawn is **cmux-mediated**: the lead asks cmux to open a tab running `omp` (reusing the cmux socket-client pattern in `tools/browser/cmux/`), so the child is a real interactive tab. In headless tests the lead spawns a plain process with the same env. NOTE: `RpcClient` / `--mode rpc` is the *rejected* headless-driving alternative (it owns the child's stdio, so a child cannot be both RpcClient-driven and an interactive tab) and is NOT used here; only its `ptree.spawn` cwd/env wiring is a reference. The child boots, connects to `$OMP_IRC_SOCKET`, registers under its agent id, receives its assignment, and runs as a normal omp.
3. **cmux: `cmux omp` launch arm (mechanical).** Mirror `runOMX`: `resolveOMPExecutable` / `createOMPShimDirectory` (`~/.cmuxterm/omp-bin`) / `configureOMPEnvironment` / `runOMP`, plus dispatch, usage, recognized-command list, Go relay. The lead runs under the shim, so when its team spawn path opens a subagent it becomes a cmux tab via the shim -> `workspace.create` / `surface.split`. The already-built `cmux-omp-session.ts` hooks + `CMUX_SURFACE_ID` feed the sidebar; nothing new needed there.

## Topology: team mode vs collab

- **collab** = N peers viewing/steering **one** host session (one agent, many viewers).
- **team mode** = N **independent** omp agents (each its own session/cwd/worktree) coordinating via one `irc` fabric.

Same transport primitives (registry mirror, bus mirror, cross-boundary commands), different graph. The lead is the registry owner; each tab is an independent agent peer, not a viewer of the lead.

## Data flow / irc contract

1. Lead boots, opens an `irc` broker on a unix socket, exports the path as `OMP_IRC_SOCKET`.
2. Spawn a subagent: lead allocates an agent id from the existing task id space (becomes the tab title), launches a child omp process in a tab with `OMP_IRC_SOCKET`, `OMP_AGENT_ID`, and the assignment.
3. Child connects, registers (`register(agentId, role)`), appears in the lead's `AgentRegistry` as a remote peer. From then on `irc` send / wait / list / broadcast work identically across the boundary.
4. Divergence + progress = ordinary `irc` traffic from child to lead; bidirectional so the lead can re-task.

## Identity, tabs, sidebar

- Agent id (task id space) is the stable handle and the tab title.
- Each tab carries `CMUX_SURFACE_ID` / `CMUX_WORKSPACE_ID`; the existing `cmux-omp-session.ts` hooks push `session_start` / `before_agent_start` / `agent_end` to cmux for sidebar status; `cmux diff --last-turn` gives a per-tab diff view.

## Human-in-the-loop

A subagent tab is a normal interactive omp. The human typing in it is just the agent doing work; its `irc` reporting to the lead continues unchanged, which is how divergence reaches the lead. No special "takeover" mode is required for v1.

## Error handling / lifecycle

- Child exit / tab close -> broker detects socket disconnect -> `AgentRegistry` marks the peer gone and emits the normal subagent-lifecycle event, so the lead reacts as to any subagent ending.
- Broker unreachable at child boot -> child retries briefly, then falls back to a standalone interactive omp (degrades to "just a tab," never hangs).
- Lead exit -> children detach but survive as normal omp tabs.

## Testing strategy

- The omp half is testable with **no Swift and no cmux**: in headless mode the lead spawns remote subagents as plain processes over the same unix socket (no tmux), exercising the cross-process irc bridge directly. Integration test: spawn two children, exchange `irc` both ways, one reports a divergence the lead observes, one exits and the registry updates. This is the load-bearing logic and stays decoupled from cmux.
- The cmux arm is verified by parity with the existing `omx` / `omc` / `omo` tmux-compat path: tabs appear, sidebar populates, `cmux diff --last-turn` works.

## Reuse vs new

- Reuse: the cmux socket-client pattern (`tools/browser/cmux/socket-client.ts`, line-delimited JSON over a unix socket) for both the team broker transport and the cmux spawn request; collab `agents` / `bus` mirroring + `agent-cmd` codec as the registry/bus-sync reference; `AgentRegistry` + `IrcBus` + the `irc` tool API (the bridge plugs a remote-delivery path into `IrcBus.send`, which today resolves the recipient via `registry.get(to).session.deliverIrcMessage` — remote refs have `session=null`, so they route over the socket instead); worktree lifecycle (`worktree` / `~/.omp/wt`, creation pattern from `gh.ts`); the entire cmux tmux-shim + `cmux-omp-session.ts` hooks + sidebar.
- New: the bidirectional cross-process `irc` peer transport + bridge (core); the team spawn/orchestrator path; the `cmux omp` launch arm in cmux. (`RpcClient` / `--mode rpc` is reference-only, not used; see layer 2.)

## Open questions (resolve in the implementation plan)

- Reuse the collab relay/codec wholesale for the local team socket, or a slimmer local-only unix-socket variant of the same frames? (Collab is encrypted + relay-oriented; a local team likely wants a thinner unencrypted unix-socket transport with the same `agents` / `bus` / `irc` frames.)
- Exact `irc` peer-frame schema for cross-process send/wait/broadcast (today `irc` is in-process; collab carries registry + bus + `agent-cmd` but not arbitrary peer `irc` chat). Define the minimal added frame set.
- Worktree policy: does the lead create a worktree per subagent by default, or only when the assignment is repo-scoped? (Reuse `worktree` / `~/.omp/wt`.)
- Tab vs split default (one tab per subagent vs splits within a window) and ordering.

## References

omp (`oh-my-pi`):
- `packages/coding-agent/src/modes/rpc/rpc-types.ts` — RPC frame contract
- `packages/coding-agent/src/modes/rpc/rpc-mode.ts` — `--mode rpc` host
- `packages/coding-agent/src/modes/rpc/rpc-client.ts` — process spawn + drive
- `packages/coding-agent/src/modes/rpc/rpc-subagents.ts` — observe-only subagent bridge (extend to full irc)
- `packages/coding-agent/src/task/executor.ts` — subagent spawn (`runSubprocess`, in-process today)
- `docs/collab.md` — cross-process session transport prior art (`agents` / `bus` / `agent-cmd` frames)
- `docs/rpc.md`, `docs/environment-variables.md` (`PI_SUBPROCESS_CMD`, `PI_TASK_*`)
- full exploration map: `agent://ExploreOmpOrchestration`

cmux (`manaflow-ai/cmux`):
- `CLI/cmux.swift` — om* launch arms (`runOMX` ~21441), tmux shim (`configureTmuxCompatEnvironment` ~19025, `runClaudeTeamsTmuxCompat` ~21618), recognized commands ~5296, usage ~14363
- `CLI/CMUXCLI+OmpExtension.swift` — existing omp session-hooks extension
- `CLI/CMUXCLI+AgentHookDefinitions.swift:202` — `AgentHookDef "omp"`
- `Sources/TerminalController.swift` — `v2Capabilities()` RPC registry
- full exploration map: `agent://ExploreCmuxOmIntegration`

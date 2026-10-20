import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { Context } from "@temporalio/activity";
import { ApplicationFailure, TimeoutFailure, TimeoutType, defaultPayloadConverter } from "@temporalio/common";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ROOM_AGENT_ENQUEUE_SIGNAL, ROOM_AGENT_STOP_SIGNAL,
  roomAgentSupervisorInputSchema, roomAgentSupervisorQueueItemSchema,
  type RoomAgentSupervisorQueueItem
} from "@space/contracts";
import { recordWorkflowHistory } from "./support/record-workflow-history.js";

// Only activities are replaced. The supervisor and its child execute from the
// production entry point inside Temporal's workflow isolate.
describe("room supervisor in the Temporal sandbox", () => {
  let env: TestWorkflowEnvironment;
  let worker: Worker;
  let running: Promise<void>;
  let sequence = 0;
  const taskQueue = `supervisor-sandbox-${randomUUID()}`;
  const handles: Array<ReturnType<TestWorkflowEnvironment["client"]["workflow"]["getHandle"]>> = [];
  const releases: Array<() => void> = [];
  let check: ReturnType<typeof vi.fn>;
  let prepare: ReturnType<typeof vi.fn>;
  let discard: ReturnType<typeof vi.fn>;
  let finish: ReturnType<typeof vi.fn>;
  let turn: ReturnType<typeof vi.fn>;

  beforeAll(async () => {
    env = await TestWorkflowEnvironment.createTimeSkipping();
    worker = await Worker.create({
      connection: env.nativeConnection, taskQueue,
      workflowsPath: fileURLToPath(new URL("../src/workflows.ts", import.meta.url)),
      activities: {
        checkRoomAgentSupervisorTurn: (...args: unknown[]) => check(...args),
        prepareRoomAgentBackgroundContinuation: (...args: unknown[]) => prepare(...args),
        discardRoomAgentSupervisorTurn: (...args: unknown[]) => discard(...args),
        finishRoomAgentSupervisorTurn: (...args: unknown[]) => finish(...args),
        runCodexAppServerTurn: (...args: unknown[]) => turn(...args)
      }
    });
    running = worker.run();
    // Keep the rejection observable in teardown without an unhandled rejection
    // if a worker fails while a test is inspecting its history.
    void running.catch(() => undefined);
  }, 120_000);

  beforeEach(() => {
    check = vi.fn(async () => "READY");
    prepare = vi.fn(async () => ({ state: "WAITING" }));
    discard = vi.fn(async () => undefined);
    finish = vi.fn(async () => undefined);
    turn = vi.fn(async () => verified());
  });

  afterEach(async () => {
    for (const release of releases.splice(0)) release();
    for (const handle of handles.splice(0)) {
      await handle.terminate("Sandbox test complete").catch(() => undefined);
    }
  });

  afterAll(async () => {
    if (worker) worker.shutdown();
    if (running) await running;
    if (env) await env.teardown();
  }, 30_000);

  function item(suffix: string, missionId = "mission:sandbox", automatic = false): RoomAgentSupervisorQueueItem {
    return roomAgentSupervisorQueueItemSchema.parse({ missionId, turn: {
      roomId: "room:sandbox", paneId: "pane:sandbox", prompt: `Request ${suffix}`,
      agentRunId: `agent_run:${sequence}-${suffix}`, traceId: `trace:${sequence}-${suffix}`,
      ...(automatic ? { roomAgentContinuation: { sourceRunId: "agent_run:source", generation: 1 } } : {})
    } });
  }

  function verified(pendingActionIds?: string[]) {
    return { status: "COMPLETED", message: "Verified", roomAgentOutcome: {
      status: "VERIFIED", executedActionCount: 1, statusReason: "Verified remaining request.",
      ...(pendingActionIds ? { pendingActionIds } : {})
    } };
  }

  async function start(input: Record<string, unknown> = {}) {
    sequence++;
    const handle = await env.client.workflow.start("roomAgentSupervisorWorkflow", {
      workflowId: `supervisor-sandbox-${randomUUID()}`, taskQueue,
      args: [roomAgentSupervisorInputSchema.parse({ roomId: "room:sandbox", ...input })]
    });
    handles.push(handle);
    return handle;
  }

  async function settled(assertion: () => void | Promise<void>) {
    await vi.waitFor(assertion, { timeout: 15_000, interval: 20 });
  }

  async function waitForTimer(handle: Awaited<ReturnType<typeof start>>, count = 1) {
    await settled(async () => {
      const history = await handle.fetchHistory();
      expect(history.events?.filter(event => event.timerStartedEventAttributes).length).toBeGreaterThanOrEqual(count);
    });
  }

  async function continued(handle: Awaited<ReturnType<typeof start>>) {
    let input: ReturnType<typeof roomAgentSupervisorInputSchema.parse> | undefined;
    await settled(async () => {
      // Pin the first run: getHandle without a run ID would follow the new run.
      const history = await env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
      const event = history.events?.find(event => event.workflowExecutionContinuedAsNewEventAttributes);
      expect(event).toBeDefined();
      const payload = event!.workflowExecutionContinuedAsNewEventAttributes!.input!.payloads![0]!;
      input = roomAgentSupervisorInputSchema.parse(defaultPayloadConverter.fromPayload(payload));
    });
    return input!;
  }

  it("accepts an enqueue signal, rejects foreign-room and duplicate turns, and runs the real child", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    releases.push(() => release());
    check.mockImplementationOnce(async () => { await gate; return "READY"; });
    const handle = await start();
    const queued = item("signal");
    await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, { ...queued, turn: { ...queued.turn, roomId: "room:other" } });
    await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, queued);
    await settled(() => expect(check).toHaveBeenCalledOnce());
    await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, queued);
    release();
    await settled(() => expect(finish).toHaveBeenCalledOnce());
    expect(turn).toHaveBeenCalledOnce();
    expect(finish).toHaveBeenCalledWith(expect.objectContaining({ item: queued, status: "COMPLETED" }));
    const history = await handle.fetchHistory();
    expect(history.events?.filter(event => event.childWorkflowExecutionStartedEventAttributes)).toHaveLength(1);
  });

  it("uses one-second timers to retry PAUSED turns and discards SKIP turns", async () => {
    check.mockResolvedValueOnce("PAUSED").mockResolvedValueOnce("SKIP");
    const queued = item("paused");
    const handle = await start({ pending: [queued] });
    await settled(() => expect(check).toHaveBeenCalledOnce());
    await waitForTimer(handle);
    await env.sleep("1 second");
    await settled(() => expect(discard).toHaveBeenCalledOnce());
    expect(check).toHaveBeenCalledTimes(2);
    expect(turn).not.toHaveBeenCalled();
    const history = await handle.fetchHistory();
    const timers = history.events?.filter(event => event.timerStartedEventAttributes);
    expect(timers?.some(event => Number(event.timerStartedEventAttributes!.startToFireTimeout!.seconds) === 1)).toBe(true);
  });

  it("polls background work after one second and executes allocation, send, verification in one mission", async () => {
    const send = item("send", "mission:sandbox", true);
    const verify = item("verify", "mission:sandbox", true);
    prepare.mockResolvedValueOnce({ state: "READY", item: send }).mockResolvedValueOnce({ state: "READY", item: verify });
    turn.mockResolvedValueOnce(verified(["action:send"]));
    const handle = await start({ monitoringMissionIds: [send.missionId] });
    await waitForTimer(handle);
    await env.sleep("1 second");
    await settled(() => expect(turn).toHaveBeenCalledOnce());
    await waitForTimer(handle, 2);
    await env.sleep("1 second");
    await settled(() => expect(finish).toHaveBeenCalledOnce());
    expect(turn.mock.calls.map(([input]) => input.agentRunId)).toEqual([send.turn.agentRunId, verify.turn.agentRunId]);
    expect(finish).toHaveBeenCalledWith(expect.objectContaining({ item: verify, status: "COMPLETED" }));
    const history = await handle.fetchHistory();
    expect(history.events?.filter(event => event.childWorkflowExecutionStartedEventAttributes)).toHaveLength(2);
  });

  it.each(["stop", "follow-up"])("retires a prepared continuation when %s arrives before the activity reply", async kind => {
    const automatic = item("prepared", "mission:sandbox", true);
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    releases.push(() => release());
    prepare.mockImplementationOnce(async () => { await gate; return { state: "READY", item: automatic }; });
    const handle = await start({ monitoringMissionIds: [automatic.missionId] });
    await waitForTimer(handle);
    await env.sleep("1 second");
    await settled(() => expect(prepare).toHaveBeenCalledOnce());
    if (kind === "stop") await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Stop" });
    else await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, item("follow-up"));
    release();
    await settled(() => expect(discard).toHaveBeenCalledWith(expect.objectContaining({ item: automatic })));
    if (kind === "stop") expect(turn).not.toHaveBeenCalled();
    else {
      await settled(() => expect(turn).toHaveBeenCalledOnce());
      expect(turn.mock.calls[0]![0].agentRunId).not.toBe(automatic.turn.agentRunId);
    }
  });

  it("cancels a running mission and drains a stopped queued turn", async () => {
    turn.mockImplementationOnce(async () => {
      // Real activity cancellation is delivered by the child workflow scope.
      Context.current().heartbeat();
      const heartbeat = setInterval(() => Context.current().heartbeat(), 50);
      try { await Context.current().cancelled; } finally { clearInterval(heartbeat); }
    });
    const active = item("active");
    const queued = item("queued", "mission:queued");
    const handle = await start({ pending: [active, queued] });
    await settled(() => expect(turn).toHaveBeenCalledOnce());
    await handle.signal(ROOM_AGENT_STOP_SIGNAL, { missionId: null, reason: "Operator stopped everything" });
    await settled(() => expect(finish).toHaveBeenCalledTimes(2));
    expect(finish.mock.calls.map(([input]) => [input.item.missionId, input.status])).toEqual([
      [active.missionId, "INTERRUPTED"], [queued.missionId, "INTERRUPTED"]
    ]);
    expect(discard).toHaveBeenCalledTimes(2);
    expect(turn).toHaveBeenCalledOnce();
  });

  it("supersedes a running automatic continuation with an operator follow-up", async () => {
    let cancelled = false;
    turn.mockImplementationOnce(async () => {
      const heartbeat = setInterval(() => Context.current().heartbeat(), 50);
      try { await Context.current().cancelled; } finally { cancelled = true; clearInterval(heartbeat); }
    });
    const automatic = item("automatic", "mission:sandbox", true);
    const operator = item("operator");
    const handle = await start({ pending: [automatic] });
    await settled(() => expect(turn).toHaveBeenCalledOnce());
    await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, operator);
    await settled(() => expect(finish).toHaveBeenCalledOnce());
    expect(cancelled).toBe(true);
    expect(discard).toHaveBeenCalledWith(expect.objectContaining({ item: automatic, reason: "Superseded by a newer operator request." }));
    expect(finish).toHaveBeenCalledWith(expect.objectContaining({ item: operator, status: "COMPLETED" }));
  });

  it.each([
    ["plain failure", "Error", "ROOM_AGENT_SUPERVISOR_FAILED"],
    ["application failure", "CODEX_APP_SERVER_TURN_TIMEOUT", "CODEX_APP_SERVER_TURN_TIMEOUT"]
  ])("finishes a child %s as FAILED with its cause code", async (_label, type, code) => {
    turn.mockImplementation(async () => { throw ApplicationFailure.nonRetryable("Child failed", type); });
    const queued = item("failure");
    await start({ pending: [queued] });
    await settled(() => expect(finish).toHaveBeenCalledOnce());
    expect(finish).toHaveBeenCalledWith(expect.objectContaining({ item: queued, status: "FAILED", errorCode: code }));
    expect(discard).toHaveBeenCalledWith(expect.objectContaining({ item: queued, errorCode: code }));
  });

  it("preserves a serialized Temporal timeout cause through the real activity and child failure chain", async () => {
    turn.mockImplementation(async () => {
      throw ApplicationFailure.create({ message: "Transport failed", type: "Error", nonRetryable: true,
        cause: new TimeoutFailure("Turn timed out", undefined, TimeoutType.START_TO_CLOSE) });
    });
    const queued = item("timeout");
    await start({ pending: [queued] });
    await settled(() => expect(finish).toHaveBeenCalledOnce());
    expect(finish).toHaveBeenCalledWith(expect.objectContaining({ status: "FAILED", errorCode: "ROOM_AGENT_SUPERVISOR_TIMEOUT" }));
  });

  it("continues after 50 processed turns carrying pending, interrupted and monitored state", async () => {
    const stopped = item("stopped", "mission:stopped");
    const alsoStopped = item("also-stopped", "mission:also-stopped");
    const pending = item("carried");
    const handle = await start({ processedCount: 49, pending: [pending],
      interrupted: [{ item: stopped, reason: "Stopped before restart" }, { item: alsoStopped, reason: "Stop carried" }], monitoringMissionIds: ["mission:monitor"] });
    const carried = await continued(handle);
    expect(carried).toMatchObject({ processedCount: 0, pending: [pending], interrupted: [{ item: alsoStopped, reason: "Stop carried" }], monitoringMissionIds: ["mission:monitor"] });
    await recordWorkflowHistory(env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId), "supervisor-current-continue-as-new");
    await settled(() => expect(turn).toHaveBeenCalledOnce());
    expect(turn.mock.calls[0]![0].agentRunId).toBe(pending.turn.agentRunId);
  });

  it("carries a prepared continuation at the 50-turn boundary and starts it once in the next run", async () => {
    const automatic = item("boundary", "mission:sandbox", true);
    prepare.mockResolvedValueOnce({ state: "READY", item: automatic });
    const handle = await start({ processedCount: 50, monitoringMissionIds: [automatic.missionId] });
    await waitForTimer(handle);
    await env.sleep("1 second");
    const carried = await continued(handle);
    expect(carried.pending).toEqual([automatic]);
    await settled(() => expect(turn).toHaveBeenCalledOnce());
    expect(turn.mock.calls[0]![0].agentRunId).toBe(automatic.turn.agentRunId);
  });

  it("continues after 250 one-second monitoring polls retaining the mission", async () => {
    const handle = await start({ monitoringMissionIds: ["mission:monitor"] });
    await waitForTimer(handle);
    await env.sleep("250 seconds");
    const carried = await continued(handle);
    expect(carried).toEqual(roomAgentSupervisorInputSchema.parse({ roomId: "room:sandbox", monitoringMissionIds: ["mission:monitor"] }));
    // The resumed run has its own timer and does not poll before its first second.
    expect(prepare).toHaveBeenCalledTimes(250);
    const history = await env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
    expect(history.events?.filter(event => event.timerStartedEventAttributes)).toHaveLength(250);
  }, 60_000);

  it("defers continue-as-new while more than 100 queued turns would exceed input bounds", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    releases.push(() => release());
    check.mockImplementationOnce(async () => { await gate; return "SKIP"; });
    check.mockResolvedValue("SKIP");
    const handle = await start({ processedCount: 49, pending: [item("initial")] });
    await settled(() => expect(check).toHaveBeenCalledOnce());
    const queued = Array.from({ length: 105 }, (_, index) => item(`flood-${index}`));
    for (const queuedItem of queued) await handle.signal(ROOM_AGENT_ENQUEUE_SIGNAL, queuedItem);
    release();
    const carried = await continued(handle);
    expect(carried.pending).toEqual(queued.slice(5));
    expect(carried.pending).toHaveLength(100);
    const history = await env.client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
    const checks = history.events?.filter(event => event.activityTaskScheduledEventAttributes?.activityType?.name === "checkRoomAgentSupervisorTurn");
    expect(checks).toHaveLength(6);
  }, 60_000);
});

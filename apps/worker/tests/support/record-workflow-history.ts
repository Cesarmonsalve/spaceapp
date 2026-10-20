import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { historyToJSON } from "@temporalio/common/lib/proto-utils";
import type { WorkflowHandle } from "@temporalio/client";

/** Recording is explicit: ordinary tests must never bless changed workflow behavior. */
export async function recordWorkflowHistory(handle: WorkflowHandle, fixtureName: string): Promise<void> {
  if (process.env.SPACE_RECORD_TEMPORAL_HISTORIES !== "1") return;
  if (!/^[a-z0-9-]+$/.test(fixtureName)) throw new Error("Invalid synthetic history fixture name");
  const directory = fileURLToPath(new URL("../fixtures/workflow-histories/", import.meta.url));
  await mkdir(directory, { recursive: true });
  const history = await handle.fetchHistory();
  // Host identities and host stack paths are diagnostic only. Event IDs,
  // commands, payloads and ordering remain the real test-server output.
  const json = JSON.parse(historyToJSON(history));
  const sanitized = JSON.stringify(json, (key, value) => key === "identity"
    ? "spaceapp-synthetic-tests" : key === "stackTrace" ? "" : value, 2);
  await writeFile(`${directory}/${fixtureName}.json`, `${sanitized}\n`, "utf8");
}

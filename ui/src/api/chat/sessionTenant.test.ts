import { expect, it } from "vitest";
import { sessionTenant } from "./types";

it("distinguishes scheduled conversations from same-named Agents", () => {
  const source = { namespace: "team", name: "daily", uid: "uid" };
  expect(sessionTenant({ ...source, kind: "Agent" })).toBe("team/daily");
  expect(sessionTenant({ ...source, kind: "ScheduledRun" })).toBe("ScheduledRun/team/daily");
});

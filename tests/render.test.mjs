import test from "node:test";
import assert from "node:assert/strict";

import { renderJobStatusReport, renderReviewResult, renderStoredJobResult } from "../plugins/codex/scripts/lib/render.mjs";

test("status and result show recorded model and effort only when present", () => {
  const job = { id: "task-123", status: "completed", request: { model: "gpt-6-sol", effort: "high" } };
  assert.match(renderJobStatusReport(job), /Model: gpt-6-sol\n  Effort: high/);
  assert.match(renderStoredJobResult(job, { rendered: "Done.\n", request: job.request }), /Model: gpt-6-sol\nEffort: high/);
  assert.doesNotMatch(renderJobStatusReport({ id: "task-456", status: "completed" }), /Model:|Effort:/);
  assert.doesNotMatch(renderStoredJobResult({ id: "task-456", status: "completed" }, { rendered: "Done.\n" }), /Model:|Effort:/);
});

test("renderReviewResult degrades gracefully when JSON is missing required review fields", () => {
  const output = renderReviewResult(
    {
      parsed: {
        verdict: "approve",
        summary: "Looks fine."
      },
      rawOutput: JSON.stringify({
        verdict: "approve",
        summary: "Looks fine."
      }),
      parseError: null
    },
    {
      reviewLabel: "Adversarial Review",
      targetLabel: "working tree diff"
    }
  );

  assert.match(output, /Codex returned JSON with an unexpected review shape\./);
  assert.match(output, /Missing array `findings`\./);
  assert.match(output, /Raw final message:/);
});

test("renderStoredJobResult prefers rendered output for structured review jobs", () => {
  const output = renderStoredJobResult(
    {
      id: "review-123",
      status: "completed",
      title: "Codex Adversarial Review",
      jobClass: "review",
      threadId: "thr_123"
    },
    {
      threadId: "thr_123",
      rendered: "# Codex Adversarial Review\n\nTarget: working tree diff\nVerdict: needs-attention\n",
      result: {
        result: {
          verdict: "needs-attention",
          summary: "One issue.",
          findings: [],
          next_steps: []
        },
        rawOutput:
          '{"verdict":"needs-attention","summary":"One issue.","findings":[],"next_steps":[]}'
      }
    }
  );

  assert.match(output, /^# Codex Adversarial Review/);
  assert.doesNotMatch(output, /^\{/);
  assert.match(output, /Codex session ID: thr_123/);
  assert.match(output, /Resume in Codex: codex resume thr_123/);
});

test("renderStoredJobResult includes captured output from a timed-out job", () => {
  const output = renderStoredJobResult(
    {
      id: "task-timeout",
      status: "failed",
      phase: "timed_out",
      title: "Codex Task",
      errorMessage: "The turn timed out."
    },
    {
      capturedOutput: "Partial answer before the stream stalled.",
      errorMessage: "The turn timed out."
    }
  );

  assert.match(output, /Captured output:/);
  assert.match(output, /Partial answer before the stream stalled\./);
  assert.match(output, /The turn timed out\./);
});

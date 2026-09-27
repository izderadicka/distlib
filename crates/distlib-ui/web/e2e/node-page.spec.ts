import { type Page, test as base, expect } from "@playwright/test";

import { aStranger, Node } from "./node";

/** Every test gets a node of its own, stopped and removed afterwards. */
const test = base.extend<{ node: Node }>({
  node: async ({}, use) => {
    const node = await Node.found();
    await use(node);
    await node.dispose();
  },
});

/**
 * Everything the browser reports as an error — a script that failed, a
 * resource the CSP refused — while the test runs.
 */
function errorsOn(page: Page): string[] {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") {
      errors.push(message.text());
    }
  });
  page.on("pageerror", (error) => errors.push(error.message));
  return errors;
}

const rows = (page: Page) => page.getByRole("row").filter({ hasNot: page.getByRole("columnheader") });

test("the link signs the tab in and shows this node, live", async ({ page, node }) => {
  const errors = errorsOn(page);

  await page.goto(node.link);

  await expect(page.getByText("Live", { exact: true })).toBeVisible();
  await expect(page.getByText(node.id)).toBeVisible();
  await expect(rows(page)).toHaveCount(1);
  await expect(rows(page).first()).toContainText("no name");
  await expect(rows(page).first()).toContainText("core (leader)");
  // The token has left the address bar, and so the history.
  expect(new URL(page.url()).hash).toBe("");
  // Nothing refused by the CSP, nothing thrown.
  expect(errors).toEqual([]);
});

test("a member admitted from the CLI appears without a reload", async ({ page, node }) => {
  await page.goto(node.link);
  await expect(page.getByText("Live", { exact: true })).toBeVisible();

  node.admit(aStranger(), "carol");

  await expect(rows(page).filter({ hasText: "carol" })).toBeVisible();
  await expect(rows(page)).toHaveCount(2);
});

test("a reload keeps the tab signed in", async ({ page, node }) => {
  await page.goto(node.link);
  await expect(page.getByText("Live", { exact: true })).toBeVisible();

  await page.reload();

  await expect(page.getByText("Live", { exact: true })).toBeVisible();
  await expect(page.getByText(node.id)).toBeVisible();
});

test("a page opened without the link says how to sign in", async ({ page, node }) => {
  await page.goto(node.base);

  await expect(page.getByRole("heading", { name: "Not signed in" })).toBeVisible();
  await expect(page.getByText("distlib ui", { exact: true })).toBeVisible();
});

test("a token the node refuses sends the tab to sign in", async ({ page, node }) => {
  await page.goto(`${node.base}#token=${"0".repeat(64)}`);

  await expect(page.getByRole("heading", { name: "Not signed in" })).toBeVisible();
});

test("the page says when the node is gone, and comes back with it", async ({ page, node }) => {
  await page.goto(node.link);
  await expect(page.getByText("Live", { exact: true })).toBeVisible();

  await node.stop();
  await expect(page.getByText(/Cannot reach the node/)).toBeVisible();
  // What it had stays on the page, marked as possibly out of date.
  await expect(page.getByText(node.id)).toBeVisible();

  await node.start();
  // Backing off, it may take a few seconds to try again.
  await expect(page.getByText("Live", { exact: true })).toBeVisible({ timeout: 30_000 });
  // And the new stream is a working one.
  node.admit(aStranger(), "dave");
  await expect(rows(page).filter({ hasText: "dave" })).toBeVisible();
});

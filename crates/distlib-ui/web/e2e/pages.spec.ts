import { readFileSync } from "node:fs";
import { join } from "node:path";

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

/** Goes from wherever the page is to the node page, the way a person would. */
async function toNodePage(page: Page, node: Node) {
  await page.getByRole("link", { name: "Node" }).click();
  await expect(page.getByText(node.id)).toBeVisible();
}

test("the link signs the tab in and shows this node, live", async ({ page, node }) => {
  const errors = errorsOn(page);

  await page.goto(node.link);

  await expect(page.getByText("Live", { exact: true })).toBeVisible();
  await expect(page.getByText("The library is empty.")).toBeVisible();
  await toNodePage(page, node);
  expect(new URL(page.url()).pathname).toBe("/node");
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
  await toNodePage(page, node);

  node.admit(aStranger(), "carol");

  await expect(rows(page).filter({ hasText: "carol" })).toBeVisible();
  await expect(rows(page)).toHaveCount(2);
});

test("a reload keeps the tab signed in, and on the page it was on", async ({ page, node }) => {
  await page.goto(node.link);
  await toNodePage(page, node);

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
  await toNodePage(page, node);

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

test("an item added from the CLI can be browsed, searched for and opened", async ({ page, node }) => {
  await page.goto(node.link);
  await expect(page.getByText("The library is empty.")).toBeVisible();

  node.add("mloci.epub", "a book", "--kind", "ebook", "--title", "Válka s mloky", "--author", "Karel Čapek");

  // Without a reload: the catalogue's own event brings it.
  await expect(rows(page).filter({ hasText: "Válka s mloky" })).toContainText("Karel Čapek");
  // Added here, and nobody else is online to have it.
  await expect(rows(page).filter({ hasText: "Válka s mloky" })).toContainText("held here, none online");

  await page.getByRole("searchbox").fill("čapek");
  await page.getByRole("button", { name: "Search" }).click();
  await expect(page.getByText("1–1 of 1")).toBeVisible();
  expect(new URL(page.url()).searchParams.get("q")).toBe("čapek");

  // The search is in the address, so a reload asks the node for it again.
  await page.reload();
  await expect(rows(page).filter({ hasText: "Válka s mloky" })).toBeVisible();

  await page.getByRole("searchbox").fill("hordubal");
  await page.getByRole("button", { name: "Search" }).click();
  await expect(page.getByText("Nothing matches “hordubal”.")).toBeVisible();

  await page.goBack();
  await expect(rows(page).filter({ hasText: "Válka s mloky" })).toBeVisible();

  await page.getByRole("link", { name: "Válka s mloky" }).click();
  await expect(page.getByRole("heading", { name: "Válka s mloky" })).toBeVisible();
  expect(new URL(page.url()).pathname).toMatch(/^\/items\/[0-9a-f]{64}$/);
  // Read from the node's own answer, so its shape is the one the page expects.
  await expect(page.getByText("Karel Čapek")).toBeVisible();
  await expect(page.getByText("held here, none online")).toBeVisible();
  const file = rows(page).filter({ hasText: "mloci.epub" });
  await expect(file).toContainText("epub");
  await expect(file).toContainText("6 B");

  // The item's address is one the node serves the page at.
  await page.reload();
  await expect(page.getByRole("heading", { name: "Válka s mloky" })).toBeVisible();
});

test("an item downloads from its page into the node's download directory", async ({ page, node }) => {
  node.add("mloci.epub", "a book to take home", "--kind", "ebook", "--title", "Válka s mloky");
  await page.goto(node.link);
  await page.getByRole("link", { name: "Válka s mloky" }).click();

  await page.getByRole("button", { name: "Download" }).click();

  const written = join(node.downloads, "mloci.epub");
  await expect(page.getByText(written)).toBeVisible();
  expect(readFileSync(written, "utf8")).toBe("a book to take home");
});

test("a file chosen in the browser becomes an item", async ({ page, node }) => {
  await page.goto(node.link);
  await page.getByRole("link", { name: "Add" }).click();

  await page
    .locator('input[type="file"]')
    .setInputFiles({ name: "Válka s mloky.epub", mimeType: "application/epub+zip", buffer: Buffer.from("a book") });
  await page.getByLabel("Title").fill("Válka s mloky");
  await page.getByLabel(/^Authors/).fill("Karel Čapek");
  await page.getByRole("button", { name: "Add" }).click();

  // Opened on its own page, with the file under the name it was chosen by.
  await expect(page.getByRole("heading", { name: "Válka s mloky" })).toBeVisible();
  await expect(page.getByText("Karel Čapek")).toBeVisible();
  await expect(rows(page).filter({ hasText: "Válka s mloky.epub" })).toContainText("6 B");

  await page.getByRole("link", { name: "Library" }).click();
  await expect(rows(page).filter({ hasText: "Válka s mloky" })).toBeVisible();
});

test("an item's details are edited from its page", async ({ page, node }) => {
  node.add("rur.epub", "a play", "--kind", "ebook", "--title", "RUR", "--author", "Karel Čapek");
  await page.goto(node.link);
  await page.getByRole("link", { name: "RUR" }).click();

  await page.getByRole("button", { name: "Edit" }).click();
  await page.getByLabel("Title").fill("R.U.R.");
  await page.getByLabel("Year").fill("1920");
  await page.getByRole("button", { name: "Save" }).click();

  // Read back from the node, the way anybody else would see it.
  await expect(page.getByRole("heading", { name: "R.U.R." })).toBeVisible();
  await expect(page.getByText("1920")).toBeVisible();
  await expect(page.getByText("Karel Čapek")).toBeVisible();
});

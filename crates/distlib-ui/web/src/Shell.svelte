<script lang="ts">
  // Everything a signed-in tab shows: the pages, how to get between them,
  // whether the node can be heard, and whether it needs an upgrade. The one connection to the node's
  // events is held here, for whichever page is open to listen to (D2, D6).
  import { onMount } from "svelte";

  import AddPage from "./AddPage.svelte";
  import BookmarksPage from "./BookmarksPage.svelte";
  import ItemPage from "./ItemPage.svelte";
  import LibraryPage from "./LibraryPage.svelte";
  import NodePage from "./NodePage.svelte";
  import WishesPage from "./WishesPage.svelte";
  import { type Connection, type Listen, type NodeEvent, watch } from "./lib/events";
  import { href, onLinkClick, onPopState, router } from "./lib/router.svelte";
  import { call, Unauthorised, type UpgradeRequired } from "./lib/rpc";

  let { onUnauthorised }: { onUnauthorised: () => void } = $props();

  let connection = $state<Connection>("connecting");
  let upgradeRequired = $state<UpgradeRequired | null>(null);

  /** Whether the node stopped following the log; a failure here is the connection line's to show. */
  async function loadUpgradeRequired() {
    try {
      upgradeRequired = (await call("node.status", null)).upgrade_required;
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      }
    }
  }

  const listeners = new Set<(event: NodeEvent) => void>();
  const listen: Listen = (listener) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  };

  onMount(() => {
    loadUpgradeRequired();
    return watch({
      onEvent: (event) => {
        if (event.type === "node.upgrade_required") {
          loadUpgradeRequired();
        }
        for (const listener of listeners) {
          listener(event);
        }
      },
      onConnection: (now) => {
        // A reconnected tab may have missed the event.
        if (now === "live" && connection !== "live") {
          loadUpgradeRequired();
        }
        connection = now;
      },
      onUnauthorised,
    });
  });
</script>

<svelte:window onpopstate={onPopState} />
<svelte:document onclick={onLinkClick} />

<nav>
  <a href="/" aria-current={router.route.page === "library" ? "page" : undefined}>Library</a>
  <a href="/add" aria-current={router.route.page === "add" ? "page" : undefined}>Add</a>
  <a href="/wishes" aria-current={router.route.page === "wishes" ? "page" : undefined}>Wishes</a>
  <a href="/bookmarks" aria-current={router.route.page === "bookmarks" ? "page" : undefined}>Bookmarks</a>
  <a href="/node" aria-current={router.route.page === "node" ? "page" : undefined}>Node</a>
</nav>

{#if upgradeRequired}
  <p class="failure" role="alert">
    Upgrade required:
    {#if upgradeRequired.reason === "protocol"}
      the group speaks protocol {upgradeRequired.group}, which this node's build does not.
    {:else}
      the core nodes send log entries this node's build cannot read.
    {/if}
    The node stopped following the group's log at index {upgradeRequired.at}; upgrade distlib to resume.
  </p>
{/if}

<p class="connection {connection}">
  {#if connection === "live"}
    Live
  {:else if connection === "connecting"}
    Connecting…
  {:else}
    Cannot reach the node — what is shown may be out of date. Retrying…
  {/if}
</p>

<!-- A new address is a new page: nothing of the last one's answer, arriving
     late, can land in it. -->
{#key href(router.route)}
  {#if router.route.page === "library"}
    <LibraryPage route={router.route} {listen} {onUnauthorised} />
  {:else if router.route.page === "item"}
    <ItemPage id={router.route.id} {listen} {onUnauthorised} />
  {:else if router.route.page === "add"}
    <AddPage {onUnauthorised} />
  {:else if router.route.page === "wishes"}
    <WishesPage {listen} {onUnauthorised} />
  {:else if router.route.page === "bookmarks"}
    <BookmarksPage route={router.route} {listen} {onUnauthorised} />
  {:else if router.route.page === "node"}
    <NodePage {listen} {onUnauthorised} />
  {:else}
    <section class="notice">
      <h2>No such page</h2>
      <p><a href="/">Go to the library</a></p>
    </section>
  {/if}
{/key}

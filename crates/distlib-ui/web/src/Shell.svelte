<script lang="ts">
  // Everything a signed-in tab shows: the pages, how to get between them,
  // and whether the node can be heard. The one connection to the node's
  // events is held here, for whichever page is open to listen to (D2, D6).
  import { onMount } from "svelte";

  import LibraryPage from "./LibraryPage.svelte";
  import NodePage from "./NodePage.svelte";
  import { type Connection, type Listen, type NodeEvent, watch } from "./lib/events";
  import { href, onLinkClick, onPopState, router } from "./lib/router.svelte";

  let { onUnauthorised }: { onUnauthorised: () => void } = $props();

  let connection = $state<Connection>("connecting");

  const listeners = new Set<(event: NodeEvent) => void>();
  const listen: Listen = (listener) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  };

  onMount(() =>
    watch({
      onEvent: (event) => {
        for (const listener of listeners) {
          listener(event);
        }
      },
      onConnection: (now) => (connection = now),
      onUnauthorised,
    }),
  );
</script>

<svelte:window onpopstate={onPopState} />
<svelte:document onclick={onLinkClick} />

<nav>
  <a href="/" aria-current={router.route.page === "library" ? "page" : undefined}>Library</a>
  <a href="/node" aria-current={router.route.page === "node" ? "page" : undefined}>Node</a>
</nav>

<p class="connection {connection}">
  {#if connection === "live"}
    Live
  {:else if connection === "connecting"}
    Connecting…
  {:else}
    Cannot reach the node — what is shown may be out of date. Retrying…
  {/if}
</p>

{#if router.route.page === "library"}
  <!-- A new search or page is a new page: nothing of the last one's answer,
       arriving late, can land in it. -->
  {#key href(router.route)}
    <LibraryPage route={router.route} {listen} {onUnauthorised} />
  {/key}
{:else if router.route.page === "node"}
  <NodePage {listen} {onUnauthorised} />
{:else}
  <section class="notice">
    <h2>No such page</h2>
    <p><a href="/">Go to the library</a></p>
  </section>
{/if}

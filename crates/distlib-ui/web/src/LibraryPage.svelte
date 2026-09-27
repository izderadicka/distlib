<script lang="ts">
  // The library: every item by title, or with a query, the items that match
  // it, best first — a page at a time, with the search and the page in the
  // address. Kept current by the event stream: any change to the catalogue
  // may move what belongs on this page, so any one reloads it.
  import { onMount } from "svelte";

  import type { Listen } from "./lib/events";
  import { reloader } from "./lib/reloader";
  import { href, navigate, type Route } from "./lib/router.svelte";
  import { call, type ItemPage, type ItemSummary, Unauthorised } from "./lib/rpc";

  type LibraryRoute = Extract<Route, { page: "library" }>;

  let {
    route,
    listen,
    onUnauthorised,
  }: { route: LibraryRoute; listen: Listen; onUnauthorised: () => void } = $props();

  /** Items to a page. */
  const PAGE = 20;

  let found = $state<ItemPage | null>(null);
  let failure = $state<string | null>(null);
  // What is typed, which is only searched for once submitted. Starting from
  // the route's query and never following it is right: a new route is a new
  // page (`Shell`'s `{#key}`).
  // svelte-ignore state_referenced_locally
  let typed = $state(route.query);

  const offset = $derived((route.number - 1) * PAGE);
  const at = (number: number) => href({ ...route, number });

  async function load() {
    try {
      found = route.query
        ? await call("library.search", { query: route.query, offset, limit: PAGE })
        : await call("library.list", { offset, limit: PAGE });
      failure = null;
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      } else {
        failure = error instanceof Error ? error.message : String(error);
      }
    }
  }

  const reload = reloader(load);

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (event.type === "resync" || event.type.startsWith("catalogue.")) {
        reload();
      }
    });
    reload();
    return stop;
  });

  function search(event: SubmitEvent) {
    event.preventDefault();
    navigate(href({ page: "library", query: typed, number: 1 }));
  }

  function series({ series }: ItemSummary): string {
    if (series === null) {
      return "";
    }
    return series.index === undefined ? series.name : `${series.name} #${series.index}`;
  }
</script>

<form role="search" onsubmit={search}>
  <input type="search" aria-label="Search the library" placeholder="Title, author, series…" bind:value={typed} />
  <button>Search</button>
</form>

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if found}
  {#if found.total === 0}
    <p>{route.query ? `Nothing matches “${route.query}”.` : "The library is empty."}</p>
  {:else if found.results.length === 0}
    <p>There is no page {route.number}. <a href={at(1)}>Go to the first page</a></p>
  {:else}
    <p class="pager">
      {offset + 1}–{offset + found.results.length} of {found.total}
      {#if route.number > 1}<a href={at(route.number - 1)}>Previous</a>{/if}
      {#if offset + found.results.length < found.total}<a href={at(route.number + 1)}>Next</a>{/if}
    </p>
    <table>
      <thead>
        <tr><th>Title</th><th>Authors</th><th>Series</th><th class="number">Year</th><th>Type</th></tr>
      </thead>
      <tbody>
        {#each found.results as item (item.item_id)}
          <tr>
            <td>{#if item.title}{item.title}{:else}<span class="unnamed">no title</span>{/if}</td>
            <td>{item.authors?.join(", ") ?? ""}</td>
            <td>{series(item)}</td>
            <td class="number">{item.year ?? ""}</td>
            <td>{item.kind ?? ""}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
{/if}

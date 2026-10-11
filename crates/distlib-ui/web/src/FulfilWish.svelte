<script lang="ts">
  // Saying which item answers a wish: a search of the library, starting
  // from the wish's title, and a button on each item found.
  import { href } from "./lib/router.svelte";
  import { call, failureOf, type ItemSummary } from "./lib/rpc";

  let { wishId, title, onUnauthorised }: { wishId: string; title: string; onUnauthorised: () => void } =
    $props();

  /** Items found to a search. */
  const LIMIT = 10;

  let open = $state(false);
  let query = $state("");
  let found = $state<ItemSummary[] | null>(null);
  /** What `found` was found for: the query as it was searched, not as it is typed since. */
  let searched = $state("");
  let busy = $state(false);
  let failure = $state<string | null>(null);

  async function search() {
    busy = true;
    const asked = query.trim();
    failure = await failureOf(async () => {
      found = (await call("library.search", { query: asked, offset: 0, limit: LIMIT })).results;
      searched = asked;
    }, onUnauthorised);
    busy = false;
  }

  function start() {
    open = true;
    query = title;
    search();
  }

  function close() {
    open = false;
    found = null;
    failure = null;
  }

  function submit(event: SubmitEvent) {
    event.preventDefault();
    search();
  }

  async function fulfil(item: ItemSummary) {
    busy = true;
    failure = await failureOf(
      () => call("community.wish_fulfill", { wish_id: wishId, item_id: item.item_id }),
      onUnauthorised,
    );
    busy = false;
    if (failure === null) {
      close();
    }
  }
</script>

{#if open}
  <form role="search" onsubmit={submit}>
    <input type="search" aria-label="Find the item that fulfils it" bind:value={query} />
    <button disabled={busy || query.trim() === ""}>Search</button>
    <button type="button" onclick={close}>Cancel</button>
  </form>
  {#if failure}
    <p class="failure">{failure}</p>
  {/if}
  {#if found?.length === 0}
    <p>Nothing matches “{searched}”.</p>
  {:else if found}
    <table>
      <thead>
        <tr><th>Title</th><th>Authors</th><th>Type</th><th></th></tr>
      </thead>
      <tbody>
        {#each found as item (item.item_id)}
          <tr>
            <td>
              <a href={href({ page: "item", id: item.item_id })}>
                {#if item.title}{item.title}{:else}<span class="unnamed">no title</span>{/if}
              </a>
            </td>
            <td>{item.authors?.join(", ") ?? ""}</td>
            <td>{item.kind ?? ""}</td>
            <td><button disabled={busy} onclick={() => fulfil(item)}>Fulfil with this</button></td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
{:else}
  <button onclick={start}>Fulfil…</button>
{/if}

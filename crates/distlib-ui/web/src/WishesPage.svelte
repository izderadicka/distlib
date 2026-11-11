<script lang="ts">
  // The wishes: items members want and nobody has added yet, newest first,
  // as a table with a way to make one under it — or, with `id`, one wish on
  // a page of its own, where it is fulfilled and commented on. Kept current
  // by the event stream — news of any wish reloads them, and so does news of
  // an item one is fulfilled with, whose title the page shows. Who is who is
  // loaded beside them, and again when the membership changes.
  import { onMount } from "svelte";

  import Wish from "./Wish.svelte";
  import type { Listen } from "./lib/events";
  import { instant, memberName } from "./lib/format";
  import { whoIsWho } from "./lib/members";
  import { reloader } from "./lib/reloader";
  import { href, navigate } from "./lib/router.svelte";
  import { call, failureOf, type WishRecord } from "./lib/rpc";

  let { id = null, listen, onUnauthorised }: { id?: string | null; listen: Listen; onUnauthorised: () => void } =
    $props();

  let wishes = $state<WishRecord[] | null>(null);
  let failure = $state<string | null>(null);
  let me = $state<string | null>(null);
  let names = $state<ReadonlyMap<string, string>>(new Map());

  const reload = reloader(async () => {
    failure = await failureOf(async () => {
      wishes = (await call("community.wish_list", {})).wishes;
    }, onUnauthorised);
  });
  // A failure leaves members shown by id, and the next change tries again.
  const reloadMembers = reloader(async () => {
    await failureOf(async () => ({ me, names } = await whoIsWho()), onUnauthorised);
  });

  /** Whether some wish is fulfilled with `itemId`. */
  const fulfilsAWish = (itemId: unknown) =>
    wishes?.some((wish) => wish.fulfilled_by.some((fulfilment) => fulfilment.item_id === itemId)) ?? false;

  const shown = $derived(id === null ? null : wishes?.find((wish) => wish.wish_id === id));

  /** When `wish` was made, as a date. */
  function wished(wish: WishRecord): string {
    return wish.created_at === null ? "" : instant(wish.created_at).toLocaleDateString();
  }

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (
        event.type === "resync" ||
        event.type === "wish.changed" ||
        (event.type.startsWith("catalogue.") && "item_id" in event && fulfilsAWish(event.item_id))
      ) {
        reload();
      }
      if (event.type === "resync" || event.type === "membership.changed") {
        reloadMembers();
      }
    });
    reload();
    reloadMembers();
    return stop;
  });
</script>

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if id !== null}
  <p><a href={href({ page: "wishes" })}>All wishes</a></p>
  {#if shown}
    <Wish wish={shown} {me} {names} {onUnauthorised} />
  {:else if wishes}
    <!-- A wish just made is here once the node has written it down. -->
    <p>This node knows of no such wish yet.</p>
  {/if}
{:else}
  <h2>Wishes</h2>
  {#if wishes?.length === 0}
    <p>Nobody has wished for anything yet.</p>
  {:else if wishes}
    <table>
      <thead>
        <tr><th>Title</th><th>Type</th><th>Status</th><th>Wished by</th><th>Wished</th><th>Fulfilled with</th></tr>
      </thead>
      <tbody>
        {#each wishes as wish (wish.wish_id)}
          <tr>
            <td><a href={href({ page: "wish", id: wish.wish_id })}>{wish.title}</a></td>
            <td>{wish.kind ?? ""}</td>
            <td>{wish.status}</td>
            <td>{memberName(wish.creator, names, me)}</td>
            <td>{wished(wish)}</td>
            <td>
              {#each wish.fulfilled_by as fulfilment, index (fulfilment.member)}
                {#if index > 0}, {/if}
                <a href={href({ page: "item", id: fulfilment.item_id })}>
                  {#if fulfilment.title}{fulfilment.title}{:else}<span class="unnamed">no title</span>{/if}
                </a>
              {/each}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
  <p><button onclick={() => navigate(href({ page: "make_wish" }))}>Make a wish</button></p>
{/if}

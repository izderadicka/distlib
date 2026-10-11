<script lang="ts">
  // The wishes: items members want and nobody has added yet, newest first,
  // with the form to make one. Kept current by the event stream — news of
  // any wish reloads them, and so does news of an item one is fulfilled
  // with, whose title the page shows. Who is who is loaded beside them, and
  // again when the membership changes.
  import { onMount } from "svelte";

  import MakeWish from "./MakeWish.svelte";
  import Wish from "./Wish.svelte";
  import type { Listen } from "./lib/events";
  import { whoIsWho } from "./lib/members";
  import { reloader } from "./lib/reloader";
  import { call, failureOf, type WishRecord } from "./lib/rpc";

  let { listen, onUnauthorised }: { listen: Listen; onUnauthorised: () => void } = $props();

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

<MakeWish {onUnauthorised} />

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if wishes?.length === 0}
  <p>Nobody has wished for anything yet.</p>
{:else if wishes}
  {#each wishes as wish (wish.wish_id)}
    <Wish {wish} {me} {names} {onUnauthorised} />
  {/each}
{/if}

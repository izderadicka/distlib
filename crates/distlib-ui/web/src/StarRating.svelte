<script lang="ts">
  // This member's rating of an item, as five stars: a click saves it, and
  // Clear takes it back. The stars show what the node last said; the item's
  // own news, once the rating is projected, brings the change back.
  import { call, failureOf, type Ratings } from "./lib/rpc";

  let { itemId, ratings, onUnauthorised }: { itemId: string; ratings: Ratings; onUnauthorised: () => void } =
    $props();

  let saving = $state(false);
  let failure = $state<string | null>(null);

  async function rate(rating: number | null) {
    saving = true;
    failure = await failureOf(() => call("community.rate", { item_id: itemId, rating }), onUnauthorised);
    saving = false;
  }
</script>

<section>
  <h2>Your rating</h2>
  <div class="actions" role="group" aria-label="Your rating">
    {#each [1, 2, 3, 4, 5] as stars (stars)}
      <button
        class="star"
        aria-label={stars === 1 ? "1 star" : `${stars} stars`}
        aria-pressed={ratings.mine === stars}
        disabled={saving}
        onclick={() => rate(stars)}>{ratings.mine !== null && stars <= ratings.mine ? "★" : "☆"}</button
      >
    {/each}
    <button disabled={saving || ratings.mine === null} onclick={() => rate(null)}>Clear rating</button>
  </div>
  {#if failure}
    <p class="failure">{failure}</p>
  {/if}
</section>

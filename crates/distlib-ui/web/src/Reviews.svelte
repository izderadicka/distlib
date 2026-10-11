<script lang="ts">
  // An item's reviews: this member's own as a form, everybody else's as
  // text.
  import OwnText from "./OwnText.svelte";
  import { memberName } from "./lib/format";
  import { call, type ReviewRecord } from "./lib/rpc";

  let {
    itemId,
    reviews,
    me,
    names,
    onUnauthorised,
  }: {
    itemId: string;
    reviews: ReviewRecord[];
    me: string | null;
    names: ReadonlyMap<string, string>;
    onUnauthorised: () => void;
  } = $props();

  const mine = $derived(reviews.find((review) => review.member === me)?.review ?? null);
  const others = $derived(reviews.filter((review) => review.member !== me));
</script>

<section>
  <h2>Reviews <span class="count">{reviews.length}</span></h2>
  <OwnText
    noun="review"
    {mine}
    save={(review) => call("community.review", { item_id: itemId, review })}
    {onUnauthorised}
  />
  {#each others as review (review.member)}
    <article class="review">
      <h3>{memberName(review.member, names, me)}</h3>
      <p class="description">{review.review}</p>
    </article>
  {/each}
</section>

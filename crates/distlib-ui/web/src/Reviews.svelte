<script lang="ts">
  // An item's reviews: this member's own as a form, everybody else's as
  // text. One review per member, so saving replaces this member's last one.
  import { memberName } from "./lib/format";
  import { call, failureOf, type ReviewRecord } from "./lib/rpc";

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

  let draft = $state("");
  let saving = $state(false);
  let failure = $state<string | null>(null);

  // The form starts from this member's review, and follows it when it
  // changes: once one is saved and projected, or once who "me" is is known.
  $effect(() => {
    draft = mine ?? "";
  });

  async function write(review: string | null) {
    saving = true;
    failure = await failureOf(() => call("community.review", { item_id: itemId, review }), onUnauthorised);
    saving = false;
  }

  function save(event: SubmitEvent) {
    event.preventDefault();
    write(draft);
  }
</script>

<section>
  <h2>Reviews <span class="count">{reviews.length}</span></h2>
  <form class="add" onsubmit={save}>
    <label>
      Your review
      <textarea rows="4" bind:value={draft} disabled={saving}></textarea>
    </label>
    <div class="actions">
      <button disabled={saving || draft.trim() === "" || draft === mine}>Save review</button>
      {#if mine !== null}
        <button type="button" disabled={saving} onclick={() => write(null)}>Clear review</button>
      {/if}
    </div>
    {#if failure}
      <p class="failure">{failure}</p>
    {/if}
  </form>
  {#each others as review (review.member)}
    <article class="review">
      <h3>{memberName(review.member, names, me)}</h3>
      <p class="description">{review.review}</p>
    </article>
  {/each}
</section>

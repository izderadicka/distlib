<script lang="ts">
  // One wish: what was wished for and by whom, which items fulfil it, and
  // the members' comments — this member's own as a form.
  import FulfilWish from "./FulfilWish.svelte";
  import OwnText from "./OwnText.svelte";
  import { instant, memberName } from "./lib/format";
  import { href } from "./lib/router.svelte";
  import { call, type WishRecord } from "./lib/rpc";

  let {
    wish,
    me,
    names,
    onUnauthorised,
  }: {
    wish: WishRecord;
    me: string | null;
    names: ReadonlyMap<string, string>;
    onUnauthorised: () => void;
  } = $props();

  const mine = $derived(wish.comments.find((comment) => comment.member === me)?.comment ?? null);
  const others = $derived(wish.comments.filter((comment) => comment.member !== me));
  const wished = $derived(wish.created_at === null ? null : instant(wish.created_at));
</script>

<section>
  <h2>{wish.title}</h2>
  <dl>
    <dt>Status</dt>
    <dd>{wish.status}</dd>
    <dt>Wished by</dt>
    <dd>{memberName(wish.creator, names, me)}</dd>
    {#if wished}<dt>Wished</dt><dd><time datetime={wished.toISOString()}>{wished.toLocaleString()}</time></dd>{/if}
    {#if wish.kind}<dt>Type</dt><dd>{wish.kind}</dd>{/if}
    {#if wish.authors}<dt>Authors</dt><dd>{wish.authors.join(", ")}</dd>{/if}
  </dl>
  {#if wish.description}<p class="description">{wish.description}</p>{/if}
  {#if wish.fulfilled_by.length > 0}
    <ul>
      {#each wish.fulfilled_by as fulfilment (fulfilment.member)}
        <li>
          {memberName(fulfilment.member, names, me)} fulfilled it with
          <a href={href({ page: "item", id: fulfilment.item_id })}>
            {#if fulfilment.title}{fulfilment.title}{:else}<span class="unnamed">no title</span>{/if}
          </a>
        </li>
      {/each}
    </ul>
  {/if}
  <FulfilWish wishId={wish.wish_id} title={wish.title} {onUnauthorised} />

  <h3>Comments <span class="count">{wish.comments.length}</span></h3>
  <OwnText
    noun="comment"
    {mine}
    save={(comment) => call("community.wish_comment", { wish_id: wish.wish_id, comment })}
    {onUnauthorised}
  />
  {#each others as comment (comment.member)}
    <p class="description"><strong>{memberName(comment.member, names, me)}:</strong> {comment.comment}</p>
  {/each}
</section>

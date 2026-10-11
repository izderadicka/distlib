<script lang="ts">
  // An item's bookmarks, every member's, newest first: shared pointers into
  // it (D13). This member's own can be edited or deleted; the form leaves a
  // new one, or saves the one being edited.
  import { memberName } from "./lib/format";
  import { type BookmarkRecord, call, failureOf } from "./lib/rpc";

  let {
    itemId,
    bookmarks,
    me,
    names,
    onUnauthorised,
  }: {
    itemId: string;
    bookmarks: BookmarkRecord[];
    me: string | null;
    names: ReadonlyMap<string, string>;
    onUnauthorised: () => void;
  } = $props();

  const newestFirst = $derived([...bookmarks].sort((a, b) => b.created_at - a.created_at));

  /** The bookmark the form is editing; `null` while it leaves a new one. */
  let editing = $state<string | null>(null);
  let position = $state("");
  let note = $state("");
  let saving = $state(false);
  let failure = $state<string | null>(null);

  function edit(bookmark: BookmarkRecord) {
    editing = bookmark.bookmark_id;
    position = bookmark.position;
    note = bookmark.note;
  }

  function reset() {
    editing = null;
    position = "";
    note = "";
  }

  async function save(event: SubmitEvent) {
    event.preventDefault();
    const which = editing === null ? {} : { bookmark_id: editing };
    saving = true;
    failure = await failureOf(
      () => call("community.bookmark", { item_id: itemId, ...which, position, note }),
      onUnauthorised,
    );
    saving = false;
    if (failure === null) {
      reset();
    }
  }

  async function remove(bookmark: BookmarkRecord) {
    saving = true;
    failure = await failureOf(
      () => call("community.bookmark_delete", { item_id: itemId, bookmark_id: bookmark.bookmark_id }),
      onUnauthorised,
    );
    saving = false;
    if (failure === null && editing === bookmark.bookmark_id) {
      reset();
    }
  }
</script>

<section>
  <h2>Bookmarks <span class="count">{bookmarks.length}</span></h2>
  {#if bookmarks.length > 0}
    <table>
      <thead>
        <tr><th>Member</th><th>Position</th><th>Note</th><th></th></tr>
      </thead>
      <tbody>
        {#each newestFirst as bookmark (`${bookmark.member}/${bookmark.bookmark_id}`)}
          <tr>
            <td>{memberName(bookmark.member, names, me)}</td>
            <td>{bookmark.position}</td>
            <td>{bookmark.note}</td>
            <td>
              {#if bookmark.member === me}
                <div class="actions">
                  <button
                    aria-label={`Edit bookmark at ${bookmark.position}`}
                    disabled={saving}
                    onclick={() => edit(bookmark)}>Edit</button
                  >
                  <button
                    aria-label={`Delete bookmark at ${bookmark.position}`}
                    disabled={saving}
                    onclick={() => remove(bookmark)}>Delete</button
                  >
                </div>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
  <form class="add" onsubmit={save}>
    <fieldset disabled={saving}>
      <div class="pair">
        <label>
          Position
          <input bind:value={position} placeholder="p. 12, chapter 3, 01:23:45" />
        </label>
        <label>
          Note
          <input bind:value={note} />
        </label>
      </div>
    </fieldset>
    <div class="actions">
      <button disabled={saving || position.trim() === ""}>{editing === null ? "Add bookmark" : "Save bookmark"}</button>
      {#if editing !== null}
        <button type="button" disabled={saving} onclick={reset}>Cancel</button>
      {/if}
    </div>
    {#if failure}
      <p class="failure">{failure}</p>
    {/if}
  </form>
</section>

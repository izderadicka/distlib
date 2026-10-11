<script lang="ts">
  // A form for wishing for an item nobody has added yet. Only the title is
  // needed; the rest helps whoever looks for it.
  import { KINDS, type Kind, nonEmptyLines } from "./lib/metadata";
  import { call, failureOf } from "./lib/rpc";

  let { onUnauthorised }: { onUnauthorised: () => void } = $props();

  let title = $state("");
  let kind = $state<Kind | "">("");
  let authors = $state("");
  let description = $state("");
  let saving = $state(false);
  let failure = $state<string | null>(null);

  async function wish(event: SubmitEvent) {
    event.preventDefault();
    saving = true;
    failure = await failureOf(
      () =>
        call("community.wish_create", {
          title: title.trim(),
          kind: kind || undefined,
          authors: nonEmptyLines(authors),
          description: description.trim() || undefined,
        }),
      onUnauthorised,
    );
    saving = false;
    if (failure === null) {
      title = "";
      kind = "";
      authors = "";
      description = "";
    }
  }
</script>

<section>
  <h2>Make a wish</h2>
  <form class="add" onsubmit={wish}>
    <fieldset disabled={saving}>
      <div class="pair">
        <label>Title <input bind:value={title} /></label>
        <label>Type
          <select bind:value={kind}>
            <option value="">any</option>
            {#each KINDS as each (each)}<option value={each}>{each}</option>{/each}
          </select>
        </label>
      </div>
      <label>Authors <span class="hint">one to a line</span>
        <textarea rows="2" bind:value={authors}></textarea>
      </label>
      <label>Description <textarea rows="3" bind:value={description}></textarea></label>
    </fieldset>
    <div class="actions">
      <button disabled={saving || title.trim() === ""}>Wish for it</button>
    </div>
    {#if failure}
      <p class="failure">{failure}</p>
    {/if}
  </form>
</section>

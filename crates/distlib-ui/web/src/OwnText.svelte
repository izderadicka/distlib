<script lang="ts">
  // This member's own text about something — a review, a comment — as a
  // form. One per member, so saving replaces the last one, and clearing
  // deletes it.
  import { failureOf } from "./lib/rpc";

  let {
    noun,
    mine,
    save,
    onUnauthorised,
  }: {
    /** What the text is called: "review", "comment". */
    noun: string;
    /** This member's text as the node has it, `null` while there is none. */
    mine: string | null;
    /** Writes the text, or with `null` clears it. */
    save: (text: string | null) => Promise<unknown>;
    onUnauthorised: () => void;
  } = $props();

  let draft = $state("");
  let saving = $state(false);
  let failure = $state<string | null>(null);

  // The form starts from this member's text, and follows it when it changes:
  // once one is saved and projected, or once who "me" is is known.
  $effect(() => {
    draft = mine ?? "";
  });

  async function write(text: string | null) {
    saving = true;
    failure = await failureOf(() => save(text), onUnauthorised);
    saving = false;
  }

  function submit(event: SubmitEvent) {
    event.preventDefault();
    write(draft);
  }
</script>

<form class="add" onsubmit={submit}>
  <label>
    Your {noun}
    <textarea rows="3" bind:value={draft} disabled={saving}></textarea>
  </label>
  <div class="actions">
    <button disabled={saving || draft.trim() === "" || draft === mine}>Save {noun}</button>
    {#if mine !== null}
      <button type="button" disabled={saving} onclick={() => write(null)}>Clear {noun}</button>
    {/if}
  </div>
  {#if failure}
    <p class="failure">{failure}</p>
  {/if}
</form>

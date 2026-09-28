<script lang="ts">
  // Editing an item's metadata. Only what was changed is sent, compared with
  // the item as it was when the form opened: the catalogue keeps the last
  // write per field, so a field left alone here is not one this edit can
  // overwrite somebody else's change to.
  import MetadataFields from "./MetadataFields.svelte";
  import { changes, draftOf, LABELS } from "./lib/metadata";
  import { call, type ItemRecord, Unauthorised } from "./lib/rpc";

  let {
    item,
    onSaved,
    onCancel,
    onUnauthorised,
  }: { item: ItemRecord; onSaved: () => void; onCancel: () => void; onUnauthorised: () => void } =
    $props();

  // Taken once, when the form opens: what this edit is measured against.
  // svelte-ignore state_referenced_locally
  const before = draftOf(item);
  let draft = $state({ ...before });
  let failure = $state<string | null>(null);
  let saving = $state(false);

  async function save(event: SubmitEvent) {
    event.preventDefault();
    const { changed, emptied } = changes(before, draft);
    if (emptied.length > 0) {
      const names = emptied.map((key) => LABELS[key]).join(", ");
      failure = `The ${names} cannot be emptied: the library cannot say a field is empty yet. Put it back, or change it instead.`;
      return;
    }
    if (Object.keys(changed).length === 0) {
      failure = "Nothing has been changed.";
      return;
    }
    failure = null;
    saving = true;
    try {
      await call("library.edit_metadata", { item_id: item.item_id, fields: changed });
      onSaved();
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      } else {
        failure = error instanceof Error ? error.message : String(error);
      }
    } finally {
      saving = false;
    }
  }
</script>

<form class="add" onsubmit={save}>
  <h2>Edit</h2>
  <fieldset disabled={saving}>
    <MetadataFields bind:draft />
  </fieldset>
  <div class="actions">
    <button disabled={saving}>Save</button>
    <button type="button" onclick={onCancel} disabled={saving}>Cancel</button>
  </div>
  {#if failure}
    <p class="failure">{failure}</p>
  {/if}
</form>

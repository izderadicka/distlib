<script lang="ts">
  // Adding an item from files chosen in the browser: each is uploaded
  // (`POST /upload`), then `library.add` makes the item of them — the same
  // path, and the same deduplication, as `distlib add`.
  //
  // **Uploaded on submit, not when picked**, so a form abandoned half-filled
  // leaves nothing on the node. **Checked here first**: a name the node
  // would refuse only after the upload is cheaper to refuse before it.
  import MetadataFields from "./MetadataFields.svelte";
  import { bytes } from "./lib/format";
  import { emptyDraft, fields } from "./lib/metadata";
  import { href, navigate } from "./lib/router.svelte";
  import { call, Unauthorised } from "./lib/rpc";
  import { upload } from "./lib/upload";

  let { onUnauthorised }: { onUnauthorised: () => void } = $props();

  let files = $state<File[]>([]);
  let draft = $state(emptyDraft());
  let failure = $state<string | null>(null);
  /** How far the uploads have got, while they run. */
  let sending = $state<{ file: number; sent: number } | null>(null);
  let adding = $state(false);
  /** An item that was already in the library, which this add changed nothing about. */
  let existing = $state<{ item_id: string; title: string | null } | null>(null);

  const total = $derived(files.reduce((sum, file) => sum + file.size, 0));
  const busy = $derived(sending !== null || adding);

  /** Why these files cannot be an item, if they cannot. */
  function problem(chosen: File[]): string | null {
    if (chosen.length === 0) {
      return "Choose the item's files first.";
    }
    const unnamed = chosen.find((file) => !/.\.[^.]+$/.test(file.name));
    if (unnamed) {
      return `${unnamed.name} has no extension to tell its format by; rename it first.`;
    }
    const names = chosen.map((file) => file.name);
    const twice = names.find((name, index) => names.indexOf(name) !== index);
    if (twice) {
      return `Two of the files are called ${twice}; an item's files need names of their own.`;
    }
    return null;
  }

  function choose(event: Event) {
    files = [...((event.currentTarget as HTMLInputElement).files ?? [])];
    failure = null;
    existing = null;
  }

  async function add(event: SubmitEvent) {
    event.preventDefault();
    failure = problem(files);
    existing = null;
    if (failure) {
      return;
    }
    try {
      const uploads: string[] = [];
      let before = 0;
      for (const [index, file] of files.entries()) {
        sending = { file: index, sent: before };
        const uploaded = await upload(file, (sent) => {
          sending = { file: index, sent: before + sent };
        });
        uploads.push(uploaded.upload);
        before += file.size;
      }
      sending = null;
      adding = true;
      const added = await call("library.add", { uploads, ...fields(draft) });
      if (added.created) {
        navigate(href({ page: "item", id: added.item_id }));
      } else {
        existing = { item_id: added.item_id, title: added.title };
      }
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      } else {
        failure = error instanceof Error ? error.message : String(error);
      }
    } finally {
      sending = null;
      adding = false;
    }
  }
</script>

<form class="add" onsubmit={add}>
  <h2>Add an item</h2>
  <label>Files <input type="file" multiple onchange={choose} disabled={busy} /></label>
  {#if files.length > 0}
    <p class="count">{files.length} file{files.length === 1 ? "" : "s"}, {bytes(total)}</p>
  {/if}

  <fieldset disabled={busy}>
    <MetadataFields bind:draft />
  </fieldset>

  <button disabled={busy}>Add</button>

  {#if sending}
    <p>
      <progress max={total} value={sending.sent}></progress>
      Uploading {sending.file + 1} of {files.length}: {files[sending.file].name}, {bytes(sending.sent)} of
      {bytes(total)}
    </p>
  {:else if adding}
    <p>Adding…</p>
  {/if}

  {#if failure}
    <p class="failure">{failure}</p>
  {/if}

  {#if existing}
    <p class="notice">
      These files are already in the library, as
      <a href={href({ page: "item", id: existing.item_id })}>{existing.title ?? "an untitled item"}</a>.
      Nothing was changed: the details above were not applied.
    </p>
  {/if}
</form>

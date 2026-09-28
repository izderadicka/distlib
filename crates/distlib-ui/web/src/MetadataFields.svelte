<script lang="ts">
  // The fields a person fills in about an item, bound to a draft its owner
  // holds — for adding an item, and for editing one.
  import { type Draft, KINDS } from "./lib/metadata";

  let { draft = $bindable() }: { draft: Draft } = $props();
</script>

<label>Type
  <select bind:value={draft.kind}>
    {#each KINDS as kind (kind)}<option value={kind}>{kind}</option>{/each}
  </select>
</label>
<label>Title <input bind:value={draft.title} /></label>
<label>Authors <span class="hint">one to a line</span>
  <textarea rows="2" bind:value={draft.authors}></textarea>
</label>
<label>Genres <span class="hint">one to a line</span>
  <textarea rows="2" bind:value={draft.genres}></textarea>
</label>
<div class="pair">
  <label>Series <input bind:value={draft.series} /></label>
  <label>Number in it <input type="number" step="any" bind:value={draft.seriesIndex} /></label>
</div>
<div class="pair">
  <label>Year <input type="number" step="1" bind:value={draft.year} /></label>
  <label>Language <input bind:value={draft.lang} placeholder="cs, en, …" /></label>
</div>
<label>Description <textarea rows="4" bind:value={draft.description}></textarea></label>

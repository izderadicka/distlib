<script lang="ts">
  // One item: everything the read model holds for it, and its files. Kept
  // current by the event stream — news of this item reloads it, including
  // its arrival, so a page opened before sync brought the item in shows it
  // when it comes.
  import { onMount } from "svelte";

  import Download from "./Download.svelte";
  import type { Listen } from "./lib/events";
  import { bytes, instant } from "./lib/format";
  import { reloader } from "./lib/reloader";
  import { call, type FileRecord, type ItemRecord, Unauthorised } from "./lib/rpc";

  let { id, listen, onUnauthorised }: { id: string; listen: Listen; onUnauthorised: () => void } =
    $props();

  let item = $state<ItemRecord | null>(null);
  let failure = $state<string | null>(null);

  async function load() {
    try {
      item = await call("library.item", { item_id: id });
      failure = null;
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      } else {
        failure = error instanceof Error ? error.message : String(error);
      }
    }
  }

  const reload = reloader(load);

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (event.type === "resync" || ("item_id" in event && event.item_id === id)) {
        reload();
      }
    });
    reload();
    return stop;
  });

  /** The files in the order they are played or read: by disc, then place, then name. */
  function inOrder(files: Record<string, FileRecord>): Array<[string, FileRecord]> {
    return Object.entries(files).sort(
      ([, a], [, b]) =>
        (a.disc ?? 0) - (b.disc ?? 0) ||
        (a.seq ?? 0) - (b.seq ?? 0) ||
        a.filename.localeCompare(b.filename),
    );
  }

  const last = $derived(item && instant(item.last_modified));
</script>

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if item}
  <section>
    <h2>{#if item.title}{item.title}{:else}<span class="unnamed">no title</span>{/if}</h2>
    <dl>
      {#if item.authors}<dt>Authors</dt><dd>{item.authors.join(", ")}</dd>{/if}
      {#if item.series}
        <dt>Series</dt>
        <dd>{item.series.name}{item.series.index === undefined ? "" : ` #${item.series.index}`}</dd>
      {/if}
      {#if item.year !== null}<dt>Year</dt><dd>{item.year}</dd>{/if}
      {#if item.kind}<dt>Type</dt><dd>{item.kind}</dd>{/if}
      {#if item.lang}<dt>Language</dt><dd>{item.lang}</dd>{/if}
      {#if item.genres}<dt>Genres</dt><dd>{item.genres.join(", ")}</dd>{/if}
      {#if item.replicas !== null}<dt>Copies kept</dt><dd>{item.replicas}</dd>{/if}
      <dt>Changed</dt>
      <dd><time datetime={last?.toISOString()}>{last?.toLocaleString()}</time></dd>
      <dt>Item</dt>
      <dd><code>{item.item_id}</code></dd>
    </dl>
    {#if item.description}<p class="description">{item.description}</p>{/if}
  </section>

  <Download {id} {listen} {onUnauthorised} />

  <section>
    <h2>Files <span class="count">{Object.keys(item.files).length}</span></h2>
    <table>
      <thead>
        <tr><th>Name</th><th>Role</th><th>Format</th><th class="number">Size</th></tr>
      </thead>
      <tbody>
        {#each inOrder(item.files) as [hash, file] (hash)}
          <tr>
            <td>{file.filename}</td>
            <td>{file.role}</td>
            <td>{file.format}</td>
            <td class="number">{bytes(file.size)}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </section>
{/if}

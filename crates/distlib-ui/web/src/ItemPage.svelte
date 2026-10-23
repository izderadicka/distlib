<script lang="ts">
  // One item: everything the read model holds for it, and its files. Kept
  // current by the event stream — news of this item reloads it, including
  // its arrival, so a page opened before sync brought the item in shows it
  // when it comes. So does news of any member's availability, which is named
  // by member rather than by item, and of its bookmarks. Who is who — this
  // node's own member, and everybody's names — is loaded beside it, and again
  // when the membership changes. Custody recomputed has no news of its own:
  // its custodians show at the next load.
  import { onMount } from "svelte";

  import Bookmarks from "./Bookmarks.svelte";
  import Download from "./Download.svelte";
  import EditItem from "./EditItem.svelte";
  import Reviews from "./Reviews.svelte";
  import StarRating from "./StarRating.svelte";
  import type { Listen } from "./lib/events";
  import { availability, bytes, instant, memberName, ratingSummary } from "./lib/format";
  import { whoIsWho } from "./lib/members";
  import { reloader } from "./lib/reloader";
  import { call, failureOf, type FileRecord, type ItemRecord, Unauthorised } from "./lib/rpc";

  let { id, listen, onUnauthorised }: { id: string; listen: Listen; onUnauthorised: () => void } =
    $props();

  let item = $state<ItemRecord | null>(null);
  let failure = $state<string | null>(null);
  let editing = $state(false);
  let me = $state<string | null>(null);
  let names = $state<ReadonlyMap<string, string>>(new Map());

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
  // A failure leaves members shown by id, and the next change tries again.
  const reloadMembers = reloader(async () => {
    await failureOf(async () => ({ me, names } = await whoIsWho()), onUnauthorised);
  });

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (
        event.type === "resync" ||
        event.type === "availability.changed" ||
        ("item_id" in event && event.item_id === id)
      ) {
        reload();
      }
      if (event.type === "resync" || event.type === "membership.changed") {
        reloadMembers();
      }
    });
    reload();
    reloadMembers();
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

  /** Whether `member` is online: this node always is, and is not listed. */
  function presence(member: string, online: string[]): string {
    return member === me || online.includes(member) ? "online" : "offline";
  }
</script>

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if item && editing}
  <EditItem
    {item}
    onSaved={() => {
      editing = false;
      reload();
    }}
    onCancel={() => (editing = false)}
    {onUnauthorised}
  />
{:else if item}
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
      <dt>Availability</dt>
      <dd>{availability(item.availability)}</dd>
      <dt>Rating</dt>
      <dd>{ratingSummary(item.ratings)}</dd>
      <dt>Changed</dt>
      <dd><time datetime={last?.toISOString()}>{last?.toLocaleString()}</time></dd>
      <dt>Item</dt>
      <dd><code>{item.item_id}</code></dd>
    </dl>
    {#if item.description}<p class="description">{item.description}</p>{/if}
    <button onclick={() => (editing = true)}>Edit</button>
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

  <section>
    <h2>Kept for the group</h2>
    {#if item.custodians === null}
      <p class="hint">Who keeps it is not worked out yet.</p>
    {:else}
      {@const online = item.availability.online}
      <table>
        <thead>
          <tr><th>Member</th><th>Keeps it as</th><th>Now</th></tr>
        </thead>
        <tbody>
          {#each item.added_by as member (member)}
            <tr><td>{memberName(member, names, me)}</td><td>its adder</td><td>{presence(member, online)}</td></tr>
          {/each}
          {#each item.custodians as member (member)}
            <tr><td>{memberName(member, names, me)}</td><td>custodian</td><td>{presence(member, online)}</td></tr>
          {/each}
        </tbody>
      </table>
      {#if item.custodians.length === 0}
        <p class="hint">No other member pledges enough to be its custodian.</p>
      {/if}
    {/if}
  </section>

  <StarRating itemId={id} ratings={item.ratings} {onUnauthorised} />
  <Reviews itemId={id} reviews={item.reviews} {me} {names} {onUnauthorised} />
  <Bookmarks itemId={id} bookmarks={item.bookmarks} {me} {names} {onUnauthorised} />
{/if}

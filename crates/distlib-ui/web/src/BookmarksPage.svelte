<script lang="ts">
  // Every member's bookmarks, newest first (D13): searched over the note, the
  // item's title and the member's name, and filtered to one member — "Mine"
  // being this node's own. Both are in the address. Kept current by the
  // event stream — news of any bookmark reloads them, as does news of an
  // item listed, whose title is shown, and of the membership, whose names
  // are searched. Bookmarks are edited on their item's page.
  import { onMount } from "svelte";

  import type { Listen } from "./lib/events";
  import { instant, memberName } from "./lib/format";
  import { whoIsWho } from "./lib/members";
  import { reloader } from "./lib/reloader";
  import { href, navigate, type Route } from "./lib/router.svelte";
  import { call, failureOf, type ListedBookmark } from "./lib/rpc";

  type BookmarksRoute = Extract<Route, { page: "bookmarks" }>;

  let {
    route,
    listen,
    onUnauthorised,
  }: { route: BookmarksRoute; listen: Listen; onUnauthorised: () => void } = $props();

  let bookmarks = $state<ListedBookmark[] | null>(null);
  let failure = $state<string | null>(null);
  let me = $state<string | null>(null);
  let names = $state<ReadonlyMap<string, string>>(new Map());
  // What is typed, which is only searched for once submitted. A new route is
  // a new page (`Shell`'s `{#key}`), so it never needs to follow the route.
  // svelte-ignore state_referenced_locally
  let typed = $state(route.query);

  /** The members to filter by besides this node's own, by name. */
  const others = $derived.by(() => {
    const members = new Set(names.keys());
    // The one filtered by keeps its option even while the group does not know it.
    if (route.member !== null) {
      members.add(route.member);
    }
    if (me !== null) {
      members.delete(me);
    }
    return [...members]
      .map((member) => ({ member, name: memberName(member, names, me) }))
      .sort((a, b) => a.name.localeCompare(b.name));
  });

  const reload = reloader(async () => {
    failure = await failureOf(async () => {
      const { bookmarks: found } = await call("community.bookmarks", {
        q: route.query || undefined,
        member: route.member ?? undefined,
      });
      bookmarks = found;
    }, onUnauthorised);
  });
  // A failure leaves members shown by id, and the next change tries again.
  const reloadMembers = reloader(async () => {
    await failureOf(async () => ({ me, names } = await whoIsWho()), onUnauthorised);
  });

  /** Whether `itemId` is the item of a bookmark listed. */
  const listed = (itemId: unknown) => bookmarks?.some((bookmark) => bookmark.item_id === itemId) ?? false;

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (
        event.type === "resync" ||
        event.type === "bookmark.changed" ||
        event.type === "membership.changed" ||
        (event.type.startsWith("catalogue.") && "item_id" in event && listed(event.item_id))
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

  function search(event: SubmitEvent) {
    event.preventDefault();
    navigate(href({ ...route, query: typed }));
  }

  function filter(event: Event) {
    const member = (event.currentTarget as HTMLSelectElement).value;
    navigate(href({ ...route, member: member || null }));
  }
</script>

<form role="search" onsubmit={search}>
  <input type="search" aria-label="Search the bookmarks" placeholder="Note, title, member…" bind:value={typed} />
  <select aria-label="Member" onchange={filter}>
    <option value="" selected={route.member === null}>Everybody</option>
    {#if me}<option value={me} selected={route.member === me}>Mine</option>{/if}
    {#each others as { member, name } (member)}
      <option value={member} selected={route.member === member}>{name}</option>
    {/each}
  </select>
  <button>Search</button>
</form>

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if bookmarks?.length === 0}
  <p>{route.query || route.member ? "No bookmarks match." : "Nobody has left a bookmark yet."}</p>
{:else if bookmarks}
  <table>
    <thead>
      <tr><th>Item</th><th>Member</th><th>Position</th><th>Note</th><th>Left</th></tr>
    </thead>
    <tbody>
      {#each bookmarks as bookmark (`${bookmark.item_id}/${bookmark.member}/${bookmark.bookmark_id}`)}
        {@const left = instant(bookmark.created_at)}
        <tr>
          <td>
            <a href={href({ page: "item", id: bookmark.item_id })}>
              {#if bookmark.title}{bookmark.title}{:else}<span class="unnamed">no title</span>{/if}
            </a>
          </td>
          <td>{memberName(bookmark.member, names, me)}</td>
          <td>{bookmark.position}</td>
          <td>{bookmark.note}</td>
          <td><time datetime={left.toISOString()}>{left.toLocaleString()}</time></td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}

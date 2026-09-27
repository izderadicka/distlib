<script lang="ts">
  // This node and its group, read-only (D8), kept current by the event
  // stream: the page loads on every `resync` — which each connection starts
  // with — and on `membership.changed`.
  import { onMount } from "svelte";

  import { type Connection, watch } from "./lib/events";
  import { call, type Member, type NodeStatus, Unauthorised } from "./lib/rpc";

  let { onUnauthorised }: { onUnauthorised: () => void } = $props();

  let status = $state<NodeStatus | null>(null);
  let members = $state<Member[]>([]);
  let connection = $state<Connection>("connecting");
  let failure = $state<string | null>(null);

  async function load() {
    try {
      const [loaded, group] = await Promise.all([
        call("node.status", null),
        call("group.members", null),
      ]);
      status = loaded;
      members = group.members;
      failure = null;
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      } else {
        failure = error instanceof Error ? error.message : String(error);
      }
    }
  }

  onMount(() =>
    watch({
      onEvent: (event) => {
        if (event.type === "resync" || event.type === "membership.changed") {
          void load();
        }
      },
      onConnection: (now) => (connection = now),
      onUnauthorised,
    }),
  );

  const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

  function bytes(count: number): string {
    let value = count;
    let unit = 0;
    while (value >= 1000 && unit < BYTE_UNITS.length - 1) {
      value /= 1000;
      unit += 1;
    }
    return `${Number.isInteger(value) ? value : value.toFixed(1)} ${BYTE_UNITS[unit]}`;
  }
</script>

<p class="connection {connection}">
  {#if connection === "live"}
    Live
  {:else if connection === "connecting"}
    Connecting…
  {:else}
    Cannot reach the node — what is shown may be out of date. Retrying…
  {/if}
</p>

{#if failure}
  <p class="failure">{failure}</p>
{/if}

{#if status}
  <section>
    <h2>This node</h2>
    <dl>
      <dt>Member</dt>
      <dd><code>{status.member}</code></dd>
      <dt>Group</dt>
      <dd>{#if status.group}<code>{status.group}</code>{:else}in no group yet{/if}</dd>
      <dt>Role</dt>
      <dd>
        {#if status.core}core{#if status.raft} — {status.raft.toLowerCase()}{/if}{:else}follower{/if}
      </dd>
      {#if status.pending > 0}
        <dt>Pending</dt>
        <dd>{status.pending} proposal{status.pending === 1 ? "" : "s"} awaiting approval</dd>
      {/if}
    </dl>
  </section>

  <section>
    <h2>Members <span class="count">{members.length}</span></h2>
    <table>
      <thead>
        <tr><th>Name</th><th>Member</th><th>Role</th><th class="number">Pledge</th></tr>
      </thead>
      <tbody>
        {#each members as member (member.member)}
          <tr class:me={member.member === status.member}>
            <!-- A founder names nobody, themselves included. -->
            <td>{#if member.name}{member.name}{:else}<span class="unnamed">no name</span>{/if}</td>
            <td><code title={member.member}>{member.member.slice(0, 12)}…</code></td>
            <td>{member.core ? "core" : "follower"}{member.member === status.leader ? " (leader)" : ""}</td>
            <td class="number">{bytes(member.pledge_bytes)}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </section>
{/if}

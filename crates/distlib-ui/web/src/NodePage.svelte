<script lang="ts">
  // This node and its group, read-only (D8), kept current by the event
  // stream: the page loads when it opens, and again on every `resync` —
  // which each connection starts with — and on `membership.changed`.
  import { onMount } from "svelte";

  import type { Listen } from "./lib/events";
  import { bytes } from "./lib/format";
  import { reloader } from "./lib/reloader";
  import { call, type Member, type NodeStatus, Unauthorised } from "./lib/rpc";

  let { listen, onUnauthorised }: { listen: Listen; onUnauthorised: () => void } = $props();

  let status = $state<NodeStatus | null>(null);
  let members = $state<Member[]>([]);
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

  const reload = reloader(load);

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (event.type === "resync" || event.type === "membership.changed") {
        reload();
      }
    });
    reload();
    return stop;
  });

  /** A core node says what Raft makes it; a follower has no Raft to ask. */
  function role(node: NodeStatus): string {
    if (!node.core) {
      return "follower";
    }
    return node.raft ? `core — ${node.raft.toLowerCase()}` : "core";
  }
</script>

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
      <dd>{role(status)}</dd>
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

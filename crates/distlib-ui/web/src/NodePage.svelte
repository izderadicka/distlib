<script lang="ts">
  // This node and its group, read-only (D8), kept current by the event
  // stream: the page loads when it opens, and again on every `resync` —
  // which each connection starts with — on `membership.changed`, on
  // `sync.status`, for each member's place in the catalogue's swarm, and on
  // an item added or a download finished, which change what is kept and what
  // is required. Custody recomputed and probe failures have no news of their
  // own, and show at the next load — or at once, for a check made here.
  import { onMount } from "svelte";

  import type { Listen } from "./lib/events";
  import { bytes, duration, instant } from "./lib/format";
  import { reloader } from "./lib/reloader";
  import { href } from "./lib/router.svelte";
  import {
    call,
    type CheckOutcome,
    type Member,
    type NodeStatus,
    type Suspect,
    Unauthorised,
  } from "./lib/rpc";

  let { listen, onUnauthorised }: { listen: Listen; onUnauthorised: () => void } = $props();

  let status = $state<NodeStatus | null>(null);
  let members = $state<Member[]>([]);
  let suspects = $state<Suspect[]>([]);
  let failure = $state<string | null>(null);
  /** How the last check of each member came out, or why it could not be made. */
  let checked = $state<Record<string, string>>({});
  let checking = $state<string | null>(null);

  function failed(error: unknown) {
    if (error instanceof Unauthorised) {
      onUnauthorised();
    } else {
      failure = error instanceof Error ? error.message : String(error);
    }
  }

  async function load() {
    try {
      const [loaded, group, suspected] = await Promise.all([
        call("node.status", null),
        call("group.members", null),
        call("group.suspects", null),
      ]);
      status = loaded;
      members = group.members;
      suspects = suspected.suspects;
      failure = null;
    } catch (error) {
      failed(error);
    }
  }

  const OUTCOMES: Record<string, string> = {
    passed: "passed",
    failed: "failed",
    unreachable: "could not be reached",
  };

  function said(outcome: CheckOutcome): string {
    return outcome === null ? "nothing of it held here to probe" : OUTCOMES[outcome];
  }

  /** Probes `member` from this node now, and loads again for what it recorded. */
  async function check(member: string) {
    checking = member;
    try {
      const { outcome } = await call("group.check", { member });
      checked[member] = said(outcome);
      await load();
    } catch (error) {
      if (error instanceof Unauthorised) {
        onUnauthorised();
      } else {
        checked[member] = error instanceof Error ? error.message : String(error);
      }
    } finally {
      checking = null;
    }
  }

  const reload = reloader(load);
  const RELOADED_ON = new Set([
    "resync",
    "membership.changed",
    "sync.status",
    "catalogue.item_added",
    "download.finished",
  ]);

  // Listening before the first load, so nothing said while it runs is missed.
  onMount(() => {
    const stop = listen((event) => {
      if (RELOADED_ON.has(event.type)) {
        reload();
      }
    });
    reload();
    return stop;
  });

  const neighbours = $derived(new Set(status?.sync.neighbours));
  const mine = $derived(members.find((member) => member.member === status?.member));
  const lastSync = $derived(new Map(status?.sync.last_sync.map((last) => [last.member, last])));
  const names = $derived(new Map(members.map((member) => [member.member, member.name])));

  /** A member's name, or the start of its id for one that has none. */
  function named(member: string, name: string | null | undefined): string {
    return name || `${member.slice(0, 12)}…`;
  }

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

  {#if status.settings}
    {@const settings = status.settings}
    <section>
      <h2>Group settings</h2>
      <p class="hint">Decided when the group was founded, and fixed since.</p>
      <dl>
        <dt>Copies of each item</dt>
        <dd>{settings.replicas}, the adder's own included</dd>
        <dt>Weight cap</dt>
        <dd>{bytes(settings.cap_bytes)}</dd>
        <dt>Weight step</dt>
        <dd>{bytes(settings.weight_step_bytes)}</dd>
        <dt>Minimum pledge</dt>
        <dd>{bytes(settings.min_pledge_bytes)}</dd>
        <dt>Custody follows membership</dt>
        <dd>after {duration(settings.settle_secs)} without a change, {duration(settings.max_settle_secs)} at most</dd>
        <dt>Emergency copy</dt>
        <dd>after {duration(settings.emergency_after_secs)} with one copy left</dd>
        <dt>Minimum transfer rate</dt>
        <dd>{bytes(settings.min_transfer_rate)}/s</dd>
        <dt>Probes</dt>
        <dd>{settings.probes ? `every ${duration(settings.probe_every_secs)}` : "off"}</dd>
        <dt>Probe failures count for</dt>
        <dd>{duration(settings.failure_window_secs)}</dd>
        <dt>Proposals wait</dt>
        <dd>{settings.pending_expiry} changes for approval</dd>
      </dl>
    </section>
  {/if}

  {@const storage = status.storage}
  <section>
    <h2>Storage</h2>
    <dl>
      <dt>Limit</dt>
      <dd>{storage.limit_bytes === null ? "none" : bytes(storage.limit_bytes)}</dd>
      <dt>Added here</dt>
      <dd>{bytes(storage.own_bytes)}</dd>
      <dt>Kept for the group</dt>
      <dd>{bytes(storage.custody_bytes)}</dd>
      <dt>Cache</dt>
      <dd>{bytes(storage.cache_bytes)}</dd>
    </dl>
    {#if storage.mandatory_over_limit}
      <p class="failure">
        What was added here and what is kept for the group are over the limit on their own. Raise
        the limit, or lower this node's pledge.
      </p>
    {:else if storage.limit_bytes !== null}
      <p class="hint">When there is no room, the cache goes first, what was used longest ago first.</p>
    {/if}
    {#if status.pins.waiting_for_room > 0}
      {@const waiting = status.pins.waiting_for_room}
      <p class="failure">
        {waiting} item{waiting === 1 ? "" : "s"} this node is custodian of wait{waiting === 1 ? "s" : ""} for room within the
        limit, each a copy short. Raise the limit.
      </p>
    {/if}
  </section>

  <section>
    <h2>Pledge and custody</h2>
    <dl>
      {#if mine}
        <dt>Pledge</dt>
        <dd>{bytes(mine.pledge_bytes)}</dd>
        <dt>Required</dt>
        <dd>{bytes(mine.required_pledge_bytes)}</dd>
      {/if}
      <dt>Custodian of</dt>
      <dd>
        {#if status.custody}
          {status.custody.items} item{status.custody.items === 1 ? "" : "s"}, {bytes(status.custody.bytes)}
        {:else}
          not worked out yet
        {/if}
      </dd>
      <dt>Group load</dt>
      <dd>
        {#if !status.custody}
          not worked out yet
        {:else if status.custody.load_factor === null}
          no pledge weighs anything yet
        {:else}
          {status.custody.load_factor.toFixed(2)}
        {/if}
      </dd>
    </dl>
    {#if mine && mine.pledge_bytes < mine.required_pledge_bytes}
      <p class="failure">
        This node's pledge is below what its adds require. Raise it with <code>distlib pledge</code>; adds
        that require far more are refused until then.
      </p>
    {/if}
    {#if status.custody && status.custody.bytes > status.custody.pledge_bytes}
      <p class="failure">
        Custodian of more than this node's pledge: {bytes(status.custody.bytes)} against {bytes(status.custody.pledge_bytes)}.
        It is all kept even so; raise the pledge with <code>distlib pledge</code>.
      </p>
    {/if}
    {#if (status.custody?.load_factor ?? 0) > 1}
      <p class="failure">The group's pledges do not cover every copy: some items are a copy short.</p>
    {/if}
  </section>

  <section>
    <h2>Members <span class="count">{members.length}</span></h2>
    <table>
      <thead>
        <tr>
          <th>Name</th><th>Member</th><th>Role</th><th class="number">Pledge</th><th class="number">Required</th><th>Coverage</th><th>Neighbour</th><th>Last sync</th>
        </tr>
      </thead>
      <tbody>
        {#each members as member (member.member)}
          {@const last = lastSync.get(member.member)}
          <tr class:me={member.member === status.member}>
            <!-- A founder names nobody, themselves included. -->
            <td>
              {#if member.name}{member.name}{:else}<span class="unnamed">no name</span>{/if}
              {#if member.red}<span class="failure" title="Failed probes by two or more members">red</span>{/if}
            </td>
            <td><code title={member.member}>{member.member.slice(0, 12)}…</code></td>
            <td>{member.core ? "core" : "follower"}{member.member === status.leader ? " (leader)" : ""}</td>
            <td class="number">{bytes(member.pledge_bytes)}</td>
            <td class="number">
              {bytes(member.required_pledge_bytes)}{member.pledge_bytes < member.required_pledge_bytes ? " — short" : ""}
            </td>
            <td>{member.coverage?.expected ? `${member.coverage.announced} of ${member.coverage.expected}` : "—"}</td>
            <td>{neighbours.has(member.member) ? "yes" : "—"}</td>
            <td>
              {#if last}
                {@const finished = instant(last.finished)}
                <time datetime={finished.toISOString()}>{finished.toLocaleString()}</time>{last.ok ? "" : " failed"}
              {:else}
                —
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </section>

  <section>
    <h2>Suspects <span class="count">{suspects.length}</span></h2>
    {#if suspects.length === 0}
      <p class="hint">No member has failed a probe that still counts.</p>
    {:else}
      <p class="hint">
        Members whose probes failed, the most probers first. Red, by two or more, is a candidate for
        expulsion: <code>distlib expel</code>.
      </p>
      <ul class="suspects">
        {#each suspects as suspect (suspect.member)}
          <li>
            <strong>{named(suspect.member, suspect.name)}</strong>
            {#if suspect.red}<span class="failure">red</span>{/if}
            {#if suspect.member !== status.member}
              <button type="button" disabled={checking !== null} onclick={() => check(suspect.member)}>
                {checking === suspect.member ? "Checking…" : "Check now"}
              </button>
            {/if}
            {#if checked[suspect.member]}<span class="checked">{checked[suspect.member]}</span>{/if}
            <ul>
              {#each suspect.failures as recorded (recorded.prober)}
                {@const when = instant(recorded.written)}
                <li>
                  by {named(recorded.prober, recorded.prober_name ?? names.get(recorded.prober))},
                  <time datetime={when.toISOString()}>{when.toLocaleString()}</time>:
                  <a href={href({ page: "item", id: recorded.item })}>{recorded.title ?? `${recorded.item.slice(0, 12)}…`}</a>,
                  chunk {recorded.chunk}, {recorded.in_a_row} in a row
                </li>
              {/each}
            </ul>
          </li>
        {/each}
      </ul>
    {/if}
  </section>
{/if}

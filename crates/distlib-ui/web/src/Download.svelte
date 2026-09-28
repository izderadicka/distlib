<script lang="ts">
  // Downloading one item into the node's own download directory, and how
  // far it has got.
  //
  // **It follows a task, not a click.** The progress comes from the event
  // stream, which on every connection first sends the downloads still
  // running — so the first download of this item heard of, started here or
  // anywhere else, is the one shown, and a page reloaded mid-download picks
  // it up again. Events can arrive before `library.download` has answered
  // with the task's id; either one can be first to name it. Its ending is
  // read from `library.task`, which is also asked after a `resync`, in case
  // the ending was missed while the stream was down.
  import { onMount } from "svelte";

  import { isDownload, type Listen, type NodeEvent } from "./lib/events";
  import { bytes } from "./lib/format";
  import { call, type Progress, type TaskState, Unauthorised } from "./lib/rpc";

  let { id, listen, onUnauthorised }: { id: string; listen: Listen; onUnauthorised: () => void } =
    $props();

  /** The download followed, once its id is known, and what is known of it. */
  let task = $state<number | null>(null);
  let progress = $state<Progress | null>(null);
  let ended = $state<TaskState | null>(null);
  let failure = $state<string | null>(null);
  let asking = $state(false);

  const running = $derived(task !== null && ended === null);

  const SOURCES = { network: "fetched", store: "had it", destination: "already there" } as const;

  function failed(error: unknown) {
    if (error instanceof Unauthorised) {
      onUnauthorised();
    } else {
      failure = error instanceof Error ? error.message : String(error);
    }
  }

  /** Follows `task_id`, unless another download of this item already is. */
  function follow(task_id: number): boolean {
    if (task === null || (ended !== null && task !== task_id)) {
      task = task_id;
      progress = null;
      ended = null;
    }
    return task === task_id;
  }

  /** Reads how the followed download stands, and keeps it if it has ended. */
  async function check() {
    if (task === null) {
      return;
    }
    try {
      const state = await call("library.task", { task_id: task });
      if (state.task_id === task && state.state !== "running") {
        ended = state;
      }
    } catch (error) {
      failed(error);
    }
  }

  function hear(event: NodeEvent) {
    if (event.type === "resync") {
      if (running) {
        void check();
      }
      return;
    }
    if (!isDownload(event) || event.item_id !== id || !follow(event.task_id)) {
      return;
    }
    if (event.type === "download.progress") {
      const { bytes_done, bytes_total, files_done, files_total } = event;
      // Bytes can go back — a provider failover starts a file again — and a
      // bar that jumps backwards says something that is not true.
      progress = {
        bytes_done: Math.max(bytes_done, progress?.bytes_done ?? 0),
        bytes_total,
        files_done,
        files_total,
      };
    } else {
      void check();
    }
  }

  onMount(() => listen(hear));

  async function download() {
    failure = null;
    asking = true;
    try {
      const started = await call("library.download", { item_id: id });
      follow(started.task_id);
    } catch (error) {
      failed(error);
    } finally {
      asking = false;
    }
  }
</script>

<section class="download">
  <button onclick={download} disabled={asking || running}>
    {running ? "Downloading…" : "Download"}
  </button>

  {#if failure}
    <p class="failure">{failure}</p>
  {/if}

  {#if running && progress}
    <p>
      <progress max={progress.bytes_total} value={progress.bytes_done}></progress>
      {progress.files_done} of {progress.files_total} files, {bytes(progress.bytes_done)} of
      {bytes(progress.bytes_total)}
    </p>
  {/if}

  {#if ended?.state === "finished"}
    <p>Downloaded:</p>
    <ul>
      {#each ended.files as file (file.file)}
        <li><code>{file.path}</code> <span class="count">{SOURCES[file.from]}</span></li>
      {/each}
    </ul>
  {:else if ended?.state === "failed"}
    <p class="failure">The download failed: {ended.error}</p>
  {/if}
</section>

<script lang="ts">
  import Shell from "./Shell.svelte";
  import { token } from "./lib/token";

  // Whether this tab holds a token the node has not refused. Refused, it is
  // forgotten, and the page says how to get a new link rather than failing
  // every call.
  let signedIn = $state(token() !== null);
</script>

<header>
  <h1>distlib</h1>
</header>

<main>
  {#if signedIn}
    <Shell onUnauthorised={() => (signedIn = false)} />
  {:else}
    <section class="notice">
      <h2>Not signed in</h2>
      <p>
        This page talks to the node with its API token. On the machine the node
        runs on, <code>distlib ui</code> prints a link that carries it — open that
        link, in this browser.
      </p>
    </section>
  {/if}
</main>

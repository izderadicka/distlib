import { mount } from "svelte";

import App from "./App.svelte";
import "./app.css";
import { adoptToken } from "./lib/token";

// Before anything else runs: the token must leave the address bar before any
// code could read it from there, or copy the address into history.
adoptToken();

const target = document.getElementById("app");
if (target) {
  mount(App, { target });
}

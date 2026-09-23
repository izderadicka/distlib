//! Driving the actual binary: one node per process, as an operator runs them.
//!
//! Extracted from `founding.rs` in 2b-3b, unchanged except for visibility.
//! `founding.rs` owned it while it was the only file that drove the
//! commands; `acceptance.rs` is the second, and §9's criterion is a
//! procedure — a fresh node joins, syncs, searches, downloads and restarts —
//! so it needs the same processes, ports and log-watching rather than a
//! second implementation of them.
//!
//! Everything here is about *running the program*: nothing in it knows what
//! a catalogue or an item is. What each test does with a running node is the
//! test's own business and stays in the test.

use std::{
    fs::File,
    io::Read as _,
    net::{TcpListener, UdpSocket},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU16, Ordering},
    time::{Duration, Instant},
};

use tempfile::TempDir;

/// Long enough for three processes to start, elect and replicate; short enough
/// that a hang fails the suite rather than stalling it.
pub const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a node gets to act on a stop signal before it is killed outright.
///
/// Generous, because what it is waiting for is the blob store's metadata
/// reaching disk and a restart depends on that having happened; short enough
/// that a node which ignores the signal does not hold the suite up.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// One friend's node: a data directory, a pinned port, and an identity.
pub struct Friend {
    pub dir: TempDir,
    pub port: u16,
    /// The local API's port.
    ///
    /// Its own, like the transport port: three nodes on one machine cannot
    /// share either.
    pub api_port: u16,
    pub id: String,
}

impl Friend {
    /// Runs `whoami` to create the identity and learn the member id.
    ///
    /// This is step one of the real procedure — the command exists so that a
    /// founder can be told who everyone is before there is a group to ask.
    pub fn introduce() -> Self {
        let dir = TempDir::new().unwrap();
        // Each probed with the protocol that will use it: the transport is
        // QUIC over UDP, the local api is HTTP over TCP, and a free port in one
        // says nothing whatever about the other.
        let port = a_free_port(Protocol::Udp);
        let api_port = a_free_port(Protocol::Tcp);

        // The port has to be pinned before `whoami`, because founding writes
        // this address into the log and an OS-chosen one would be gone by the
        // next restart.
        std::fs::write(
            dir.path().join("config.toml"),
            format!(
                "[net]\nbind_addr_v4 = \"127.0.0.1:{port}\"\nrelay_mode = \"disabled\"\n\n\
                 [api]\nbind_addr = \"127.0.0.1:{api_port}\"\n"
            ),
        )
        .unwrap();

        let output = distlib(dir.path()).arg("whoami").output().unwrap();
        assert!(
            output.status.success(),
            "whoami failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).unwrap();
        let id = stdout
            .lines()
            .find_map(|line| line.strip_prefix("identity   "))
            .expect("whoami prints the identity")
            .trim()
            .to_owned();
        assert_eq!(id.len(), 64, "a member id is 32 hex-encoded bytes");
        assert!(
            stdout.contains(&format!("member = \"{id}\"")),
            "whoami prints a line to paste into [consensus] core; got:\n{stdout}"
        );

        Self {
            dir,
            port,
            api_port,
            id,
        }
    }

    /// Writes the founding core group — the same list for everyone.
    pub fn agree_on(&self, everyone: &[(String, u16)]) {
        let core = everyone
            .iter()
            .map(|(id, port)| {
                format!("  {{ member = \"{id}\", addrs = [\"127.0.0.1:{port}\"] }},\n")
            })
            .collect::<String>();
        std::fs::write(
            self.dir.path().join("config.toml"),
            format!(
                "[net]\nbind_addr_v4 = \"127.0.0.1:{}\"\nrelay_mode = \"disabled\"\n\n\
                 [api]\nbind_addr = \"127.0.0.1:{}\"\n\n\
                 [consensus]\ncore = [\n{core}]\n",
                self.port, self.api_port
            ),
        )
        .unwrap();
    }

    /// Starts the node, optionally founding the group.
    pub fn run(&self, found: bool) -> Running {
        self.spawn(found, false)
    }

    /// The same, at `debug`, for a test whose subject only says so there.
    ///
    /// Not the default: the extra lines are per-connection and make
    /// `log_contents` a poor thing to search for a phrase that also appears
    /// inside them.
    pub fn run_verbosely(&self, found: bool) -> Running {
        self.spawn(found, true)
    }

    fn spawn(&self, found: bool, verbose: bool) -> Running {
        let log = self.dir.path().join("node.log");
        let mut command = distlib(self.dir.path());
        if verbose {
            command.arg("-v");
        }
        command.arg("run");
        if found {
            command.arg("--found-group");
        }
        let child = command
            .stdout(Stdio::from(File::create(&log).unwrap()))
            .stderr(Stdio::from(File::create(&log).unwrap()))
            .spawn()
            .unwrap();
        Running { child, log }
    }

    /// Admits `member` through the CLI, against this node's running API.
    pub fn admit(&self, member: &str) {
        let output = distlib(self.dir.path())
            .args(["admit", member, "--name", "newcomer"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "admit failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Renumbers this node's transport port, as a renumbered machine would be.
    ///
    /// `[consensus] core` is rewritten with everyone's *old* addresses, this
    /// node's own included, because that is the position the group is really in
    /// after a machine moves: nobody has been told, and the stale list is what
    /// is on disk. It does not matter either — once a group is founded the log
    /// decides who votes and where they are, and this is the test that says so.
    pub fn move_to(&mut self, everyone: &[(String, u16)], port: u16) {
        self.port = port;
        self.agree_on(everyone);
    }

    /// Tells the group where a core node is now, through the CLI.
    pub fn core_set(&self, member: &str, port: u16) -> String {
        let output = distlib(self.dir.path())
            .args([
                "core",
                "set",
                member,
                "--addr",
                &format!("127.0.0.1:{port}"),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "core set failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    /// Approves a pending change through the CLI.
    pub fn approve(&self, proposal: u64) {
        let output = distlib(self.dir.path())
            .args(["approve", &proposal.to_string()])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "approve failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// The index of the one change waiting for approval.
    ///
    /// **Polled, not asked once.** A proposal is pending on the leader the
    /// moment `core set` returns, but this is usually called against a
    /// *different* node — a proposal is only pending there once Raft has
    /// replicated it, which the proposer's own CLI call returning success
    /// says nothing about. Asking once is asserting the absence of that
    /// replication delay, the same reasoning `wait_for_status` gives for
    /// polling a promotion.
    pub fn the_pending_one(&self) -> u64 {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let output = distlib(self.dir.path()).arg("pending").output().unwrap();
            assert!(
                output.status.success(),
                "pending failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let listed = String::from_utf8(output.stdout).unwrap();
            if let Some(proposal) = listed
                .lines()
                .find_map(|line| line.split_whitespace().next()?.parse::<u64>().ok())
            {
                return proposal;
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for a proposal to become pending here; last saw:\n{listed}"
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Waits for this node's own account of itself to satisfy `settled`.
    ///
    /// Polled rather than asserted, because the last step of a promotion is
    /// the leader noticing a learner has caught up — and it notices on its own
    /// retry rather than being woken, since replication progress is not
    /// something openraft reports through the metrics the reconciler watches.
    /// A couple of seconds, then, and asserting straight away is asserting the
    /// absence of that delay rather than the promotion.
    pub fn wait_for_status(&self, what: &str, settled: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        loop {
            let status = self.status();
            if settled(&status) {
                return status;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for {what}; this node last said:\n{status}");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// What this node says about itself, live.
    pub fn status(&self) -> String {
        let output = distlib(self.dir.path()).arg("status").output().unwrap();
        assert!(
            output.status.success(),
            "status failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    /// Asks this node for a join ticket.
    pub fn ticket(&self) -> String {
        let output = distlib(self.dir.path()).arg("ticket").output().unwrap();
        assert!(
            output.status.success(),
            "ticket failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .next()
            .expect("the ticket is the first line")
            .trim()
            .to_owned()
    }

    /// Takes a ticket and writes the group into this node's configuration.
    pub fn join(&self, ticket: &str) {
        let output = distlib(self.dir.path())
            .args(["join", ticket])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "join failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    pub fn members(&self) -> String {
        let output = distlib(self.dir.path()).arg("members").output().unwrap();
        assert!(
            output.status.success(),
            "members failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}

/// Waits for `needle` in every node's log, and gives up the moment any of them
/// has stopped.
///
/// Across *all* of them, which is the point rather than a convenience. The node
/// that fails at startup is usually not the one being waited on: the survivors
/// carry on trying to elect a leader and look perfectly healthy, so waiting on
/// them one at a time sits out the whole bound against a node that is fine and
/// then reports the wrong thing. That is what happened — an api listener lost a
/// port race, and it surfaced thirty seconds later as a convergence failure
/// blamed on a different node, with the line that said so buried in a log
/// nobody had reason to read.
///
/// So this checks liveness before content, and prints *every* node's log when
/// it does give up.
pub fn wait_for_all(nodes: &mut [&mut Running], needle: &str) {
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    loop {
        for node in nodes.iter_mut() {
            if let Some(status) = node.exited() {
                panic!(
                    "a node exited ({status}) while the group waited for {needle:?}; its log was:\n{}",
                    node.log_contents()
                );
            }
        }
        if nodes
            .iter()
            .all(|node| node.log_contents().contains(needle))
        {
            return;
        }
        if Instant::now() >= deadline {
            let logs: String = nodes
                .iter()
                .map(|node| format!("--- {}\n{}\n", node.log.display(), node.log_contents()))
                .collect();
            panic!("timed out waiting for {needle:?} on every node; logs were:\n{logs}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A node process, killed when the test ends however it ends.
pub struct Running {
    pub child: Child,
    pub log: PathBuf,
}

impl Running {
    /// Waits for `needle` to appear in this node's log.
    ///
    /// Gives up early if the node is no longer running, which is worth the two
    /// extra lines: a node that fails at startup can never print anything, so
    /// waiting the full bound turns "this process exited immediately" into a
    /// thirty-second timeout blamed on whatever the test was waiting for. That
    /// is not hypothetical — an api listener losing a port race was reported as
    /// a convergence failure two steps further on, and the log line saying so
    /// was three screens above the panic.
    pub fn wait_for(&mut self, needle: &str) {
        wait_for_all(&mut [self], needle);
    }

    /// The exit status, if this node has stopped on its own.
    pub fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().ok().flatten()
    }

    pub fn log_contents(&self) -> String {
        let mut text = String::new();
        if let Ok(mut file) = File::open(&self.log) {
            let _ = file.read_to_string(&mut text);
        }
        text
    }

    /// Stops the node the way an operator does, and waits for it to be gone.
    ///
    /// **Ctrl-C rather than a kill**, which the acceptance run made necessary
    /// rather than tidy. The membership log survives either way — redb commits
    /// per entry and releases its lock when the process dies — but the blob
    /// store does not: its metadata is flushed when the router closes it on the
    /// way out, and a killed node comes back with every downloaded blob's
    /// *data* still on disk and no record that it holds any of them. A node
    /// stopped that way re-fetches what it already has, which is the opposite
    /// of what "after restart still serves it" is asking about.
    ///
    /// So the signal is the one `distlib run` listens for, and this waits for
    /// the process to actually exit before returning — a restart that reopens
    /// the same data directory needs the old process gone, not merely asked.
    pub fn stop(self) {
        drop(self);
    }

    /// Sends `signal` — `"INT"`, `"TERM"` — to this node.
    ///
    /// Through `kill(1)` rather than a signalling crate: one command in one
    /// test harness is not worth a dependency, and every platform this runs its
    /// process tests on has it.
    pub fn signal(&mut self, signal: &str) {
        let _ = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status();
    }

    /// Waits for the node to exit of its own accord, within the bound.
    pub fn wait_until_gone(&mut self) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = self.exited() {
                return Some(status);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }

    /// Stops the node the way a machine that vanished does: no signal, no
    /// shutdown, no goodbye to its peers.
    ///
    /// For the tests whose scenario *is* an abrupt disappearance — a renumbered
    /// machine, a pulled cable — where [`Self::stop`] would be modelling the
    /// wrong thing. It matters more than it looks: a node that closes cleanly
    /// tells its peers so, and they then adopt the new address it dials them
    /// from when it comes back, which is the group healing an address change by
    /// itself. A machine that was renumbered did no such thing.
    ///
    /// Not the default, and not interchangeable with [`Self::stop`]: an abrupt
    /// stop loses the blob store's record of what this node fetched (P2-25).
    pub fn crash(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Asks the node to stop, and says whether it did within the bound.
    fn interrupt(&mut self) -> bool {
        // A node that has already gone is not signalled: its pid is free to
        // have been handed to something else by now.
        if self.exited().is_some() {
            return true;
        }
        self.signal("INT");
        self.wait_until_gone().is_some()
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // In `Drop` so that a failing assertion stops these too. Without that a
        // panic leaves nodes running — holding ports and their databases —
        // until somebody notices them in `ps` much later.
        //
        // A test that has already failed has nothing left to read out of these
        // nodes, so it does not pay the shutdown wait — three of them would be
        // half a minute added to every panic. Otherwise the kill is only the
        // fallback for a node that ignored the interrupt.
        if std::thread::panicking() || !self.interrupt() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

pub fn distlib(data_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_distlib"));
    command.arg("--data-dir").arg(data_dir);
    command
}

/// Which protocol will bind the port, because a free UDP port is not a free
/// TCP port and this test needs one of each per node.
#[derive(Clone, Copy)]
pub enum Protocol {
    Udp,
    Tcp,
}

/// Hands out the next port in [`PINNED`], so two nodes in one run cannot be
/// given the same number even if both probe it while it is still free.
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

/// Where pinned ports come from: below the kernel's ephemeral range, and above
/// the crowded low numbers.
///
/// Linux hands out 32768–60999 for anything that does not ask for a specific
/// port, which is every outgoing connection and every socket bound to `:0`.
const PINNED: std::ops::Range<u16> = 20_000..30_000;

/// A port this test can pin, chosen where nothing else will be given it.
///
/// Founding needs pinned ports — the founder writes them into every node's
/// configuration before anything binds — so the test cannot let the OS choose
/// at bind time, and there is an unavoidable gap between deciding on a number
/// and using it. What matters is *where the number comes from*.
///
/// This used to bind `:0`, read the port back and release it, which hands back
/// an **ephemeral** port: exactly the range the kernel draws from for every
/// unpinned socket, and this test starts three nodes that each open several.
/// About one run in ten something took the number in between, and the failure
/// was `Address already in use` on the api listener and a node that never
/// started — reported as a thirty-second convergence timeout two steps later,
/// which is nowhere near where the problem was.
///
/// So: a fixed range the kernel will not allocate from, walked by a counter so
/// two nodes in one run cannot collide, offset by the process id so two runs on
/// one machine do not either, and probed with the protocol that will use it.
pub fn a_free_port(protocol: Protocol) -> u16 {
    let span = PINNED.end - PINNED.start;
    // Spreads concurrent runs apart. Not a guarantee — hence the probe — but it
    // means two runs do not start walking from the same place.
    let offset = (std::process::id() as u16).wrapping_mul(64);

    for _ in 0..span {
        let step = offset.wrapping_add(NEXT_PORT.fetch_add(1, Ordering::Relaxed));
        let port = PINNED.start + step % span;
        let free = match protocol {
            Protocol::Udp => UdpSocket::bind(("127.0.0.1", port)).is_ok(),
            Protocol::Tcp => TcpListener::bind(("127.0.0.1", port)).is_ok(),
        };
        if free {
            return port;
        }
    }
    panic!("no free port in {PINNED:?}");
}

/// Waits for a process that is expected to give up on its own.
///
/// `Command::output` would be shorter and would hang forever the day the
/// refusal stops working — which is precisely the day this test has to fail.
pub fn wait_for_exit(mut child: Child, what: &str) -> std::process::Output {
    let deadline = Instant::now() + REFUSAL_TIMEOUT;
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    panic!("{what} did not exit within {REFUSAL_TIMEOUT:?}");
}

/// A refusal happens before any network work, so it is immediate or it is broken.
pub const REFUSAL_TIMEOUT: Duration = Duration::from_secs(10);

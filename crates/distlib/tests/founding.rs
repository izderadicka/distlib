//! Three friends found a group, through the actual commands.
//!
//! Every other test in this workspace drives the library. This one drives the
//! binary, because the thing being checked is the *procedure*: nobody can found
//! a group without first collecting ids and addresses from the others, and that
//! exchange happens outside the program. A library test cannot get it wrong, so
//! it cannot catch it being wrong either.

// Builds the binary and runs three of it, so it costs seconds and varies with
// the machine. Skipped by `--no-default-features`; see the `slow-tests` feature
// in Cargo.toml.
#![cfg(feature = "slow-tests")]
#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

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
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);

/// One friend's node: a data directory, a pinned port, and an identity.
struct Friend {
    dir: TempDir,
    port: u16,
    /// The local API's port.
    ///
    /// Its own, like the transport port: three nodes on one machine cannot
    /// share either.
    api_port: u16,
    id: String,
}

impl Friend {
    /// Runs `whoami` to create the identity and learn the member id.
    ///
    /// This is step one of the real procedure — the command exists so that a
    /// founder can be told who everyone is before there is a group to ask.
    fn introduce() -> Self {
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
    fn agree_on(&self, everyone: &[(String, u16)]) {
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
    fn run(&self, found: bool) -> Running {
        let log = self.dir.path().join("node.log");
        let mut command = distlib(self.dir.path());
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
    fn admit(&self, member: &str) {
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

    /// Asks this node for a join ticket.
    fn ticket(&self) -> String {
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
    fn join(&self, ticket: &str) {
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

    fn members(&self) -> String {
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
fn wait_for_all(nodes: &mut [&mut Running], needle: &str) {
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
struct Running {
    child: Child,
    log: PathBuf,
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
    fn wait_for(&mut self, needle: &str) {
        wait_for_all(&mut [self], needle);
    }

    /// The exit status, if this node has stopped on its own.
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().ok().flatten()
    }

    fn log_contents(&self) -> String {
        let mut text = String::new();
        if let Ok(mut file) = File::open(&self.log) {
            let _ = file.read_to_string(&mut text);
        }
        text
    }

    /// Stops the node so its database can be opened by `members`.
    ///
    /// A kill rather than a signal: redb releases its lock when the process
    /// dies, and everything committed is already durable, so what `members`
    /// reads afterwards is exactly what replication delivered.
    fn stop(self) {
        // The work is in `Drop`, so that a failing assertion above kills these
        // too. Without that a panic leaves nodes running — holding ports and
        // their databases — until somebody notices them in `ps` much later.
        drop(self);
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn distlib(data_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_distlib"));
    command.arg("--data-dir").arg(data_dir);
    command
}

/// Which protocol will bind the port, because a free UDP port is not a free
/// TCP port and this test needs one of each per node.
#[derive(Clone, Copy)]
enum Protocol {
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
fn a_free_port(protocol: Protocol) -> u16 {
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
fn wait_for_exit(mut child: Child, what: &str) -> std::process::Output {
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
const REFUSAL_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn run_refuses_a_data_directory_with_no_identity() {
    // The failure this prevents: `distlib run` pointed at the wrong directory
    // used to mint a fresh key and start a node that was nobody, with the only
    // symptom a member id nobody recognised and a port nobody was told about.
    let dir = TempDir::new().unwrap();

    let child = distlib(dir.path())
        .arg("run")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = wait_for_exit(child, "`distlib run` on an empty data directory");

    assert!(
        !output.status.success(),
        "an empty data directory must fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no identity"),
        "the error must name the problem, got:\n{stderr}"
    );
    assert!(
        stderr.contains("distlib init"),
        "the error must name the way out, got:\n{stderr}"
    );
    assert!(
        !dir.path().join("keys").join("node.key").exists(),
        "refusing must leave no identity behind"
    );
}

#[test]
fn three_friends_found_a_group() {
    // 1. Each of them runs `whoami` and sends the founder the line it prints.
    let friends: Vec<Friend> = (0..3).map(|_| Friend::introduce()).collect();
    let everyone: Vec<(String, u16)> = friends
        .iter()
        .map(|friend| (friend.id.clone(), friend.port))
        .collect();

    // 2. The founder assembles the core group and sends it back, so all three
    //    configs are identical. A founder who is not in their own core list is
    //    refused, and a member missing from someone else's cannot be reached.
    for friend in &friends {
        friend.agree_on(&everyone);
    }

    // 3. The other two start first. With three voters the founder needs one of
    //    them to grant its vote before it can commit anything.
    let mut second = friends[1].run(false);
    let mut third = friends[2].run(false);
    let mut first = friends[0].run(true);

    // 4. All three converge on one group, and only the founder was told to
    //    found it — the others learned everything by replication.
    let converged = "members=3 core=3";
    wait_for_all(&mut [&mut first, &mut second, &mut third], converged);
    assert!(
        !second.log_contents().contains("founding the group"),
        "only the founder founds"
    );

    let group = group_id(&first.log_contents());
    for node in [&second, &third] {
        assert_eq!(
            group_id(&node.log_contents()),
            group,
            "one group, not three separate ones"
        );
    }

    // 5. A fourth member is admitted from the command line, against a running
    //    node. This is the part that cannot be done any other way: the node
    //    holds its database exclusively, so the CLI has to go through its API.
    let newcomer = Friend::introduce();
    friends[0].admit(&newcomer.id);

    wait_for_all(&mut [&mut first, &mut second, &mut third], "members=4");

    // ...and a node that was told nothing about it lists them, live, while
    //    still running.
    let listed = friends[2].members();
    assert!(
        listed.contains(&newcomer.id),
        "a node that never heard the command should still list the newcomer; got:\n{listed}"
    );

    // 6. The newcomer joins for real: it takes a ticket, writes the group into
    //    its own configuration, and follows the log without ever having been
    //    told what is in it. §4.3 end to end.
    newcomer.join(&friends[1].ticket());
    let mut joined = newcomer.run(false);
    joined.wait_for("members=4");

    let listed = newcomer.members();
    for friend in &friends {
        assert!(
            listed.contains(&friend.id),
            "a joiner must derive the whole group from the log; got:\n{listed}"
        );
    }
    let own_line = listed
        .lines()
        .find(|line| line.contains(&newcomer.id))
        .expect("the joiner lists itself");
    assert!(
        !own_line.contains("core"),
        "a joiner follows rather than votes; got: {own_line}"
    );
    assert!(
        listed.contains("(3 core)"),
        "the three founders still vote; got:\n{listed}"
    );
    joined.stop();

    // 7. Stopped, each of them can be asked who is in the group, and the answer
    //    comes from its own copy of the log rather than from its config.
    for node in [first, second, third] {
        node.stop();
    }
    for friend in &friends {
        let listed = friend.members();
        assert!(listed.contains(&group), "every node names the same group");
        for other in friends.iter().chain([&newcomer]) {
            assert!(
                listed.contains(&other.id),
                "{} should list {}; got:\n{listed}",
                friend.id,
                other.id
            );
        }
    }
}

/// The group id from a `membership` log line.
fn group_id(log: &str) -> String {
    log.split("group=")
        .nth(1)
        .expect("a membership line names the group")
        .split_whitespace()
        .next()
        .expect("the group id is one word")
        .to_owned()
}

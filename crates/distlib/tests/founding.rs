//! Three friends found a group, through the actual commands.
//!
//! Every other test in this workspace drives the library. This one drives the
//! binary, because the thing being checked is the *procedure*: nobody can found
//! a group without first collecting ids and addresses from the others, and that
//! exchange happens outside the program. A library test cannot get it wrong, so
//! it cannot catch it being wrong either.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    fs::File,
    io::Read as _,
    net::UdpSocket,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
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
        let port = a_free_port();
        let api_port = a_free_port();

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

/// A node process, killed when the test ends however it ends.
struct Running {
    child: Child,
    log: PathBuf,
}

impl Running {
    /// Waits for `needle` to appear in this node's log.
    fn wait_for(&self, needle: &str) {
        let deadline = Instant::now() + CONVERGE_TIMEOUT;
        while Instant::now() < deadline {
            if self.log_contents().contains(needle) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!(
            "timed out waiting for {needle:?} in {}; log was:\n{}",
            self.log.display(),
            self.log_contents()
        );
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

/// A port nothing is listening on, most likely.
///
/// Founding needs pinned ports, so the test cannot let the OS choose at bind
/// time. Binding and releasing is the closest available approximation.
fn a_free_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
    let second = friends[1].run(false);
    let third = friends[2].run(false);
    let first = friends[0].run(true);

    // 4. All three converge on one group, and only the founder was told to
    //    found it — the others learned everything by replication.
    let converged = "members=3 core=3";
    for node in [&first, &second, &third] {
        node.wait_for(converged);
    }
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

    for node in [&first, &second, &third] {
        node.wait_for("members=4");
    }

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
    let joined = newcomer.run(false);
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

//! Configuration precedence and the rules the loader enforces.

#![allow(clippy::unwrap_used)]
// test code: a panic on a broken invariant is the point
// `Jail::expect_with` closures return figment's own large error type; the size is not
// ours to fix. CoreError boxes it for exactly this reason.
#![allow(clippy::result_large_err)]

use std::{net::SocketAddr, path::Path};

use distlib_core::{Config, CoreError, MemberId, RelayMode};
use figment::Jail;

const CONFIG: &str = "config.toml";

fn a_member() -> MemberId {
    MemberId::from(iroh::SecretKey::generate().public())
}

#[test]
fn a_missing_file_yields_defaults() {
    let config = Config::load(Path::new("does-not-exist.toml")).unwrap();

    assert_eq!(config, Config::default());
}

#[test]
fn the_file_overrides_defaults() {
    Jail::expect_with(|jail| {
        jail.create_file(CONFIG, "[net]\nbind_addr_v4 = \"127.0.0.1:4242\"\n")?;

        let config = Config::load(Path::new(CONFIG)).unwrap();

        assert_eq!(
            config.net.bind_addr_v4,
            "127.0.0.1:4242".parse::<SocketAddr>().unwrap()
        );
        // Untouched keys keep their defaults rather than being reset.
        assert_eq!(config.net.relay_mode, RelayMode::Default);
        Ok(())
    });
}

#[test]
fn the_environment_overrides_the_file() {
    Jail::expect_with(|jail| {
        jail.create_file(CONFIG, "[net]\nbind_addr_v4 = \"127.0.0.1:4242\"\n")?;
        jail.set_env("DISTLIB_NET__BIND_ADDR_V4", "127.0.0.1:9999");

        let config = Config::load(Path::new(CONFIG)).unwrap();

        assert_eq!(
            config.net.bind_addr_v4,
            "127.0.0.1:9999".parse::<SocketAddr>().unwrap()
        );
        Ok(())
    });
}

#[test]
fn data_dir_in_the_environment_is_ignored_not_rejected() {
    // DISTLIB_DATA_DIR is a documented override consumed before this loader
    // runs. With deny_unknown_fields it would otherwise be a hard error.
    Jail::expect_with(|jail| {
        jail.set_env("DISTLIB_DATA_DIR", "/tmp/somewhere");

        let config = Config::load(Path::new(CONFIG)).unwrap();

        assert_eq!(config, Config::default());
        Ok(())
    });
}

#[test]
fn data_dir_in_the_file_is_rejected_with_an_explanation() {
    Jail::expect_with(|jail| {
        jail.create_file(CONFIG, "data_dir = \"/tmp/elsewhere\"\n")?;

        let error = Config::load(Path::new(CONFIG)).unwrap_err();

        assert!(
            matches!(error, CoreError::DataDirInConfig { .. }),
            "expected DataDirInConfig, got {error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains("--data-dir"),
            "unhelpful message: {message}"
        );
        Ok(())
    });
}

#[test]
fn an_unknown_key_is_rejected() {
    Jail::expect_with(|jail| {
        jail.create_file(CONFIG, "[net]\nbnid_addr_v4 = \"127.0.0.1:1\"\n")?;

        assert!(
            Config::load(Path::new(CONFIG)).is_err(),
            "typo went unnoticed"
        );
        Ok(())
    });
}

#[test]
fn the_allowlist_parses_member_ids() {
    let member = a_member();
    Jail::expect_with(move |jail| {
        jail.create_file(CONFIG, &format!("[net]\nallowlist = [\"{member}\"]\n"))?;

        let config = Config::load(Path::new(CONFIG)).unwrap();

        assert_eq!(config.net.allowlist, vec![member]);
        Ok(())
    });
}

#[test]
fn a_bad_member_id_in_the_allowlist_is_rejected() {
    Jail::expect_with(|jail| {
        jail.create_file(CONFIG, "[net]\nallowlist = [\"nonsense\"]\n")?;

        assert!(Config::load(Path::new(CONFIG)).is_err());
        Ok(())
    });
}

#[test]
fn the_starter_file_reloads_as_what_it_came_from() {
    // The starter file is hand-rendered so it can carry comments; this keeps it
    // honest about matching the struct it claims to describe.
    let config = Config {
        net: distlib_core::NetConfig {
            bind_addr_v4: "127.0.0.1:11204".parse().unwrap(),
            relay_mode: RelayMode::Custom,
            relay_urls: vec!["https://relay.example/".to_owned()],
            allowlist: vec![a_member()],
        },
    };

    let rendered = config.to_starter_toml();
    Jail::expect_with(move |jail| {
        jail.create_file(CONFIG, &rendered)?;

        assert_eq!(Config::load(Path::new(CONFIG)).unwrap(), config);
        Ok(())
    });
}

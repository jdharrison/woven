#![deny(unsafe_code)]

use std::{
    io::{IsTerminal, Read},
    process::ExitCode,
};

use woven_server::{
    MAX_MANAGED_STATE_BYTES, ManagedServerConfig, RemoteServerConfig, ServerConfig,
    import_managed_state, import_managed_state_baseline, serve, serve_managed, serve_remote,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivityLogMode {
    None,
    All,
    Transform,
}

impl ActivityLogMode {
    fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut mode = Self::None;
        let mut selected = false;
        for argument in arguments {
            let parsed = match argument.as_str() {
                "--log-none" => Self::None,
                "--log-all" => Self::All,
                "--log-transform" => Self::Transform,
                "--help" | "-h" => return Err(Self::usage().to_owned()),
                _ => return Err(format!("unknown argument: {argument}\n\n{}", Self::usage())),
            };
            if selected {
                return Err(format!(
                    "choose exactly one activity log mode\n\n{}",
                    Self::usage()
                ));
            }
            mode = parsed;
            selected = true;
        }
        Ok(mode)
    }

    const fn usage() -> &'static str {
        "Usage: woven-server [--log-none | --log-all | --log-transform]\n\
         \n\
         Development activity logging is disabled by default.\n\
         --log-all        Print all safe activity metadata to stdout in debug builds.\n\
         --log-transform  Print entity position and entity-scoped latest-state activity only.\n\
         --log-none       Disable development activity logging (the default).\n\
         Managed mode: set WOVEN_MANAGED_QUIC=1 and the required managed settings.\n\
         WOVEN_MANAGED_STATE_DIR enables durable managed configuration (existing 0700 dir).\n\
         --managed-state-import reads a privileged manifest from stdin, initializes only\n\
         an empty private store, and exits without listeners. Missing state never initializes empty.\n\
         --managed-state-import-baseline is the separate lost-history baseline importer;\n\
         it requires migrationBaseline metadata, a new lineage and explicit stdin approval.\n\
         Optionally add WOVEN_MANAGED_WEBTRANSPORT=1, WOVEN_WEBTRANSPORT_BIND,\n\
         WOVEN_WEBTRANSPORT_PATH, and WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS.\n\
         WOVEN_WEBTRANSPORT_ORIGIN_POLICY defaults to allowlist (exact required origins).\n\
         Set it to https to accept canonical HTTPS origins and missing Origin; omit\n\
         WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS in https mode. Bearer auth remains required.\n\
         Static remote QUIC: set WOVEN_REMOTE_QUIC=1 and its required settings.\n\
         Management HTTP must remain loopback; inference is disabled in managed/remote modes."
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    let first = arguments.next();
    let baseline_import = match first.as_deref() {
        Some("--managed-state-import") => Some(false),
        Some("--managed-state-import-baseline") => Some(true),
        _ => None,
    };
    if let Some(baseline) = baseline_import {
        if arguments.next().is_some() {
            eprintln!(
                "Offline import flags must be used alone; supply approval only in the privileged stdin manifest."
            );
            return ExitCode::FAILURE;
        }
        return import_state_cli(baseline).await;
    }
    let activity_log_mode = match ActivityLogMode::parse(first.into_iter().chain(arguments)) {
        Ok(mode) => mode,
        Err(message) => {
            println!("{message}");
            return ExitCode::FAILURE;
        }
    };
    if !activity_logging_supported(activity_log_mode) {
        eprintln!("Activity logging is available only in debug builds; release binaries omit it.");
        return ExitCode::FAILURE;
    }

    init_logging(activity_log_mode);
    let result = match ManagedServerConfig::from_env() {
        Ok(Some(config)) => {
            if activity_log_mode != ActivityLogMode::None {
                eprintln!("Managed mode does not permit development activity logging.");
                return ExitCode::FAILURE;
            }
            serve_managed(config).await
        }
        Err(error) => Err(error),
        Ok(None) => match RemoteServerConfig::from_env() {
            Ok(Some(config)) => {
                if activity_log_mode != ActivityLogMode::None {
                    eprintln!("Remote mode does not permit development activity logging.");
                    return ExitCode::FAILURE;
                }
                serve_remote(config).await
            }
            Ok(None) => serve(ServerConfig::default()).await,
            Err(error) => Err(error),
        },
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Woven node stopped: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn import_state_cli(baseline: bool) -> ExitCode {
    let result = async {
        let config = ManagedServerConfig::from_env()?
            .ok_or_else(|| std::io::Error::other("managed configuration required"))?;
        if std::io::stdin().is_terminal() {
            return Err(woven_server::ServerError::Io(std::io::Error::other(
                "privileged manifest must be supplied on stdin, not interactively",
            )));
        }
        let manifest = tokio::task::spawn_blocking(|| {
            let mut manifest = Vec::new();
            std::io::stdin()
                .lock()
                .take(MAX_MANAGED_STATE_BYTES as u64 + 1)
                .read_to_end(&mut manifest)?;
            Ok::<_, std::io::Error>(manifest)
        })
        .await
        .map_err(|_| std::io::Error::other("manifest read failed"))??;
        if baseline {
            import_managed_state_baseline(config, manifest).await
        } else {
            import_managed_state(config, manifest).await
        }
    }
    .await;
    if let Ok(receipt) = result {
        // The receipt contains only the approved lineage, never checkpoint/verifier bytes.
        println!("{}", serde_json::to_string(&receipt).unwrap_or_default());
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "Managed state import failed; check import mode, approved manifest, baseline approval/new lineage when required, current admin credential, empty private directory and exclusive access. No listeners started."
        );
        ExitCode::FAILURE
    }
}

fn activity_logging_supported(mode: ActivityLogMode) -> bool {
    #[cfg(debug_assertions)]
    {
        let _ = mode;
        true
    }
    #[cfg(not(debug_assertions))]
    {
        mode == ActivityLogMode::None
    }
}

fn init_logging(activity_log_mode: ActivityLogMode) {
    #[cfg(debug_assertions)]
    {
        let filter = match activity_log_mode {
            ActivityLogMode::None => "warn,woven_relay=info,woven_activity=off",
            ActivityLogMode::All => "warn,woven_relay=info,woven_activity=info",
            ActivityLogMode::Transform => {
                "warn,woven_relay=info,woven_activity=off,woven_activity::transform=info"
            }
        };
        tracing_subscriber::fmt()
            .compact()
            .with_target(false)
            .with_writer(std::io::stdout)
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .init();
    }

    // Production: structured JSON to stdout, one object per line, so a log pipeline (e.g.
    // Cloud Logging on Cloud Run/GKE, which captures stdout/stderr automatically) can parse
    // and filter on fields like `connection_id`/`namespace_id`/`session_id` without a custom
    // parser. Default level is deliberately narrow (only our own curated `woven_relay`
    // lifecycle events at info; everything else, including dependencies, at warn+) so log
    // volume stays bounded; RUST_LOG overrides this when set.
    #[cfg(not(debug_assertions))]
    {
        let _ = activity_log_mode;
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,woven_relay=info"));
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    }
}

#[cfg(test)]
mod tests {
    use super::ActivityLogMode;

    #[test]
    fn activity_log_modes_are_explicit_and_exclusive() {
        assert_eq!(
            ActivityLogMode::parse(["--log-all".to_owned()].into_iter()),
            Ok(ActivityLogMode::All)
        );
        assert_eq!(
            ActivityLogMode::parse(["--log-transform".to_owned()].into_iter()),
            Ok(ActivityLogMode::Transform)
        );
        assert!(
            ActivityLogMode::parse(
                ["--log-all".to_owned(), "--log-transform".to_owned()].into_iter()
            )
            .is_err()
        );
    }
}

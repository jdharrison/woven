#![deny(unsafe_code)]

use std::process::ExitCode;

use woven_server::{RemoteServerConfig, ServerConfig, serve, serve_remote};

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
         Remote QUIC: set WOVEN_REMOTE_QUIC=1 and WOVEN_QUIC_BIND, WOVEN_MANAGEMENT_BIND,\n\
         WOVEN_TLS_CERT_FILE, WOVEN_TLS_KEY_FILE, WOVEN_AUTH_TOKEN_FILE.\n\
         Management HTTP must remain loopback. Remote WebTransport/inference are disabled."
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let activity_log_mode = match ActivityLogMode::parse(std::env::args().skip(1)) {
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
    let result = match RemoteServerConfig::from_env() {
        Ok(Some(config)) => {
            if activity_log_mode != ActivityLogMode::None {
                eprintln!("Remote mode does not permit development activity logging.");
                return ExitCode::FAILURE;
            }
            serve_remote(config).await
        }
        Ok(None) => serve(ServerConfig::default()).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Woven node stopped: {error}");
            ExitCode::FAILURE
        }
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

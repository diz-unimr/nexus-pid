use crate::config::Config;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Router};
use chrono::Utc;
use clap::Parser;
use cron::Schedule;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

mod config;

#[derive(Clone)]
struct DataStore {
    data: Arc<RwLock<HashMap<String, Data>>>,
}

impl DataStore {
    fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn find(&self, id: &str) -> Option<Data> {
        self.data
            .read()
            .await
            .iter()
            .find(|(key, _)| id.eq(*key))
            .map(|(_, data)| data.clone())
    }

    async fn add(&self, data: &Data) {
        self.data
            .write()
            .await
            .insert(data.befund_id.clone(), data.clone());
    }

    async fn len(&self) -> usize {
        self.data.read().await.len()
    }

    async fn is_empty(&self) -> bool {
        self.data.read().await.len() == 0
    }
}

#[derive(Deserialize)]
struct IdQuery {
    id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Data {
    #[serde(rename = "befundID")]
    pub befund_id: String,
    #[serde(rename = "patientennummer")]
    pub patientennummer: String,
}

async fn connect(config: Config, tx: tokio::sync::mpsc::Sender<Data>) -> Result<(), ()> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", &config.bootstrap_servers);

    let mut consumer_client_config =
        if config.ssl_cert_file.is_some() || config.ssl_key_file.is_some() {
            client_config
                .set("security.protocol", "ssl")
                .set(
                    "ssl.ca.location",
                    config.ssl_ca_file.clone().unwrap_or_default(),
                )
                .set(
                    "ssl.certificate.location",
                    config.ssl_cert_file.clone().unwrap_or_default(),
                )
                .set(
                    "ssl.key.location",
                    config.ssl_key_file.clone().unwrap_or_default(),
                );
            if let Some(ssl_key_password) = &config.ssl_key_password {
                client_config.set("ssl.key.password", ssl_key_password);
            }
            client_config
        } else {
            client_config
        };

    let consumer: StreamConsumer = consumer_client_config
        .set("group.id", &config.group_id)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .map_err(|_| ())?;

    let _ = consumer.subscribe(&[&config.topic]).map_err(|_| ())?;

    log::info!("Subscribed to topic: {}", config.topic);

    while let Ok(msg) = consumer.recv().await {
        let Some(Ok(payload)) = msg.payload_view::<str>() else {
            continue;
        };
        let Ok(data) = serde_json::from_str::<Data>(payload) else {
            continue;
        };
        let Ok(_) = tx.send(data).await else {
            continue;
        };
    }

    Ok(())
}

async fn handle_request(
    query: Query<IdQuery>,
    Extension(data_store): Extension<DataStore>,
) -> Response {
    if let Some(data) = data_store.find(&query.id).await {
        return Response::new(data.patientennummer.into());
    }
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body("Not found".into())
        .expect("Failed to build response")
}

#[tokio::main]
async fn main() {
    #[cfg(debug_assertions)]
    {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    }

    #[cfg(not(debug_assertions))]
    {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
    }

    let config = Config::parse();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Data>(10);
    let data_store = DataStore::new();

    let routes = Router::new()
        .route("/", get(handle_request))
        .layer(Extension(data_store.clone()));

    let a = tokio::spawn(connect(config.clone(), tx));
    let data_store_b = data_store.clone();
    let b = tokio::spawn(async move {
        while let Some(x) = rx.recv().await {
            if data_store_b.is_empty().await {
                log::info!("Start filling the data store");
            }
            data_store_b.add(&x).await;
        }
    });
    let c = tokio::spawn(async move {
        let _ = match tokio::net::TcpListener::bind(&config.listen).await {
            Ok(listener) => {
                log::info!("Starting application listening on '{}'", &config.listen);
                if let Err(err) = axum::serve(listener, routes)
                    .with_graceful_shutdown(shutdown_signal())
                    .await
                {
                    log::error!("Failed to start application: {}", err);
                }
            }
            Err(err) => {
                log::error!("Failed to start application: {}", err);
            }
        };
    });
    let data_store_d = data_store.clone();
    let d = tokio::spawn(async move {
        let schedule = Schedule::from_str("0 * * * * *").expect("invalid cron expression");
        loop {
            let next = schedule.upcoming(Utc).next().expect("no next execution");

            let now = Utc::now();
            let duration = (next - now)
                .to_std()
                .expect("next execution is in the past");

            tokio::time::sleep(duration).await;
            log::info!("Current data store size: {}", data_store_d.len().await);
        }
    });

    tokio::select! {
        _ = a => {
            log::info!("Finished due to kafka connection error");
        },
        _ = b => {
            log::info!("Finished due to early service exit");
        },
        _ = c => {
            log::info!("Finished due to web error");
        },
        _ = d => {
            log::info!("Finished due to early monitoring exit");
        },
    }
}

#[allow(clippy::expect_used)]
async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    terminate.await;
}

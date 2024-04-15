use actix_cors::Cors;
use actix_web::web::Json;
use actix_web::{get, middleware, post, App, HttpRequest, HttpResponse, HttpServer, Result};
use log::LevelFilter;
use serde::{Deserialize, Serialize};

use merkle_trie_clock::merkle::MerkleTrie;
use merkle_trie_clock::timestamp::Timestamp;

use crate::db::{add_messages, find_late_messages, MERKLE_BASE};
use crate::models::Message;

pub mod db;
pub mod models;

const NODE_NAME: &str = "SERVER";

#[get("/ping")]
async fn ping(req: HttpRequest) -> Result<HttpResponse> {
    println!("REQ: {req:?}");

    Ok(HttpResponse::Ok().body("Ok".to_string()))
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncRequest {
    group_id: String,
    client_id: String,
    messages: Vec<Message>,
    merkle: MerkleTrie<MERKLE_BASE>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SyncResponse {
    messages: Vec<Message>,
    merkle: MerkleTrie<MERKLE_BASE>,
}

#[post("/sync")]
async fn sync(req: Json<SyncRequest>) -> Result<HttpResponse> {
    let SyncRequest {
        group_id,
        client_id,
        messages,
        merkle: client_merkle,
    } = req.into_inner();

    println!(
        "Got sync request, messages: {:?}, merkle: {:?}",
        messages, client_merkle
    );

    let trie = add_messages(&group_id, &messages).unwrap();

    let mut new_messages = vec![];

    // Get the point in time (in minutes?) at which the two collections of
    // messages "forked." In other words, at this point in time, something
    // changed (e.g., one collection inserted a message that the other lacks)
    // which resulted in differing hashes.
    if let Some(diff_time) = trie.diff(&client_merkle) {
        let timestamp = Timestamp::new(diff_time, 0, NODE_NAME.to_string()).to_string();
        new_messages = find_late_messages(&group_id, &client_id, &timestamp).unwrap();
    };

    Ok(HttpResponse::Ok().json(SyncResponse {
        messages: new_messages,
        merkle: trie,
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // 初始化日志系统
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();
    log::info!("starting HTTP server at http://localhost:8006");

    HttpServer::new(|| {
        let cors = Cors::permissive();
        App::new()
            // enable logger
            .wrap(middleware::Logger::default())
            .wrap(cors)
            .service(ping)
            .service(sync)
    })
    .bind(("127.0.0.1", 8006))?
    .run()
    .await
}

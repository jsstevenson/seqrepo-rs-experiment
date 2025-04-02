use axum::{extract::Path, http::StatusCode, routing::get, Json, Router};
use serde::Serialize;

fn app() -> Router {
    Router::new()
        .route("/ping", get(get_ping))
        .route("/sequence/{alias}", get(get_sequence))
        .route("/metadata/{alias}", get(get_metadata))
}

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app()).await.unwrap();
}

#[derive(Serialize)]
struct PingResponse {
    version: String,
    url: String,
}

async fn get_ping() -> (StatusCode, Json<PingResponse>) {
    let response = PingResponse {
        version: String::from("zzzz"),
        url: String::from("http://google.com"),
    };
    (StatusCode::OK, Json(response))
}

// TODO
//  404 if not found
//  422 if invalid request
async fn get_sequence(Path(alias): Path<String>) -> (StatusCode, String) {
    (StatusCode::OK, alias)
}

#[derive(Serialize)]
struct MetadataResponse {
    length: i64,
    aliases: String,  // todo array
    alphabet: String, // todo nullable
    added: String,    // todo nullable
}

// TODO
//  404 if not found
//  422 if invalid request
async fn get_metadata(Path(alias): Path<String>) -> (StatusCode, Json<MetadataResponse>) {
    let response = MetadataResponse {
        length: 10,
        aliases: alias,
        alphabet: String::from("zzz"),
        added: String::from("zzz"),
    };
    (StatusCode::OK, Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn ping() {
        let app = app();

        let response = app
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_sequence() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sequence/aaaaa")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_metadata() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metadata/aaaaa")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}

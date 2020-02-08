#![allow(deprecated)]
use futures::compat::Stream01CompatExt;
use futures::stream::{StreamExt, TryStreamExt};

use anyhow::Context;

use actix_web::*;
use lazy_static::lazy_static;
use reql as rq;
use reql::Run;
use reql_types::WriteStatus;

include!("types.rs");

lazy_static! {
    static ref R: rq::Client = rq::Client::new().db("demo");
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let conn = R.connect(rq::Config::default()).unwrap();

    HttpServer::new(move || {
        App::new().data(conn.clone()).service(
            web::scope("/teapot")
                .service(
                    web::resource("/{teapot}")
                        .route(web::get().to(|teapot_id, dbconn| {
                            async move { get_teapot(teapot_id, dbconn).await.unwrap() }
                        }))
                        .route(web::put().to(|teapot_id, teapot_patch, dbconn| {
                            async move {
                                update_teapot(teapot_id, teapot_patch, dbconn)
                                    .await
                                    .unwrap()
                            }
                        }))
                        .route(web::delete().to(|teapot_id, dbconn| {
                            async move { delete_teapot(teapot_id, dbconn).await.unwrap() }
                        })),
                )
                .service(
                    web::resource(["", "/"])
                        .route(
                            web::get()
                                .to(|dbconn| async move { get_teapots(dbconn).await.unwrap() }),
                        )
                        .route(web::post().to(|teapot, dbconn| {
                            async move { create_teapot(teapot, dbconn).await.unwrap() }
                        })),
                ),
        )
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}

async fn create_teapot(
    teapot: web::Json<Teapot>,
    dbconn: web::Data<rq::Connection>,
) -> anyhow::Result<impl Responder> {
    let doc = R
        .table("teapots")
        .insert(serde_json::to_value(&*teapot)?)
        .run::<WriteStatus>(**dbconn)
        .context("Failed to send query")?
        .compat()
        .next()
        .await
        .expect("Expected one element in response")
        .context("Failed to recieve query")?;

    match doc {
        Some(rq::Document::Expected(status)) => {
            if status.errors == 0 {
                if let Some(keys) = status.generated_keys {
                    if let Some(id) = keys.get(0) {
                        return Ok(
                            HttpResponse::Created().json(TeapotCreateRes { id: id.to_string() })
                        );
                    }
                }
            }

            Err(anyhow::format_err!(
                "Errors writing document: {}",
                status.first_error.unwrap_or_else(|| "".into())
            ))
        }
        Some(rq::Document::Unexpected(res)) => Err(anyhow::format_err!(
            "Recieved unexpected response from DB: {}",
            res.to_string()
        )),
        None => Err(anyhow::format_err!("Recieved no response from database")),
    }
}

async fn get_teapots(dbconn: web::Data<rq::Connection>) -> anyhow::Result<impl Responder> {
    let stream = R
        .table("teapots")
        .run::<Teapot>(**dbconn)
        .context("Failed to send query")?
        .compat();

    let teapots = stream
        .map_err(|e| anyhow::format_err!("Error: {}", e))
        .try_fold(Vec::new(), |mut teapots, doc| {
            async move {
                match doc {
                    Some(rq::Document::Expected(teapot)) => {
                        teapots.push(teapot);
                        Ok(teapots)
                    }
                    Some(rq::Document::Unexpected(res)) => Err(anyhow::format_err!(
                        "Recieved unexpected response from DB: {}",
                        res.to_string()
                    )),
                    None => Err(anyhow::format_err!("Got empty document")),
                }
            }
        })
        .await?;

    Ok(HttpResponse::Ok().json(teapots))
}

async fn get_teapot(
    teapot_id: web::Path<String>,
    dbconn: web::Data<rq::Connection>,
) -> anyhow::Result<impl Responder> {
    let doc = R
        .table("teapots")
        .get(&*teapot_id)
        .run::<Teapot>(**dbconn)
        .context("Failed to send query")?
        .compat()
        .next()
        .await
        .expect("Expected one element in response")
        .context("Failed to recieve query")?;

    match doc {
        Some(rq::Document::Expected(teapot)) => Ok(HttpResponse::Ok().json(teapot)),
        Some(rq::Document::Unexpected(res)) => Err(anyhow::format_err!(
            "Recieved unexpected response from DB: {}",
            res.to_string()
        )),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

async fn update_teapot(
    teapot_id: web::Path<String>,
    teapot_patch: web::Json<TeapotPatch>,
    dbconn: web::Data<rq::Connection>,
) -> anyhow::Result<impl Responder> {
    let doc = R
        .table("teapots")
        .get(&*teapot_id)
        .update(serde_json::to_value(&*teapot_patch)?)
        .run::<WriteStatus>(**dbconn)
        .context("Failed to send query")?
        .compat()
        .next()
        .await
        .expect("Expected one element in response")
        .context("Failed to recieve query")?;

    match doc {
        Some(rq::Document::Expected(status)) => {
            if status.errors == 0 {
                return Ok(HttpResponse::Ok());
            }

            Err(anyhow::format_err!(
                "Errors writing document: {}",
                serde_json::to_string(&status)?
            ))
        }
        Some(rq::Document::Unexpected(res)) => Err(anyhow::format_err!(
            "Recieved unexpected response from DB: {}",
            res.to_string()
        )),
        None => Err(anyhow::format_err!("Recieved no response from database")),
    }
}

async fn delete_teapot(
    teapot_id: web::Path<String>,
    dbconn: web::Data<rq::Connection>,
) -> anyhow::Result<impl Responder> {
    let doc = R
        .table("teapots")
        .get(&*teapot_id)
        .delete()
        .run::<WriteStatus>(**dbconn)
        .context("Failed to send query")?
        .compat()
        .next()
        .await
        .expect("Expected one element in response")
        .context("Failed to recieve query")?;

    match doc {
        Some(rq::Document::Expected(status)) => {
            if status.errors == 0 {
                return Ok(HttpResponse::Ok());
            }

            Err(anyhow::format_err!(
                "Errors writing document: {}",
                serde_json::to_string(&status)?
            ))
        }
        Some(rq::Document::Unexpected(res)) => Err(anyhow::format_err!(
            "Recieved unexpected response from DB: {}",
            res.to_string()
        )),
        None => Err(anyhow::format_err!("Recieved no response from database")),
    }
}

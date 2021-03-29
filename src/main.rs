use crate::utils::common::GenericTerm;
use actix_web::{post, web, App, HttpResponse, HttpServer, Result};
use pretty_env_logger;
use std::collections::HashMap;

mod utils;

use serde::{Deserialize, Serialize};

mod backend;

/*
  All threads share the same instance of this
*/
struct AppData {
    backend: backend::Backend,
}

/*
  Handle requests for `get-terms`. User sends an array of genes to a given target
  and the server replies with any found annotations
*/
#[derive(Serialize, Deserialize)]
struct ReqGetTerms {
    genes: Vec<String>,
    include_parents: Option<bool>, // Traverse graph up to include all parent terms
    include_root: Option<bool>,    // Include root terms
    parents_as_graph: Option<bool>, // Return parents as edge graph
}

#[post("/{uri}/{name}/term-to-gene")]
async fn get_terms(
    appdata: web::Data<AppData>,
    reqdata: web::Json<ReqGetTerms>,
    web::Path((uri, name)): web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let mut ret: HashMap<String, Option<backend::RetGetTerms>> = HashMap::new();
    let include_parents = match reqdata.include_parents {
        Some(e) => e,
        None => false,
    };
    let include_root = match reqdata.include_root {
        Some(e) => e,
        None => false,
    };
    let parents_as_graph = match reqdata.parents_as_graph {
        Some(e) => e,
        None => false,
    };
    let g = appdata.backend.query_mapping_terms(
        &uri,
        &name,
        &reqdata.genes,
        &include_parents,
        &include_root,
        &parents_as_graph,
    );
    if g.is_ok() {
        ret = g.unwrap();
    }
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Serialize, Deserialize)]
struct ReqGetSpMat {
    genes: Vec<String>,
    include_parents: Option<bool>, // Traverse graph up to include all parent terms
    include_root: Option<bool>,    // Include root terms
}
#[post("/{uri}/{name}/sparse-mat")]
async fn get_spmat(
    appdata: web::Data<AppData>,
    reqdata: web::Json<ReqGetSpMat>,
    web::Path((uri, name)): web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let include_parents = match reqdata.include_parents {
        Some(e) => e,
        None => false,
    };
    let include_root = match reqdata.include_root {
        Some(e) => e,
        None => false,
    };
    let g =
        appdata
            .backend
            .query_sparsemat(&uri, &name, &reqdata.genes, include_parents, include_root);
    if g.is_ok() {
        Ok(HttpResponse::Ok().json(g.unwrap()))
    } else {
        Ok(HttpResponse::Ok().json(Vec::<u8>::new())) // FIXME: handle this
    }
}

/*
  Handle requests for `term-info`. User sends an array of terms to a given target
  and the server replies with any found annotation details for the terms
*/
#[derive(Serialize, Deserialize)]
struct ReqGetTermInfo {
    terms: Vec<String>,
}

#[post("/annotation/{name}/info")]
async fn annotation_info(
    appdata: web::Data<AppData>,
    reqdata: web::Json<ReqGetTermInfo>,
    web::Path(name): web::Path<String>,
) -> Result<HttpResponse> {
    let mut ret: HashMap<String, GenericTerm> = HashMap::new();
    for term in &reqdata.terms {
        let search = appdata.backend.query_annotation_info(&name, &term);
        if search.is_ok() {
            match search.unwrap() {
                Some(t) => {
                    ret.insert((&term).to_string(), t);
                }
                None => (),
            };
        }
    }
    Ok(HttpResponse::Ok().json(ret))
}

#[derive(Serialize, Deserialize)]
struct ReqGetSemSim {
    genes: Vec<String>,
    mergestrategy: String,
    namespace: Option<String>,
}
#[post("/{uri}/{name}/semantic-similarity/gene-pair")]
async fn get_semsim(
    appdata: web::Data<AppData>,
    reqdata: web::Json<ReqGetSemSim>,
    web::Path((uri, name)): web::Path<(String, String)>,
) -> Result<HttpResponse> {
    let g = appdata.backend.query_semsim_genes(
        &uri,
        &name,
        &reqdata.genes,
        &reqdata.mergestrategy,
        &reqdata.namespace,
    );
    Ok(HttpResponse::Ok().json(g.unwrap()))
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();

    let c = backend::Backend::new(String::from("example/example-conf.json")).unwrap();
    let data = web::Data::new(AppData { backend: c });

    HttpServer::new(move || {
        let json_config = web::JsonConfig::default().limit(100000000);
        App::new()
            .app_data(data.clone())
            .app_data(json_config)
            .service(get_terms)
            .service(annotation_info)
            .service(get_spmat)
            .service(get_semsim)
    })
    .bind("127.0.0.1:8088")?
    .run()
    .await
}

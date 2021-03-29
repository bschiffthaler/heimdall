use crate::utils::common::make_hashset;
use crate::utils::common::tslist_to_vec;
use crate::utils::common::vec_to_tslist;
use crate::utils::common::Edges;
use crate::utils::common::MapOptions;
use crate::utils::common::SpMatTriplet;
use crate::utils::flatfile::Flatfile;
use crate::utils::mapping::Mapping;
use crate::utils::obo::GoRelationship;
use crate::utils::obo::Obo;
use crate::utils::rd;
use crate::utils::semanticsimilarity::GoUniversal;
use crate::utils::traits::Annotation as AnnotationTrait;
use crate::utils::traits::SemanticSimilarity;
use crate::GenericTerm;
use ahash::RandomState as ARandomState;
use log::{debug, error, info, trace};
use ndarray::Array1;
use ndarray::Array2;
use petgraph::prelude::DiGraphMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::Error as ioError;
use std::io::ErrorKind;
use std::iter::FromIterator;
use std::path::Path;

use crate::utils::sparsematrix::*;
use crate::utils::sql;

use redis::Commands;

use lazy_static::lazy_static;

#[derive(Deserialize, Debug)]
pub struct Enrichment {
    name: Option<String>,
    test: Option<Vec<String>>,
    annotation: Option<String>,
    mapping: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Background {
    name: Option<String>,
    sourcefile: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Org {
    name: Option<String>,
    uri: Option<String>,
    enrichment: Option<Vec<Enrichment>>,
    background: Option<Vec<Background>>,
}

#[derive(Deserialize, Debug)]
pub struct Annotation {
    name: Option<String>,
    sourcefile: Option<String>,
    r#type: Option<String>,
    parent: Option<Vec<String>>,
    go_universal_parent: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
pub struct ConfigRaw {
    port: Option<u16>,
    ip: Option<String>,
    redis_host: Option<String>,
    redis_port: Option<u16>,
    redis_db: Option<i64>,
    redis_password: Option<String>,
    redis_user: Option<String>,
    db_host: Option<String>,
    db_user: Option<String>,
    db_password: Option<String>,
    db_name: Option<String>,
    db_port: Option<u16>,
    org: Option<Vec<Org>>,
    annotation: Option<Vec<Annotation>>,
    _is_valid: Option<bool>,
}

enum AnnotationType {
    OBO,
    FLAT,
    NONE,
}

struct AnnotationIndex {
    target: AnnotationType,
    index: usize,
}

struct MappingIndex {
    index: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetGetTerms {
    terms: Vec<String>,
    parents: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetGetSpMat {
    sp_mat: SpMatTriplet,
    row_names: Vec<String>,
    col_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RetGetSemSim {
    similarity: Array1<f64>,
    row_names: Vec<String>,
    col_names: Vec<String>,
}

pub struct Backend {
    json: ConfigRaw,
    annotation: HashMap<String, AnnotationIndex>,
    mapping: HashMap<String, MappingIndex>,
    redis_cli: Option<rd::Client>,
    sql_cli: Option<sql::Client>,
    obo_data: Vec<Obo>,
    flat_data: Vec<Flatfile>,
    map_data: Vec<Mapping>,
}

#[derive(Debug)]
pub struct Redis<'a> {
    pub port: u16,
    pub host: &'a String,
    pub db: &'a i64,
    pub password: Option<String>,
    pub user: Option<String>,
}

#[derive(Debug)]
pub struct MySQL {
    pub port: u16,
    pub host: Option<String>,
    pub db: Option<String>,
    pub password: Option<String>,
    pub user: Option<String>,
}

fn is_valid_name(name: &String) -> bool {
    // Compile regex only once and use static ref subsequently
    lazy_static! {
        static ref RE: Regex = Regex::new(r"^([a-z]|[0-9]|[._-])+$").unwrap();
    }
    RE.is_match(name)
}

impl Backend {
    pub fn new(file: String) -> Result<Backend, Box<dyn Error>> {
        let mut f = File::open(file)?;
        let mut json_raw = String::new();
        f.read_to_string(&mut json_raw)?;
        let v: ConfigRaw = serde_json::from_str(&json_raw)?;
        let mut prelim_config = Backend {
            json: v,
            annotation: HashMap::new(),
            mapping: HashMap::new(),
            sql_cli: None,
            redis_cli: None,
            obo_data: Vec::new(),
            flat_data: Vec::new(),
            map_data: Vec::new(),
        };

        prelim_config.validate()?;

        let redis_cli: Option<rd::Client> = match rd::Client::new(&prelim_config) {
            Ok(x) => Some(x),
            Err(e) => {
                error!(
                    "Redis error: {:?}. Consider setting up Redis for increased performance",
                    e
                );
                None
            }
        };
        let sql_cli: Option<sql::Client> = match sql::Client::new(&prelim_config) {
            Ok(x) => Some(x),
            Err(e) => {
                error!(
                    "SQL error {:?}. Consider setting up MySQL for increased efficiency.",
                    e
                );
                None
            }
        };

        prelim_config.sql_cli = sql_cli;
        prelim_config.redis_cli = redis_cli;

        // Load annotation data
        for (_i, ann) in prelim_config
            .json
            .annotation
            .as_ref()
            .unwrap()
            .iter()
            .enumerate()
        {
            if ann.r#type.as_ref().unwrap() == "go" {
                let key = format!("annotation_{}", ann.name.as_ref().unwrap());
                prelim_config.obo_data.push(Obo::new(
                    ann.sourcefile.as_ref().unwrap().clone(),
                    MapOptions {
                        parent_terms: match &ann.parent {
                            Some(v) => Some(HashSet::<String, ARandomState>::from_iter(
                                v.iter().cloned(),
                            )),
                            None => None,
                        },
                        gou_parent_terms: match &ann.go_universal_parent {
                            Some(v) => Some(HashSet::<String, ARandomState>::from_iter(
                                v.iter().cloned(),
                            )),
                            None => None,
                        },
                        db_prefix: key.clone(),
                    },
                    prelim_config.sql_cli.as_ref(),
                    prelim_config.redis_cli.as_ref(),
                )?);
                prelim_config.annotation.insert(
                    key,
                    AnnotationIndex {
                        target: AnnotationType::OBO,
                        index: prelim_config.obo_data.len() - 1,
                    },
                );
            } else if ann.r#type.as_ref().unwrap() == "flat" {
                let key = format!("annotation_{}", ann.name.as_ref().unwrap());
                prelim_config.flat_data.push(Flatfile::new(
                    ann.sourcefile.as_ref().unwrap().clone(),
                    MapOptions {
                        parent_terms: None,
                        gou_parent_terms: None,
                        db_prefix: key.clone(),
                    },
                    prelim_config.sql_cli.as_ref(),
                )?);
                prelim_config.annotation.insert(
                    key,
                    AnnotationIndex {
                        target: AnnotationType::FLAT,
                        index: prelim_config.flat_data.len() - 1,
                    },
                );
            }
        }

        for (_i, org) in prelim_config.json.org.as_ref().unwrap().iter().enumerate() {
            let uri = &org.uri.as_ref().unwrap().clone();
            for (_j, enr) in org.enrichment.as_ref().unwrap().iter().enumerate() {
                let key = format!("org_{}_{}", uri, &enr.name.as_ref().unwrap());
                prelim_config.map_data.push(Mapping::new(
                    enr.mapping.as_ref().unwrap().clone(),
                    MapOptions {
                        parent_terms: None,
                        gou_parent_terms: None,
                        db_prefix: key.clone(),
                    },
                    prelim_config.sql_cli.as_ref(),
                )?);
                prelim_config.mapping.insert(
                    key,
                    MappingIndex {
                        index: prelim_config.map_data.len() - 1,
                    },
                );
            }
        }

        if prelim_config.redis_cli.is_some() {
            debug!("Caching sparse matrices for all mappings");
            let cli = prelim_config.redis_cli.as_ref().unwrap();
            let mut con = cli.con()?;
            for (_i, org) in prelim_config.json.org.as_ref().unwrap().iter().enumerate() {
                let uri = &org.uri.as_ref().unwrap().clone();
                for (_j, enr) in org.enrichment.as_ref().unwrap().iter().enumerate() {
                    let name = &enr.name.as_ref().unwrap();
                    let key = format!("org_{}_{}_spmat", &uri, &name);

                    if con.exists(&key)? {
                        debug!("Have mat in Redis. Nothing to do");
                        continue;
                    }

                    debug!("Caching sparse matrix for {}/{}", &uri, &name);
                    let now = std::time::Instant::now();
                    let annot = prelim_config.get_annotation_for_mapping(&uri, &name)?;
                    let mapping = prelim_config.get_mapping(&uri, &name)?;
                    let genes: Vec<String> = mapping
                        .gene_to_term
                        .iter()
                        .map(|(k, _v)| k)
                        .cloned()
                        .collect();
                    let tripl = prep_mat_triplet(
                        annot,
                        mapping,
                        &genes,
                        prelim_config.redis_cli.as_ref(),
                        true,
                        true,
                    )?;

                    let rows: String = vec_to_tslist(&tripl.row_idx);
                    let cols: String = vec_to_tslist(&tripl.col_idx);

                    con.set(format!("{}_rows", &key), &rows)?;
                    con.set(format!("{}_cols", &key), &cols)?;
                    con.set(&key, true)?;

                    debug!("Took {} seconds", now.elapsed().as_secs());
                }
            }
        }

        Ok(prelim_config)
    }

    pub fn get_redis(&self) -> Redis {
        match self.json._is_valid {
            Some(x) => {
                if x == false {
                    panic!("JSON config is not valid");
                }
            }
            None => {
                panic!("JSON config was not validated");
            }
        }
        let ret = Redis {
            port: self.json.redis_port.unwrap(),
            host: self.json.redis_host.as_ref().unwrap(),
            db: self.json.redis_db.as_ref().unwrap(),
            password: self.json.redis_password.clone(),
            user: self.json.redis_user.clone(),
        };
        ret
    }

    pub fn get_mysql(&self) -> MySQL {
        match self.json._is_valid {
            Some(x) => {
                if x == false {
                    panic!("JSON config is not valid");
                }
            }
            None => {
                panic!("JSON config was not validated");
            }
        }

        let ret = MySQL {
            port: self.json.db_port.unwrap(),
            host: self.json.db_host.clone(),
            db: self.json.db_name.clone(),
            user: self.json.db_user.clone(),
            password: self.json.db_password.clone(),
        };
        ret
    }

    pub fn query_mapping_terms(
        &self,
        uri: &String,
        name: &String,
        genes: &Vec<String>,
        include_parents: &bool,
        include_root: &bool,
        parents_as_graph: &bool,
    ) -> Result<HashMap<String, Option<RetGetTerms>>, Box<dyn Error>> {
        let mut ret: HashMap<String, Option<RetGetTerms>> = HashMap::new();
        let annot = self.get_annotation_for_mapping(uri, name)?;
        let graph = annot.graph(self.redis_cli.as_ref(), self.sql_cli.as_ref())?;

        for gene in genes {
            let res = self.query_mapping_term(
                uri,
                name,
                gene,
                include_parents,
                include_root,
                parents_as_graph,
                annot,
                &graph,
            )?;
            ret.insert(gene.clone(), Some(res));
        }

        Ok(ret)
    }

    fn query_mapping_term(
        &self,
        uri: &String,
        name: &String,
        gene: &String,
        include_parents: &bool,
        include_root: &bool,
        parents_as_graph: &bool,
        annot: &dyn AnnotationTrait,
        graph: &DiGraphMap<u32, i8>,
    ) -> Result<RetGetTerms, Box<dyn Error>> {
        let db_prefix = format!("org_{}_{}", uri, name);
        trace!("{}/get-terms/{} -> {}", uri, name, gene);
        let redis_cli = &self.redis_cli.as_ref();

        let hits: Vec<String> = match self.mapping.get(&db_prefix) {
            // Check if we can get the mapping data for the URI + name
            Some(i) => {
                let set = self.map_data[i.index].get_terms(gene)?;
                // Check if the node returned any annotations
                match set {
                    Some(ret) => Vec::from_iter(ret),
                    None => Vec::new(),
                }
            }
            None => {
                error!("Could not find a mapping for {}", &db_prefix);
                Vec::new()
            }
        };

        if *include_parents {
            if !annot.supports_hierarchical() {
                return Ok(RetGetTerms {
                    terms: hits,
                    parents: None,
                });
            }

            let mut parents: HashMap<String, Vec<String>> = HashMap::new();

            match &self.redis_cli {
                Some(client) => {
                    let key = format!("{}_p_{}", db_prefix, gene);
                    let mut con = client.con()?;
                    if con.exists(&key)? {
                        let rd_res: HashMap<String, String> = con.hgetall(&key)?;
                        for (k, v) in rd_res.iter() {
                            let v_vec: Vec<u32> = tslist_to_vec(&v);
                            let edges = &annot.process_edges(
                                &Edges {
                                    graph: v_vec,
                                    weights: None,
                                    is_graph: true,
                                },
                                &include_root,
                                &false,
                                &parents_as_graph,
                            )?;
                            parents.insert(k.clone(), annot.index_to_id_vunsafe(&edges.graph)?);
                        }
                    } else {
                        for hit in &hits {
                            let p = annot.parents(&hit, true, true, false, graph, *redis_cli)?;
                            match p {
                                Some(_p) => {
                                    // Serialize full graph into redis
                                    let x: String = vec_to_tslist(&_p.graph);
                                    con.hset(&key, hit, x)?;
                                    // Now process edges accoring to user's req
                                    let px = annot.process_edges(
                                        &_p,
                                        &include_root,
                                        &false,
                                        &parents_as_graph,
                                    )?;
                                    parents.insert(
                                        hit.to_string(),
                                        annot.index_to_id_vunsafe(&px.graph)?,
                                    );
                                }
                                None => {
                                    con.hset(&key, hit, "".to_string())?;
                                    parents.insert(hit.to_string(), Vec::new());
                                }
                            };
                        }
                    }
                    // If we had redis, we finish, as all data is there
                    return Ok(RetGetTerms {
                        terms: hits,
                        parents: Some(parents),
                    });
                }
                None => (),
            };

            for hit in &hits {
                let p = annot.parents(
                    &hit,
                    *parents_as_graph,
                    *include_root,
                    false,
                    graph,
                    *redis_cli,
                )?;
                match p {
                    Some(_p) => {
                        parents.insert(hit.to_string(), annot.index_to_id_vunsafe(&_p.graph)?)
                    }
                    None => parents.insert(hit.to_string(), Vec::new()),
                };
            }
            return Ok(RetGetTerms {
                terms: hits,
                parents: Some(parents),
            });
        }

        Ok(RetGetTerms {
            terms: hits,
            parents: None,
        })
    }

    pub fn query_annotation_info(
        &self,
        name: &String,
        term: &String,
    ) -> Result<Option<GenericTerm>, Box<dyn Error>> {
        trace!("/annotation/{}/info -> {}", name, term);
        let annot = self.get_annotation(&name)?;
        let hit = annot.get_term_def(&term, self.sql_cli.as_ref(), self.redis_cli.as_ref())?;
        Ok(hit)
    }

    pub fn query_sparsemat(
        &self,
        uri: &String,
        name: &String,
        genes: &Vec<String>,
        include_parents: bool,
        include_root: bool,
    ) -> Result<RetGetSpMat, Box<dyn Error>> {
        let annot = self.get_annotation_for_mapping(uri, name)?;
        let mapping = self.get_mapping(uri, name)?;
        let tripl = prep_mat_triplet(
            annot,
            mapping,
            genes,
            self.redis_cli.as_ref(),
            include_parents,
            include_root,
        )?;

        let mut rn: Vec<String> = Vec::with_capacity(mapping.n_genes());
        let mut cn: Vec<String> = Vec::with_capacity(annot.n_terms());

        for i in 0..annot.n_terms() {
            cn.push(annot.index_to_id(&(i as u32))?.unwrap());
        }

        for i in 0..mapping.n_genes() {
            rn.push(mapping.index_to_id.get(&(i as u32)).unwrap().clone());
        }

        Ok(RetGetSpMat {
            sp_mat: tripl,
            row_names: rn,
            col_names: cn,
        })
    }

    pub fn query_semsim_genes(
        &self,
        uri: &String,
        name: &String,
        genes: &Vec<String>,
        strategy: &String,
        namespace: &Option<String>,
    ) -> Result<RetGetSemSim, Box<dyn Error>> {
        let annot = self.get_annotation_for_mapping(uri, name)?;
        let mapping = self.get_mapping(uri, name)?;

        let mut genes_filt: Vec<String> = Vec::with_capacity(genes.len());

        for gene in genes {
            if mapping.get_terms(gene)?.is_some() {
                genes_filt.push(gene.clone());
            }
        }

        let mut gou_rel: HashSet<GoRelationship, ARandomState> = make_hashset();
        // Always considered an edge
        gou_rel.insert(GoRelationship::IsA);

        let go_uni = GoUniversal {
            allowed_rel: gou_rel,
        };

        let sims = go_uni.genes_groupwise_sym(
            &genes_filt,
            Box::new(annot),
            mapping,
            strategy,
            self.redis_cli.as_ref(),
            namespace,
        )?;

        Ok(RetGetSemSim {
            similarity: sims.unwrap_or(Array1::<f64>::zeros(0)),
            row_names: genes_filt.clone(),
            col_names: genes_filt.clone(),
        })
    }

    /*
        Given the identifiers of an enrichment (uri and name), get a reference
        to the corresponding annotation.
    */
    fn get_annotation_for_mapping(
        &self,
        uri: &String,
        name: &String,
    ) -> Result<&dyn AnnotationTrait, Box<dyn Error>> {
        for org in self.json.org.as_ref().unwrap().iter() {
            if org.uri.as_ref().unwrap() == uri {
                for enr in org.enrichment.as_ref().unwrap().iter() {
                    if enr.name.as_ref().unwrap() == name {
                        let target_name = enr.annotation.as_ref().unwrap().to_string();
                        let key = format!("annotation_{}", target_name);
                        let index = self.annotation.get(&key).unwrap_or(&AnnotationIndex {
                            index: 0,
                            target: AnnotationType::NONE,
                        }); // FIXME: unwrapor
                        match index.target {
                            AnnotationType::OBO => {
                                return Ok(&self.obo_data[index.index]);
                            }
                            AnnotationType::FLAT => {
                                return Ok(&self.flat_data[index.index]);
                            }
                            _ => (),
                        }
                    }
                }
            }
        }
        Err(Box::new(ioError::new(
            ErrorKind::Other,
            format!("Could not find an annotation for {}/{}", uri, name),
        )))
    }

    fn get_annotation(&self, name: &String) -> Result<&dyn AnnotationTrait, Box<dyn Error>> {
        let key = format!("annotation_{}", &name);
        let index = self.annotation.get(&key).unwrap_or(&AnnotationIndex {
            index: 0,
            target: AnnotationType::NONE,
        }); // FIXME: unwrapor
        match index.target {
            AnnotationType::OBO => {
                return Ok(&self.obo_data[index.index]);
            }
            AnnotationType::FLAT => {
                return Ok(&self.flat_data[index.index]);
            }
            _ => (),
        }
        Err(Box::new(ioError::new(
            ErrorKind::Other,
            format!("Could not find an annotation for {}", name),
        )))
    }

    fn get_mapping(&self, uri: &String, name: &String) -> Result<&Mapping, Box<dyn Error>> {
        let key = format!("org_{}_{}", &uri, &name);
        let index = match self.mapping.get(&key) {
            Some(x) => x,
            None => {
                return Err(Box::new(ioError::new(
                    ErrorKind::Other,
                    format!("Could not find an mapping for {}/{}", uri, name),
                )));
            }
        };
        Ok(&self.map_data[index.index])
    }

    pub fn validate(&mut self) -> Result<bool, Box<dyn Error>> {
        info!("Validating JSON config...");
        if self.json.port.is_none() {
            trace!("Setting port to 5432");
            self.json.port = Some(5432);
        }
        if self.json.ip.is_none() {
            trace!("Setting IP to 127.0.0.1");
            self.json.ip = Some(String::from("127.0.0.1"));
        }
        if self.json.redis_port.is_none() {
            trace!("Setting redis port to 6379");
            self.json.redis_port = Some(6379);
        }
        if self.json.redis_host.is_none() {
            trace!("Setting Redis IP to 127.0.0.1");
            self.json.redis_host = Some(String::from("127.0.0.1"));
        }
        if self.json.redis_db.is_none() {
            trace!("Setting Redis DB to '0'");
            self.json.redis_db = Some(0);
        }
        if self.json.db_host.is_none() {
            trace!("Setting MySQL host to '127.0.0.1'");
            self.json.db_host = Some(String::from("127.0.0.1"));
        }
        if self.json.db_user.is_none() {
            trace!("Setting MySQL user to 'root'");
            self.json.db_user = Some(String::from("root"));
        }
        if self.json.db_name.is_none() {
            trace!("Setting MySQL DB name to 'gofer'");
            self.json.db_name = Some(String::from("gofer"));
        }
        if self.json.db_port.is_none() {
            trace!("Setting MySQL port to '3306'");
            self.json.db_port = Some(3306);
        }
        assert!(
            self.json.org.is_some(),
            "Error in JSON config: At least one 'org' must be defined"
        );
        assert!(
            self.json.annotation.is_some(),
            "Error in JSON config: At least one 'annotation' must be defined"
        );

        let mut ann_names: HashSet<&String> = HashSet::new();

        for (i, ann) in self.json.annotation.as_ref().unwrap().iter().enumerate() {
            assert!(
                ann.name.is_some(),
                "In annotation[{}]: 'name' is not set.",
                i
            );
            assert!(
                is_valid_name(ann.name.as_ref().unwrap()),
                "In annotation[{}]: 'name' is invalid. Allowed are lower case \
                alphanumeric and the characters '_', '-' and '.'. \
                REGEX: ^([a-z]|[0-9]|[._-])+$",
                i
            );
            assert!(
                ann.sourcefile.is_some(),
                "In annotation[{}]: 'sourcefile' is not set.",
                i
            );
            assert!(
                ann.r#type.is_some(),
                "In annotation[{}]: 'type' is not set.",
                i
            );
            let sourcefile = Path::new(ann.sourcefile.as_ref().unwrap());
            assert!(
                sourcefile.exists(),
                "In annotation[{}]: '{}' does not exist",
                i,
                sourcefile.display()
            );
            match ann.r#type.as_ref().unwrap().as_str() {
                "go" => (),
                "flat" => (),
                "hierarchical" => (),
                _ => {
                    panic!(
                        "In annotation[{}]: Only types 'go', 'flat' and \
                           'hierarchical' are supported.",
                        i
                    );
                }
            }
            if ann.r#type.as_ref().unwrap().as_str() == "go" {
                assert!(
                    ann.parent.is_some(),
                    "In annotation[{}]: 'parent' is not set.",
                    i
                );
                for (j, parent) in ann.parent.as_ref().unwrap().iter().enumerate() {
                    match parent.as_str() {
                        "part_of" => (),
                        "regulates" => (),
                        "negatively_regulates" => (),
                        "positively_regulates" => (),
                        "occurs_in" => (),
                        "all" => (),
                        _ => {
                            panic!(
                                "In annotation[{}]parent[{}]: The \
                                relationship '{}' is not defined. Please see \
                                the documentation",
                                i, j, parent
                            );
                        }
                    }
                }
            }
            let ann_name = ann.name.as_ref().unwrap();
            assert!(
                !ann_names.contains(&ann_name),
                "In annotation[{}]: annotation names must be unique.",
                i
            );
            ann_names.insert(&ann_name);
        }
        for (i, org) in self.json.org.as_ref().unwrap().iter().enumerate() {
            assert!(org.name.is_some(), "In org[{}]: 'name' is not set.", i);
            assert!(org.uri.is_some(), "In org[{}]: 'uri' is not set.", i);
            assert!(
                is_valid_name(org.uri.as_ref().unwrap()),
                "In org[{}]: 'uri' is invalid. Allowed are lower case \
                alphanumeric and the characters '_', '-' and '.'. \
                REGEX: ^([a-z]|[0-9]|[._-])+$",
                i
            );
            assert!(
                org.enrichment.is_some(),
                "In org[{}]: At least one 'enrichment' must be defined.",
                i
            );
            // Check common mistakes related to enrichment definitions
            let mut enr_names: HashSet<&String> = HashSet::new();
            for (j, enr) in org.enrichment.as_ref().unwrap().iter().enumerate() {
                assert!(
                    enr.name.is_some(),
                    "In org[{}]enrichment[{}]: 'name' is not set.",
                    i,
                    j
                );
                assert!(
                    is_valid_name(enr.name.as_ref().unwrap()),
                    "In org[{}]enrichment[{}]: 'name' is invalid. Allowed are \
                    lower case alphanumeric and the characters '_', '-' and '.'. \
                    REGEX: ^([a-z]|[0-9]|[._-])+$",
                    i,
                    j
                );
                assert!(
                    enr.test.is_some(),
                    "In org[{}]enrichment[{}]: 'test' is not set.",
                    i,
                    j
                );
                assert!(
                    enr.annotation.is_some(),
                    "In org[{}]enrichment[{}]: 'annotation' is not set.",
                    i,
                    j
                );
                assert!(
                    enr.mapping.is_some(),
                    "In org[{}]enrichment[{}]: 'mapping' is not set.",
                    i,
                    j
                );
                let ann_name = enr.annotation.as_ref().unwrap();
                assert!(
                    ann_names.contains(&ann_name),
                    "In org[{}]enrichment[{}]: No annotation '{}' is defined.",
                    i,
                    j,
                    ann_name
                );
                let mapping = Path::new(enr.mapping.as_ref().unwrap());
                assert!(
                    mapping.exists(),
                    "In org[{}]enrichment[{}]: mapping '{}' does not exist.",
                    i,
                    j,
                    mapping.display()
                );
                for (k, test) in enr.test.as_ref().unwrap().iter().enumerate() {
                    match test.as_str() {
                        "fisher" => (),
                        "parent_child" => (),
                        _ => {
                            panic!(
                                "In org[{}]enrichment[{}]test[{}]:\nOnly tests 'fisher' and 'parent_child' are implemented",
                                i, j, k
                            );
                        }
                    }
                }
                let enr_name = enr.name.as_ref().unwrap();
                assert!(
                    !enr_names.contains(&enr_name),
                    "In org[{}]enrichment[{}]: names of enrichments must be unique",
                    i,
                    j
                );
                enr_names.insert(&enr_name);
            }
            // Check common mistakes related to backround definitions
            let mut bg_names: HashSet<&String> = HashSet::new();
            if org.background.is_some() {
                for (j, background) in org.background.as_ref().unwrap().iter().enumerate() {
                    assert!(
                        background.name.is_some(),
                        "In org[{}]background[{}]: 'name' is not defined",
                        i,
                        j
                    );
                    assert!(
                        background.sourcefile.is_some(),
                        "In org[{}]background[{}]: 'sourcefile' is not defined",
                        i,
                        j
                    );
                    let sourcefile = Path::new(background.sourcefile.as_ref().unwrap());
                    assert!(
                        sourcefile.exists(),
                        "In  org[{}]background[{}]: sourcefile {} does not exist",
                        i,
                        j,
                        sourcefile.display()
                    );
                    let bg_name = background.name.as_ref().unwrap();
                    assert!(
                        !bg_names.contains(&bg_name),
                        "In org[{}]background[{}]: names of backgrounds must be unique.",
                        i,
                        j
                    );
                    bg_names.insert(&bg_name);
                }
            }
        }
        info!("Config is valid");
        self.json._is_valid = Some(true);
        Ok(true)
    }
}

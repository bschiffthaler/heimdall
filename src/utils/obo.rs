use crate::utils::common::enum_children;
use crate::utils::common::enum_parents;
use crate::utils::common::hash_file;
use crate::utils::common::make_hashmap;
use crate::utils::common::make_hashset;
use crate::utils::common::tslist_to_vec;
use crate::utils::common::vec_to_tslist;
use crate::utils::common::Edges;
use crate::utils::common::MapOptions;
use crate::utils::rd;
use crate::utils::semanticsimilarity::GoUniversal;
use crate::utils::sql;
use crate::utils::traits::Annotation;
use crate::utils::traits::FromFile;
use crate::utils::traits::SemanticSimilarity;
use crate::GenericTerm;
use ahash::RandomState as ARandomState;
use lazy_static::lazy_static;
use log::{debug, info, trace, warn};
use mysql::params;
use mysql::prelude::*;
use petgraph::algo::connected_components;
use petgraph::graphmap::DiGraphMap;
use redis::Commands;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::Error as ioError;
use std::io::ErrorKind;
use std::iter::FromIterator;
use std::iter::Iterator;
use std::str::FromStr;

/*
  Static references to important structures.
  ------------------------------------------
  GO_ROOT -> Used to check if a term is a root term
  GO_ROOT_IDS -> Used to check which root the term is
*/

lazy_static! {
    static ref GO_ROOT: HashSet<String, ARandomState> = HashSet::from_iter(
        [
            "GO:0000004".to_string(),
            "GO:0003674".to_string(),
            "GO:0005554".to_string(),
            "GO:0005575".to_string(),
            "GO:0007582".to_string(),
            "GO:0008150".to_string(),
            "GO:0008372".to_string(),
            "GO:0044699".to_string(),
        ]
        .iter()
        .cloned(),
    );
}

lazy_static! {
    static ref GO_ROOT_CATS: HashMap<String, GoNamespace, ARandomState> = HashMap::from_iter(
        [
            ("GO:0000004".to_string(), GoNamespace::Bp),
            ("GO:0003674".to_string(), GoNamespace::Mf),
            ("GO:0005554".to_string(), GoNamespace::Mf),
            ("GO:0005575".to_string(), GoNamespace::Cc),
            ("GO:0007582".to_string(), GoNamespace::Bp),
            ("GO:0008150".to_string(), GoNamespace::Bp),
            ("GO:0008372".to_string(), GoNamespace::Cc),
            ("GO:0044699".to_string(), GoNamespace::Bp),
        ]
        .iter()
        .cloned(),
    );
}

/*
  Struct definitions.
  -------------------
*/

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Term {
    id: String,
    name: String,
    description: String,
    namespace: GoNamespace,
    obsolete: bool,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum GoRelationship {
    IsA = 1,
    PartOf = 2,
    Regulates = 3,
    PositivelyRegulates = 4,
    NegativelyRegulates = 5,
    NoRel = -1,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum GoNamespace {
    Bp = 1,
    Cc = 2,
    Mf = 3,
    NoCat = -1,
}

pub struct Obo {
    file: String,
    db_prefix: String,
    obsolete: HashSet<String, ARandomState>,
    alt_id: HashMap<String, String, ARandomState>,
    term_map: HashMap<String, Term, ARandomState>,
    id_to_index: HashMap<String, u32, ARandomState>,
    index_to_id: HashMap<u32, String, ARandomState>,
    graph: DiGraphMap<u32, i8>,
    hash: u64,
    opts: MapOptions,
    root_ids: HashSet<u32, ARandomState>,
    go_universal: GoUniversal,
}

struct GoNamespaceVisitor;

/*
  Parser and Ser/De impls for GO Namespaces
  -----------------------------------------
  These let us represent the namespaces easily as an enum, but interact
  with code outside this scope via their shorthands: CC, MF, BP
*/

impl std::fmt::Display for GoNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            GoNamespace::Bp => write!(f, "BP"),
            GoNamespace::Cc => write!(f, "CC"),
            GoNamespace::Mf => write!(f, "MF"),
            GoNamespace::NoCat => write!(f, "--"),
        }
    }
}

// This is a strict parser that only allows for case variations of the
// shorthands. The serde parser allows for more leeway
impl FromStr for GoNamespace {
    type Err = ioError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "bp" => Ok(GoNamespace::Bp),
            "cc" => Ok(GoNamespace::Cc),
            "mf" => Ok(GoNamespace::Mf),
            "--" => Ok(GoNamespace::NoCat),
            _ => Err(ioError::new(
                ErrorKind::Other,
                format!("Can't parse {:?} to GO namespace", s),
            )),
        }
    }
}

impl Serialize for GoNamespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            GoNamespace::Bp => serializer.serialize_str("BP"),
            GoNamespace::Cc => serializer.serialize_str("CC"),
            GoNamespace::Mf => serializer.serialize_str("MF"),
            GoNamespace::NoCat => serializer.serialize_str("--"),
        }
    }
}

impl<'de> Visitor<'de> for GoNamespaceVisitor {
    type Value = GoNamespace;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("A GO namespace")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let upcast = String::from(value);
        let cat = guess_go_namespace(&upcast);
        match cat {
            GoNamespace::Bp => Ok(cat),
            GoNamespace::Cc => Ok(cat),
            GoNamespace::Mf => Ok(cat),
            GoNamespace::NoCat => Err(E::custom(format!("No such GO namespace {}", value))),
        }
    }
}

impl<'de> Deserialize<'de> for GoNamespace {
    fn deserialize<D>(deserializer: D) -> Result<GoNamespace, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(GoNamespaceVisitor)
    }
}

/*
  Helper functions for various tasks
*/

pub fn to_go_relationship(i: &i8) -> GoRelationship {
    match i {
        1 => GoRelationship::IsA,
        2 => GoRelationship::PartOf,
        3 => GoRelationship::Regulates,
        4 => GoRelationship::PositivelyRegulates,
        5 => GoRelationship::NegativelyRegulates,
        _ => GoRelationship::NoRel,
    }
}

// Used in obo file parser to match a textual annotation of the relationship to
// the enum
fn determine_rel(key: &String) -> GoRelationship {
    match key.as_str() {
        "is_a:" => GoRelationship::IsA,
        "is_a" => GoRelationship::IsA,
        "part_of" => GoRelationship::PartOf,
        "regulates" => GoRelationship::Regulates,
        "positively_regulates" => GoRelationship::PositivelyRegulates,
        "negatively_regulates" => GoRelationship::NegativelyRegulates,
        _ => {
            warn!("Unknown relationship {}", key);
            GoRelationship::NoRel
        }
    }
}

// Used in several parsers as a lax conversion from textual namespace to the enum
pub fn guess_go_namespace(query: &String) -> GoNamespace {
    match query.to_lowercase().as_str() {
        "biological_process" => GoNamespace::Bp,
        "molecular_function" => GoNamespace::Mf,
        "cellular_component" => GoNamespace::Cc,
        "biological process" => GoNamespace::Bp,
        "molecular function" => GoNamespace::Mf,
        "cellular component" => GoNamespace::Cc,
        "bp" => GoNamespace::Bp,
        "mf" => GoNamespace::Mf,
        "cc" => GoNamespace::Cc,
        _ => GoNamespace::NoCat,
    }
}

/*
  Impls specific to Obo. Note that most impls should be in the Annotation trait,
  which it shares with other annotations.
*/
impl<'a> Obo {
    pub fn new(
        file: String,
        opts: MapOptions,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Obo, Box<dyn Error>> {
        let db_prefix = opts.db_prefix.clone();
        let mut o = Obo {
            file: file.clone(),
            db_prefix: opts.db_prefix.clone(),
            obsolete: make_hashset(),
            alt_id: make_hashmap(),
            term_map: make_hashmap(),
            id_to_index: make_hashmap(),
            index_to_id: make_hashmap(),
            graph: DiGraphMap::new(),
            root_ids: make_hashset(),
            hash: 0,
            opts: opts,
            go_universal: GoUniversal {
                allowed_rel: make_hashset(),
            },
        };
        let io_res = o.from_file(sql_client);
        match io_res {
            Err(msg) => panic!("I/O error for file {}: {}", file, msg),
            Ok(()) => (),
        };
        o.set_root_ids(sql_client, rd_client);
        info!("Caching true path for  {}", db_prefix);
        let now = std::time::Instant::now();
        let _unused = &o.cache_path(sql_client, rd_client);
        info!("Took {} seconds", now.elapsed().as_secs());

        info!("Caching IC values for {}", db_prefix);
        let now = std::time::Instant::now();

        let mut gou_rel: HashSet<GoRelationship, ARandomState> = make_hashset();
        match &o.opts().gou_parent_terms {
            Some(v) => {
                for rel in v {
                    gou_rel.insert(determine_rel(&rel));
                }
            }
            None => (),
        };
        // Always considered an edge
        gou_rel.insert(GoRelationship::IsA);

        let go_uni = GoUniversal {
            allowed_rel: gou_rel,
        };
        go_uni.cache_ics(Box::new(&o), rd_client)?;
        o.go_universal = go_uni;
        info!("Took {} mseconds", now.elapsed().as_millis());
        Ok(o)
    }

    fn get_term(
        &self,
        id: &String,
        _sql_client: Option<&sql::Client>,
        _rd_client: Option<&rd::Client>,
    ) -> Result<Option<Term>, Box<dyn Error>> {
        let _db_prefix = &self.db_prefix;
        let hit = self.term_map.get(&id.clone());
        match hit {
            Some(h) => Ok(Some(h.clone())),
            None => Ok(None),
        }
    }

    fn upcycle_alt_id(&self, id: &String) -> Result<Option<String>, Box<dyn Error>> {
        match self.alt_id.get(id) {
            Some(hit) => {
                trace!("upcycle_alt_id {:?} -> {:?}", id, hit);
                Ok(Some(hit.clone()))
            }
            None => Ok(Some(id.clone())),
        }
    }

    fn sql_serialize(&self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        if sql_client.is_none() {
            return Ok(());
        }
        let client = sql_client.unwrap();
        let db_prefix = &self.db_prefix;

        let q1 = format!(
            "CREATE TABLE IF NOT EXISTS {}_graph (\
            src INTEGER UNSIGNED, \
            tgt INTEGER UNSIGNED, \
            typ INTEGER SIGNED\
            );",
            db_prefix
        );
        let q2 = format!(
            "CREATE TABLE IF NOT EXISTS {}_annot (\
            id INTEGER UNSIGNED PRIMARY KEY NOT NULL, \
            term VARCHAR(2048), \
            name VARCHAR(2048), \
            description VARCHAR(16384), \
            namespace CHAR(2), \
            obsolete BOOLEAN \
            );",
            db_prefix
        );

        let q3 = format!(
            "CREATE TABLE IF NOT EXISTS {}_alt_id (\
            alt VARCHAR(2048), \
            id VARCHAR(2048)\
            );",
            db_prefix
        );

        let q4 = format!(
            "INSERT INTO {}_graph (src, tgt, typ) VALUES (:src, :tgt, :typ);",
            db_prefix
        );
        let q5 = format!(
            "INSERT INTO {}_annot (id, term, name, description, namespace, obsolete) \
            VALUES (:id, :term, :name, :description, :namespace, :obsolete);",
            db_prefix
        );

        let q6 = format!(
            "INSERT INTO {}_alt_id (alt, id) VALUES (:alt, :id)",
            db_prefix
        );

        let q7 = format!(
            "INSERT INTO tbl_hash (name, hash, version) VALUES ('{}', {}, NULL) \
            ON DUPLICATE KEY UPDATE name = '{}', hash = {}, version = NULL;",
            db_prefix, self.hash, db_prefix, self.hash
        );

        let q8 = format!(
            "CREATE INDEX {}_graph_isrc ON {}_graph (src)",
            db_prefix, db_prefix
        );
        let q9 = format!(
            "CREATE INDEX {}_graph_itgt ON {}_graph (tgt)",
            db_prefix, db_prefix
        );
        let q10 = format!(
            "CREATE INDEX {}_alt_id_alt ON {}_alt_id (alt)",
            db_prefix, db_prefix
        );
        let q11 = format!(
            "CREATE INDEX {}_annot_term ON {}_annot (term)",
            db_prefix, db_prefix
        );
        let q12 = format!(
            "CREATE INDEX {}_annot_id ON {}_annot (id)",
            db_prefix, db_prefix
        );

        let mut conn = client.get_conn()?;

        conn.query_drop("SET AUTOCOMMIT=0;")?;

        trace!("SQL: {:?}", &q1);
        conn.query_drop(&q1)?;

        trace!("SQL: {:?}", &q2);
        conn.query_drop(&q2)?;

        trace!("SQL: {:?}", &q3);
        conn.query_drop(&q3)?;

        trace!("SQL: {:?}", &q4);
        conn.exec_batch(
            &q4,
            self.graph.all_edges().map(|(s, t, e)| {
                params! {
                    "src" => s,
                    "tgt" => t,
                    "typ" => e,
                }
            }),
        )?;

        trace!("SQL: {:?}", &q5);
        conn.exec_batch(
            &q5,
            self.term_map.iter().map(|t| {
                let ns = guess_go_namespace(&t.1.namespace.to_string());
                params! {
                    "id" => self.id_to_index[&t.1.id],
                    "term" => &t.1.id,
                    "name" => &t.1.name,
                    "description" => &t.1.description,
                    "namespace" => &ns.to_string(),
                    "obsolete" => &t.1.obsolete,
                }
            }),
        )?;

        trace!("SQL: {:?}", &q6);
        conn.exec_batch(
            &q6,
            self.alt_id.iter().map(|(a, i)| {
                params! {
                    "alt" => a,
                    "id" => i,
                }
            }),
        )?;

        trace!("SQL: {:?}", &q8);
        conn.query_drop(&q8)?;

        trace!("SQL: {:?}", &q9);
        conn.query_drop(&q9)?;

        trace!("SQL: {:?}", &q10);
        conn.query_drop(&q10)?;

        trace!("SQL: {:?}", &q11);
        conn.query_drop(&q11)?;

        trace!("SQL: {:?}", &q12);
        conn.query_drop(&q12)?;

        trace!("SQL: {:?}", &q7);
        conn.query_drop(&q7)?;

        conn.query_drop("COMMIT; SET AUTOCOMMIT=1;")?;

        Ok(())
    }
    fn sql_check_hash_in_db(
        &self,
        sql_client: Option<&sql::Client>,
    ) -> Result<bool, Box<dyn Error>> {
        if sql_client.is_none() {
            return Ok(false);
        }
        let client = sql_client.unwrap();
        let mut conn = client.get_conn().unwrap();
        let s_res: Option<u64> =
            conn.exec_first("SELECT hash FROM tbl_hash WHERE name=?", (&self.db_prefix,))?;
        let res: bool = match s_res {
            Some(x) => x == self.hash,
            None => false,
        };
        Ok(res)
    }
    fn sql_drop(&self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        if sql_client.is_none() {
            return Ok(());
        }
        let client = sql_client.unwrap();
        let db_prefix = &self.db_prefix;
        let mut conn = client.get_conn()?;

        let q0 = format!("DROP TABLE IF EXISTS {}_graph;", db_prefix);
        let q1 = format!("DROP TABLE IF EXISTS {}_annot;", db_prefix);

        let q2 = format!("DROP TABLE IF EXISTS {}_alt_id;", db_prefix);

        trace!("SQL: {:?}", &q0);
        conn.query_drop(&q0)?;

        trace!("SQL: {:?}", &q1);
        conn.query_drop(&q1)?;

        trace!("SQL: {:?}", &q2);
        conn.query_drop(&q2)?;

        Ok(())
    }

    fn sql_unserialize(&mut self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        if sql_client.is_none() {
            return Ok(());
        }
        let client = sql_client.unwrap();
        let db_prefix = self.db_prefix.clone();

        let mut conn = client.get_conn()?;

        let q1 = format!(
            "SELECT id, term, name, description, namespace, obsolete FROM {}_annot;",
            db_prefix
        );
        let _r: Vec<Result<(), Box<dyn Error>>> = conn.query_map(
            &q1,
            |(id, term, name, description, namespace, obsolete): (
                u32,
                String,
                String,
                String,
                String,
                bool,
            )| {
                let ns = GoNamespace::from_str(namespace.as_str());
                let t = Term {
                    id: term.clone(),
                    name: name,
                    description: description,
                    namespace: ns.expect("Unexpected error parsing namespace from SQL"),
                    obsolete: obsolete,
                };
                let index = id;
                self.id_to_index.insert(term.clone(), index);
                self.index_to_id.insert(index, term.clone());
                self.term_map.insert(term.clone(), t);
                Ok(())
            },
        )?;

        let q2 = format!("SELECT src, tgt, typ FROM {}_graph;", db_prefix);
        let gr_res: Vec<(u32, u32, i8)> = conn.query_map(&q2, |(src, tgt, typ)| (src, tgt, typ))?;
        self.graph = DiGraphMap::from_edges(gr_res);

        let q3 = format!("SELECT alt, id FROM {}_alt_id;", db_prefix);
        let _r2: Vec<()> = conn.query_map(&q3, |(alt, id): (String, String)| {
            self.alt_id.insert(alt, id);
        })?;
        Ok(())
    }

    fn set_root_ids(&mut self, _sql_client: Option<&sql::Client>, _rd_client: Option<&rd::Client>) {
        for t in GO_ROOT.iter() {
            let id = self.id_to_index(t).unwrap();
            match id {
                Some(i) => {
                    self.root_ids.insert(i);
                }
                None => {}
            }
        }
    }
}

impl<'a> FromFile for Obo {
    fn from_file(&mut self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        info!("Parsing annotation {}", self.file);
        self.hash = hash_file(&self.file).unwrap();
        trace!("Hash for {:?} -> {:?}", &self.file, &self.hash);
        if self.sql_check_hash_in_db(sql_client)? {
            trace!("Have hash in SQL DB. Unserializing data from SQL");
            let now = std::time::Instant::now();
            self.sql_unserialize(sql_client)?;
            debug!("Unserializing took {} seconds", now.elapsed().as_secs());
            return Ok(());
        } else {
            trace!("No/different hash in SQL DB. Rebuilding");
            self.sql_drop(sql_client)?;
        }
        let f = File::open(self.file.clone())?;
        let reader = BufReader::new(f);
        let mut cur_term = Term {
            id: String::new(),
            name: String::new(),
            description: String::new(),
            namespace: GoNamespace::NoCat,
            obsolete: false,
        };
        let mut edges: Vec<(String, String)> = Vec::new();
        let mut edgtypes: Vec<i8> = Vec::new();
        let empty_hash: HashSet<String, ARandomState> = make_hashset();
        let mut allowed_rel = match &self.opts.parent_terms {
            Some(x) => x.clone(),
            None => empty_hash,
        };
        if allowed_rel.contains(&String::from("all")) {
            allowed_rel.insert(String::from("part_of"));
            allowed_rel.insert(String::from("regulates"));
            allowed_rel.insert(String::from("negatively_regulates"));
            allowed_rel.insert(String::from("positively_regulates"));
        }
        let mut skip_block: bool = false;
        for (_lno, line) in reader.lines().enumerate() {
            let l = line?;
            if skip_block && (!l.starts_with("[")) {
                continue;
            } else {
                skip_block = false;
            }
            if l.starts_with("[Typedef]") {
                if !cur_term.id.is_empty() {
                    self.term_map.insert(cur_term.id.clone(), cur_term.clone());
                    cur_term = Term {
                        id: String::new(),
                        name: String::new(),
                        description: String::new(),
                        namespace: GoNamespace::NoCat,
                        obsolete: false,
                    };
                }
                skip_block = true;
                continue;
            }
            if l.starts_with("[Term]") {
                if !cur_term.id.is_empty() {
                    self.term_map.insert(cur_term.id.clone(), cur_term.clone());
                    cur_term = Term {
                        id: String::new(),
                        name: String::new(),
                        description: String::new(),
                        namespace: GoNamespace::NoCat,
                        obsolete: false,
                    };
                }
                continue;
            }
            if l.starts_with("id: ") {
                let l_trim = l.trim_start_matches("id: ");
                cur_term.id = String::from(l_trim);
                continue;
            }
            if l.starts_with("name: ") {
                let l_trim = l.trim_start_matches("name: ");
                cur_term.name = String::from(l_trim);
                continue;
            }
            if l.starts_with("namespace: ") {
                let fields: Vec<&str> = l.split_ascii_whitespace().collect();
                cur_term.namespace = guess_go_namespace(&fields[1].to_string());
                continue;
            }
            if l.starts_with("def: ") {
                let l_trim = l.trim_start_matches("def: ");
                cur_term.description = String::from(l_trim);
                continue;
            }
            if l.starts_with("is_obsolete: ") {
                let fields: Vec<&str> = l.split_ascii_whitespace().collect();
                if fields[1] == "true" {
                    cur_term.obsolete = true;
                    self.obsolete.insert(cur_term.id.clone());
                }
                continue;
            }
            if l.starts_with("alt_id: ") {
                let fields: Vec<&str> = l.split_ascii_whitespace().collect();
                self.alt_id
                    .insert(String::from(fields[1]), cur_term.id.clone());
                continue;
            }
            if l.starts_with("replaced_by: ") {
                let fields: Vec<&str> = l.split_ascii_whitespace().collect();
                self.alt_id
                    .insert(cur_term.id.clone(), String::from(fields[1]));
                continue;
            }
            if l.starts_with("is_a: ") {
                let fields: Vec<&str> = l.split_ascii_whitespace().collect();
                edges.push((cur_term.id.clone(), String::from(fields[1])));
                edgtypes.push(determine_rel(&fields[0].to_string()) as i8);
                continue;
            }
            if l.starts_with("relationship: ") {
                let fields: Vec<&str> = l.split_ascii_whitespace().collect();
                let rel = fields[1];
                if allowed_rel.contains(&String::from(rel)) {
                    edges.push((cur_term.id.clone(), String::from(fields[2])));
                    edgtypes.push(determine_rel(&fields[1].to_string()) as i8);
                }
            }
        }
        // Last term
        self.term_map.insert(cur_term.id.clone(), cur_term.clone());
        // Populate graph
        self.index_to_id = make_hashmap();
        self.id_to_index = make_hashmap();
        for (i, term) in self.term_map.iter().enumerate() {
            self.id_to_index
                .insert(term.0.clone(), i.try_into().unwrap());
            self.index_to_id
                .insert(i.try_into().unwrap(), term.0.clone());
        }
        let mut edges_numeric: Vec<(u32, u32, i8)> = Vec::new();
        for (i, edge) in edges.iter().enumerate() {
            let src = self.id_to_index[&edge.0];
            let tgt = self.id_to_index[&edge.1];
            let typ = edgtypes[i];
            edges_numeric.push((src, tgt, typ));
        }
        self.graph = DiGraphMap::from_edges(edges_numeric);
        debug!(
            "GO graph has {} nodes {} connected components.",
            self.graph.node_count(),
            connected_components(&self.graph)
        );
        self.sql_serialize(sql_client)?;
        Ok(())
    }
}

impl Annotation for Obo {
    fn supports_hierarchical(&self) -> bool {
        true
    }

    fn parents(
        &self,
        term: &String,
        as_graph: bool,
        include_root: bool,
        include_weights: bool,
        graph: &DiGraphMap<u32, i8>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Edges>, Box<dyn Error>> {
        let mut tmp_edges: Vec<u32> = Vec::new();
        let mut tmp_weights: Vec<i8> = Vec::new();
        //let mut ret: Vec<String> = Vec::new();
        let node_num = self.id_to_index(term)?;

        if node_num.is_none() {
            return Ok(None);
        }

        let n = node_num.unwrap();

        // Check if we can use redis
        match rd_client {
            Some(client) => {
                //let db_prefix = self.db_prefix.clone();
                let key = format!("{}_pi_{}", self.db_prefix, term);
                let key2 = format!("{}_pw_{}", self.db_prefix, term);
                let mut con = client.con()?;
                // If we have never looked up the parents for `term` we add them to
                // redis
                if !con.exists(&key)? {
                    //let graph = &self.graph(rd_client, sql_client).await.unwrap();
                    enum_parents(
                        &graph,
                        &n,
                        &true,
                        &mut tmp_edges,
                        &mut Some(&mut tmp_weights),
                        &true,
                        &self.root_ids,
                        &true,
                    );
                    let serial = vec_to_tslist(&tmp_edges);
                    let serial_w = vec_to_tslist(&tmp_weights);
                    con.set(&key, &serial)?;
                    con.set(&key2, &serial_w)?;
                }
                // end !con.exists(...)
                let serial = con.get(&key)?;
                let serial_w = con.get(&key2)?;
                let parent_terms: Edges = Edges {
                    graph: tslist_to_vec(&serial),
                    weights: Some(tslist_to_vec(&serial_w)),
                    is_graph: true,
                };
                return Ok(Some(self.process_edges(
                    &parent_terms,
                    &include_root,
                    &include_weights,
                    &as_graph,
                )?));
            }
            None => (),
        }
        // END Redis

        //let graph = &self.graph(rd_client, sql_client).await.unwrap();
        match node_num {
            Some(n) => {
                enum_parents(
                    &graph,
                    &n,
                    &as_graph,
                    &mut tmp_edges,
                    &mut Some(&mut tmp_weights),
                    &include_root,
                    &self.root_ids,
                    &include_weights,
                );
            }
            None => (),
        }
        Ok(Some(Edges {
            graph: tmp_edges,
            weights: if include_weights {
                Some(tmp_weights)
            } else {
                None
            },
            is_graph: if as_graph { true } else { false },
        }))
    }

    fn children(
        &self,
        term: &String,
        as_graph: bool,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Vec<String>>, Box<dyn Error>> {
        let mut tmp_store: Vec<u32> = Vec::new();
        let mut ret: Vec<String> = Vec::new();
        let node_num = self.id_to_index(term).unwrap();
        let graph = &self.graph(rd_client, sql_client).unwrap();
        match node_num {
            Some(n) => {
                enum_children(&graph, &n, &as_graph, &mut tmp_store);
            }
            None => (),
        }
        for n in tmp_store.iter() {
            let n_str = self.index_to_id(&n).unwrap();
            match n_str {
                Some(n_i) => {
                    ret.push(n_i.clone());
                }
                None => (),
            }
        }
        Ok(Some(ret))
    }

    fn get_term_def(
        &self,
        term: &String,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<GenericTerm>, Box<dyn Error>> {
        let search = self.get_term(&term, sql_client, rd_client)?;
        match search {
            Some(h) => Ok(Some(GenericTerm {
                id: h.id,
                name: h.name,
                description: h.description,
                namespace: Some(h.namespace.to_string()),
                obsolete: Some(h.obsolete),
            })),
            None => Ok(None),
        }
    }

    fn graph(
        &self,
        _rd_client: Option<&rd::Client>,
        _sql_client: Option<&sql::Client>,
    ) -> Result<&DiGraphMap<u32, i8>, Box<dyn Error>> {
        trace!("Obo::graph()");
        // Need to unserialize
        // if self.graph.node_count() == 0 {
        //     let db_prefix = &self.db_prefix;
        //     let key = format!("{}_graph_edges", db_prefix);
        //     let key2 = format!("{}_graph_edgetypes", db_prefix);
        //     match rd_client {
        //         Some(client) => {
        //             let mut rd_con = client.con()?;
        //             if rd_con.exists(&key)? && rd_con.exists(&key2)? {
        //                 let edges_str = rd_con.get(&key)?;
        //                 let edgetypes_str = rd_con.get(&key2)?;
        //                 let edges_i: Vec<u32> = tslist_to_vec(&edges_str);
        //                 let edgetypes_i: Vec<i8> = tslist_to_vec(&edgetypes_str);

        //                 let mut zipped: Vec<(u32, u32, i8)> = Vec::new();
        //                 for (i, (src, tgt)) in edges_i
        //                     .iter()
        //                     .step_by(2)
        //                     .zip(edges_i.iter().skip(1).step_by(2))
        //                     .enumerate()
        //                 {
        //                     zipped.push((*src, *tgt, edgetypes_i[i]));
        //                 }
        //                 self.graph
        //                 return Ok(&DiGraphMap::from_edges(zipped));
        //             }
        //         }
        //         None => (),
        //     };
        //     match sql_client {
        //         Some(client) => {
        //             let mut conn = client.get_conn()?;
        //             let q = format!("SELECT src, tgt, typ FROM {}_graph;", db_prefix);
        //             let gr_res: Vec<(u32, u32, i8)> =
        //                 conn.query_map(q, |(src, tgt, typ)| (src, tgt, typ))?;
        //             match rd_client {
        //                 Some(rd_cl) => {
        //                     let mut rd_con = rd_cl.con()?;
        //                     let mut edges_i: Vec<u32> = Vec::new();
        //                     let mut edgetypes_i: Vec<i8> = Vec::new();
        //                     for (src, tgt, typ) in &gr_res {
        //                         edges_i.push(*src);
        //                         edges_i.push(*tgt);
        //                         edgetypes_i.push(*typ);
        //                     }
        //                     let edges_str = vec_to_tslist(&edges_i);
        //                     let edgetypes_str = vec_to_tslist(&edgetypes_i);
        //                     rd_con.set(&key, &edges_str)?;
        //                     rd_con.set(&key, &edgetypes_str)?;
        //                 }
        //                 None => (),
        //             };
        //             return Ok(&DiGraphMap::from_edges(gr_res));
        //         }
        //         None => {
        //             return Err(Box::new(ioError::new(
        //                 ErrorKind::Other,
        //                 format!("Graph is empty and no SQL/Redis client present"),
        //             )))
        //         }
        //     }
        // }
        Ok(&self.graph) // FIXME: inefficient
    }

    fn cache_path(
        &self,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<bool, Box<dyn Error>> {
        let db_prefix = &self.db_prefix;
        match sql_client {
            Some(client) => {
                let mut conn = client.get_conn()?;
                let q = format!("SELECT id, term FROM {}_annot;", db_prefix);
                trace!("cache_path() -> SQL -> {:?}", &q);
                let sql_res: Vec<(u32, String)> = conn.query_map(q, |(id, term)| (id, term))?;
                match rd_client {
                    Some(rd_cl) => {
                        let graph = self.graph(rd_client, sql_client)?;
                        let mut rd_con = rd_cl.con()?;

                        // Get all translations so we don't have to waste time in redis/sql
                        let mut id_to_term: HashMap<u32, String> = HashMap::new();
                        for (id, term) in &sql_res {
                            id_to_term.insert(*id, term.to_string());
                        }

                        let mut now = std::time::Instant::now();
                        // Term name -> Parent names
                        let mut hmap: HashMap<String, String> = HashMap::new();
                        // Term name -> Parent indices
                        let mut hmap2: HashMap<String, String> = HashMap::new();
                        // Term name -> Parent relationships
                        let mut hmap3: HashMap<String, String> = HashMap::new();

                        for (id, term) in &sql_res {
                            let mut rvec1: Vec<u32> = Vec::new();
                            let mut rvecw: Vec<i8> = Vec::new();
                            enum_parents(
                                &graph,
                                &id,
                                &true,
                                &mut rvec1,
                                &mut Some(&mut rvecw),
                                &true,
                                &self.root_ids,
                                &true,
                            );
                            let mut rvec2: Vec<String> = Vec::new();
                            for inx in &rvec1 {
                                // Unwrap here as we probably want to panic if we can't translate
                                // Would indicate something majorly wrong
                                rvec2.push(id_to_term.get(inx).unwrap().to_string());
                            }
                            hmap.insert(term.clone(), vec_to_tslist(&rvec2));
                            hmap2.insert(term.clone(), vec_to_tslist(&rvec1));
                            hmap3.insert(term.clone(), vec_to_tslist(&rvecw));
                        }
                        debug!("Built GO hierarchy in {} seconds", now.elapsed().as_secs());
                        now = std::time::Instant::now();
                        let mut ctr: u64 = 0;
                        for (t, pt) in hmap.iter() {
                            let key = format!("{}_p_{}", &self.db_prefix, &t);
                            if rd_con.exists(&key)? {
                                continue;
                            } else {
                                rd_con.set(&key, pt)?;
                                ctr += 1;
                            }
                        }
                        for (t, pt) in hmap2.iter() {
                            let key = format!("{}_pi_{}", &self.db_prefix, &t);
                            if rd_con.exists(&key)? {
                                continue;
                            } else {
                                rd_con.set(&key, pt)?;
                            }
                        }
                        for (t, pt) in hmap3.iter() {
                            let key = format!("{}_pw_{}", &self.db_prefix, &t);
                            if rd_con.exists(&key)? {
                                continue;
                            } else {
                                rd_con.set(&key, pt)?;
                            }
                        }
                        debug!(
                            "Added {} / {} (new / total) keys to redis in {} seconds",
                            ctr,
                            hmap.len(),
                            now.elapsed().as_secs()
                        );
                    }
                    None => (),
                };
            }
            None => return Ok(true),
        };
        Ok(true)
    }

    fn process_edges(
        &self,
        edges: &Edges,
        include_root: &bool,
        include_weights: &bool,
        as_graph: &bool,
    ) -> Result<Edges, Box<dyn Error>> {
        if *as_graph {
            if *include_root {
                return Ok(edges.clone());
            }

            let mut pruned: Vec<u32> = Vec::new();
            let mut pruned_w: Vec<i8> = Vec::new();

            for (edge, weight) in edges.iter() {
                let src = *edge.0;
                let tgt = *edge.1;
                if !self.root_ids.contains(&tgt) {
                    pruned.push(src);
                    pruned.push(tgt);
                    pruned_w.push(*weight.unwrap_or(&-1));
                }
            }
            return Ok(Edges {
                graph: pruned,
                weights: if *include_weights {
                    Some(pruned_w)
                } else {
                    None
                },
                is_graph: true,
            });
        } else {
            if *include_root {
                let pruned: Vec<u32> = edges.iter().map(|(e, _w)| e.1).cloned().collect();
                let pruned_u: HashSet<u32> = HashSet::from_iter(pruned.iter().cloned());
                return Ok(Edges {
                    graph: pruned_u.iter().cloned().collect(),
                    weights: None,
                    is_graph: false,
                });
            } else {
                let mut pruned: Vec<u32> = Vec::new();
                for ((_src, tgt), _weight) in edges.iter() {
                    let x: u32 = *tgt;
                    if !self.root_ids.contains(&x) {
                        pruned.push(x);
                    }
                }
                let pruned_u: HashSet<u32> = HashSet::from_iter(pruned.iter().cloned());
                return Ok(Edges {
                    graph: pruned_u.iter().cloned().collect(),
                    weights: None,
                    is_graph: false,
                });
            }
        }
    }
    fn n_terms(&self) -> usize {
        self.term_map.len()
    }

    fn id_to_index(&self, id: &String) -> Result<Option<u32>, Box<dyn Error>> {
        trace!("id_to_index {:?}", id);
        match self.id_to_index.get(&self.upcycle_alt_id(id)?.unwrap()) {
            Some(hit) => Ok(Some(*hit)),
            None => Ok(None),
        }
    }

    fn index_to_id(&self, id: &u32) -> Result<Option<String>, Box<dyn Error>> {
        trace!("index_to_id {:?}", id);
        match self.index_to_id.get(id) {
            Some(hit) => Ok(Some(hit.clone())),
            None => Ok(None),
        }
    }

    fn index_to_id_vunsafe(&self, ids: &Vec<u32>) -> Result<Vec<String>, Box<dyn Error>> {
        let mut ret: Vec<String> = Vec::new();
        for i in ids {
            ret.push(self.index_to_id.get(i).unwrap().clone());
        }
        Ok(ret)
    }

    fn all_ids(&self) -> Result<Vec<u32>, Box<dyn Error>> {
        Ok(self.index_to_id.iter().map(|(k, _v)| k).cloned().collect())
    }

    fn root_ids(&self) -> Result<&HashSet<u32, ARandomState>, Box<dyn Error>> {
        Ok(&self.root_ids)
    }

    fn db_prefix(&self) -> &String {
        &self.db_prefix
    }

    fn opts(&self) -> &MapOptions {
        &self.opts
    }

    fn get_namespace(&self, id: &String) -> Option<i8> {
        let t = self.term_map.get(id);
        match t {
            Some(hit) => Some(hit.namespace.clone() as i8),
            None => None,
        }
    }

    fn filter_terms(&self, term: &String) -> Result<Option<String>, Box<dyn Error>> {
        let nt = self.upcycle_alt_id(term)?.unwrap();
        match self.term_map.get(&nt) {
            Some(t) => {
                if t.obsolete {
                    Ok(None)
                } else {
                    Ok(Some(nt))
                }
            }
            None => Ok(None),
        }
    }
}

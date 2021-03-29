use crate::utils::common::hash_file;
use crate::utils::common::make_hashset;
use crate::utils::common::Edges;
use crate::utils::common::GenericTerm;
use crate::utils::common::MapOptions;
use crate::utils::rd;
use crate::utils::sql;
use crate::utils::traits::Annotation;
use crate::utils::traits::FromFile;
use ahash::RandomState as ARandomState;
use async_trait::async_trait;
use log::{info, trace};
use mysql::params;
use mysql::prelude::*;
use petgraph::graphmap::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Term {
    id: String,
    name: String,
    description: String,
}

pub struct Flatfile {
    file: String,
    db_prefix: String,
    alt_id: HashMap<String, String>,
    term_map: HashMap<String, Term>,
    id_to_index: HashMap<String, u32>,
    index_to_id: HashMap<u32, String>,
    hash: u64,
    opts: MapOptions,
    root_ids: HashSet<u32, ARandomState>,
    graph: DiGraphMap<u32, i8>,
}

impl<'a> Flatfile {
    pub fn new(
        file: String,
        opts: MapOptions,
        sql_client: Option<&sql::Client>,
    ) -> Result<Flatfile, Box<dyn Error>> {
        let mut o = Flatfile {
            file: file.clone(),
            db_prefix: opts.db_prefix.clone(),
            alt_id: HashMap::new(),
            term_map: HashMap::new(),
            id_to_index: HashMap::new(),
            index_to_id: HashMap::new(),
            hash: 0,
            opts: opts,
            root_ids: make_hashset(),
            graph: DiGraphMap::<u32, i8>::new(),
        };
        let io_res = o.from_file(sql_client);
        match io_res {
            Err(msg) => panic!("I/O error for file {}: {}", file, msg),
            Ok(()) => (),
        };
        Ok(o)
    }

    fn get_term(
        &self,
        id: &String,
        _sql_client: Option<&sql::Client>,
        _rd_client: Option<&rd::Client>,
    ) -> Result<Option<Term>, Box<dyn Error>> {
        let hit = self.term_map.get(&id.clone());
        match hit {
            Some(h) => Ok(Some(h.clone())),
            None => Ok(None),
        }
    }
    fn sql_serialize(&self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        if sql_client.is_none() {
            return Ok(());
        }
        let client = sql_client.unwrap();
        let db_prefix = &self.db_prefix;

        let q2 = format!(
            "CREATE TABLE IF NOT EXISTS {}_annot (\
            id INTEGER UNSIGNED PRIMARY KEY NOT NULL, \
            term VARCHAR(2048), \
            name VARCHAR(2048), \
            description VARCHAR(16384) \
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

        let q5 = format!(
            "INSERT INTO {}_annot (id, term, name, description) \
            VALUES (:id, :term, :name, :description);",
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

        trace!("SQL: {:?}", &q2);
        conn.query_drop(&q2)?;

        trace!("SQL: {:?}", &q3);
        conn.query_drop(&q3)?;

        trace!("SQL: {:?}", &q5);
        conn.exec_batch(
            &q5,
            self.term_map.iter().map(|t| {
                params! {
                    "id" => self.id_to_index[&t.1.id],
                    "term" => &t.1.id,
                    "name" => &t.1.name,
                    "description" => &t.1.description,
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

        let q1 = format!("DROP TABLE IF EXISTS {}_annot;", db_prefix);

        let q2 = format!("DROP TABLE IF EXISTS {}_alt_id;", db_prefix);

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
            "SELECT id, term, name, description FROM {}_annot;",
            db_prefix
        );
        let _r: Vec<()> = conn.query_map(
            &q1,
            |(id, term, name, description): (u32, String, String, String)| {
                let t = Term {
                    id: term.clone(),
                    name: name,
                    description: description,
                };
                let index = id;
                self.id_to_index.insert(term.clone(), index);
                self.index_to_id.insert(index, term.clone());
                self.term_map.insert(term.clone(), t);
            },
        )?;

        let q3 = format!("SELECT alt, id FROM {}_alt_id;", db_prefix);
        let _r2: Vec<()> = conn.query_map(&q3, |(alt, id): (String, String)| {
            self.alt_id.insert(alt, id);
        })?;
        Ok(())
    }
}

impl<'a> FromFile for Flatfile {
    fn from_file(&mut self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        info!("Parsing annotation {}", self.file);
        self.hash = hash_file(&self.file).unwrap();
        trace!("Hash for {:?} -> {:?}", &self.file, &self.hash);
        if self.sql_check_hash_in_db(sql_client)? {
            trace!("Have hash in SQL DB. Unserializing from SQL");
            self.sql_unserialize(sql_client)?;
            return Ok(());
        } else {
            trace!("No/different hash in SQL DB. Rebuilding");
            self.sql_drop(sql_client)?;
        }
        let f = File::open(self.file.clone())?;
        let reader = BufReader::new(f);
        for (_lno, line) in reader.lines().enumerate() {
            let l = line?;
            let fields: Vec<&str> = l.split('\t').collect();
            if fields.len() < 3 {
                panic!(
                    "Panic for {:?}. Each line must have at least three fields separated by \\t",
                    &self.db_prefix
                );
            }
            self.term_map.insert(
                String::from(fields[0]),
                Term {
                    id: String::from(fields[0]),
                    name: String::from(fields[1]),
                    description: String::from(fields[2]),
                },
            );
        }

        // Populate graph
        self.index_to_id = HashMap::new();
        self.id_to_index = HashMap::new();
        for (i, term) in self.term_map.iter().enumerate() {
            self.id_to_index
                .insert(term.0.clone(), i.try_into().unwrap());
            self.index_to_id
                .insert(i.try_into().unwrap(), term.0.clone());
        }
        self.sql_serialize(sql_client)?;
        Ok(())
    }
}

#[async_trait]
impl Annotation for Flatfile {
    fn supports_hierarchical(&self) -> bool {
        false
    }

    fn parents(
        &self,
        _term: &String,
        _as_graph: bool,
        _include_root: bool,
        _include_weights: bool,
        _graph: &DiGraphMap<u32, i8>,
        _rd_client: Option<&rd::Client>,
    ) -> Result<Option<Edges>, Box<dyn Error>> {
        Ok(None)
    }

    fn children(
        &self,
        _term: &String,
        _as_graph: bool,
        _sql_client: Option<&sql::Client>,
        _rd_client: Option<&rd::Client>,
    ) -> Result<Option<Vec<String>>, Box<dyn Error>> {
        Ok(None)
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
                namespace: None,
                obsolete: None,
            })),
            None => Ok(None),
        }
    }

    fn graph(
        &self,
        _rd_client: Option<&rd::Client>,
        _sql_client: Option<&sql::Client>,
    ) -> Result<&DiGraphMap<u32, i8>, Box<dyn Error>> {
        Ok(&self.graph)
    }

    fn cache_path(
        &self,
        _sql_client: Option<&sql::Client>,
        _rd_client: Option<&rd::Client>,
    ) -> Result<bool, Box<dyn Error>> {
        Ok(true)
    }

    fn process_edges(
        &self,
        _edges: &Edges,
        _include_root: &bool,
        _include_weights: &bool,
        _as_graph: &bool,
    ) -> Result<Edges, Box<dyn Error>> {
        Ok(Edges {
            graph: Vec::new(),
            weights: None,
            is_graph: false,
        })
    }

    fn n_terms(&self) -> usize {
        self.term_map.len()
    }

    fn id_to_index(&self, id: &String) -> Result<Option<u32>, Box<dyn Error>> {
        trace!("id_to_index {:?}", id);
        match self.id_to_index.get(id) {
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
    fn get_namespace(&self, _id: &String) -> Option<i8> {
        None
    }

    fn filter_terms(&self, term: &String) -> Result<Option<String>, Box<dyn Error>> {
        Ok(Some(term.clone()))
    }
}

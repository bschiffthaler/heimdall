/*
  Mapping implements a basic flat file format:
      Gene    TermA|TermB|TermC
  The parser creates a a mapping between any unique Term <-> Gene combination.
*/
use crate::utils::common::hash_file;
use crate::utils::common::MapOptions;
use crate::utils::rd;
use crate::utils::sql;
use crate::utils::traits::FromFile;
use ahash::RandomState as ARandomState;
use log::{debug, info, trace, warn};
use mysql::params;
use mysql::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

pub struct Mapping {
    file: String,
    pub term_to_gene: HashMap<String, HashSet<String>>,
    pub gene_to_term: HashMap<String, HashSet<String>>,
    pub id_to_index: HashMap<String, u32>,
    pub index_to_id: HashMap<u32, String>,
    hash: u64,
    db_prefix: String,
}

// Constructor
impl<'a> Mapping {
    pub fn new(
        file: String,
        opts: MapOptions,
        sql_client: Option<&sql::Client>,
    ) -> Result<Mapping, Box<dyn Error>> {
        let mut m = Mapping {
            file: file.clone(),
            term_to_gene: HashMap::new(),
            gene_to_term: HashMap::new(),
            id_to_index: HashMap::new(),
            index_to_id: HashMap::new(),
            hash: 0,
            db_prefix: opts.db_prefix.clone(),
        };
        let io_res = m.from_file(sql_client);
        match io_res {
            Err(msg) => panic!("I/O error for file {}: {}", file, msg),
            Ok(()) => (),
        };
        Ok(m)
    }
    pub fn n_genes(&self) -> usize {
        self.gene_to_term.len()
    }
    pub fn n_terms(&self) -> usize {
        self.term_to_gene.len()
    }
    pub fn get_terms(&self, gene: &String) -> Result<Option<HashSet<String>>, Box<dyn Error>> {
        let res = self.gene_to_term.get(gene);
        match res {
            Some(hits) => Ok(Some(hits.clone())),
            None => Ok(None),
        }
    }
    pub fn get_genes(&self, term: &'a String) -> Result<Option<HashSet<String>>, Box<dyn Error>> {
        let res = self.term_to_gene.get(term);
        match res {
            Some(hits) => Ok(Some(hits.clone())),
            None => Ok(None),
        }
    }
    fn sql_serialize(&self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        if sql_client.is_none() {
            return Ok(());
        }
        let client = sql_client.unwrap();
        let db_prefix = &self.db_prefix;

        let q1 = format!(
            "CREATE TABLE IF NOT EXISTS {}_index (\
            id INTEGER PRIMARY KEY NOT NULL, \
            gene VARCHAR(2048)\
            );",
            db_prefix
        );

        let q2 = format!(
            "CREATE TABLE IF NOT EXISTS {}_map (\
            gene VARCHAR(2048), \
            term VARCHAR(2048)\
            );",
            db_prefix
        );

        let q5 = format!(
            "INSERT INTO {}_map (gene, term) \
            VALUES (:gene, :term);",
            db_prefix
        );

        let q7 = format!(
            "INSERT INTO tbl_hash (name, hash, version) VALUES ('{}', {}, NULL) \
            ON DUPLICATE KEY UPDATE name = '{}', hash = {}, version = NULL;",
            db_prefix, self.hash, db_prefix, self.hash
        );

        let q8 = format!(
            "CREATE INDEX {}_map_igene ON {}_map (gene)",
            db_prefix, db_prefix
        );

        let q9 = format!(
            "CREATE INDEX {}_map_iterm ON {}_map (term)",
            db_prefix, db_prefix
        );

        let q10 = format!(
            "INSERT INTO {}_index (id, gene) VALUES (:id, :gene)",
            db_prefix
        );

        let q11 = format!(
            "CREATE INDEX {0}_index_igene ON {0}_index (gene)",
            db_prefix
        );

        let mut conn = client.get_conn()?;

        conn.query_drop("SET AUTOCOMMIT=0;")?;

        trace!("SQL: {:?}", &q1);
        conn.query_drop(&q1)?;

        trace!("SQL: {:?}", &q2);
        conn.query_drop(&q2)?;

        trace!("SQL: {:?}", &q5);
        for (k, v) in &self.gene_to_term {
            let gene = &k.clone();
            conn.exec_batch(
                &q5,
                v.iter().map(|t| {
                    params! {
                        "gene" => &gene,
                        "term" => &t,
                    }
                }),
            )?;
        }

        trace!("SQL: {:?}", &q10);
        for (k, v) in &self.id_to_index {
            let gene = &k.clone();
            let index = v;
            conn.exec_drop(&q10, params! {"id" => index, "gene" => gene})?;
        }

        trace!("SQL: {:?}", &q8);
        conn.query_drop(&q8)?;

        trace!("SQL: {:?}", &q9);
        conn.query_drop(&q9)?;

        trace!("SQL: {:?}", &q7);
        conn.query_drop(&q7)?;

        trace!("SQL: {:?}", &q11);
        conn.query_drop(&q11)?;

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

        let q1 = format!("DROP TABLE IF EXISTS {}_map;", db_prefix);
        let q2 = format!("DROP TABLE IF EXISTS {}_index;", db_prefix);

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

        let q1 = format!("SELECT gene, term FROM {}_map;", db_prefix);
        let _r: Vec<()> = conn.query_map(&q1, |(gene, term): (String, String)| {
            let term_ptr = self
                .term_to_gene
                .entry(term.clone())
                .or_insert(HashSet::new());
            let gene_ptr = self
                .gene_to_term
                .entry(gene.clone())
                .or_insert(HashSet::new());

            (*term_ptr).insert(gene.clone());
            (*gene_ptr).insert(term.clone());
        })?;
        let q2 = format!("SELECT id, gene FROM {}_index;", db_prefix);
        let _r: Vec<()> = conn.query_map(&q2, |(id, gene): (u32, String)| {
            self.id_to_index.insert(gene.clone(), id);
            self.index_to_id.insert(id, gene.clone());
        })?;
        Ok(())
    }
}

// Lax file reader. Lines with errors are skipped
impl<'a> FromFile for Mapping {
    fn from_file(&mut self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>> {
        info!("Parsing mapping {}", self.file);
        self.hash = hash_file(&self.file).unwrap();
        debug!("Hash for {:?} -> {:?}", &self.file, &self.hash);
        if self.sql_check_hash_in_db(sql_client)? {
            debug!("Have hash in SQL DB. Unserialiizing data from SQL");
            self.sql_unserialize(sql_client)?;
            return Ok(());
        } else {
            debug!("No/different hash in SQL DB. Rebuilding");
            self.sql_drop(sql_client)?;
        }
        let f = File::open(self.file.clone())?;
        let reader = BufReader::new(f);

        let mut gset: HashSet<String> = HashSet::new();

        for (lno, line) in reader.lines().enumerate() {
            let l = line?;
            let mut sp_iter = l.split_ascii_whitespace();
            // Parse first column (genes)
            let gene = sp_iter.next();
            if gene.is_none() {
                warn!(
                    "Could not parse a gene in file {}:{}\nLine was {}",
                    self.file, lno, l
                );
                continue;
            }
            let _gene = String::from(gene.unwrap());
            gset.insert(_gene.clone());
            // Parse second column (terms)
            let terms = sp_iter.next();
            if terms.is_none() {
                warn!(
                    "Could not parse terms in file {}:{}\nLine was {}",
                    self.file, lno, l
                );
                continue;
            }
            let _terms = terms.unwrap();
            // Insert a new HashSet for the gene if it isn't there already
            let gene_ptr = self
                .gene_to_term
                .entry(_gene.clone())
                .or_insert(HashSet::new());

            for term in _terms.split('|') {
                let _term = String::from(term);
                let term_ptr = self
                    .term_to_gene
                    .entry(_term.clone())
                    .or_insert(HashSet::new());
                (*gene_ptr).insert(_term.clone());
                (*term_ptr).insert(_gene.clone());
            }
        }

        for (i, g) in gset.iter().enumerate() {
            self.id_to_index.insert(g.clone(), i as u32);
            self.index_to_id.insert(i as u32, g.clone());
        }

        self.sql_serialize(sql_client)?;
        Ok(())
    }
}

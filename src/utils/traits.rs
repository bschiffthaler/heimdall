use crate::utils::common::Edges;
use crate::utils::common::GenericTerm;
use crate::utils::common::MapOptions;
use crate::utils::mapping::Mapping;
use crate::utils::obo::GoNamespace;
use crate::utils::rd;
use crate::utils::sql;
use ahash::RandomState as ARandomState;
use ndarray::Array1;
use ndarray::Array2;
use petgraph::prelude::DiGraphMap;
use std::collections::{HashMap, HashSet};
use std::error::Error;

pub trait FromFile {
    fn from_file(&mut self, sql_client: Option<&sql::Client>) -> Result<(), Box<dyn Error>>;
}

pub trait Annotation {
    fn supports_hierarchical(&self) -> bool;
    fn parents(
        &self,
        term: &String,
        as_graph: bool,
        include_root: bool,
        include_weights: bool,
        graph: &DiGraphMap<u32, i8>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Edges>, Box<dyn Error>>;
    fn children(
        &self,
        term: &String,
        as_graph: bool,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Vec<String>>, Box<dyn Error>>;
    fn get_term_def(
        &self,
        term: &String,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<GenericTerm>, Box<dyn Error>>;
    fn graph(
        &self,
        rd_client: Option<&rd::Client>,
        sql_client: Option<&sql::Client>,
    ) -> Result<&DiGraphMap<u32, i8>, Box<dyn Error>>;
    fn cache_path(
        &self,
        sql_client: Option<&sql::Client>,
        rd_client: Option<&rd::Client>,
    ) -> Result<bool, Box<dyn Error>>;
    fn process_edges(
        &self,
        edges: &Edges,
        include_root: &bool,
        include_weights: &bool,
        as_graph: &bool,
    ) -> Result<Edges, Box<dyn Error>>;
    fn n_terms(&self) -> usize;
    fn all_ids(&self) -> Result<Vec<u32>, Box<dyn Error>>;
    fn id_to_index(&self, id: &String) -> Result<Option<u32>, Box<dyn Error>>;
    fn index_to_id(&self, id: &u32) -> Result<Option<String>, Box<dyn Error>>;
    fn index_to_id_vunsafe(&self, ids: &Vec<u32>) -> Result<Vec<String>, Box<dyn Error>>;
    fn root_ids(&self) -> Result<&HashSet<u32, ARandomState>, Box<dyn Error>>;
    fn db_prefix(&self) -> &String;
    fn opts(&self) -> &MapOptions;
    fn get_namespace(&self, id: &String) -> Option<i8>;
    fn filter_terms(&self, term: &String) -> Result<Option<String>, Box<dyn Error>>;
}

// pub trait SemanticMergeStrategy {
//     fn merge(&self, a: Vec<f64>, b: Vec<f64>) -> Result<f64, Box<dyn Error>>;
// }

pub trait SemanticSimilarity {
    fn cache_ics(
        &self,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<(), Box<dyn Error>>;
    fn ic(
        &self,
        term: &String,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
        with_subgraph: &bool,
    ) -> Result<(Option<f64>, Option<HashMap<u32, f64>>), Box<dyn Error>>;
    fn term_pairwise(
        &self,
        term_a: &String,
        term_b: &String,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<f64>, Box<dyn Error>>;
    fn term_groupwise(
        &self,
        terms_a: &Vec<String>,
        terms_b: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Array2<f64>>, Box<dyn Error>>;
    fn term_groupwise_sym(
        &self,
        terms: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Array2<f64>>, Box<dyn Error>>;
    fn genes_pairwise(
        &self,
        gene_a: &String,
        gene_b: &String,
        annotation: Box<&dyn Annotation>,
        mapping: &Mapping,
        merger: &String,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<f64>, Box<dyn Error>>;
    fn genes_groupwise(
        &self,
        genes_a: &Vec<String>,
        genes_b: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        mapping: &Mapping,
        merger: &String,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Array2<f64>>, Box<dyn Error>>;
    fn genes_groupwise_sym(
        &self,
        genes: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        mapping: &Mapping,
        merger: &String,
        rd_client: Option<&rd::Client>,
        namespace: &Option<String>,
    ) -> Result<Option<Array1<f64>>, Box<dyn Error>>;
}

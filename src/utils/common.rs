use ahash::RandomState as ARandomState;
use core::ops::{Index, IndexMut};
use petgraph::graphmap::DiGraphMap;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::iter::Iterator;

// All structs here implement this
pub struct MapOptions {
    pub parent_terms: Option<HashSet<String, ARandomState>>,
    pub gou_parent_terms: Option<HashSet<String, ARandomState>>,
    pub db_prefix: String,
}

pub fn max<'a>(a: &'a f64, b: &'a f64) -> &'a f64 {
    if a.is_finite() && b.is_finite() {
        if a > b {
            return &a;
        }
        return &b;
    }
    return &f64::NAN;
}

/*
  Edge structure that represents a simple vector of node pairs
  and optionally associated weights (relationships)

  Iteration should be implemented for reference yielding only
*/
#[derive(Clone)]
pub struct Edges {
    pub graph: Vec<u32>,
    pub weights: Option<Vec<i8>>,
    pub is_graph: bool,
}

impl Edges {
    pub fn iter(&self) -> EdgeIterator {
        EdgeIterator::from(&self)
    }
}

impl<'a, 'b> IntoIterator for &'a Edges {
    type Item = ((&'a u32, &'a u32), Option<&'a i8>);
    type IntoIter = EdgeIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        EdgeIterator::from(&self)
    }
}

pub struct EdgeIterator<'a> {
    ig: usize,
    iw: usize,
    data: &'a Edges,
}

impl<'a> EdgeIterator<'a> {
    pub fn from(edges: &'a Edges) -> Self {
        Self {
            ig: 0,
            iw: 0,
            data: edges,
        }
    }
}

impl<'a> Iterator for EdgeIterator<'a> {
    type Item = ((&'a u32, &'a u32), Option<&'a i8>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.ig == self.data.graph.len() {
            return None;
        }
        let e = (&self.data.graph[self.ig], &self.data.graph[self.ig + 1]);
        self.ig += 2;
        let w = match &self.data.weights {
            Some(weights) => Some(&weights[self.iw]),
            None => None,
        };
        self.iw += 1;
        Some((e, w))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpMatTriplet {
    pub row_idx: Vec<u32>,
    pub col_idx: Vec<u32>,
    pub vals: Vec<u8>,
    pub size: (usize, usize),
}

pub fn hash_file(file: &String) -> Result<u64, Box<dyn Error>> {
    let mut f = File::open(file)?;
    let mut file_raw = String::new();
    f.read_to_string(&mut file_raw)?;
    let mut hasher = DefaultHasher::new();
    file_raw.hash(&mut hasher);
    Ok(hasher.finish())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenericTerm {
    pub id: String,
    pub name: String,
    pub description: String,
    pub namespace: Option<String>,
    pub obsolete: Option<bool>,
}

pub fn make_hashmap<K, V>() -> HashMap<K, V, ARandomState> {
    HashMap::with_hasher(ARandomState::new())
}

pub fn make_hashset<T>() -> HashSet<T, ARandomState> {
    HashSet::with_hasher(ARandomState::new())
}

pub fn enum_parents(
    graph: &DiGraphMap<u32, i8>,
    node: &u32,
    as_graph: &bool,
    ret: &mut Vec<u32>,
    ret_weights: &mut Option<&mut Vec<i8>>,
    include_root: &bool,
    root_ids: &HashSet<u32, ARandomState>,
    include_weights: &bool,
) {
    for n in graph.neighbors(*node) {
        if !include_root && root_ids.contains(&n) {
            continue;
        }

        if *as_graph {
            ret.push(*node);
            if *include_weights {
                let w = graph.edge_weight(*node, n).expect("Typeless edge in graph"); //
                                                                                      // ret_weights.as_ref().unwrap().push(w.clone())
                match ret_weights {
                    Some(x) => x.push(*w),
                    None => (),
                };
            }
        }
        ret.push(n);
        enum_parents(
            graph,
            &n,
            as_graph,
            ret,
            ret_weights,
            include_root,
            root_ids,
            include_weights,
        );
    }
}

pub fn enum_children(graph: &DiGraphMap<u32, i8>, node: &u32, as_graph: &bool, ret: &mut Vec<u32>) {
    for n in graph.neighbors_directed(*node, petgraph::Direction::Incoming) {
        if *as_graph {
            ret.push(*node);
        }
        ret.push(n);
        enum_children(graph, &n, as_graph, ret);
    }
}

// Converts a tab delimited string to a vector of type T
pub fn tslist_to_vec<T: std::str::FromStr>(list: &String) -> Vec<T>
where
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    let mut ret: Vec<T> = Vec::new();
    if list == &"".to_string() {
        return ret;
    }
    for item in list.split('\t') {
        let x: T = item.to_string().parse().unwrap();
        ret.push(x);
    }
    ret
}

// Converts a vector of type T to a tab delimited string
pub fn vec_to_tslist<T: std::fmt::Display>(list: &Vec<T>) -> String {
    let mut ret: String = String::new();
    if list.len() == 0 {
        return ret;
    }
    for (i, item) in list.iter().enumerate() {
        let x: String = item.to_string();
        ret.push_str(&x);
        if i != (list.len() - 1) {
            ret.push('\t');
        }
    }
    ret
}

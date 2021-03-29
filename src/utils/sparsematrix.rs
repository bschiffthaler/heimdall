use crate::utils::common::SpMatTriplet;
use crate::utils::mapping::Mapping;
use crate::utils::rd;
use crate::utils::traits::Annotation;
use log::{debug, trace};

use std::error::Error;

pub fn prep_mat_triplet(
    annotation: &dyn Annotation,
    mapping: &Mapping,
    genes: &Vec<String>,
    rd_client: Option<&rd::Client>,
    include_parents: bool,
    include_root: bool,
) -> Result<SpMatTriplet, Box<dyn Error>> {
    let mut row_idx: Vec<u32> = Vec::new();
    let mut col_idx: Vec<u32> = Vec::new();
    let mut vals: Vec<u8> = Vec::new();
    let graph = annotation.graph(None, None)?;
    for gene in genes {
        match mapping.gene_to_term.get(gene) {
            // We have at least one annotation
            Some(terms) => {
                trace!("gene->{}", gene);
                let row: u32 = *mapping.id_to_index.get(gene).unwrap();
                for term in terms {
                    trace!("term->{}", term);
                    let col: u32 = annotation.id_to_index(term)?.unwrap();
                    row_idx.push(row);
                    col_idx.push(col);
                    vals.push(1);
                    if include_parents {
                        let parents = annotation.parents(
                            term,
                            false,
                            include_root,
                            false,
                            &graph,
                            rd_client,
                        )?;
                        match parents {
                            Some(pars) => {
                                for col in pars.graph {
                                    trace!("parent term->{}", col);
                                    row_idx.push(row);
                                    col_idx.push(col);
                                    vals.push(1);
                                }
                            }
                            None => (),
                        };
                    }
                }
            }
            None => (),
        };
    }

    let ret = SpMatTriplet {
        size: (mapping.n_genes(), annotation.n_terms()),
        row_idx: row_idx,
        col_idx: col_idx,
        vals: vals,
    };

    let density: f64 =
        ret.vals.len() as f64 / (mapping.n_genes() as f64 * annotation.n_terms() as f64);

    debug!("to_sparse_mat() -> density={}", density);

    Ok(ret)
}

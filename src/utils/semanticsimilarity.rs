use crate::utils::common::make_hashmap;
use crate::utils::common::make_hashset;
use crate::utils::common::max;
use crate::utils::common::Edges;
use crate::utils::mapping::Mapping;
use crate::utils::obo::guess_go_namespace;
use crate::utils::obo::to_go_relationship;
use crate::utils::obo::GoNamespace;
use crate::utils::obo::GoRelationship;
use crate::utils::rd;
use crate::utils::traits::Annotation;
use crate::utils::traits::SemanticSimilarity;
use ahash::RandomState as ARandomState;
use approx::ulps_eq;
use log::{debug, trace};
use ndarray::s;
use ndarray::Array1;
use ndarray::Array2;
use ndarray::Axis;
use ndarray_stats::QuantileExt;
use petgraph::graphmap::DiGraphMap;
use redis::Commands;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::error::Error;

#[derive(Debug)]
pub struct GoUniversal {
    pub allowed_rel: HashSet<GoRelationship, ARandomState>,
}

fn bma(data: &Array2<f64>) -> Result<f64, Box<dyn Error>> {
    let nr: usize = data.nrows();
    let nc: usize = data.ncols();

    let mut rowmaxs: Array1<f64> = Array1::zeros(nr);
    let mut colmaxs: Array1<f64> = Array1::zeros(nc);

    let mut ci: usize = 0;
    for col in data.lanes(Axis(0)) {
        let m = col.max()?;
        colmaxs[ci] = *m;
        ci += 1;
    }

    let mut ri: usize = 0;
    for row in data.lanes(Axis(1)) {
        let m = row.max()?;
        rowmaxs[ri] = *m;
        ri += 1;
    }

    trace!("bma rowmaxs -> {:?}, colmaxs -> {:?}", rowmaxs, colmaxs);

    Ok((rowmaxs.sum() + colmaxs.sum()) / ((nr + nc) as f64))
}

fn bma_submatrix(
    data: &Array2<f64>,
    rows: &Vec<usize>,
    cols: &Vec<usize>,
) -> Result<f64, Box<dyn Error>> {
    let nr: usize = rows.len();
    let nc: usize = cols.len();

    let mut rowmaxs: Array1<f64> = Array1::zeros(nr);
    let mut colmaxs: Array1<f64> = Array1::zeros(nc);

    trace!("bma submat rows -> {:?}, cols -> {:?}", rows, cols);
    trace!("data: [{:?}, {:?}]", data.nrows(), data.ncols());
    for (ci, col_index) in cols.iter().enumerate() {
        let slice = data.slice(s![.., *col_index]);
        for (ri, row_index) in rows.iter().enumerate() {
            unsafe {
                let val = slice.uget(*row_index);

                if val > colmaxs.uget(ci) {
                    *colmaxs.uget_mut(ci) = *val;
                }
                if val > rowmaxs.uget(ri) {
                    *rowmaxs.uget_mut(ri) = *val;
                }
            }
        }
    }

    trace!("bma rowmaxs -> {:?}, colmaxs -> {:?}", rowmaxs, colmaxs);

    Ok((rowmaxs.sum() + colmaxs.sum()) / ((nr + nc) as f64))
}

fn filtered_neighbors(
    gr: &DiGraphMap<u32, i8>,
    allowed: &HashSet<GoRelationship, ARandomState>,
    direction: petgraph::Direction,
    id: &u32,
) -> Vec<u32> {
    let mut ret: Vec<u32> = Vec::with_capacity(256);
    for n in gr.neighbors_directed(*id, direction) {
        let rel = match direction {
            petgraph::Direction::Incoming => to_go_relationship(gr.edge_weight(n, *id).unwrap()),
            petgraph::Direction::Outgoing => to_go_relationship(gr.edge_weight(*id, n).unwrap()),
        };
        if allowed.contains(&rel) {
            ret.push(n);
        }
    }
    ret
}

fn mica_sim(hm_a: &HashMap<u32, f64>, hm_b: &HashMap<u32, f64>, a: &u32, b: &u32) -> f64 {
    if a == b {
        return 1.0;
    }

    let mut max_score: f64 = 0.0;
    let mut _max_ancestor: u32 = 0;

    trace!("mica_sim -> a:{:?} b:{:?}", a, b);

    let ic_a = hm_a.get(a).unwrap();
    let ic_b = hm_b.get(b).unwrap();

    for (node_a, score_a) in hm_a {
        if hm_b.contains_key(node_a) {
            if *score_a > max_score {
                max_score = *score_a;
                _max_ancestor = *node_a;
            }
        }
    }

    max_score / max(&ic_a, &ic_b)
}

fn mica_sim_batch(
    hm_parents: &HashMap<String, Vec<String>, ARandomState>,
    hm_scores: &HashMap<String, f64, ARandomState>,
    a: &String,
    b: &String,
) -> f64 {
    if a == b {
        return 1.0;
    }

    trace!("mica_sim_batch -> a:{:?} b:{:?}", a, b);
    let ic_a = hm_scores.get(a).unwrap();
    let ic_b = hm_scores.get(b).unwrap();
    let mut max_score: f64 = 0.0;

    let mut tmp_hs: HashSet<&String, ARandomState> = make_hashset();
    for p in hm_parents.get(a).unwrap() {
        tmp_hs.insert(p);
    }
    let mut bp = "";
    for p in hm_parents.get(b).unwrap() {
        if tmp_hs.contains(p) {
            let s = hm_scores.get(p).unwrap();
            if s > &max_score {
                max_score = *s;
                bp = p;
            }
        }
    }

    trace!("bp -> {:?}", bp);

    #[cfg(debug_assertions)]
    {
        if max_score > *max(&ic_a, &ic_b) {
            trace!("ic_a -> {:?}, ic_b -> {:?}", &ic_a, &ic_b);
            panic!("MICA > 1: {:?}, a -> {:?}, b -> {:?}", max_score, a, b);
        }
    }

    max_score / *max(&ic_a, &ic_b)
}

impl GoUniversal {
    fn recurse_graph_top_down(
        &self,
        map: &mut HashMap<u32, f64>,
        map2: &mut HashMap<u32, usize>,
        gr: &DiGraphMap<u32, i8>,
        deque: &mut VecDeque<u32>,
    ) {
        if let Some(id) = deque.pop_front() {
            // In a directed graph, the incoming edges represent children and outgoing
            // edges are parents.
            let ci_vec: Vec<u32> =
                filtered_neighbors(gr, &self.allowed_rel, petgraph::Direction::Incoming, &id);

            let c: f64 = (ci_vec.len() as u32).into(); // number of children as f64
            let w: f64 = *map.get(&id).unwrap(); // node weight

            // Now we multiply  our current weight and the number of children
            // down into each child node
            for i in &ci_vec {
                // New weight of child as the sum of logs
                *map.get_mut(i).unwrap() += c.ln() + w;
                *map2.get_mut(i).unwrap() -= 1;
            }
            // Check if a child has been visited by all parents
            for i in &ci_vec {
                // If yes, add to node queue
                if map2.get(i).unwrap() == &0 {
                    deque.push_back(*i);
                }
            }
            self.recurse_graph_top_down(map, map2, gr, deque);
        }
    }

    fn recurse_graph_bottom_up(
        &self,
        gr: &DiGraphMap<u32, i8>,
        gr2: &mut DiGraphMap<u32, i8>,
        id: &u32,
    ) {
        // In a directed graph, the incoming edges represent children and outgoing
        // edges are parents.
        let parents: Vec<u32> =
            filtered_neighbors(gr, &self.allowed_rel, petgraph::Direction::Outgoing, &id);

        for p in &parents {
            gr2.add_edge(*id, *p, GoRelationship::IsA as i8);
            for s in filtered_neighbors(gr, &self.allowed_rel, petgraph::Direction::Incoming, p) {
                gr2.add_edge(s, *p, GoRelationship::IsA as i8);
            }
        }
        for p in &parents {
            self.recurse_graph_bottom_up(gr, gr2, p);
        }
    }
}

impl SemanticSimilarity for GoUniversal {
    fn cache_ics(
        &self,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<(), Box<dyn Error>> {
        if !rd_client.is_some() {
            return Ok(());
        }

        let mut con = rd_client.unwrap().con()?;

        let key = format!("{}_icgou_is_cached", annotation.db_prefix());
        if con.exists(&key)? {
            return Ok(());
        }

        // Get all GO terms known in the annotation and the graph
        // structure
        let terms: Vec<u32> = annotation.all_ids()?;
        let graph = annotation.graph(None, None)?;

        // Initialize a hashmap with all GO terms to contain
        // ln(1)
        let mut hmap_scores: HashMap<u32, f64> = HashMap::with_capacity(terms.len());
        let mut hmap_nparents: HashMap<u32, usize> = HashMap::with_capacity(terms.len());
        let mut deque: VecDeque<u32> = VecDeque::with_capacity(1024);
        for term in terms {
            hmap_scores.insert(term.clone(), 0.0);
            hmap_nparents.insert(
                term,
                filtered_neighbors(
                    &graph,
                    &self.allowed_rel,
                    petgraph::Direction::Outgoing,
                    &term,
                )
                .len(),
            );
        }

        // Iterate over each annotation root (MF, BP, CC) with top-down algorithm
        debug!("{:?}", annotation.root_ids()?);
        for root in annotation.root_ids()? {
            deque.push_back(*root);
            self.recurse_graph_top_down(&mut hmap_scores, &mut hmap_nparents, &graph, &mut deque);
        }

        for (term, ic) in hmap_scores.iter() {
            let key = format!(
                "{}_icgou_{}",
                annotation.db_prefix(),
                annotation.index_to_id(&term)?.unwrap()
            );
            con.set(&key, ic.clone())?;
        }

        con.set(&key, true)?;

        Ok(())
    }
    fn ic(
        &self,
        term: &String,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
        with_subgraph: &bool,
    ) -> Result<(Option<f64>, Option<HashMap<u32, f64>>), Box<dyn Error>> {
        let key = format!("{}_icgou_{}", &annotation.db_prefix(), term);
        let graph = annotation.graph(None, None)?;
        let id = annotation.id_to_index(&term)?;

        if id.is_none() {
            return Ok((None, None));
        }
        // Check if we have IC cached in Redis
        match rd_client {
            Some(cli) => {
                let mut con = cli.con()?;
                if con.exists(&key)? {
                    let ret: f64 = con.get(&key)?;
                    if !*with_subgraph {
                        return Ok((Some(ret), None));
                    } else {
                        let mut hmap: HashMap<u32, f64> = HashMap::new();
                        hmap.insert(id.unwrap().clone(), ret);
                        for p in annotation
                            .parents(term, false, true, false, &graph, rd_client)?
                            .unwrap()
                            .graph
                        {
                            let t = &annotation.index_to_id(&p)?.unwrap();
                            let key2 = format!("{}_icgou_{}", &annotation.db_prefix(), &t);
                            if con.exists(&key2)? {
                                hmap.insert(p, con.get(&key2)?);
                            } else {
                                // Theoretically redis always has the data  cached
                                // if we are in this code block, so getting it from there
                                // would be faster
                                let ic = self.ic(&t, Box::new(*annotation), rd_client, &false)?;
                                hmap.insert(p, ic.0.unwrap());
                            }
                        }
                        return Ok((Some(ret), Some(hmap)));
                    }
                }
            }
            None => (),
        };

        // Go graph with bottom up algorithm
        let mut hashmap_scores: HashMap<u32, f64> = HashMap::new();
        let mut hashmap_parents: HashMap<u32, usize> = HashMap::new();
        let mut deque: VecDeque<u32> = VecDeque::new();
        let mut subgraph: DiGraphMap<u32, i8> = DiGraphMap::new();

        self.recurse_graph_bottom_up(&graph, &mut subgraph, &id.unwrap());
        let ann_roots = annotation.root_ids()?;

        for node in subgraph.nodes() {
            if ann_roots.contains(&node) {
                deque.push_back(node);
                self.recurse_graph_top_down(
                    &mut hashmap_scores,
                    &mut hashmap_parents,
                    &subgraph,
                    &mut deque,
                );
                break;
            }
        }

        if *with_subgraph {
            Ok((
                Some(hashmap_scores.get(&id.unwrap()).unwrap().clone()),
                Some(hashmap_scores),
            ))
        } else {
            Ok((
                Some(hashmap_scores.get(&id.unwrap()).unwrap().clone()),
                None,
            ))
        }
    }
    fn term_pairwise(
        &self,
        term_a: &String,
        term_b: &String,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<f64>, Box<dyn Error>> {
        let id_a = (*annotation).id_to_index(&term_a)?;
        let id_b = (*annotation).id_to_index(&term_b)?;

        if id_a.is_none() {
            return Ok(None);
        }
        if id_b.is_none() {
            return Ok(None);
        }

        if term_a == term_b {
            return Ok(Some(1.0));
        }

        let (_ic_a, graph_a) = self.ic(&term_a, Box::new(*annotation), rd_client, &true)?;
        let (_ic_b, graph_b) = self.ic(&term_b, Box::new(*annotation), rd_client, &true)?;

        let sim = mica_sim(
            &graph_a.unwrap(),
            &graph_b.unwrap(),
            &id_a.unwrap(),
            &id_b.unwrap(),
        );

        Ok(Some(sim))
    }

    fn term_groupwise(
        &self,
        terms_a: &Vec<String>,
        terms_b: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Array2<f64>>, Box<dyn Error>> {
        trace!("term_groupwise() -> a:{:?}, b:{:?}", terms_a, terms_b);
        let mut ids_a: Vec<String> = Vec::with_capacity(terms_a.len());
        for t in terms_a {
            match annotation.filter_terms(t)? {
                Some(hit) => ids_a.push(hit),
                None => (),
            };
        }
        let mut ids_b: Vec<String> = Vec::with_capacity(terms_b.len());
        for t in terms_b {
            match annotation.filter_terms(t)? {
                Some(hit) => ids_b.push(hit),
                None => (),
            };
        }

        trace!("term_groupwise() -> ia:{:?}, ib:{:?}", ids_a, ids_b);

        if ids_a.is_empty() {
            return Ok(None);
        }
        if ids_b.is_empty() {
            return Ok(None);
        }

        let graph = annotation.graph(None, None)?;

        let mut parents: HashMap<String, Vec<String>, ARandomState> = make_hashmap();
        let mut p_union: HashMap<String, f64, ARandomState> = make_hashmap();

        // Get all parents first, process their IC
        for term in ids_a.iter().chain(ids_b.iter()) {
            let p_tmp = annotation
                .parents(term, false, true, false, &graph, rd_client)?
                .unwrap()
                .graph;
            let p_tmp_terms = annotation.index_to_id_vunsafe(&p_tmp)?;
            trace!("p_tmp_terms->{:?}", p_tmp_terms);
            for p_term in &p_tmp_terms {
                if !p_union.contains_key(&term.clone()) {
                    trace!("term_groupwise->ic->{:?}", term);
                    let (ic, _termmap) = self.ic(term, Box::new(*annotation), rd_client, &false)?;
                    p_union.insert(term.to_string(), ic.unwrap());
                }
                if !p_union.contains_key(p_term) {
                    trace!("term_groupwise->pic->{:?}", p_term);
                    let (ic, _termmap) =
                        self.ic(p_term, Box::new(*annotation), rd_client, &false)?;
                    p_union.insert(p_term.clone(), ic.unwrap());
                }
            }
            parents.insert(term.to_string(), p_tmp_terms);
        }

        let mut arr: Array2<f64> = Array2::zeros((ids_a.len(), ids_b.len()));
        for (i, row) in ids_a.iter().enumerate() {
            for (j, col) in ids_b.iter().enumerate() {
                let ns1 = annotation.get_namespace(row);
                let ns2 = annotation.get_namespace(col);

                if ns1 != ns2 {
                    arr[[i, j]] = 0.0
                } else if row == col {
                    arr[[i, j]] = 1.0;
                } else {
                    arr[[i, j]] = mica_sim_batch(&parents, &p_union, row, col);
                }
            }
        }
        Ok(Some(arr))
    }

    fn term_groupwise_sym(
        &self,
        terms: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Array2<f64>>, Box<dyn Error>> {
        let mut ids: Vec<String> = Vec::with_capacity(terms.len());
        for t in terms {
            match annotation.filter_terms(t)? {
                Some(hit) => ids.push(hit),
                None => (),
            };
        }

        if ids.is_empty() {
            return Ok(None);
        }

        let graph = annotation.graph(None, None)?;

        let mut parents: HashMap<String, Vec<String>, ARandomState> = make_hashmap();
        let mut p_union: HashMap<String, f64, ARandomState> = make_hashmap();

        // Get all parents first, process their IC
        for term in ids.iter() {
            let p_tmp = annotation
                .parents(term, false, true, false, &graph, rd_client)?
                .unwrap_or(Edges {
                    graph: Vec::new(),
                    weights: None,
                    is_graph: true,
                })
                .graph;
            let p_tmp_terms = annotation.index_to_id_vunsafe(&p_tmp)?;
            trace!("p_tmp_terms->{:?}", p_tmp_terms);
            for p_term in &p_tmp_terms {
                if !p_union.contains_key(&term.clone()) {
                    trace!("term_groupwise->ic->{:?}", term);
                    let (ic, _termmap) = self.ic(term, Box::new(*annotation), rd_client, &false)?;
                    p_union.insert(term.to_string(), ic.unwrap());
                }
                if !p_union.contains_key(p_term) {
                    trace!("term_groupwise->pic->{:?}", p_term);
                    let (ic, _termmap) =
                        self.ic(p_term, Box::new(*annotation), rd_client, &false)?;
                    p_union.insert(p_term.clone(), ic.unwrap());
                }
            }
            parents.insert(term.to_string(), p_tmp_terms);
        }

        let mut arr: Array2<f64> = Array2::zeros((ids.len(), ids.len()));
        arr.diag_mut().fill(1.0);
        for i in 1..ids.len() {
            for j in 0..i {
                let row = &ids[i];
                let col = &ids[j];

                let ns1 = annotation.get_namespace(row);
                let ns2 = annotation.get_namespace(col);

                if ns1 != ns2 {
                    arr[[i, j]] = 0.0
                } else {
                    let v = mica_sim_batch(&parents, &p_union, row, col);
                    arr[[i, j]] = v;
                    arr[[j, i]] = v;
                }
            }
        }
        Ok(Some(arr))
    }

    fn genes_pairwise(
        &self,
        gene_a: &String,
        gene_b: &String,
        annotation: Box<&dyn Annotation>,
        mapping: &Mapping,
        merger: &String,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<f64>, Box<dyn Error>> {
        let term_a = mapping.get_terms(gene_a)?;
        let term_b = mapping.get_terms(gene_b)?;

        if term_a.is_none() {
            return Ok(None);
        }
        if term_b.is_none() {
            return Ok(None);
        }

        let a: Vec<String> = term_a.unwrap().iter().cloned().collect();
        let b: Vec<String> = term_b.unwrap().iter().cloned().collect();

        let resmat = self.term_groupwise(&a, &b, annotation, rd_client)?;
        trace!("genes_pairwise-> resmat-> {:?}", resmat);

        if resmat.is_none() {
            return Ok(None);
        }

        let mut ret: f64 = 0.0;
        if merger == &String::from("BMA") {
            ret = bma(&resmat.unwrap())?;
        }

        Ok(Some(ret))
    }

    fn genes_groupwise(
        &self,
        genes_a: &Vec<String>,
        genes_b: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        mapping: &Mapping,
        merger: &String,
        rd_client: Option<&rd::Client>,
    ) -> Result<Option<Array2<f64>>, Box<dyn Error>> {
        let mut hs_a: HashMap<String, Vec<String>, ARandomState> = make_hashmap();
        let mut hs_b: HashMap<String, Vec<String>, ARandomState> = make_hashmap();
        let mut ind_a: HashMap<String, Vec<usize>, ARandomState> = make_hashmap();
        let mut ind_b: HashMap<String, Vec<usize>, ARandomState> = make_hashmap();

        for g in genes_a {
            let lookup = mapping.get_terms(g)?;
            if lookup.is_some() {
                for term in lookup.unwrap() {
                    match annotation.filter_terms(&term)? {
                        Some(hit) => {
                            let v = hs_a.entry(hit).or_insert(Vec::new());
                            v.push(g.clone());
                        }
                        None => (),
                    };
                }
            }
        }
        for g in genes_b {
            let lookup = mapping.get_terms(g)?;
            if lookup.is_some() {
                for term in lookup.unwrap() {
                    match annotation.filter_terms(&term)? {
                        Some(hit) => {
                            let v = hs_b.entry(hit).or_insert(Vec::new());
                            v.push(g.clone());
                        }
                        None => (),
                    };
                }
            }
        }

        if hs_a.is_empty() || hs_b.is_empty() {
            return Ok(None);
        }

        let mut a: Vec<String> = Vec::with_capacity(hs_a.len());
        let mut b: Vec<String> = Vec::with_capacity(hs_b.len());

        for (i, (term, v_genes)) in hs_a.iter().enumerate() {
            a.push(term.clone());
            for g in v_genes {
                let v = ind_a.entry(g.clone()).or_insert(Vec::new());
                v.push(i);
            }
        }

        for (i, (term, v_genes)) in hs_b.iter().enumerate() {
            b.push(term.clone());
            for g in v_genes {
                let v = ind_b.entry(g.clone()).or_insert(Vec::new());
                v.push(i);
            }
        }

        let resmat = self.term_groupwise(&a, &b, annotation, rd_client)?;
        if resmat.is_none() {
            return Ok(None);
        }

        let resmat = resmat.unwrap();
        let mut resgenes: Array2<f64> = Array2::zeros((genes_a.len(), genes_b.len()));

        for (i, row) in genes_a.iter().enumerate() {
            for (j, col) in genes_b.iter().enumerate() {
                let ia = ind_a.get(row);
                let ib = ind_b.get(col);

                if ia.is_none() {
                    continue;
                }
                if ib.is_none() {
                    continue;
                }
                let mut ret: f64 = 0.0;
                if merger == &String::from("BMA") {
                    ret = bma_submatrix(&resmat, ia.unwrap(), ib.unwrap())?;
                }
                resgenes[[i, j]] = ret;
            }
        }

        Ok(None)
    }

    fn genes_groupwise_sym(
        &self,
        genes: &Vec<String>,
        annotation: Box<&dyn Annotation>,
        mapping: &Mapping,
        merger: &String,
        rd_client: Option<&rd::Client>,
        namespace: &Option<String>,
    ) -> Result<Option<Array1<f64>>, Box<dyn Error>> {
        let mut hs: HashMap<String, Vec<String>, ARandomState> = make_hashmap();
        let mut ind: HashMap<String, Vec<usize>, ARandomState> = make_hashmap();

        let ns: i8 = match namespace {
            Some(s) => guess_go_namespace(s) as i8,
            None => GoNamespace::NoCat as i8,
        };

        for g in genes {
            let lookup = mapping.get_terms(g)?;
            if lookup.is_some() {
                for term in lookup.unwrap() {
                    match annotation.filter_terms(&term)? {
                        Some(hit) => {
                            let xns = annotation.get_namespace(&hit).unwrap_or(-1);
                            if ns == (GoNamespace::NoCat as i8) || xns == ns {
                                let v = hs.entry(hit.clone()).or_insert(Vec::new());
                                v.push(g.clone());
                                trace!("Some gene {:?} -> {:?}", g, hit.clone());
                            }
                        }
                        None => (),
                    };
                }
            } else {
                trace!("None gene {:?}", g);
            }
        }

        if hs.is_empty() {
            return Ok(None);
        }

        let mut v: Vec<String> = Vec::with_capacity(hs.len());

        for (i, (term, v_genes)) in hs.iter().enumerate() {
            v.push(term.clone());
            for g in v_genes {
                let v = ind.entry(g.clone()).or_insert(Vec::new());
                v.push(i);
            }
        }

        trace!("{:?}", hs);
        trace!("{:?}", ind);

        debug!("genes_groupwise_sym -> getting pairwise MICA");
        trace!("{:?}", v);

        let resmat = self.term_groupwise_sym(&v, annotation, rd_client)?;
        if resmat.is_none() {
            return Ok(None);
        }

        let resmat = resmat.unwrap();

        debug!(
            "genes_groupwise_sym -> resmat ({:?}, {:?})",
            resmat.nrows(),
            resmat.ncols()
        );

        trace!("{:?}", resmat);

        let ng = genes.len();
        let mut resgenes: Array1<f64> = Array1::zeros((ng * (ng - 1)) / 2);

        for i in 1..genes.len() {
            for j in 0..i {
                let row = &genes[i];
                let col = &genes[j];

                let ia = ind.get(row);
                let ib = ind.get(col);

                if ia.is_none() {
                    continue;
                }
                if ib.is_none() {
                    continue;
                }

                trace!("genes_groupwise_sym -> row {:?}, col {:?}", row, col);

                let mut ret: f64 = 0.0;
                if merger == &String::from("BMA") {
                    ret = bma_submatrix(&resmat, ia.unwrap(), ib.unwrap())?;
                }

                #[cfg(debug_assertions)]
                {
                    // Sim is bound in [0, 1]. Check in debug mode
                    if ret > 1.0 || ret < 0.0 {
                        panic!("Sim > 1.0");
                    }
                }

                let index = (i * (i - 1)) / 2 + j;
                resgenes[index] = ret;
            }
        }

        Ok(Some(resgenes))
    }
}

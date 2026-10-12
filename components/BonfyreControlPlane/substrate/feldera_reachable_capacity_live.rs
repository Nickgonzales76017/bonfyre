// The live ReachableCapacity engine: reads real facts from stdin, maintains the
// relation, emits the reachable opportunity set. The Python bridge feeds it the
// current resolved-blocker facts backfilled from the fabric and control plane;
// this computes which opportunities are reachable, incrementally, in DBSP.
//
// stdin format, tab-separated, one fact per line:
//   B<TAB>opp<TAB>blocker_id     structure: opp has this blocker
//   R<TAB>blocker_id             this blocker is currently resolved
//
// stdout: {"relation":"ReachableCapacity","reachable":[...],"count":N}

use std::io::BufRead;

use dbsp::typed_batch::IndexedZSetReader;
use dbsp::utils::Tup2;
use dbsp::{Runtime, ZWeight};

fn main() {
    let mut blockers: Vec<(String, String)> = Vec::new();
    let mut resolved: Vec<String> = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let mut p = line.split('\t');
        match p.next() {
            Some("B") => {
                let o = p.next().unwrap_or("").to_string();
                let b = p.next().unwrap_or("").to_string();
                if !o.is_empty() && !b.is_empty() {
                    blockers.push((o, b));
                }
            }
            Some("R") => {
                let b = p.next().unwrap_or("").to_string();
                if !b.is_empty() {
                    resolved.push(b);
                }
            }
            _ => {}
        }
    }

    let (mut circuit, (blocker_in, resolved_in, reachable_out)) =
        Runtime::init_circuit(1, |circuit| {
            let (blocker, blocker_in) = circuit.add_input_zset::<Tup2<String, String>>();
            let (resolved_s, resolved_in) = circuit.add_input_zset::<String>();
            let req_by_b = blocker.map_index(|Tup2(opp, b)| (b.clone(), opp.clone()));
            let resolved_idx = resolved_s.map_index(|b| (b.clone(), ()));
            let open = req_by_b.antijoin(&resolved_idx);
            let blocked_idx = open.map_index(|(_b, opp)| (opp.clone(), ()));
            let all_idx = blocker.map_index(|Tup2(opp, _b)| (opp.clone(), ()));
            let reachable = all_idx
                .antijoin(&blocked_idx)
                .distinct()
                .map(|(opp, _v)| opp.clone())
                .integrate();
            Ok((blocker_in, resolved_in, reachable.output()))
        })
        .expect("build ReachableCapacity circuit");

    let mut bf: Vec<Tup2<Tup2<String, String>, ZWeight>> = blockers
        .iter()
        .map(|(o, b)| Tup2(Tup2(o.clone(), b.clone()), 1))
        .collect();
    let mut rf: Vec<Tup2<String, ZWeight>> =
        resolved.iter().map(|b| Tup2(b.clone(), 1)).collect();
    blocker_in.append(&mut bf);
    resolved_in.append(&mut rf);
    circuit.transaction().expect("apply facts");

    // the consolidated output of the integrated reachable set is exactly the
    // current reachable opportunities, one positive-weight entry each.
    // typed iter over the consolidated reachable set: (opp, (), weight)
    let mut result: Vec<String> = Vec::new();
    for (opp, _v, w) in reachable_out.consolidate().iter() {
        if w > 0 {
            result.push(opp);
        }
    }
    result.sort();

    let items: Vec<String> = result.iter().map(|s| format!("\"{s}\"")).collect();
    println!(
        "{{\"relation\":\"ReachableCapacity\",\"reachable\":[{}],\"count\":{}}}",
        items.join(","),
        result.len()
    );
}

// A persistent, delta-fed ReachableCapacity circuit. Built once; then each line
// on stdin is a single change-delta applied as one DBSP transaction, and the
// current reachable set is emitted after it. No reseed, no recompute -- a proof
// reheat is one -1 and the reachable set contracts incrementally.
//
// stdin, tab-separated, one delta per line:
//   B<TAB>opp<TAB>blocker_id     add blocker structure (opp has this blocker)
//   +<TAB>blocker_id             a blocker became resolved   (+1)
//   -<TAB>blocker_id             a resolution was retracted  (-1, reheat/revoke)
//
// stdout: one JSON line per delta:
//   {"step":N,"delta":"...","reachable":[...],"count":M}

use std::io::{BufRead, Write};

use dbsp::typed_batch::IndexedZSetReader;
use dbsp::utils::Tup2;
use dbsp::{Runtime, ZWeight};

fn main() {
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

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut step: u64 = 0;

    for line in std::io::stdin().lock().lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            _ => continue,
        };
        let mut p = line.split('\t');
        let tag = p.next().unwrap_or("");
        let applied: &str = match tag {
            "B" => {
                let opp = p.next().unwrap_or("").to_string();
                let b = p.next().unwrap_or("").to_string();
                if opp.is_empty() || b.is_empty() {
                    continue;
                }
                blocker_in.append(&mut vec![Tup2(Tup2(opp, b), 1 as ZWeight)]);
                "structure"
            }
            "+" => {
                let b = p.next().unwrap_or("").to_string();
                if b.is_empty() {
                    continue;
                }
                resolved_in.append(&mut vec![Tup2(b, 1 as ZWeight)]);
                "resolve"
            }
            "-" => {
                let b = p.next().unwrap_or("").to_string();
                if b.is_empty() {
                    continue;
                }
                resolved_in.append(&mut vec![Tup2(b, -1 as ZWeight)]);
                "retract"
            }
            _ => continue,
        };

        circuit.transaction().expect("apply delta");

        let mut reachable: Vec<String> = Vec::new();
        for (opp, _v, w) in reachable_out.consolidate().iter() {
            if w > 0 {
                reachable.push(opp);
            }
        }
        reachable.sort();
        let items: Vec<String> = reachable.iter().map(|s| format!("\"{s}\"")).collect();
        writeln!(
            out,
            "{{\"step\":{step},\"delta\":\"{applied}\",\"reachable\":[{}],\"count\":{}}}",
            items.join(","),
            reachable.len()
        )
        .ok();
        out.flush().ok();
        step += 1;
    }
}

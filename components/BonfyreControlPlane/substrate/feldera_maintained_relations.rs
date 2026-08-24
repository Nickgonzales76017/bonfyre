// Generic maintained-relations engine -- one long-lived DBSP circuit that
// maintains MANY named consequence relations with +1/-1, instead of a binary per
// relation. Each relation is a gated set: an item is a member while its gate
// holds. Feed the control plane's deltas; a gate going down (-1) withdraws every
// member it gated, incrementally, across every relation at once.
//
// This is the SS2 family that fits the single-gate shape -- EligibleProviders
// (capability while provider up), CurrentAuthority (grant while not revoked),
// AddressAdvertisements (route while eligible), FormReadiness (form while slots
// filled), ResidentModels (model while loaded), etc. -- folded into one engine.
//
// stdin, tab-separated, one delta per line:
//   S<TAB>rel<TAB>item<TAB>gate   structure: (rel, item) is gated by gate
//   +<TAB>gate                    gate holds   (+1)
//   -<TAB>gate                    gate withdrawn (-1)
//
// stdout, one JSON line per delta: {"step":N,"delta":"...","relations":{rel:[items]},"count":M}

use std::collections::BTreeMap;
use std::io::{BufRead, Write};

use dbsp::typed_batch::IndexedZSetReader;
use dbsp::utils::Tup2;
use dbsp::{Runtime, ZWeight};

fn main() {
    let (mut circuit, (struct_in, gate_in, member_out)) =
        Runtime::init_circuit(1, |circuit| {
            // structure: (rel, (item, gate))
            let (structure, struct_in) =
                circuit.add_input_zset::<Tup2<String, Tup2<String, String>>>();
            // gates currently held
            let (held, gate_in) = circuit.add_input_zset::<String>();

            // index structure by gate: gate -> (rel, item)
            let by_gate = structure.map_index(|Tup2(rel, Tup2(item, gate))| {
                (gate.clone(), Tup2(rel.clone(), item.clone()))
            });
            let held_idx = held.map_index(|g| (g.clone(), ()));
            // a member exists while its gate is held; integrate to the cumulative set
            let member = by_gate
                .join(&held_idx, |_g, ri, _| ri.clone())
                .integrate();
            Ok((struct_in, gate_in, member.output()))
        })
        .expect("build maintained-relations circuit");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut step: u64 = 0;

    for line in std::io::stdin().lock().lines() {
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            _ => continue,
        };
        let mut p = line.split('\t');
        let applied: &str = match p.next().unwrap_or("") {
            "S" => {
                let rel = p.next().unwrap_or("").to_string();
                let item = p.next().unwrap_or("").to_string();
                let gate = p.next().unwrap_or("").to_string();
                if rel.is_empty() || item.is_empty() || gate.is_empty() {
                    continue;
                }
                struct_in.append(&mut vec![Tup2(Tup2(rel, Tup2(item, gate)), 1 as ZWeight)]);
                "structure"
            }
            "+" => {
                let gate = p.next().unwrap_or("").to_string();
                if gate.is_empty() {
                    continue;
                }
                gate_in.append(&mut vec![Tup2(gate, 1 as ZWeight)]);
                "gate_up"
            }
            "-" => {
                let gate = p.next().unwrap_or("").to_string();
                if gate.is_empty() {
                    continue;
                }
                gate_in.append(&mut vec![Tup2(gate, -1 as ZWeight)]);
                "gate_down"
            }
            _ => continue,
        };
        circuit.transaction().expect("apply delta");

        // group current members by relation
        let mut rels: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut total = 0u64;
        for (ri, _v, w) in member_out.consolidate().iter() {
            if w > 0 {
                rels.entry(ri.0.clone()).or_default().push(ri.1.clone());
                total += 1;
            }
        }
        let body: Vec<String> = rels
            .iter()
            .map(|(rel, items)| {
                let mut items = items.clone();
                items.sort();
                let arr: Vec<String> = items.iter().map(|s| format!("\"{s}\"")).collect();
                format!("\"{rel}\":[{}]", arr.join(","))
            })
            .collect();
        writeln!(
            out,
            "{{\"step\":{step},\"delta\":\"{applied}\",\"relations\":{{{}}},\"count\":{total}}}",
            body.join(",")
        )
        .ok();
        out.flush().ok();
        step += 1;
    }
}

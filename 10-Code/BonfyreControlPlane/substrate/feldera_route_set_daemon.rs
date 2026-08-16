// Route advertise / withdraw over the DBSP delta engine -- the last forwarding
// layer. A provider advertises capability routes; a route is UP only while its
// provider is up (healthy AND authorized). A provider going down is one -1 and
// every route through it withdraws incrementally -- control-plane convergence,
// the same primitive the reachability daemon uses.
//
// stdin, tab-separated, one delta per line:
//   A<TAB>capability<TAB>provider   advertise a route (structure)
//   +<TAB>provider                  provider up   (+1)
//   -<TAB>provider                  provider down (-1: failure / revoke / drain)
//
// stdout, one JSON line per delta:
//   {"step":N,"delta":"...","routes":["cap@provider",...],"count":M}

use std::io::{BufRead, Write};

use dbsp::typed_batch::IndexedZSetReader;
use dbsp::utils::Tup2;
use dbsp::{Runtime, ZWeight};

fn main() {
    let (mut circuit, (adv_in, up_in, routes_out)) =
        Runtime::init_circuit(1, |circuit| {
            let (advertises, adv_in) = circuit.add_input_zset::<Tup2<String, String>>();
            let (up, up_in) = circuit.add_input_zset::<String>();
            // (provider -> capability)
            let adv_by_prov = advertises.map_index(|Tup2(cap, prov)| (prov.clone(), cap.clone()));
            let up_idx = up.map_index(|prov| (prov.clone(), ()));
            // a route exists while its provider is up
            let routes = adv_by_prov
                .join(&up_idx, |prov, cap, _up| Tup2(cap.clone(), prov.clone()))
                .integrate();
            Ok((adv_in, up_in, routes.output()))
        })
        .expect("build route-set circuit");

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
            "A" => {
                let cap = p.next().unwrap_or("").to_string();
                let prov = p.next().unwrap_or("").to_string();
                if cap.is_empty() || prov.is_empty() {
                    continue;
                }
                adv_in.append(&mut vec![Tup2(Tup2(cap, prov), 1 as ZWeight)]);
                "advertise"
            }
            "+" => {
                let prov = p.next().unwrap_or("").to_string();
                if prov.is_empty() {
                    continue;
                }
                up_in.append(&mut vec![Tup2(prov, 1 as ZWeight)]);
                "up"
            }
            "-" => {
                let prov = p.next().unwrap_or("").to_string();
                if prov.is_empty() {
                    continue;
                }
                up_in.append(&mut vec![Tup2(prov, -1 as ZWeight)]);
                "down"
            }
            _ => continue,
        };
        circuit.transaction().expect("apply delta");

        let mut routes: Vec<String> = Vec::new();
        for (route, _v, w) in routes_out.consolidate().iter() {
            if w > 0 {
                routes.push(format!("{}@{}", route.0, route.1));
            }
        }
        routes.sort();
        let items: Vec<String> = routes.iter().map(|s| format!("\"{s}\"")).collect();
        writeln!(
            out,
            "{{\"step\":{step},\"delta\":\"{applied}\",\"routes\":[{}],\"count\":{}}}",
            items.join(","),
            routes.len()
        )
        .ok();
        out.flush().ok();
        step += 1;
    }
}

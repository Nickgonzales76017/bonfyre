// ReachableCapacity as a maintained DBSP relation -- not a recomputed snapshot.
//
// Source relations:
//   blocker(opp, blocker_id)  static: an opportunity and each blocker it has
//   resolved(blocker_id)      dynamic: which blockers are currently resolved,
//                             driven by proof / authority / work / verification
//                             facts as +1 (resolved) and -1 (reheat / revoke).
//
// Derived, incrementally:
//   open(opp)      = a blocker of opp is not resolved      (antijoin)
//   reachable(opp) = opp has no open blocker               (all - blocked)
//
// A retraction of a resolved fact reopens its blocker, which reblocks its
// opportunity, which withdraws it from reachable -- with no rebuild. This is the
// point where reachability stops being computed and starts being maintained.

use dbsp::utils::Tup2;
use dbsp::{Runtime, ZSet, ZWeight};

fn main() {
    // static blocker structure: the FPQ->SLI->KV organism plus a couple gated ones
    let blocker_facts: Vec<(&str, &str)> = vec![
        ("organism", "fpq_proven"),
        ("organism", "sli_proven"),
        ("organism", "kv_proven"),
        ("acm", "acm_verified"),
        ("acm", "acm_human"),
        ("celld", "celld_authority"),
    ];

    let (mut circuit, (blocker_in, resolved_in, reachable_out, organism_out)) =
        Runtime::init_circuit(1, |circuit| {
        // feed the raw delta streams to the incremental operators; DBSP maintains
        // the traces. Integrate only at the end, for the cumulative output.
        let (blocker, blocker_in) = circuit.add_input_zset::<Tup2<String, String>>();
        let (resolved, resolved_in) = circuit.add_input_zset::<String>();

        // (blocker_id -> opp)
        let req_by_b = blocker.map_index(|Tup2(opp, b)| (b.clone(), opp.clone()));
        // (blocker_id -> ())
        let resolved_idx = resolved.map_index(|b| (b.clone(), ()));
        // open blockers: rows whose blocker_id is not resolved
        let open = req_by_b.antijoin(&resolved_idx);

        // opps with >=1 open blocker, indexed by opp
        let blocked_idx = open.map_index(|(_b, opp)| (opp.clone(), ()));
        // all opps, indexed by opp
        let all_idx = blocker.map_index(|Tup2(opp, _b)| (opp.clone(), ()));
        // reachable = all opps that are not blocked; integrate the derived delta
        // stream into the current cumulative reachable set.
        let reachable_idx = all_idx.antijoin(&blocked_idx).distinct();
        let reachable = reachable_idx.map(|(opp, _v)| opp.clone()).integrate();
        let organism = reachable.filter(|o: &String| o == "organism");

        Ok((blocker_in, resolved_in, reachable.output(), organism.output()))
    })
    .expect("build ReachableCapacity circuit");

    // load the static blocker structure once (it integrates and persists)
    let mut bf: Vec<Tup2<Tup2<String, String>, ZWeight>> = blocker_facts
        .iter()
        .map(|(o, b)| Tup2(Tup2(o.to_string(), b.to_string()), 1 as ZWeight))
        .collect();
    blocker_in.append(&mut bf);
    circuit.transaction().expect("load blockers");

    // stream of resolution facts: prove the organism, verify acm partially, then
    // reheat SLI (retract) and watch the organism withdraw.
    let steps: Vec<(&str, ZWeight)> = vec![
        ("fpq_proven", 1),
        ("sli_proven", 1),
        ("kv_proven", 1),
        ("acm_verified", 1),
        ("sli_proven", -1), // reheat
    ];

    let mut reachable_count: Vec<i64> = vec![reachable_out.consolidate().weighted_count() as i64];
    let mut organism_reachable: Vec<i64> = vec![organism_out.consolidate().weighted_count() as i64];
    for (blocker, weight) in steps {
        resolved_in.append(&mut vec![Tup2(blocker.to_string(), weight)]);
        circuit.transaction().expect("resolution delta");
        reachable_count.push(reachable_out.consolidate().weighted_count() as i64);
        organism_reachable.push(organism_out.consolidate().weighted_count() as i64);
    }

    // organism becomes reachable only when fpq+sli+kv all resolved, then the SLI
    // reheat withdraws it -- maintained, not recomputed.
    assert_eq!(organism_reachable, vec![0, 0, 0, 1, 1, 0]);

    println!(
        "{{\"engine\":\"feldera-dbsp\",\"relation\":\"ReachableCapacity\",\"reachable_count\":{:?},\"organism_reachable\":{:?},\"maintained\":\"organism withdrawn incrementally on SLI reheat\",\"state\":\"passed\"}}",
        reachable_count, organism_reachable
    );
}

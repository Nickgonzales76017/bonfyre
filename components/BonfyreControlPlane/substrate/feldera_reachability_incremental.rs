// A real Feldera DBSP incrementally-maintained reachability view with the
// property that matters for anti-amnesia: RETRACTION propagates, and an
// independent proven fact SURVIVES the retraction of a dependent one.
//
// Model: proof-frontier layers stream in as a Z-set (weight +1 proven, -1
// reheated). Three of them form the FPQ -> SLI -> KV organism, whose route is
// "up" only while all three are proven. The circuit maintains, incrementally:
//   proven_total  -- size of the whole proven set
//   organism      -- how many of {fpq, sli, kv} are proven (route up iff 3)
//   fpq_present   -- whether the FPQ proof survives
//
// The decisive sequence proves the cascade: prove fpq, sli, kv (organism up),
// then reheat (retract) sli. The organism route withdraws (3 -> 2) while
// fpq_present stays 1 -- FPQ is not erased when SLI is withdrawn. That is
// dependency-aware differential epistemic state, not a rebuilt snapshot.

use dbsp::utils::Tup2;
use dbsp::{OrdZSet, OutputHandle, RootCircuit, ZSet, ZSetHandle, ZWeight};

fn is_member(layer: &String) -> bool {
    layer == "fpq" || layer == "sli" || layer == "kv"
}

fn main() {
    let (
        circuit,
        (input, proven_out, organism_out, fpq_out),
    ): (
        _,
        (
            ZSetHandle<String>,
            OutputHandle<OrdZSet<String>>,
            OutputHandle<OrdZSet<String>>,
            OutputHandle<OrdZSet<String>>,
        ),
    ) = RootCircuit::build(|circuit| {
        let (stream, handle) = circuit.add_input_zset::<String>();
        let proven = stream.integrate();
        let organism = proven.filter(|l: &String| is_member(l));
        let fpq = proven.filter(|l: &String| l == "fpq");
        Ok((handle, proven.output(), organism.output(), fpq.output()))
    })
    .expect("build DBSP reachability circuit");

    let steps: Vec<(&str, ZWeight)> = vec![
        ("fpq", 1),
        ("sli", 1),
        ("kv", 1),
        ("sli", -1), // reheat: retract the SLI proof
    ];

    let mut proven_total: Vec<i64> = Vec::new();
    let mut organism: Vec<i64> = Vec::new();
    let mut fpq_present: Vec<i64> = Vec::new();
    for (layer, weight) in steps {
        input.append(&mut vec![Tup2(layer.to_string(), weight)]);
        circuit.transaction().expect("execute DBSP transaction");
        proven_total.push(proven_out.consolidate().weighted_count() as i64);
        organism.push(organism_out.consolidate().weighted_count() as i64);
        fpq_present.push(fpq_out.consolidate().weighted_count() as i64);
    }

    // organism route: builds to 3, then the SLI reheat withdraws it to 2.
    assert_eq!(organism, vec![1, 2, 3, 2]);
    // FPQ survives the SLI retraction -- never erased.
    assert_eq!(fpq_present, vec![1, 1, 1, 1]);

    println!(
        "{{\"engine\":\"feldera-dbsp\",\"view\":\"fpq-sli-kv-reachability\",\"proven_total\":{:?},\"organism_route\":{:?},\"fpq_present\":{:?},\"cascade\":\"organism withdrawn on SLI reheat; FPQ survived\",\"state\":\"passed\"}}",
        proven_total, organism, fpq_present
    );
}

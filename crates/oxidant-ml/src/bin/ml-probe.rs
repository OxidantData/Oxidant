//! SPIKE (issue #118): `cargo run -p oxidant-ml --bin ml-probe -- <rows.csv> <model.onnx>...`
//!
//! Pass `--dump` to also print every node of each raw graph.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dump = args.iter().any(|a| a == "--dump");
    let mut rest = args.iter().filter(|a| *a != "--dump");
    let rows = rest
        .next()
        .map(|p| oxidant_ml::probe::read_csv(p))
        .unwrap_or_default();
    for path in rest {
        if dump {
            oxidant_ml::probe::dump_graph(path);
        }
        oxidant_ml::probe::score(path, &rows);
    }
}

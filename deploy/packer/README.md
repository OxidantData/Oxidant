# Oxidant Packer AMI (AL2023)

Builds the hardened runtime AMI used by
[`deploy/cloudformation/oxidant-cluster.yaml`](../cloudformation/oxidant-cluster.yaml).

See [`docs/distributed-ec2.md`](../../docs/distributed-ec2.md) for the full bake →
deploy flow.

```sh
cargo build -p oxidant-cli --release
./deploy/packer/build-ami.sh --binary ./target/release/oxidant
```

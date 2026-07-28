# Weft Packer AMI (AL2023)

Builds the hardened runtime AMI used by
[`deploy/cloudformation/weft-cluster.yaml`](../cloudformation/weft-cluster.yaml).

See [`docs/distributed-ec2.md`](../../docs/distributed-ec2.md) for the full bake →
deploy flow.

```sh
cargo build -p weft-cli --release
./deploy/packer/build-ami.sh --binary ./target/release/weft
```

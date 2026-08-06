# Oxidant Packer AMI (AL2023)

Builds the hardened runtime AMI used by
[`deploy/cloudformation/oxidant-cluster.yaml`](../cloudformation/oxidant-cluster.yaml)
and by the free Community AMI on AWS Marketplace (listing in progress).

See [`docs/distributed-ec2.md`](../../docs/distributed-ec2.md) for the full bake →
deploy flow.

```sh
cargo build -p oxidant-cli --release
./deploy/packer/build-ami.sh --binary ./target/release/oxidant
```

`build-ami.sh` exports `GIT_SHA`, which the template stamps into
`/etc/oxidant/VERSION` and the `EngineVersion` AMI tag.

## Boot modes (chosen at first boot by `oxidant-bootstrap`)

| Instance tags | Mode | What starts |
|---------------|------|-------------|
| none | **standalone** (Marketplace single-node) | `oxidant-standalone.service`: Connect on 50051, UI on loopback 4040 |
| `oxidant:role=driver` + cluster tags | cluster driver | `oxidant-driver.service` |
| `oxidant:role=worker` + cluster tags | cluster worker | `oxidant-worker.service` |

The untagged standalone path is what a Marketplace customer gets when they
launch the AMI with zero configuration.

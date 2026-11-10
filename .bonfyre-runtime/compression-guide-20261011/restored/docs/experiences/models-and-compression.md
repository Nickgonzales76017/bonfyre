# Models, compression and cost

Run and examine models with the native tooling. Keep the model's identity, representation, inference cost and fidelity measurements separate so developers can select the right tradeoff for an application.

## Select a model representation

Model selection is a dependency decision as well as an inference decision. The registered model, installed weights, tokenizer, device, representation passport and requested workload must agree.

```sh Terminal
bonfyre-model list
bonfyre-model show <model-id>
bonfyre-model representations <model-id>
bonfyre-model verify <model-id>
```

The model registry describes the available material. It does not manufacture a compatible representation if a file is missing. A remote provider and a local model may satisfy different cost, privacy, accuracy and latency constraints.

## Quantize a specific real tensor

Bonfÿre Quant can encode a tensor from Safetensors into the shared BQFP format. SLI reads the encoded weights; Quant compares the decoded tensor to its original.

```sh Terminal
bonfyre-quant encode ./model.safetensors   --tensor <tensor-name> --bits 3 --out ./tensor.bqfp
bonfyre-sli inspect --model ./tensor.bqfp
bonfyre-sli decode --model ./tensor.bqfp --out ./tensor.f32
bonfyre-quant compare ./model.safetensors   --tensor <tensor-name> --weights ./tensor.f32
```

The shared BQFP contract is owned in the native library, rather than duplicated by the encoder and two readers. A truncated encoded file should fail before writing a result.

## Interpret the numbers

| Metric | What it answers |
|---|---|
| Source weights / encoded bytes | How much space this representation occupies |
| Bits per weight | The effective representation density |
| Cosine and RMSE | How close the reconstructed tensor is to the original |
| Model-level inference output | Whether the full runtime completes the requested task |
| Throughput and resource usage | The actual cost of running this workload |
| Execution and provider receipts | What ran, where, when and under whose authority |

A strong cosine score from one encoded weight matrix is not the same thing as full-model compression or task-level quality. Measure the representation scope requested and name it in the result view.

## Compare transform families

```sh Terminal
bonfyre-fpqx align ./encoder-a.bqfp ./encoder-b.bqfp --out ./alignment
bonfyre-sli bench --in ./vectors.bin --model ./encoder-a.bqfp --n 1000
```

Use two distinct tensors for cross-family alignment; comparing an artifact with itself establishes format/readback correctness, not useful transfer across models. The benchmark requires the actual vector input and encoded representation under test.

## Compress application data

Model compression and record-family compression solve related but different problems. Bonfÿre Compress supports family-aware packing, unpacking and measured savings.

```sh Terminal
bonfyre-compress savings ./receipts
bonfyre-compress family ./receipts --out ./receipts.pack
bonfyre-compress benchmark-structural --db ./registry.db
```

Read the size reduction together with input type, dictionary, output identity and exactness requirements. The recorded representation receipt should tell an application whether data is lossless, approximate or reconstructed.

## Connect inference to business cost

A local model can be selected for a workload with an explicit cost/resource policy. An external model can be selected when its capability is needed. Both routes belong to the same execution, meter and economic owners; the documentation and UI should not maintain their own provider accounting.

The useful result view shows provider, model, representation, inputs, actual duration, measured usage, quality result, accepted cost and receipts. Unknown provider rates are unknown, not zero-dollar calls.

## Compose with applications

A recording workspace may use local speech recognition, a customer site may use its own search and MCP tools, and a finance application may call a model under a budget. None requires customers to know the tensor format. Developers can start here, or follow [recording to workspace](/recording-to-workspace) and [request to books](/request-to-books) to see the integration at application level.

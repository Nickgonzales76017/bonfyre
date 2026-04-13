#!/usr/bin/env python3
"""
fpq_bridge.py — Python ctypes wrapper around libfpq + PyTorch integration.

Usage:
    from fpq_bridge import FPQModel, patch_model

    fpq = FPQModel("model.fpq")
    print(fpq.info())

    # Patch a HuggingFace model to use SLI matmul
    patch_model(hf_model, fpq, prefix="model")
"""
import ctypes
import os
import sys
import platform
from pathlib import Path

import numpy as np

# ─── Locate libfpq ──────────────────────────────────────────────────────────

def _find_libfpq():
    """Find libfpq.dylib / libfpq.so relative to this script or via env."""
    if "LIBFPQ_PATH" in os.environ:
        return os.environ["LIBFPQ_PATH"]

    base = Path(__file__).resolve().parent.parent
    ext = "dylib" if platform.system() == "Darwin" else "so"
    candidates = [
        base / f"libfpq.{ext}",
        base / "build" / f"libfpq.{ext}",
        Path(f"./libfpq.{ext}"),
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    raise FileNotFoundError(
        f"Cannot find libfpq.{ext}. Build with `make libfpq` or set LIBFPQ_PATH."
    )


# ─── C struct mirrors ───────────────────────────────────────────────────────

class FPQInfoC(ctypes.Structure):
    _fields_ = [
        ("n_tensors",       ctypes.c_size_t),
        ("n_sli_tensors",   ctypes.c_size_t),
        ("n_passthrough",   ctypes.c_size_t),
        ("total_params",    ctypes.c_size_t),
        ("format_version",  ctypes.c_uint32),
    ]

class FPQTensorInfoC(ctypes.Structure):
    _fields_ = [
        ("name",    ctypes.c_char_p),
        ("rows",    ctypes.c_size_t),
        ("cols",    ctypes.c_size_t),
        ("has_sli", ctypes.c_int),
        ("bpw",     ctypes.c_float),
    ]


# ─── Load and bind ──────────────────────────────────────────────────────────

_lib = None

def _load_lib():
    global _lib
    if _lib is not None:
        return _lib
    path = _find_libfpq()
    _lib = ctypes.CDLL(path)

    # fpq_open(path) → fpq_model_t*
    _lib.fpq_open.argtypes = [ctypes.c_char_p]
    _lib.fpq_open.restype = ctypes.c_void_p

    # fpq_matmul(model, name, x, y) → int
    _lib.fpq_matmul.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_float), ctypes.POINTER(ctypes.c_float),
    ]
    _lib.fpq_matmul.restype = ctypes.c_int

    # fpq_close(model)
    _lib.fpq_close.argtypes = [ctypes.c_void_p]
    _lib.fpq_close.restype = None

    # fpq_decode_one(model, name, out) → int
    _lib.fpq_decode_one.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_float),
    ]
    _lib.fpq_decode_one.restype = ctypes.c_int

    # fpq_info(model) → FPQInfoC
    _lib.fpq_info.argtypes = [ctypes.c_void_p]
    _lib.fpq_info.restype = FPQInfoC

    # fpq_tensor_at(model, index) → FPQTensorInfoC*
    _lib.fpq_tensor_at.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    _lib.fpq_tensor_at.restype = ctypes.POINTER(FPQTensorInfoC)

    # fpq_tensor_find(model, name) → FPQTensorInfoC*
    _lib.fpq_tensor_find.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    _lib.fpq_tensor_find.restype = ctypes.POINTER(FPQTensorInfoC)

    # fpq_get_passthrough(model, name) → float*
    _lib.fpq_get_passthrough.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    _lib.fpq_get_passthrough.restype = ctypes.POINTER(ctypes.c_float)

    return _lib


# ─── FPQModel: Pythonic wrapper ─────────────────────────────────────────────

class FPQModel:
    """Load a .fpq file and expose SLI matmul + tensor queries."""

    def __init__(self, path):
        self._lib = _load_lib()
        if isinstance(path, Path):
            path = str(path)
        self._path = path
        self._handle = self._lib.fpq_open(path.encode("utf-8"))
        if not self._handle:
            raise RuntimeError(f"fpq_open failed for {path}")
        self._info = self._lib.fpq_info(self._handle)

    def close(self):
        if self._handle:
            self._lib.fpq_close(self._handle)
            self._handle = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ── Info ──────────────────────────────────────────────────────────

    def info(self):
        i = self._info
        return {
            "n_tensors": i.n_tensors,
            "n_sli": i.n_sli_tensors,
            "n_passthrough": i.n_passthrough,
            "total_params": i.total_params,
            "format_version": i.format_version,
        }

    def tensor_names(self):
        """Return list of all tensor names."""
        names = []
        for idx in range(self._info.n_tensors):
            ti = self._lib.fpq_tensor_at(self._handle, idx)
            if ti:
                names.append(ti.contents.name.decode("utf-8"))
        return names

    def tensor_info(self, name):
        """Get info for a single tensor."""
        ti = self._lib.fpq_tensor_find(self._handle, name.encode("utf-8"))
        if not ti:
            return None
        t = ti.contents
        return {
            "name": t.name.decode("utf-8"),
            "rows": t.rows,
            "cols": t.cols,
            "has_sli": bool(t.has_sli),
            "bpw": t.bpw,
        }

    # ── Matmul ────────────────────────────────────────────────────────

    def matmul(self, tensor_name, x_np):
        """
        Compute y = W[tensor_name] @ x via SLI.

        x_np: numpy float32 array of shape [cols]
        Returns: numpy float32 array of shape [rows]
        """
        ti = self.tensor_info(tensor_name)
        if ti is None:
            raise KeyError(f"Tensor '{tensor_name}' not found")

        rows, cols = ti["rows"], ti["cols"]
        x_np = np.ascontiguousarray(x_np, dtype=np.float32)
        if x_np.shape != (cols,):
            raise ValueError(f"Expected x of shape ({cols},), got {x_np.shape}")

        y_np = np.zeros(rows, dtype=np.float32)
        x_ptr = x_np.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        y_ptr = y_np.ctypes.data_as(ctypes.POINTER(ctypes.c_float))

        rc = self._lib.fpq_matmul(
            self._handle, tensor_name.encode("utf-8"), x_ptr, y_ptr
        )
        if rc != 0:
            raise RuntimeError(f"fpq_matmul failed for '{tensor_name}' (rc={rc})")
        return y_np

    # ── Passthrough ──────────────────────────────────────────────────

    def get_passthrough(self, tensor_name):
        """Get raw fp32 for passthrough tensors (norms, biases, embeddings)."""
        ti = self.tensor_info(tensor_name)
        if ti is None:
            raise KeyError(f"Tensor '{tensor_name}' not found")
        rows, cols = ti["rows"], ti["cols"]
        n = rows * cols

        ptr = self._lib.fpq_get_passthrough(
            self._handle, tensor_name.encode("utf-8")
        )
        if not ptr:
            return None  # not a passthrough tensor

        # Copy from borrowed pointer to owned numpy array
        arr = np.ctypeslib.as_array(ptr, shape=(n,)).copy()
        if cols > 1:
            return arr.reshape(rows, cols)
        return arr

    # ── Decode (escape hatch) ────────────────────────────────────────

    def decode_tensor(self, tensor_name):
        """Full decode of one tensor to fp32. Slow but exact."""
        ti = self.tensor_info(tensor_name)
        if ti is None:
            raise KeyError(f"Tensor '{tensor_name}' not found")
        rows, cols = ti["rows"], ti["cols"]
        out = np.zeros(rows * cols, dtype=np.float32)
        out_ptr = out.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        rc = self._lib.fpq_decode_one(
            self._handle, tensor_name.encode("utf-8"), out_ptr
        )
        if rc != 0:
            raise RuntimeError(f"fpq_decode_one failed for '{tensor_name}'")
        return out.reshape(rows, cols) if cols > 1 else out


# ─── PyTorch integration ─────────────────────────────────────────────────────

try:
    import torch
    import torch.nn as nn

    class FPQLinear(nn.Module):
        """Drop-in replacement for nn.Linear that uses libfpq SLI matmul."""

        def __init__(self, fpq_model, tensor_name, bias_data=None):
            super().__init__()
            self.fpq_model = fpq_model
            self.tensor_name = tensor_name
            ti = fpq_model.tensor_info(tensor_name)
            self.in_features = ti["cols"]
            self.out_features = ti["rows"]
            if bias_data is not None:
                self.bias = nn.Parameter(bias_data, requires_grad=False)
            else:
                self.bias = None

        def forward(self, x):
            # x: [..., in_features] → [..., out_features]
            orig_shape = x.shape
            x_flat = x.reshape(-1, self.in_features)
            batch = x_flat.shape[0]

            # Convert to fp32 numpy for C bridge
            x_np = x_flat.detach().float().cpu().numpy()
            y_np = np.zeros((batch, self.out_features), dtype=np.float32)

            for i in range(batch):
                y_np[i] = self.fpq_model.matmul(self.tensor_name, x_np[i])

            y = torch.from_numpy(y_np).to(x.device, x.dtype)
            out_shape = orig_shape[:-1] + (self.out_features,)
            y = y.reshape(out_shape)

            if self.bias is not None:
                y = y + self.bias
            return y

        def extra_repr(self):
            return (f"in_features={self.in_features}, out_features={self.out_features}, "
                    f"fpq_tensor='{self.tensor_name}'")


    def patch_model(hf_model, fpq_model, prefix="", verbose=True, name_resolver=None):
        """
        Monkey-patch a HuggingFace model to use FPQ SLI matmul.

        Walks the module tree, finds nn.Linear layers, and replaces them
        with FPQLinear if the corresponding tensor exists in the .fpq file.

        Args:
            name_resolver: Optional callable(module_path) → fpq_tensor_name.
                           Use when model key names differ from .fpq tensor names
                           (e.g. diffusers WanTransformer3DModel vs original Wan).
                           Return None to skip a layer.

        Returns dict of {module_path: tensor_name} for all patched layers.
        """
        fpq_names = set(fpq_model.tensor_names())
        patched = {}
        skipped_no_tensor = []

        def _patch_recursive(module, path):
            for name, child in module.named_children():
                full_path = f"{path}.{name}" if path else name
                if isinstance(child, nn.Linear):
                    # Map HF module path to .fpq tensor name
                    if name_resolver is not None:
                        tensor_name = name_resolver(full_path)
                    else:
                        # Default: HF path + .weight
                        # e.g. model.layers.0.self_attn.q_proj → same name + .weight
                        tensor_name = f"{full_path}.weight"
                    if tensor_name and tensor_name in fpq_names:
                        ti = fpq_model.tensor_info(tensor_name)
                        if ti and ti["rows"] == child.out_features and ti["cols"] == child.in_features:
                            bias_data = child.bias.data.clone() if child.bias is not None else None
                            fpq_linear = FPQLinear(fpq_model, tensor_name, bias_data)
                            setattr(module, name, fpq_linear)
                            patched[full_path] = tensor_name
                            if verbose:
                                print(f"  [FPQ] {full_path} → SLI ({ti['rows']}×{ti['cols']})")
                        else:
                            skipped_no_tensor.append(full_path)
                    else:
                        skipped_no_tensor.append(full_path)
                else:
                    _patch_recursive(child, full_path)

        _patch_recursive(hf_model, prefix)

        if verbose:
            print(f"\nPatched {len(patched)} layers via FPQ SLI")
            if skipped_no_tensor:
                print(f"Skipped {len(skipped_no_tensor)} Linear layers (no .fpq tensor)")

        # Load passthrough tensors (layernorm weights, embeddings, etc.)
        state = hf_model.state_dict()
        loaded_passthrough = 0
        for tname in fpq_names:
            # Check if it's a passthrough tensor
            ti = fpq_model.tensor_info(tname)
            if ti and not ti["has_sli"]:
                # Find corresponding parameter in HF model
                # tname = "model.layers.0.input_layernorm.weight"
                # Need to strip prefix if present
                param_key = tname
                if param_key in state:
                    pt_data = fpq_model.get_passthrough(tname)
                    if pt_data is not None:
                        t = torch.from_numpy(pt_data)
                        if t.shape == state[param_key].shape:
                            state[param_key] = t.to(state[param_key].dtype)
                            loaded_passthrough += 1

        if loaded_passthrough > 0:
            hf_model.load_state_dict(state, strict=False)
            if verbose:
                print(f"Loaded {loaded_passthrough} passthrough tensors (norms, biases)")

        return patched


    def load_fpq_model(fpq_paths, hf_model_id, device="cpu", dtype=torch.float32,
                       verbose=True):
        """
        High-level: load HF model skeleton + replace weights from .fpq files.

        fpq_paths: single path or list of paths to .fpq shard files
        hf_model_id: HuggingFace model ID or local path for config/tokenizer
        """
        from transformers import AutoModelForCausalLM, AutoTokenizer, AutoConfig

        if isinstance(fpq_paths, (str, Path)):
            fpq_paths = [fpq_paths]

        if verbose:
            print(f"Loading model config from {hf_model_id}...")

        tokenizer = AutoTokenizer.from_pretrained(hf_model_id)

        # Load model with random weights (we'll replace them all)
        config = AutoConfig.from_pretrained(hf_model_id)
        with torch.device("meta"):
            model = AutoModelForCausalLM.from_config(config)

        # Materialize to CPU with empty tensors
        model = model.to_empty(device="cpu")

        # Open all .fpq shards
        fpq_models = []
        for p in fpq_paths:
            if verbose:
                print(f"Opening {p}...")
            fpq_models.append(FPQModel(str(p)))

        # Patch with first shard (or combined)
        # For multi-shard, we need to figure out which shard has which tensor
        total_patched = {}
        for fpq in fpq_models:
            p = patch_model(model, fpq, prefix="model", verbose=verbose)
            total_patched.update(p)

        model = model.to(device=device, dtype=dtype)

        if verbose:
            info = fpq_models[0].info()
            print(f"\nFPQ model loaded: {info['total_params']/1e6:.0f}M params, "
                  f"{len(total_patched)} SLI layers")

        return model, tokenizer, fpq_models

except ImportError:
    # PyTorch not available — just provide the numpy-level API
    pass


# ─── CLI test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: fpq_bridge.py <model.fpq> [--test-matmul <tensor_name>]")
        sys.exit(1)

    fpq = FPQModel(sys.argv[1])
    info = fpq.info()
    print(f"Model: {sys.argv[1]}")
    print(f"  Tensors: {info['n_tensors']} ({info['n_sli']} SLI, {info['n_passthrough']} passthrough)")
    print(f"  Params:  {info['total_params']/1e6:.1f}M")
    print(f"  Version: v{info['format_version']}")

    # List tensors
    names = fpq.tensor_names()
    print(f"\nTensors ({len(names)}):")
    for n in names[:20]:
        ti = fpq.tensor_info(n)
        tag = "SLI" if ti["has_sli"] else "passthrough"
        print(f"  {n}: {ti['rows']}×{ti['cols']} [{tag}, {ti['bpw']:.1f} bpw]")
    if len(names) > 20:
        print(f"  ... and {len(names) - 20} more")

    # Optional matmul test
    if len(sys.argv) >= 4 and sys.argv[2] == "--test-matmul":
        tname = sys.argv[3]
        ti = fpq.tensor_info(tname)
        print(f"\nTesting matmul on {tname} ({ti['rows']}×{ti['cols']})...")
        x = np.random.randn(ti["cols"]).astype(np.float32)
        y = fpq.matmul(tname, x)
        print(f"  x: [{x[:4]}...]  → y: [{y[:4]}...]")
        print(f"  y norm: {np.linalg.norm(y):.4f}")

    fpq.close()

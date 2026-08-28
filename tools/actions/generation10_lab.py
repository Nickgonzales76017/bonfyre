#!/usr/bin/env python3
"""Generation-10 GitHub Actions compiler and evidence runner."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = Path(__file__).with_name("generation10_contract.json")
RECEIPT_SCHEMA = "bonfyre.generation10.action-receipt.v1"


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def validate_contract(contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if contract.get("generation") != 10:
        errors.append("generation must be 10")
    if contract.get("version") != "V8.1":
        errors.append("version must be V8.1")
    expected = {
        "public_powers": 91,
        "frappe_lineages": 9,
        "format_profiles": 83,
        "partner_commons_profiles": 112,
        "deep_promoted_specialties": 82,
        "active_closure_projections": 410,
    }
    if contract.get("canonical_counts") != expected:
        errors.append("canonical_counts changed")
    if len(contract.get("frappe_lineages", [])) != 9:
        errors.append("exactly nine Frappe lineages are required")
    if len(set(contract.get("conservation_laws", []))) != 8:
        errors.append("exactly eight unique conservation laws are required")
    if contract.get("completion_chain") != [
        "semantic_lineage", "execution_lineage", "kernel_witnessed_effect",
        "receipt_lineage", "evidence_lineage", "replay_lineage",
    ]:
        errors.append("completion chain changed")
    covered = {o for c in contract.get("circuits", {}).values() for o in c.get("owners", [])}
    required = {"WorkGraph", "EffectKernel", "EvidenceGraph", "ProviderGraph", "MoneyGraph", "ObjectFabric", "Aurekai"}
    missing = sorted(required - covered)
    if missing:
        errors.append(f"critical owners absent from circuits: {missing}")
    return errors


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True, check=check)


def changed_paths(base: str | None, head: str | None) -> list[str]:
    if base and head:
        proc = git("diff", "--name-only", f"{base}...{head}", check=False)
        if proc.returncode == 0:
            return sorted({x.strip() for x in proc.stdout.splitlines() if x.strip()})
    proc = git("status", "--porcelain", check=False)
    return sorted({x[3:].strip() for x in proc.stdout.splitlines() if len(x) > 3 and x[3:].strip()})


def discover_requirement_tests() -> list[str]:
    root = ROOT / "tests" / "requirements"
    if not root.is_dir():
        return []
    return sorted(p.relative_to(ROOT).as_posix() for p in root.iterdir() if p.is_file() and p.suffix in {".sh", ".py"})


def discover_command_dirs() -> list[str]:
    cmd = ROOT / "cmd"
    if not cmd.is_dir():
        return []
    return sorted(p.name for p in cmd.iterdir() if p.is_dir() and p.name.startswith("Bonfyre") and (p / "Makefile").is_file())


def inventory_contract_observation(contract: dict[str, Any]) -> dict[str, Any]:
    observation: dict[str, Any] = {
        "canonical_public_powers": contract["canonical_counts"]["public_powers"],
        "live_command_dirs": len(discover_command_dirs()),
        "manifest_command_count": None,
        "topology_drift": [],
    }
    manifest = ROOT / "output" / "inventory" / "command_manifest.json"
    if manifest.is_file():
        try:
            observation["manifest_command_count"] = json.loads(manifest.read_text(encoding="utf-8")).get("command_count")
        except (OSError, json.JSONDecodeError):
            observation["topology_drift"].append("inventory_manifest_unreadable")
    canonical = observation["canonical_public_powers"]
    if observation["live_command_dirs"] != canonical:
        observation["topology_drift"].append(f"live_command_dirs={observation['live_command_dirs']} differs from canonical_public_powers={canonical}")
    if observation["manifest_command_count"] is not None and observation["manifest_command_count"] != canonical:
        observation["topology_drift"].append(f"manifest_command_count={observation['manifest_command_count']} differs from canonical_public_powers={canonical}")
    return observation


def circuit_for_path(path: str, contract: dict[str, Any]) -> set[str]:
    lowered = path.lower()
    matches: set[str] = set()
    hints = {
        "cmd/": {"work-effect-lifecycle", "provider-context-model"},
        "engine/": {"constitution-contract", "work-effect-lifecycle", "evidence-replay"},
        "integrations/": {"projection-frappe", "open-world-partner"},
        "frappe": {"projection-frappe"}, "model": {"provider-context-model"},
        "provider": {"provider-context-model"}, "evidence": {"evidence-replay"},
        "receipt": {"evidence-replay"}, "artifact": {"object-format-fabric"},
        "format": {"object-format-fabric"}, "money": {"institutional-money"},
        "ledger": {"institutional-money"}, "partner": {"open-world-partner"},
        "authority": {"constitution-contract", "work-effect-lifecycle"},
    }
    for hint, circuits in hints.items():
        if hint in lowered:
            matches.update(circuits)
    for name, spec in contract["circuits"].items():
        if any(pattern.lower() in lowered for pattern in spec.get("patterns", [])):
            matches.add(name)
    return matches


def write_github_output(values: dict[str, str]) -> None:
    target = os.environ.get("GITHUB_OUTPUT")
    if not target:
        for key, value in values.items():
            print(f"{key}={value}")
        return
    with open(target, "a", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


def cmd_plan(args: argparse.Namespace) -> int:
    contract = load_contract()
    errors = validate_contract(contract)
    if errors:
        for error in errors:
            print(f"CONTRACT ERROR: {error}", file=sys.stderr)
        return 2
    paths = changed_paths(args.base, args.head)
    infrastructure_change = any(p.startswith(".github/workflows/generation10-") or p.startswith("tools/actions/") for p in paths)
    mode = args.mode
    if mode == "auto":
        mode = "full" if infrastructure_change else "affected"
    if mode in {"full", "chaos"}:
        circuits = sorted(contract["circuits"])
    else:
        circuits = sorted({c for p in paths for c in circuit_for_path(p, contract)}) or ["constitution-contract"]
    commands = discover_command_dirs()
    shard_total = min(args.power_shards, max(1, len(commands)))
    if mode == "affected":
        changed_commands = {PurePosixPath(p).parts[1] for p in paths if len(PurePosixPath(p).parts) >= 2 and PurePosixPath(p).parts[0] == "cmd" and PurePosixPath(p).parts[1].startswith("Bonfyre")}
        power_scope = "changed" if changed_commands else "inventory"
        shards = list(range(shard_total if changed_commands else min(2, shard_total)))
    else:
        power_scope = "full"
        shards = list(range(shard_total))
    chaos = mode == "chaos" or args.force_chaos or infrastructure_change
    plan = {
        "schema": "bonfyre.generation10.action-plan.v1",
        "generation": contract["generation"], "version": contract["version"],
        "git_sha": args.head or os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA") or git("rev-parse", "HEAD").stdout.strip(),
        "mode": mode, "changed_paths": paths, "affected_circuits": circuits,
        "power_scope": power_scope, "power_shards": shards,
        "fault_scenarios": contract["fault_scenarios"] if chaos else [],
        "requirements_discovered": discover_requirement_tests(),
        "inventory_observation": inventory_contract_observation(contract),
        "contract_sha256": sha256_file(CONTRACT_PATH),
    }
    plan["plan_sha256"] = sha256_value(plan)
    out = Path(args.output); out.parent.mkdir(parents=True, exist_ok=True); out.write_bytes(canonical_bytes(plan))
    write_github_output({
        "mode": mode, "plan_sha256": plan["plan_sha256"],
        "circuit_matrix": json.dumps({"include": [{"circuit": c} for c in circuits]}, separators=(",", ":")),
        "power_matrix": json.dumps({"include": [{"shard": s, "total": shard_total, "scope": power_scope} for s in shards]}, separators=(",", ":")),
        "chaos_matrix": json.dumps({"include": [{"scenario": s} for s in plan["fault_scenarios"]]}, separators=(",", ":")),
        "run_chaos": "true" if chaos else "false",
    })
    print(json.dumps(plan, indent=2))
    return 0


def select_circuit_tests(circuit: str, contract: dict[str, Any]) -> list[str]:
    patterns = [p.lower() for p in contract["circuits"][circuit].get("patterns", [])]
    selected = [test for test in discover_requirement_tests() if any(pattern in Path(test).stem.lower() for pattern in patterns)]
    event = "tests/requirements/event_schema.sh"
    if circuit in {"work-effect-lifecycle", "evidence-replay"} and (ROOT / event).is_file():
        selected.append(event)
    return sorted(set(selected))


def run_test_file(path: str, timeout: int) -> dict[str, Any]:
    full = ROOT / path
    started = time.monotonic()
    argv = ["bash", str(full)] if full.suffix == ".sh" else [sys.executable, str(full)]
    env = os.environ.copy()
    state_key = re.sub(r"[^A-Za-z0-9_.-]+", "-", Path(path).stem)
    state_dir = ROOT / ".generation10" / "state" / state_key
    if state_dir.exists():
        shutil.rmtree(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    env.update({
        "BONFYRE_CI":"1",
        "BONFYRE_CI_NO_EXTERNAL_EFFECTS":"1",
        "BONFYRE_AUTHORITY":"observe",
        "BONFYRE_STATE_DIR":str(state_dir),
        "NO_COLOR":"1",
    })
    try:
        proc = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, env=env, timeout=timeout)
        return {"path":path,"argv":argv,"exit_code":proc.returncode,"duration_ms":int((time.monotonic()-started)*1000),"stdout_sha256":hashlib.sha256(proc.stdout.encode()).hexdigest(),"stderr_sha256":hashlib.sha256(proc.stderr.encode()).hexdigest(),"stdout_tail":proc.stdout[-4000:],"stderr_tail":proc.stderr[-4000:],"outcome":"passed" if proc.returncode==0 else "failed"}
    except subprocess.TimeoutExpired as exc:
        stdout = (exc.stdout or "").encode() if isinstance(exc.stdout, str) else (exc.stdout or b"")
        stderr = (exc.stderr or "").encode() if isinstance(exc.stderr, str) else (exc.stderr or b"")
        return {"path":path,"argv":argv,"exit_code":None,"duration_ms":int((time.monotonic()-started)*1000),"stdout_sha256":hashlib.sha256(stdout).hexdigest(),"stderr_sha256":hashlib.sha256(stderr).hexdigest(),"stdout_tail":"","stderr_tail":f"timeout after {timeout}s","outcome":"timeout"}


def receipt_base(kind: str, subject: str) -> dict[str, Any]:
    return {"schema":RECEIPT_SCHEMA,"kind":kind,"subject":subject,"git_sha":os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA") or git("rev-parse","HEAD").stdout.strip(),"run_id":os.environ.get("GITHUB_RUN_ID"),"run_attempt":os.environ.get("GITHUB_RUN_ATTEMPT"),"workflow":os.environ.get("GITHUB_WORKFLOW"),"runner_os":os.environ.get("RUNNER_OS"),"runner_arch":os.environ.get("RUNNER_ARCH"),"contract_sha256":sha256_file(CONTRACT_PATH)}


def write_receipt(receipt: dict[str, Any], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True); body = dict(receipt)
    body["factor_validity"] = {"git_sha":body["git_sha"],"contract_sha256":body["contract_sha256"],"workflow":body.get("workflow"),"run_attempt":body.get("run_attempt")}
    body["content_sha256"] = sha256_value(body)
    path = out_dir / f"{re.sub(r'[^A-Za-z0-9_.-]+','-',body['subject'])}.json"; path.write_bytes(canonical_bytes(body)); return path


def cmd_run_circuit(args: argparse.Namespace) -> int:
    contract = load_contract()
    if args.circuit not in contract["circuits"]:
        return 2
    tests = select_circuit_tests(args.circuit, contract)
    if args.max_tests > 0:
        tests = tests[:args.max_tests]
    results = [run_test_file(test,args.timeout) for test in tests]
    failures=[r for r in results if r["outcome"]!="passed"]
    receipt=receipt_base("semantic_circuit",args.circuit)
    receipt.update({"owners":contract["circuits"][args.circuit]["owners"],"tests_selected":tests,"results":results,"coverage_debt":[] if tests else ["no executable requirement test matched this circuit"],"outcome":"failed" if failures else ("coverage_debt" if not tests else "passed")})
    path=write_receipt(receipt,Path(args.output_dir)); print(path)
    if failures:
        print(json.dumps({"circuit":args.circuit,"failed_tests":[{"path":r["path"],"outcome":r["outcome"],"exit_code":r["exit_code"],"stderr_tail":r["stderr_tail"][-1200:]} for r in failures]},indent=2),file=sys.stderr)
    return 1 if failures else 0


def build_native_foundations(timeout: int) -> dict[str, Any]:
    started=time.monotonic()
    env=os.environ.copy()
    env.update({"CFLAGS":"-O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE","BONFYRE_CI":"1","BONFYRE_CI_NO_EXTERNAL_EFFECTS":"1"})
    targets=["lib"]
    try:
        proc=subprocess.run(["make","-j2","lib","MARCH=x86-64"],cwd=ROOT,text=True,capture_output=True,env=env,timeout=timeout)
        return {"targets":targets,"exit_code":proc.returncode,"duration_ms":int((time.monotonic()-started)*1000),"stdout_sha256":hashlib.sha256(proc.stdout.encode()).hexdigest(),"stderr_sha256":hashlib.sha256(proc.stderr.encode()).hexdigest(),"stderr_tail":proc.stderr[-4000:],"outcome":"passed" if proc.returncode==0 else "failed"}
    except subprocess.TimeoutExpired:
        return {"targets":targets,"exit_code":None,"duration_ms":int((time.monotonic()-started)*1000),"stderr_tail":f"timeout after {timeout}s","outcome":"timeout"}


def build_one_power(command: str, timeout: int) -> dict[str, Any]:
    directory=ROOT/"cmd"/command
    started=time.monotonic()
    env=os.environ.copy()
    env.update({"CFLAGS":"-O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE","BONFYRE_CI":"1","BONFYRE_CI_NO_EXTERNAL_EFFECTS":"1"})
    try:
        proc=subprocess.run(["make","-C",str(directory),"-j2"],cwd=ROOT,text=True,capture_output=True,env=env,timeout=timeout)
        binaries=sorted(p.name for p in directory.iterdir() if p.is_file() and os.access(p,os.X_OK) and p.name!="Makefile")
        return {"command":command,"exit_code":proc.returncode,"duration_ms":int((time.monotonic()-started)*1000),"stdout_sha256":hashlib.sha256(proc.stdout.encode()).hexdigest(),"stderr_sha256":hashlib.sha256(proc.stderr.encode()).hexdigest(),"stderr_tail":proc.stderr[-3000:],"built_executables":binaries,"outcome":"passed" if proc.returncode==0 else "failed"}
    except subprocess.TimeoutExpired:
        return {"command":command,"exit_code":None,"duration_ms":int((time.monotonic()-started)*1000),"stderr_tail":f"timeout after {timeout}s","built_executables":[],"outcome":"timeout"}


def cmd_run_power_shard(args: argparse.Namespace) -> int:
    contract=load_contract(); commands=discover_command_dirs()
    if args.scope=="changed":
        changed=set()
        for path in changed_paths(args.base,args.head):
            parts=PurePosixPath(path).parts
            if len(parts)>=2 and parts[0]=="cmd" and parts[1].startswith("Bonfyre"):
                changed.add(parts[1])
        commands=sorted(changed & set(commands))
    assigned=[cmd for i,cmd in enumerate(commands) if i%args.total==args.shard]
    foundation=build_native_foundations(args.foundation_timeout)
    results=[] if foundation["outcome"]!="passed" else [build_one_power(cmd,args.timeout) for cmd in assigned]
    failures=[r for r in results if r["outcome"]!="passed"] + ([] if foundation["outcome"]=="passed" else [foundation])
    receipt=receipt_base("power_shard",f"powers-{args.shard}-of-{args.total}")
    receipt.update({"canonical_public_power_count":contract["canonical_counts"]["public_powers"],"inventory_observation":inventory_contract_observation(contract),"scope":args.scope,"commands_assigned":assigned,"native_foundation":foundation,"results":results,"outcome":"failed" if failures else "passed"})
    path=write_receipt(receipt,Path(args.output_dir)); print(path)
    if failures:
        print(json.dumps({"power_shard":f"{args.shard}/{args.total}","foundation":foundation,"failed_commands":[{"command":r.get("command"),"outcome":r.get("outcome"),"exit_code":r.get("exit_code"),"stderr_tail":r.get("stderr_tail","")[-1200:]} for r in results if r.get("outcome")!="passed"]},indent=2),file=sys.stderr)
    return 1 if failures else 0


def reject_effect(effect: dict[str, Any]) -> list[str]:
    errors=[]
    if effect.get("exit_code")==0 and not effect.get("terminal_process_occurrence"):
        errors.append("exit_zero_without_terminal_process_occurrence")
    if effect.get("external_effect") and not effect.get("authority_granted"):
        errors.append("external_effect_without_authority")
    if effect.get("external_effect") and not effect.get("receipt"):
        errors.append("external_effect_without_receipt")
    if effect.get("factor_sha")!=effect.get("current_factor_sha"):
        errors.append("stale_factor_envelope")
    if effect.get("child") and effect.get("child_parent")!=effect.get("parent"):
        errors.append("broken_parent_child")
    if effect.get("model") and effect.get("provider_model")!=effect.get("model"):
        errors.append("model_provider_mismatch")
    if effect.get("effect_occurrences",1)>1 and not effect.get("dedupe_receipt"):
        errors.append("duplicate_effect_occurrence")
    if effect.get("semantic_gc") and not effect.get("deletion_proof"):
        errors.append("semantic_gc_without_deletion_proof")
    return errors


def fault_fixture(name: str) -> tuple[dict[str, Any], str]:
    base={"exit_code":0,"terminal_process_occurrence":True,"external_effect":False,"authority_granted":False,"receipt":None,"factor_sha":"factor-a","current_factor_sha":"factor-a","parent":"work-1","child":None,"child_parent":None,"model":None,"provider_model":None,"effect_occurrences":1,"dedupe_receipt":False,"semantic_gc":False,"deletion_proof":None}; expected=name
    if name=="cli_exit_zero_without_witness":
        base["terminal_process_occurrence"]=False; expected="exit_zero_without_terminal_process_occurrence"
    elif name=="effect_without_receipt":
        base["external_effect"]=True; base["authority_granted"]=True; expected="external_effect_without_receipt"
    elif name=="stale_factor_envelope":
        base["current_factor_sha"]="factor-b"
    elif name=="authority_leak":
        base["external_effect"]=True; base["receipt"]="receipt-1"; expected="external_effect_without_authority"
    elif name=="broken_parent_child":
        base["child"]="work-2"; base["child_parent"]="wrong-parent"
    elif name=="model_provider_mismatch":
        base["model"]="model-a"; base["provider_model"]="model-b"
    elif name=="duplicate_effect_occurrence":
        base["effect_occurrences"]=2
    elif name=="semantic_gc_without_deletion_proof":
        base["semantic_gc"]=True
    else:
        raise ValueError(name)
    return base,expected


def cmd_fault(args: argparse.Namespace) -> int:
    contract=load_contract()
    if args.scenario not in contract["fault_scenarios"]:
        return 2
    fixture,expected=fault_fixture(args.scenario); rejected=reject_effect(fixture); passed=expected in rejected
    receipt=receipt_base("semantic_fault",args.scenario)
    receipt.update({"expected_rejection":expected,"observed_rejections":rejected,"fail_closed":passed,"outcome":"passed" if passed else "failed"})
    print(write_receipt(receipt,Path(args.output_dir)))
    return 0 if passed else 1


def iter_receipts(root: Path) -> list[Path]:
    return [] if not root.exists() else sorted(p for p in root.rglob("*.json") if p.name not in {"proof-bundle.json","plan.json"} and p.is_file())


def cmd_bundle(args: argparse.Namespace) -> int:
    contract=load_contract(); source=Path(args.input_dir); entries=[]; outcomes={}
    for path in iter_receipts(source):
        try:
            payload=json.loads(path.read_text(encoding="utf-8"))
        except (OSError,json.JSONDecodeError):
            payload={"outcome":"malformed"}
        outcome=str(payload.get("outcome","unknown")); outcomes[outcome]=outcomes.get(outcome,0)+1
        entries.append({"path":path.relative_to(source).as_posix(),"sha256":sha256_file(path),"bytes":path.stat().st_size,"kind":payload.get("kind"),"subject":payload.get("subject"),"outcome":outcome,"git_sha":payload.get("git_sha"),"contract_sha256":payload.get("contract_sha256")})
    git_shas=sorted({e["git_sha"] for e in entries if e["git_sha"]}); contract_shas=sorted({e["contract_sha256"] for e in entries if e["contract_sha256"]})
    bundle={"schema":"bonfyre.generation10.proof-bundle.v1","generation":contract["generation"],"version":contract["version"],"canonical_counts":contract["canonical_counts"],"conservation_laws":contract["conservation_laws"],"git_shas":git_shas,"contract_shas":contract_shas,"receipt_count":len(entries),"outcomes":outcomes,"receipts":entries,"proof_complete":bool(entries) and len(git_shas)==1 and len(contract_shas)==1 and not any(k in outcomes for k in ("failed","timeout","malformed"))}
    bundle["bundle_sha256"]=sha256_value(bundle)
    out=Path(args.output); out.parent.mkdir(parents=True,exist_ok=True); out.write_bytes(canonical_bytes(bundle))
    summary=Path(args.summary); summary.parent.mkdir(parents=True,exist_ok=True)
    summary.write_text("\n".join(["# Generation-10 ProofBundle","",f"- generation: `{bundle['generation']}` / `{bundle['version']}`",f"- receipts: **{bundle['receipt_count']}**",f"- outcomes: `{json.dumps(outcomes,sort_keys=True)}`",f"- proof complete: **{str(bundle['proof_complete']).lower()}**",f"- bundle sha256: `{bundle['bundle_sha256']}`",f"- git shas: `{', '.join(git_shas) or 'none'}`","","A green bundle is valid only for the exact source SHA and Generation-10 contract hash above.",""]),encoding="utf-8")
    print(json.dumps(bundle,indent=2))
    return 0 if bundle["proof_complete"] else 1


def cmd_contract(_: argparse.Namespace) -> int:
    contract=load_contract(); errors=validate_contract(contract)
    print(json.dumps({"contract_valid":not errors,"errors":errors,"contract_sha256":sha256_file(CONTRACT_PATH),"inventory_observation":inventory_contract_observation(contract)},indent=2))
    return 0 if not errors else 1


def parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="command",required=True)
    c=sub.add_parser("contract"); c.set_defaults(func=cmd_contract)
    plan=sub.add_parser("plan"); plan.add_argument("--mode",choices=["auto","affected","full","chaos"],default="auto"); plan.add_argument("--base"); plan.add_argument("--head"); plan.add_argument("--power-shards",type=int,default=8); plan.add_argument("--force-chaos",action="store_true"); plan.add_argument("--output",default=".generation10/plan.json"); plan.set_defaults(func=cmd_plan)
    circuit=sub.add_parser("run-circuit"); circuit.add_argument("--circuit",required=True); circuit.add_argument("--timeout",type=int,default=240); circuit.add_argument("--max-tests",type=int,default=0); circuit.add_argument("--output-dir",default=".generation10/receipts"); circuit.set_defaults(func=cmd_run_circuit)
    power=sub.add_parser("run-power-shard"); power.add_argument("--shard",type=int,required=True); power.add_argument("--total",type=int,required=True); power.add_argument("--scope",choices=["full","changed","inventory"],default="full"); power.add_argument("--base"); power.add_argument("--head"); power.add_argument("--timeout",type=int,default=180); power.add_argument("--foundation-timeout",type=int,default=420); power.add_argument("--output-dir",default=".generation10/receipts"); power.set_defaults(func=cmd_run_power_shard)
    fault=sub.add_parser("fault"); fault.add_argument("--scenario",required=True); fault.add_argument("--output-dir",default=".generation10/receipts"); fault.set_defaults(func=cmd_fault)
    bundle=sub.add_parser("bundle"); bundle.add_argument("--input-dir",default=".generation10/artifacts"); bundle.add_argument("--output",default=".generation10/proof-bundle.json"); bundle.add_argument("--summary",default=".generation10/summary.md"); bundle.set_defaults(func=cmd_bundle)
    return p


def main() -> int:
    args=parser().parse_args(); return args.func(args)


if __name__=="__main__":
    raise SystemExit(main())

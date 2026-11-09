#!/usr/bin/env python3
"""Full-article Qwen3 master; review only, no public publishing."""
import argparse,hashlib,json,re,subprocess
import numpy as np
import soundfile as sf
from studio_qwen3 import ROOT,REF

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--bf16",action="store_true")
    a=ap.parse_args()
    if not re.fullmatch(r"[a-z0-9-]+",a.slug):raise ValueError("Invalid slug")
    raw=(ROOT/"editorial/posts"/(a.slug+".md")).read_text()
    head,body=raw[4:].split("\n---\n",1)
    if "status: edited-public" not in head:raise ValueError("Not edited-public")
    title=re.search(r"(?m)^title:\s*(.+)$",head).group(1)
    blocks=[title.rstrip(".!?")+"."]
    for para in re.split(r"\n\s*\n",body):
        para=para.strip()
        if para.startswith("## Sources"):break
        if not para or para.startswith(("![",chr(96)*3,"> ","| ")):continue
        para=re.sub(r"(?m)^#{1,5}\s+","",para)
        para=re.sub(r"\[([^\]]+)\]\(https?://[^)]+\)",r"\1",para)
        para=re.sub(r"https?://\S+","",para).replace("**","")
        para=re.sub(r"\s+"," ",para).strip()
        if len(para.split())>=4:blocks.append(para)
    source=hashlib.sha256(body.strip().encode()).hexdigest()
    output=ROOT/"editorial/audio/master"/a.slug/source[:12]
    output.mkdir(parents=True,exist_ok=True)
    generate(blocks,output,a.bf16,source)

if __name__=="__main__":main()

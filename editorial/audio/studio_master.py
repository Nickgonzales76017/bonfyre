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


def generate(blocks,output,bf16,source):
    from kokoro_onnx import Kokoro
    from mlx_audio.tts.utils import load_model
    import mlx.core as mx
    model_id="mlx-community/Qwen3-TTS-12Hz-1.7B-Base-"+("bf16" if bf16 else "4bit")
    manifest=output/"manifest.json"
    if manifest.exists():
        cached=json.loads(manifest.read_text())
        if cached.get("source_sha256")==source and cached.get("model")==model_id and (output/"master.flac").is_file():
            print("STUDIO_MASTER_SOURCE_UNCHANGED",source,flush=True)
            return
    speaker=Kokoro(".voice-cache/kokoro-v1.0.fp16.onnx",".voice-cache/voices-v1.0.bin")
    try:ref,sr=speaker.create(REF,voice="af_sarah",speed=.82,lang="en-us")
    finally:speaker.voices.close()
    if sr!=24000:raise RuntimeError("Reference sample rate mismatch")
    reference=mx.array(np.asarray(ref,dtype=np.float32))
    model=load_model(model_id)
    pieces=[]
    for paragraph in blocks:
        buffer=""
        for sentence in re.split(r"(?<=[.!?])\s+",paragraph):
            if len(sentence)>440:raise RuntimeError("Long sentence needs editorial segmentation")
            if buffer and len(buffer)+len(sentence)+1>440:
                pieces.extend(speak(model,buffer,reference,mx))
                buffer=""
            buffer+=(" " if buffer else "")+sentence
        if buffer:pieces.extend(speak(model,buffer,reference,mx))
        pieces.append(np.zeros(7200,dtype=np.float32))
    audio=np.concatenate(pieces)
    lossless=output/"master.flac"
    sf.write(lossless,np.clip(audio,-1,1),24000,subtype="PCM_24")
    mp3=output/"edition.mp3"
    subprocess.run(["ffmpeg","-nostdin","-hide_banner","-loglevel","error","-y",
        "-i",str(lossless),"-af","loudnorm=I=-18:TP=-2:LRA=9",
        "-c:a","libmp3lame","-b:a","96k",str(mp3)],check=True)
    transcript="\n\n".join(blocks)
    (output/"spoken.txt").write_text(transcript+"\n")
    row={"model":model_id,"source_sha256":source,
         "spoken_sha256":hashlib.sha256(transcript.encode()).hexdigest(),
         "voice":"synthetic Sarah","seconds":round(len(audio)/24000,2),
         "audio_sha256":hashlib.sha256(mp3.read_bytes()).hexdigest(),
         "flac_sha256":hashlib.sha256(lossless.read_bytes()).hexdigest(),
         "editorial_review":"pending; never automatically publish"}
    manifest.write_text(json.dumps(row,indent=2)+"\n")
    print("STUDIO_MASTER_DRAFT "+json.dumps(row),flush=True)

def speak(model,text,reference,mx):
    parts=[]
    for fragment in model.generate(text=text,ref_audio=reference,ref_text=REF):
        mx.eval(fragment.audio)
        signal=np.asarray(fragment.audio,dtype=np.float32).reshape(-1)
        if not signal.size or not np.all(np.isfinite(signal)):raise RuntimeError("Bad generated audio")
        parts.append(signal)
    return parts

if __name__=="__main__":main()

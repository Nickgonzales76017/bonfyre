#!/usr/bin/env python3
"""Offline review-only Qwen3 1.7B Sarah audition; never publishes."""
import gc, json, hashlib, re, subprocess
from pathlib import Path
import numpy as np
import soundfile as sf
ROOT=Path(__file__).resolve().parents[2]
OUT=ROOT/"editorial/audio/studio-output"
REF="Some of the best tools on the internet never felt like tools. They felt like places we could make our own. A good story deserves time to be heard, and space for its ideas to settle."
slugs=["sites-are-dead-long-live-the-web","the-machine-told-me-so"]
def main():
    OUT.mkdir(parents=True,exist_ok=True)
    from kokoro_onnx import Kokoro
    voice=Kokoro(".voice-cache/kokoro-v1.0.fp16.onnx",".voice-cache/voices-v1.0.bin")
    try: samples,sr=voice.create(REF,voice="af_sarah",speed=.82,lang="en-us")
    finally: voice.voices.close()
    if sr!=24000: raise RuntimeError("Reference sample rate")
    ref=OUT/"sarah-reference.wav"
    sf.write(ref,np.asarray(samples,dtype=np.float32),sr)
    del voice,samples
    gc.collect()
    from mlx_audio.tts.utils import load_model
    import mlx.core as mx
    model=load_model("mlx-community/Qwen3-TTS-12Hz-1.7B-Base-4bit")
    manifest=[]
    for slug in slugs:
        raw=(ROOT/"editorial/posts"/(slug+".md")).read_text()
        head,body=raw[4:].split("\n---\n",1)
        if "status: edited-public" not in head: raise RuntimeError("Unpublished article")
        title=re.search(r"(?m)^title:\s*(.+)$",head).group(1)
        p=next(x for x in re.split(r"\n\s*\n",body) if len(x.split())>25 and not x.startswith(("![","> ","| ")))
        p=re.sub(r"\[([^\]]+)\]\(https?://[^)]+\)",r"\1",p).replace("**","").replace(chr(96),"")
        spoken=title.rstrip(".!?")+". "+p
        if len(spoken)>440: spoken=spoken[:spoken.rfind(".",100,440)+1]
        if len(spoken)<100: raise RuntimeError("No coherent excerpt")
        sound=[]
        for piece in model.generate(text=spoken,ref_audio=str(ref),ref_text=REF):
            mx.eval(piece.audio)
            sound.append(np.asarray(piece.audio,dtype=np.float32).reshape(-1))
        audio=np.concatenate(sound)
        output=OUT/(slug+".mp3")
        wav=output.with_suffix(".wav")
        sf.write(wav,audio,24000)
        subprocess.run(["ffmpeg","-nostdin","-hide_banner","-loglevel","error","-y","-i",str(wav),"-af","loudnorm=I=-18:TP=-2:LRA=9","-c:a","libmp3lame","-b:a","96k",str(output)],check=True)
        wav.unlink()
        (OUT/(slug+".spoken.txt")).write_text(spoken+"\n")
        manifest.append(dict(article=slug,model="Qwen3-TTS-1.7B-Base-4bit",voice="Sarah synthetic reference",body_sha256=hashlib.sha256(body.strip().encode()).hexdigest(),audio_sha256=hashlib.sha256(output.read_bytes()).hexdigest(),seconds=len(audio)/24000,words=len(spoken.split()),review="not approved or published"))
    (OUT/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    print("STUDIO_QWEN_PASS",len(manifest),flush=True)
if __name__=="__main__": main()

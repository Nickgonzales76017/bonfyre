#!/usr/bin/env python3
"""Lossless, content-deduplicated ChatGPT export member archive.

The original ZIP remains authoritative. "Lossless" means every member's
uncompressed bytes, order, name and recorded ZIP metadata are restorable.
Recompression does not reproduce the original ZIP's exact container bytes.
"""
import argparse
import base64
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import uuid
import zipfile

CHUNK = 1 << 20
PREFIX = "chatgpt-20261028-"
MANIFEST = "_bonfyre/manifest-v1.json"
SCHEMA = "bonfyre.chat-export.member-bundle.v1"
OVERHEAD = 128 << 20

def refuse(reason):
    raise SystemExit("REFUSED: " + reason)

def digest_file(path):
    sha = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(CHUNK), b""):
            sha.update(chunk)
    return sha.hexdigest()

def catalog(root):
    path = root / (PREFIX + "member-manifest.sqlite3")
    if not path.is_file() or path.is_symlink():
        refuse("member catalog missing or symlinked")
    with sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True) as db:
        info = db.execute("SELECT path,size,mtime_ns,sha256 FROM archive").fetchone()
        members = db.execute("SELECT path,sha256,raw_bytes,is_directory,zip_bytes FROM member").fetchall()
    if not info or not members:
        refuse("catalog lacks source provenance or members")
    source = Path(info[0])
    if not source.is_file() or source.is_symlink():
        refuse("original ZIP missing or symlinked")
    st = source.stat()
    if (st.st_size,st.st_mtime_ns)!=(info[1],info[2]):
        refuse("original ZIP changed since catalog")
    if len(info[3])!=64:
        refuse("invalid source digest in catalog")
    return source,info[3],members

def indexed(members):
    counts = Counter()
    by_path = defaultdict(set)
    unique = {}
    for name,digest,size,is_dir,zip_size in members:
        if not isinstance(name,str) or size is None or size<0 or (not is_dir and not isinstance(digest,str)):
            refuse("invalid member catalog row")
        if is_dir:
            digest=hashlib.sha256(b"").hexdigest()
            if size!=0:
                refuse("directory member contains nonempty payload")
        if not is_dir and len(digest)!=64:
            refuse("missing member SHA-256; rebuild source catalog")
        key=(name,int(size),bool(is_dir),digest)
        counts[key]+=1
        by_path[(name,int(size),bool(is_dir))].add(digest)
        if not is_dir:
            if digest in unique and unique[digest][0]!=int(size):
                refuse("one source hash has two different sizes")
            unique[digest] = (int(size),min(unique.get(digest,(size,zip_size))[1],int(zip_size)))
    return counts,by_path,unique

def zip_infos(source,members):
    with zipfile.ZipFile(source) as z:
        infos=z.infolist()
        if len(infos)!=len(members):
            refuse("ZIP central directory and indexed members disagree")
        if any(i.flag_bits&1 for i in infos):
            refuse("encrypted members are unsupported")
        return infos

def space_guard(target,required,reserve):
    parent=target.parent.resolve()
    if not parent.is_dir():
        refuse("output directory does not exist: "+str(parent))
    free=shutil.disk_usage(parent).free
    if required+reserve>free:
        refuse("insufficient free space on output volume: free=%d needed_at_most=%d reserve=%d; use an external volume"%(free,required,reserve))
    return free

class BoundedWriter:
    """Non-seekable ZIP sink; reserves free space and aborts cleanly before exhaustion."""
    def __init__(self,path,reserve):
        self.path=path
        self.reserve=reserve
        self.file=path.open("xb")
        self.position=0
    def write(self,data):
        free=shutil.disk_usage(self.path.parent.resolve()).free
        if free<len(data)+self.reserve+OVERHEAD:
            refuse("bounded bundle would exhaust disk reserve; temporary bundle will be removed")
        count=self.file.write(data)
        self.position+=count
        if shutil.disk_usage(self.path.parent.resolve()).free<self.reserve+OVERHEAD:
            refuse("bounded bundle crossed disk reserve; temporary bundle will be removed")
        return count
    def tell(self):
        return self.position
    def flush(self):
        self.file.flush()
    def close(self):
        if not self.file.closed:
            self.file.close()

def info_dict(i,digest):
    enc=lambda b:base64.b64encode(b).decode("ascii")
    return dict(name=i.filename,sha256=digest,raw_bytes=i.file_size,crc32=i.CRC,
                directory=i.is_dir(),date_time=list(i.date_time),compress_type=i.compress_type,
                comment=enc(i.comment),extra=enc(i.extra),external_attr=i.external_attr,
                internal_attr=i.internal_attr,create_system=i.create_system,
                create_version=i.create_version,extract_version=i.extract_version,
                flag_bits=i.flag_bits,volume=i.volume)

def stream_digest(src,out=None):
    h=hashlib.sha256();size=0
    while True:
        data=src.read(CHUNK)
        if not data:
            break
        h.update(data);size+=len(data)
        if out is not None:
            out.write(data)
    return h.hexdigest(),size

def ensure_source(source,expected):
    if digest_file(source)!=expected:
        refuse("original ZIP SHA-256 differs from catalog")

def plan(root,target,reserve):
    source,sha,members=catalog(root)
    counts,by_path,unique=indexed(members)
    infos=zip_infos(source,members)
    current=Counter((i.filename,i.file_size,i.is_dir()) for i in infos)
    recorded=Counter()
    for (name,size,kind,_),n in counts.items():
        recorded[(name,size,kind)]+=n
    if current!=recorded:
        refuse("ZIP member names/sizes disagree with catalog")
    raw=sum(v[0] for v in unique.values())
    compressed=sum(v[1] for v in unique.values())
    upper=raw+OVERHEAD
    free=shutil.disk_usage(target.parent.resolve()).free
    return dict(original_zip=str(source),source_sha256=sha,members=len(members),
                unique_payloads=len(unique),unique_raw_bytes=raw,
                indexed_unique_zip_bytes=compressed,
                potential_duplicate_zip_bytes=sum(i.compress_size for i in infos)-compressed,
                conservative_bundle_space_bytes=upper,reserve_bytes=reserve,
                free_output_volume_bytes=free,can_build=(free>=upper+reserve),
                bounded_initial_reserve_available=(free>=reserve+OVERHEAD),
                bounded_note="Opt-in build --bounded guards every write; indexed compression is an estimate, not a final size guarantee")

def load_bundle(z):
    try:
        data=z.read(MANIFEST)
        doc=json.loads(data)
    except (KeyError,ValueError,UnicodeError) as e:
        refuse("bundle manifest unreadable: "+str(e))
    if doc.get("schema")!=SCHEMA or not isinstance(doc.get("members"),list):
        refuse("unsupported bundle manifest")
    if not isinstance(doc.get("source_sha256"),str) or len(doc["source_sha256"])!=64:
        refuse("invalid source identity")
    return doc

def verify_bundle(path):
    if not path.is_file() or path.is_symlink():
        refuse("bundle unavailable")
    with zipfile.ZipFile(path) as z:
        doc=load_bundle(z)
        members=doc["members"]
        blobs={}
        for n,member in enumerate(members):
            if member.get("index")!=n or "name" not in member or "sha256" not in member:
                refuse("invalid member position")
            if not member["directory"]:
                digest=member["sha256"]
                if digest in blobs and blobs[digest]!=member["raw_bytes"]:
                    refuse("same digest with conflicting sizes")
                blobs[digest]=member["raw_bytes"]
        expected={MANIFEST}|{"blobs/"+d for d in blobs}
        actual={i.filename for i in z.infolist()}
        if expected!=actual or len(z.infolist())!=len(expected):
            refuse("bundle blob inventory differs from manifest")
        for digest,size in blobs.items():
            with z.open("blobs/"+digest) as stream:
                observed,n=stream_digest(stream)
            if observed!=digest or n!=size:
                refuse("corrupt blob: "+digest)
        print("VERIFIED %d members %d unique blobs; source ZIP retained separately"%(len(members),len(blobs)))
    return doc

def build(root,target,reserve,bounded=False):
    if target.exists() or target.is_symlink():
        refuse("output exists; never overwrite a bundle")
    report=plan(root,target,reserve)
    space_guard(target,OVERHEAD if bounded else report["conservative_bundle_space_bytes"],reserve)
    source,sha,members=catalog(root)
    ensure_source(source,sha)
    counts,by_path,unique=indexed(members)
    tmp=target.with_name("."+target.name+"."+uuid.uuid4().hex+".partial")
    seen=set()
    sink=None
    try:
        sink=BoundedWriter(tmp,reserve) if bounded else tmp
        with zipfile.ZipFile(source) as z,zipfile.ZipFile(sink,"w",compression=zipfile.ZIP_DEFLATED,allowZip64=True) as dst:
            recovered=[]
            for n,info in enumerate(z.infolist()):
                identity=(info.filename,info.file_size,info.is_dir())
                candidates=by_path.get(identity,set())
                if not candidates:
                    refuse("absent indexed identity for "+info.filename)
                if len(candidates)>1:
                    with z.open(info) as stream:
                        selected,size=stream_digest(stream)
                    if size!=info.file_size or selected not in candidates:
                        refuse("ambiguous indexed member bytes disagree: "+info.filename)
                    expected=selected
                else:
                    expected=next(iter(candidates))
                if info.is_dir():
                    observed,size=hashlib.sha256(b'').hexdigest(),0
                else:
                    with z.open(info) as stream:
                        if expected in seen:
                            observed,size=stream_digest(stream)
                        else:
                            with dst.open("blobs/"+expected,"w",force_zip64=True) as blob:
                                observed,size=stream_digest(stream,blob)
                if (observed,size)!=(expected,info.file_size) and not (info.is_dir() and size==0):
                    refuse("source member hash or size differs: "+info.filename)
                key=(info.filename,info.file_size,info.is_dir(),expected)
                counts[key]-=1
                if counts[key]<0:
                    refuse("indexed member multiplicity differs")
                if not info.is_dir():
                    seen.add(expected)
                recovered.append(dict(info_dict(info,observed),index=n))
            if any(counts.values()):
                refuse("catalog includes members absent from ZIP")
            manifest=dict(schema=SCHEMA,source_sha256=sha,source_zip_basename=source.name,
                          original_zip_comment=base64.b64encode(z.comment).decode("ascii"),
                          members=recovered)
            dst.writestr(MANIFEST,json.dumps(manifest,ensure_ascii=False,separators=(",",":")).encode(),compress_type=zipfile.ZIP_STORED)
        if bounded:
            sink.close()
        verify_bundle(tmp)
        ensure_source(source,sha) # Source identity must still agree after a multi-gigabyte build.
        if target.exists() or target.is_symlink():
            refuse("output appeared during build")
        os.rename(tmp,target)
        print("BUNDLE_READY "+str(target))
    finally:
        if bounded and sink is not None:
            sink.close()
        if tmp.exists():
            tmp.unlink()

def restored_info(member):
    i=zipfile.ZipInfo(member["name"],tuple(member["date_time"]))
    for key in ("compress_type","external_attr","internal_attr","create_system","create_version","extract_version","volume"):
        setattr(i,key,member[key])
    i.comment=base64.b64decode(member["comment"],validate=True)
    i.extra=base64.b64decode(member["extra"],validate=True)
    return i

def verify_restoration(path,doc):
    with zipfile.ZipFile(path) as z:
        actual=z.infolist()
        if len(actual)!=len(doc["members"]):
            refuse("restored ZIP has wrong member count")
        if z.comment!=base64.b64decode(doc["original_zip_comment"]):
            refuse("restored ZIP comment differs")
        for info,member in zip(actual,doc["members"]):
            if info.filename!=member["name"] or info.file_size!=member["raw_bytes"]:
                refuse("restored ZIP name/size differs")
            if info.date_time!=tuple(member["date_time"]) or info.external_attr!=member["external_attr"]:
                refuse("restored ZIP metadata differs: "+info.filename)
            with z.open(info) as src:
                h,n=stream_digest(src)
            if h!=member["sha256"] or n!=member["raw_bytes"]:
                refuse("restored bytes differ: "+info.filename)
    print("RESTORED_VERIFIED %d members; logical member bytes preserved"%len(actual))

def restore(source,target,reserve):
    if target.exists() or target.is_symlink():
        refuse("restoration target exists")
    doc=verify_bundle(source)
    original=sum(m["raw_bytes"] for m in doc["members"])+OVERHEAD
    space_guard(target,original,reserve)
    tmp=target.with_name("."+target.name+"."+uuid.uuid4().hex+".partial")
    try:
        with zipfile.ZipFile(source) as z,zipfile.ZipFile(tmp,"w",allowZip64=True) as dst:
            dst.comment=base64.b64decode(doc["original_zip_comment"],validate=True)
            for member in doc["members"]:
                info=restored_info(member)
                if member["directory"]:
                    dst.writestr(info,b"")
                    continue
                with z.open("blobs/"+member["sha256"]) as src,dst.open(info,"w",force_zip64=True) as out:
                    observed,n=stream_digest(src,out)
                if observed!=member["sha256"] or n!=member["raw_bytes"]:
                    refuse("restoration source disagrees")
        verify_restoration(tmp,doc)
        if target.exists():
            refuse("target appeared during restoration")
        os.rename(tmp,target)
        print("RESTORED "+str(target)+"; container ZIP SHA-256 may differ from the original")
    finally:
        if tmp.exists():
            tmp.unlink()

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root",type=Path,default=Path.home()/".bonfyre/archive-recovery")
    p.add_argument("--reserve-gib",type=int,default=1,help="required untouched free space")
    sp=p.add_subparsers(dest="command",required=True)
    for name in ("plan","build","verify","restore"):
        cmd=sp.add_parser(name)
        if name in ("plan","build","restore"):
            cmd.add_argument("--out",required=True,type=Path)
        if name in ("verify","restore"):
            cmd.add_argument("--bundle",required=True,type=Path)
    build_command=sp.choices["build"]
    build_command.add_argument("--bounded",action="store_true",help="opt-in guarded streaming; enforce disk reserve per compressed write")
    a=p.parse_args()
    if not (1<=a.reserve_gib<=100):
        refuse("reserve-gib must be 1-100")
    reserve=a.reserve_gib<<30
    if a.command=="plan":
        print(json.dumps(plan(a.root, a.out,reserve),indent=2))
    elif a.command=="build":
        build(a.root,a.out,reserve,bounded=a.bounded)
    elif a.command=="verify":
        verify_bundle(a.bundle)
    else:
        restore(a.bundle,a.out,reserve)

if __name__=="__main__":
    main()

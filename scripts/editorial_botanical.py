"""Deterministic original botanical covers for Aurekai Journal.

Only the Python standard library. The subject slug is the seed: a source edit
does not randomly change the visual identity. Each article has original vector
geometry; a finite set of editorial layouts controls placement, not the art.
"""
from __future__ import annotations
import hashlib
import html
import math
import random

PALETTES = (
    ("#153e31", "#e7d6ad", "#e8ba86", "#f8f2e4"),
    ("#173a32", "#cfddbc", "#dda17e", "#f8f0e0"),
    ("#214738", "#e0d3aa", "#e8bb9c", "#fbf4e6"),
    ("#15352e", "#d8e2c7", "#cfa96a", "#f6f0e4"),
    ("#244c3d", "#d4e4bd", "#dfbb8d", "#f7f2e9"),
    ("#103c34", "#d8d3a8", "#d79c77", "#fcf3e3"),
    ("#2b4838", "#e3d9c3", "#e3b58b", "#fff6e9"),
    ("#1b443d", "#d5e0c7", "#d8ad85", "#f8efe1"),
)
SPECIES = ("cedar", "pine", "oak", "birch", "fern", "willow")
COMPOSITIONS = (
    "folio", "reverse", "centered", "framed",
    "panorama", "offset", "letterpress", "diptych"
)

def style_for(post):
    seed = int(hashlib.sha256(post["slug"].encode("utf-8")).hexdigest()[:16], 16)
    dark, ink, accent, paper = PALETTES[(seed >> 12) % len(PALETTES)]
    family = SPECIES[(seed >> 6) % len(SPECIES)]
    variant = COMPOSITIONS[(seed >> 20) % len(COMPOSITIONS)]
    return {
        "seed": seed, "dark": dark, "ink": ink, "accent": accent,
        "paper": paper, "family": family, "variant": variant,
        "index": f"{seed % 997 + 1:03d}",
        "rotation": (seed % 13) - 6,
    }

def _point(x, y):
    return f"{x:.1f},{y:.1f}"

def _path(d, color, width=1, opacity=1, extra=""):
    return f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width:.2f}" opacity="{opacity:.3f}" stroke-linecap="round" stroke-linejoin="round" {extra}/>'

def _leaf(cx, cy, angle, scale, color, opacity):
    rad = math.radians(angle)
    dx, dy = math.cos(rad) * 11 * scale, math.sin(rad) * 11 * scale
    nx, ny = -math.sin(rad) * 4 * scale, math.cos(rad) * 4 * scale
    d = (f"M {_point(cx-dx, cy-dy)} "
         f"Q {_point(cx+nx, cy+ny)} {_point(cx+dx, cy+dy)} "
         f"Q {_point(cx-nx, cy-ny)} {_point(cx-dx, cy-dy)}")
    return _path(d, color, .72, opacity)

def _tree(rng, x, bottom, height, spread, family, ink, accent, density):
    result = []
    sway = rng.uniform(-35, 35)
    top = bottom-height
    main = f"M {_point(x,bottom)} C {_point(x+sway*.2,bottom-height*.36)} {_point(x+sway*.75,top+height*.22)} {_point(x+sway,top)}"
    result.append(_path(main, ink, rng.uniform(1.6, 2.6), .8))
    result.append(_path(f"M {_point(x-3,bottom)} Q {_point(x+sway*.3,top+height*.4)} {_point(x+sway-2,top+18)}", accent,.45,.48))
    branch_count = density + rng.randint(-1, 2)
    for j in range(branch_count):
        u=(j+1)/(branch_count+1)
        y=bottom-height*(.12+.79*u)
        tx=x+sway*u
        canopy = (1-u)**.72
        for side in (-1,1):
            if family == "willow":
                bx=tx+side*spread*canopy*.82
                by=y-height*.14*canopy
                d=f"M {_point(tx,y)} Q {_point(tx+side*spread*.51*canopy,y-height*.20*canopy)} {_point(bx,by)}"
                result.append(_path(d,ink,.85,.64))
                for k in range(3):
                    ox=bx+side*(k*6+3)
                    result.append(_path(f"M {_point(ox,by)} Q {_point(ox+side*5,by+23)} {_point(ox+side*2,by+55+12*k)}",accent,.7,.63))
            elif family == "fern":
                bx=tx+side*spread*canopy*.63
                by=y-height*.07
                d=f"M {_point(tx,y)} Q {_point(tx+side*spread*.27,y-height*.10)} {_point(bx,by)}"
                result.append(_path(d,ink,.82,.69))
                for k in range(5):
                    t=(k+1)/6
                    lx=tx+(bx-tx)*t
                    ly=y+(by-y)*t
                    ll=10+17*(1-t)
                    result.append(_path(f"M {_point(lx,ly)} l {side*ll*.42:.1f},{-ll:.1f}",accent,.6,.6))
                    result.append(_path(f"M {_point(lx,ly)} l {side*ll*.5:.1f},{ll*.64:.1f}",accent,.6,.53))
            else:
                reach=spread*canopy*(.80 if family=="pine" else .58 if family=="birch" else .72)
                bx=tx+side*reach
                by=y-height*(.025 if family=="pine" else .17 if family=="oak" else .10)*canopy
                d=f"M {_point(tx,y)} Q {_point(tx+side*reach*.52,y-height*.08*canopy)} {_point(bx,by)}"
                result.append(_path(d,ink,rng.uniform(.6,1.08),.72))
                result.append(_path(f"M {_point(tx+side*reach*.3,y-3)} Q {_point(tx+side*reach*.47,y-height*.21*canopy)} {_point(tx+side*reach*.66,by-17*canopy)}",accent,.52,.53))
                if family in ("cedar","pine"):
                    for k in range(5):
                        t=(k+1)/6
                        nx=tx+(bx-tx)*t
                        ny=y+(by-y)*t
                        length=(9+13*canopy)*(1-.4*t)
                        for up in (-1,1):
                            result.append(_path(f"M {_point(nx,ny)} l {side*length*.38:.1f},{up*length:.1f}",ink,.48,.48))
                else:
                    for k in range(4):
                        t=(k+1)/5
                        nx=tx+(bx-tx)*t
                        ny=y+(by-y)*t
                        for sign in (-1,1):
                            result.append(_leaf(nx+side*5,ny+sign*7,sign*36+side*17,.75*canopy+.4,accent,.62))
    if family=="birch":
        for j in range(11):
            yy=bottom-(j+1)*height/13
            xx=x+sway*(j/13)
            result.append(_path(f"M {_point(xx-2,yy)} l {-5-rng.random()*4:.1f},{-2-rng.random()*4:.1f}", accent,.5,.53))
    if family=="oak":
        for j in range(9):
            cx=x+rng.uniform(-spread*.55,spread*.55)
            cy=top+rng.uniform(height*.1,height*.52)
            result.append(_leaf(cx,cy,rng.randint(0,180),rng.uniform(.75,1.2),ink,.54))
    return ''.join(result)

def illustration_svg(post):
    st=style_for(post)
    rng=random.Random(st["seed"])
    width,height=760,900
    ink,accent,dark=st["ink"],st["accent"],st["dark"]
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="art-title">',
           f'<title id="art-title">Original {st["family"]} botanical line illustration for {html.escape(post["title"])}</title>',
           f'<rect width="{width}" height="{height}" fill="{dark}"/>',
           f'<rect x="28" y="28" width="704" height="844" rx="1" fill="none" stroke="{ink}" opacity=".42"/>']
    # Hand-engraved radiating/contour rings, with a unique eccentric center.
    cx=380+rng.randint(-90,90); cy=377+rng.randint(-125,95)
    for j in range(10):
        radius=52+j*29+rng.uniform(-3,3)
        dash=("4 8" if j%3==0 else "none")
        parts.append(f'<ellipse cx="{cx}" cy="{cy}" rx="{radius:.1f}" ry="{radius*.83:.1f}" fill="none" stroke="{accent}" stroke-width=".7" opacity="{max(.08,.4-j*.025):.2f}" stroke-dasharray="{dash}" transform="rotate({st["rotation"]} {cx} {cy})"/>')
    # Tiny marginal latitude marks and nature-notebook plotting ticks.
    for j in range(24):
        y=55+j*34
        x=54 if j%2==0 else 706
        length=10 if j%4 else 20
        parts.append(_path(f"M {_point(x,y)} h {length if x<380 else -length}",accent,.7,.35))
    placements=((-16,825,565,185),(350,850,675,220),(742,830,580,165))
    family=st["family"]
    for i,(x,y,h,s) in enumerate(placements):
        dx=rng.uniform(-18,18)
        shift=0 if i==1 else rng.uniform(-24,24)
        stem_family=family if i!=2 else SPECIES[(SPECIES.index(family)+1+st["seed"]%3)%len(SPECIES)]
        parts.append(_tree(rng,x+dx,y+shift,h+rng.randint(-45,55),s,stem_family,ink,accent,10+(st["seed"]//(i+1))%5))
    for i in range(35):
        x=rng.uniform(60,700); y=rng.uniform(65,800)
        size=rng.uniform(1.2,4.0)
        if i%4==0:
            parts.append(_path(f"M {_point(x-size,y)} h {2*size:.1f} M {_point(x,y-size)} v {2*size:.1f}",accent,.7,.5))
        else:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{size*.34:.2f}" fill="{accent}" opacity=".3"/>')
    parts.append(f'<path d="M 56 813 H 704 M 56 82 H 190 M 570 82 H 704" stroke="{ink}" stroke-width=".85" fill="none" opacity=".62"/>')
    parts.append(f'<circle cx="380" cy="82" r="5.5" fill="none" stroke="{accent}" opacity=".85"/>')
    parts.append('</svg>\n')
    return ''.join(parts)

package control

import "list"

#NarrationAssembly: {
 schema: "bonfyre.narration-assembly.v1"
 sample_rate: int & >=8000 & <=96000
 clips: [...#NarrationAssemblyClip] & [_, ...] & list.MaxItems(64)
}
#NarrationAssemblyClip: {
 path: string & != ""
 sha256: string & =~"^[a-f0-9]{64}$"
 at: int & >=0
 frames: int & >0
 gain: number & >=0 & <=1
 script: string & != ""
}

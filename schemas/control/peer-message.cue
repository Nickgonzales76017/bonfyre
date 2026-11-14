package control

import "strings"

#PeerContextMessage: {
 facet: string & =~"^[a-z0-9-]{1,31}$"
 sender: string & =~"^person:[A-Za-z0-9_.-]+$"
 recipient: string & =~"^person:[A-Za-z0-9_.-]+$" & !=sender
 nonce: string & =~"^[a-f0-9]{32}$"
 body: string & !="" & strings.MaxRunes(1000)
 sender_epoch: int & >0
 recipient_epoch: int & >0
 sender_choice_revision: int & >0
 recipient_choice_revision: int & >0
}

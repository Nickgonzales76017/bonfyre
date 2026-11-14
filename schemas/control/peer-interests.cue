package control

#PeerContextInterests: {
 subject: string & =~"^person:[A-Za-z0-9_.-]+$"
 facet: string & =~"^[a-z0-9-]{1,31}$"
 participation_revision: int & >0 & <=4294967295
 revision: int & >0 & <=4294967295
 topics: string & =~"^$|^([a-z0-9]|[a-z0-9][a-z0-9-]{0,30}[a-z0-9])(,([a-z0-9]|[a-z0-9][a-z0-9-]{0,30}[a-z0-9])){0,7}$"
}

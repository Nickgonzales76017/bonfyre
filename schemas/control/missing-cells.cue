// The four cells nothing owned, and what each one is allowed to claim.
//
// Nine of the thirty interaction grammars could not be built at all, not because
// their surfaces were hard but because each needed one of these and no organ held
// it: there was no version control over the object estate, no outbound message
// rail, no bound signature, and no live media path. A surface built over a
// missing cell describes a capability instead of carrying one, so these are the
// prerequisite.
//
// Each is deliberately the smallest honest thing rather than a clone of the
// product that made it famous:
//
//   - a version is a content-addressed snapshot and a diff. It is not a branch
//     and cannot be merged, because merging derived material nobody can rebuild
//     would produce a tree with no provenance.
//   - a message is composed and held. Nothing here sends, because reachability
//     in this estate stops at the human line, and an outbox that quietly
//     delivered would move that line.
//   - a signature is assent to an exact digest. It dies when the bytes change,
//     which is the only property that makes it worth anything.
//   - a stream is live only while something keeps saying so. Without a heartbeat
//     it is a scheduled or ended session, never a live one.
package control

#CellContract: {
	cell: string & =~"^[a-z][a-z_]*$"
	label: string & != ""
	// The record type that holds it.
	record: string & =~"^[a-z][a-z_]*$"
	// What this implementation does, stated narrowly.
	does: string & != ""
	// What the famous product does that this deliberately does not.
	refuses: string & != ""
	cannot_infer: string & != ""
}

nativeCellContracts: [...#CellContract] & [
	{cell: "version", label: "A snapshot of a namespace", record: "estate_version",
	 does: "records the digest of every file under a namespace at one moment, and diffs two such snapshots into added, removed and changed paths",
	 refuses: "branching and merging. A snapshot is a reading of what was there, and a merge would have to reconstruct derived material this estate cannot rebuild",
	 cannot_infer: "a snapshot is not a backup and not a restore point: the bytes it names may be gone, and nothing here can put them back"},
	{cell: "message", label: "Something composed for somebody", record: "outbound_message",
	 does: "records a message, its intended recipient, and the act somebody would have to perform to send it",
	 refuses: "sending. There is no transport here, on purpose: the estate computes what is reachable and stops before anything leaves it",
	 cannot_infer: "a held message is not a sent one, not a delivered one, and not a claim the recipient knows it exists"},
	{cell: "signature", label: "Assent to exact bytes", record: "assent_signature",
	 does: "binds one person's recorded assent to one digest, and reports the assent as broken the moment the bytes hash differently",
	 refuses: "identity verification and legal effect. It says who this estate recorded as assenting, not who they are or what it obliges",
	 cannot_infer: "a signature here is not a legal signature, not notarised, and not evidence the signer read what they assented to"},
	{cell: "stream", label: "Something happening now", record: "live_session",
	 does: "records a session, the heartbeats that keep it live, and what it points at for replay once it ends",
	 refuses: "carrying media. No bytes flow through this estate; a session names where something is happening and what was kept",
	 cannot_infer: "a live session is not an audience, not a viewer count, and not a claim anybody is watching"},
]

// A version diff classifies a path by what happened to it. The classes are
// declared so a diff cannot invent a fifth.
#PathChange: {
	change: "added" | "removed" | "changed" | "unchanged"
	label: string & != ""
	means: string & != ""
}

nativePathChanges: [...#PathChange] & [
	{change: "added", label: "New here", means: "the path exists in the later snapshot and not the earlier one"},
	{change: "removed", label: "Gone", means: "the path existed in the earlier snapshot and not the later one"},
	{change: "changed", label: "Different bytes", means: "the path exists in both and its digest differs"},
	{change: "unchanged", label: "Same bytes", means: "the path exists in both with the same digest"},
]

// How a message stops. Every one of these is a state where the estate is holding
// something and not sending it.
#MessageState: {
	state: "composed" | "withheld" | "handed_over" | "abandoned"
	label: string & != ""
	// Did the estate transmit anything? Always false, and generated so it stays
	// false rather than being remembered as false.
	transmitted: bool
	means: string & != ""
}

nativeMessageStates: [...#MessageState] & [
	{state: "composed", label: "Written and held", transmitted: false,
	 means: "the text exists and the recipient is named; nothing has left this estate"},
	{state: "withheld", label: "Held back on purpose", transmitted: false,
	 means: "somebody decided it should not go, and the reason is recorded"},
	{state: "handed_over", label: "Given to a person to send", transmitted: false,
	 means: "a named person took it to send themselves; the estate still transmitted nothing"},
	{state: "abandoned", label: "Dropped", transmitted: false,
	 means: "it will not be sent and is kept only as a record that it was written"},
]

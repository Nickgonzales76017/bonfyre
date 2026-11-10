// PhysicalCarrierProfile is a representation contract, not a second owner of
// a Matrix subject.  It validates the fields a carrier compiler may consume;
// AddressPlane, AuthorityGraph, MoneyGraph and ProtocolMachine retain their
// independent semantic and effect boundaries.
package control

#PhysicalCarrierProfile: {
	carrier: "qr" | "datamatrix" | "nfc" | "ble" | "uwb" | "barcode"
	payloadProfile: "web-uri" | "gs1-digital-link" | "app-clip" | "emv-qr" | "matter-onboarding" | "fido-cross-device" | "wallet" | "signed-bonfyre-invocation"
	subjectAddress: =~"^https://"
	purpose: "identify" | "resolve" | "open" | "claim" | "pair" | "authenticate" | "pay" | "admit" | "redeem" | "check-in" | "configure"
	temporal: "permanent" | "bounded" | "single-use" | "rotating"
	proximity: "none" | "scanned-present" | "nfc" | "ble-confirmed" | "uwb-confirmed"
	audience: "self" | "household" | "group" | "community" | "public" | "market" | "institution"
	authority: "none" | "propose" | "authenticate" | "requires-confirmation" | "bounded-effect"
	security: {
		nonce?: string
		expiresAt?: string
		audience?: string
		signature?: string
		replayPolicy: "not-applicable" | "allow" | "single-use" | "rotate"
	}
	// A permanent code can identify/resolve/open, never carry a direct effect.
	if temporal == "permanent" {
		authority: "none" | "propose"
		security: replayPolicy: "not-applicable" | "allow"
	}
	// A bounded invocation must be replay-governed; its eventual effect remains
	// behind separate AuthorityGraph/ProtocolMachine confirmation.
	if authority == "bounded-effect" {
		temporal: "bounded" | "single-use" | "rotating"
		security: {
			nonce: string
			expiresAt: string
			signature: string
			replayPolicy: "single-use" | "rotate"
		}
	}
}

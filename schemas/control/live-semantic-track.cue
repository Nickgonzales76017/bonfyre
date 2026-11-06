package control

// Retention is ordered by the duration of responsibility, not popularity.
liveRetention: {
  tiers: ["flash", "chat", "moment", "social", "saved", "institutional", "evidentiary", "economic"]
  coolingFloor: "saved"
  promotionReasons: ["explicit_action", "evidence"]
  kindTier: {
    reaction: "flash"
    presence: "flash"
    chat: "chat"
    utterance: "chat"
    speaker: "chat"
    topic: "moment"
    entity: "moment"
    place: "moment"
    chapter: "moment"
    clip_candidate: "moment"
    claim: "evidentiary"
    sponsor_mention: "evidentiary"
    cta: "social"
    commerce: "economic"
  }
}

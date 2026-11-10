package control
import "list"

// A read projection of public local conversation starters, shared by the
// four territory homes. It carries no member list or participation authority.
#CommunityDiscovery: {
    namespace: "root"
    policy: "aurekai-member-conversation-v1"
    candidate_limit: 24
    display_limit: 3
    distinct_communities: true
    roots_only: true
    visibility: "public"
    status: "published"
    moderation: "accepted" | "visible" | "published"
    publication_time: "valid-and-not-future"
    host: "aurekai.life" | "aurekai.live" | "aurekai.work" | "aurekai.shop"
    route: "/"
    destination: "/community"
    cache: {
        origin: "exact-public-selection-digest"
        client: "must-revalidate"
    }
    records: [...{
        community: string & =~"^[a-zA-Z0-9._:-]{1,96}$"
        post: string & =~"^[a-zA-Z0-9._:-]{1,96}$"
        community_digest: string & =~"^[a-f0-9]{64}$"
        post_digest: string & =~"^[a-f0-9]{64}$"
    }] & list.MaxItems(3)
}

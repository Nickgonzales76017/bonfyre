package control

#CommerceStage: "product" | "listing" | "inventory" | "availability_observation" |
    "offer" | "quote" | "cart" | "checkout" | "commitment" | "payment" |
    "settlement" | "fulfilment" | "delivery" | "satisfaction" | "ownership"

#CommerceProgress: {
    exchange: string & != ""
    stage: #CommerceStage
    owner: string & != ""
    detail?: string
    at?: string
}

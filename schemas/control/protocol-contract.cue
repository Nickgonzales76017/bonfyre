package control

#ProtocolRole: {
    name: string & != ""
    authority?: string
    requires?: [...string & != ""]
    produces?: [...string & != ""]
    reads?: [...string & != ""]
}

#ProtocolCommitment: {
    key: string & != ""
    committer: string & != ""
    beneficiary: string & != ""
    resource: string & != ""
    discharged_by?: string
}

#ProtocolContract: {
    name: string & != ""
    family?: string & != ""
    keys?: [...string & != ""]
    given?: [...string & != ""]
    evidence_required?: [...string & != ""]
    roles: [...#ProtocolRole]
    commitments?: [...#ProtocolCommitment]
}

quoteAcceptContract: #ProtocolContract & {
    name: "quote-accept"
    keys: ["exchange_id"]
    given: ["capability", "buyer_identity"]
    roles: [
        {name:"buyer", requires:["capability","buyer_identity"], produces:["request"], authority:"request:capability"},
        {name:"seller", requires:["request"], produces:["quote"], authority:"quote:capability"},
        {name:"settlement", requires:["quote"], produces:["acceptance"], authority:"accept:quote"},
        {name:"deliverer", requires:["acceptance"], produces:["delivery"], authority:"deliver:capability"},
    ]
    commitments: [
        {key:"delivery", committer:"deliverer", beneficiary:"buyer", resource:"the quoted capability", discharged_by:"delivery"},
        {key:"payment", committer:"buyer", beneficiary:"seller", resource:"the quoted amount", discharged_by:"acceptance"},
    ]
}

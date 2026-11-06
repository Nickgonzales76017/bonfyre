package control

// The documentation publisher supplies this declaration. It carries exact
// InformationPolicy fields; it neither publishes a page nor grants effects.
#ReferenceDisclosure: {
    schema: "bonfyre.reference-disclosure.v1"
    operator: string & =~"^command\\.[a-z0-9-]+$"
    contract_generation: string & =~"^[0-9a-f]{64}$"
    fields: [...{
        field_name: string & =~"^bonfyre://artifact/[0-9a-f]{64}#excerpt$"
        audiences: [...#DisclosureAudience]
    }]
}

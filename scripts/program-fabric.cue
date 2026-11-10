programFabric: {
  namespace: "root"
  template_cannot_infer: "a template is what a program of this kind would require; it is not a program anyone is running, not an award, not an application for one, and naming a funding stream is not a claim that this estate has any relationship with that funder"
  instance_cannot_infer: "this subject is running this kind of program in this estate; that is not authority to act on anyone's behalf, not a funded award, and not a relationship with any funder named by the template"
  package_cannot_infer: "a work package is a declared scope kept distinct from other streams so one real effort supported by several of them can be told apart from one cost claimed more than once; it is never a claim that money was awarded, applied for, billed, paid, booked or authorized"
  binding_cannot_infer: "a slot with no lowering application is a declared part of this program that no surface can fill yet; it is not a feature that exists and not an empty one that failed"
  lowerings: {
    notices: {application: "Newsroom", selector: "source_collection"}
    newsroom: {application: "Newsroom", selector: "source_collection"}
    sources: {application: "PublicationFeed", selector: "source_collection"}
    evidence: {application: "PublicationFeed", selector: "source_collection"}
    board: {application: "Board", selector: "world"}
    work: {application: "Board", selector: "world"}
    case: {application: "Board", selector: "world"}
    community: {application: "Community", selector: "community"}
  }
  slot: {extent: "section", min_material_quality: "observed", max_items: 12}
  source: {grouping: "program", resolver: "SourceFrontier", material_kind: ["publication_occurrence"], refresh_policy: "on_observation"}
  service_packs: {
    "sourceLibrary": "experience.source-library.workspace",
    "instruments": "experience.connected-instruments.workspace",
    "analysis": "experience.skill-instruments.workspace",
    "creator": "experience.native-creator-studio.workspace",
    "connections": "experience.connection-studio.workspace",
    "finance": "experience.finance-workbooks.workspace",
    "catalogue": "experience.multi-merchant-catalogue.workspace",
    "procurement": "experience.retail-procurement.workspace",
    "shipments": "experience.shipment-archive.workspace",
    "programs": "experience.source-programs.workspace",
    "campaigns": "experience.source-campaign.workspace"
  }
  interaction_grammar_basis: {
    artifact: {state: "pending_exact_owner", owner_families: ["ArtifactGraph"]}
    collection: {state: "typed", continuation: "board", owner_families: ["WorldPublication"]}
    commerce: {state: "typed", continuation: "commerce", owner_families: ["Offer"]}
    community: {state: "typed", continuation: "community", owner_families: ["Community", "RelationshipGraph"]}
    correspondence: {state: "pending_exact_owner", owner_families: ["Correspondence"]}
    knowledge: {state: "pending_exact_owner", owner_families: ["KnowledgeGraph"]}
    media: {state: "typed", continuation: "media", owner_families: ["CreatorChannel", "MediaLibrary", "RelationshipGraph", "Stage"]}
    reservation: {state: "pending_exact_owner", owner_families: ["ProtocolContract"]}
    work: {state: "typed", continuation: "work", owner_families: ["WorkGraph"]}
    workspace: {state: "typed", continuation: "work", owner_families: ["WorkGraph"]}
  }
  continuation_aliases: {
    "case-work": "case",
    "source": "sources",
    "connection": "connections",
    "provider": "connections",
    "budget": "finance",
    "economic": "finance",
    "product": "commerce",
    "delivery": "shipments",
    "shipment": "shipments",
    "data": "analysis",
    "schema": "analysis"
  }
  continuations: {
    "sources": [
      {
        "kind": "workspace",
        "pack": "experience.source-library.workspace",
        "label": "Source library",
        "description": "Keep and review exact retained source material."
      },
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Turn reviewed sources into research, checks and planning material."
      },
      {
        "kind": "surface",
        "id": "sources",
        "label": "Sources",
        "href": "/sources",
        "description": "Browse the native source estate without copying this program."
      }
    ],
    "research": [
      {
        "kind": "workspace",
        "pack": "experience.source-library.workspace",
        "label": "Source library",
        "description": "Keep and review exact retained source material."
      },
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Continue research in the installed instruments workspace."
      },
      {
        "kind": "surface",
        "id": "sources",
        "label": "Sources",
        "href": "/sources",
        "description": "Browse the native source estate."
      }
    ],
    "evidence": [
      {
        "kind": "workspace",
        "pack": "experience.source-library.workspace",
        "label": "Source library",
        "description": "Retain evidence-bearing source material in its source owner."
      },
      {
        "kind": "workspace",
        "pack": "experience.finance-workbooks.workspace",
        "label": "Financial workbooks",
        "description": "Review financial evidence in its workbook owner."
      }
    ],
    "work": [
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Prepare operational work in connected instruments."
      },
      {
        "kind": "workspace",
        "pack": "experience.skill-instruments.workspace",
        "label": "Planning and analysis instruments",
        "description": "Continue planning and analysis in its own editor."
      }
    ],
    "case": [
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Prepare case material in connected instruments."
      },
      {
        "kind": "workspace",
        "pack": "experience.skill-instruments.workspace",
        "label": "Planning and analysis instruments",
        "description": "Continue case analysis in its own editor."
      }
    ],
    "board": [
      {
        "kind": "workspace",
        "pack": "experience.skill-instruments.workspace",
        "label": "Planning and analysis instruments",
        "description": "Prepare structured board material in its editor."
      },
      {
        "kind": "surface",
        "id": "board",
        "label": "Boards",
        "href": "/board",
        "description": "Open the native board surface; this does not attach the program to a board."
      }
    ],
    "newsroom": [
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Prepare newsroom research and source material."
      },
      {
        "kind": "workspace",
        "pack": "experience.native-creator-studio.workspace",
        "label": "Creator studio",
        "description": "Continue media production in the native creator workspace."
      },
      {
        "kind": "surface",
        "id": "channels",
        "label": "Channels",
        "href": "/channels",
        "description": "Open the native channel surface without claiming publication."
      }
    ],
    "notices": [
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Prepare notice material in connected instruments."
      },
      {
        "kind": "surface",
        "id": "inbox",
        "label": "Inbox",
        "href": "/inbox",
        "description": "Open the native Inbox surface; recipient state remains separately owned."
      }
    ],
    "community": [
      {
        "kind": "surface",
        "id": "community",
        "label": "Community",
        "href": "/community",
        "description": "Open HumanCommunityBody. This does not create membership or bind this program to a community."
      }
    ],
    "social": [
      {
        "kind": "surface",
        "id": "community",
        "label": "Community",
        "href": "/community",
        "description": "Open the native human community surface."
      },
      {
        "kind": "surface",
        "id": "channels",
        "label": "Channels",
        "href": "/channels",
        "description": "Open creator/community channels without claiming a subscription."
      }
    ],
    "media": [
      {
        "kind": "workspace",
        "pack": "experience.native-creator-studio.workspace",
        "label": "Creator studio",
        "description": "Continue audio and media preparation in the creator workspace."
      },
      {
        "kind": "surface",
        "id": "channels",
        "label": "Channels",
        "href": "/channels",
        "description": "Open the native channel surface without claiming publication."
      }
    ],
    "creator": [
      {
        "kind": "workspace",
        "pack": "experience.native-creator-studio.workspace",
        "label": "Creator studio",
        "description": "Continue creator work in its native editor."
      },
      {
        "kind": "surface",
        "id": "channels",
        "label": "Channels",
        "href": "/channels",
        "description": "Open the native channel surface without claiming publication."
      }
    ],
    "connections": [
      {
        "kind": "account",
        "id": "connections",
        "label": "Connections",
        "path": "/apps/connections",
        "description": "Review the destination’s external-account and provider prerequisites."
      }
    ],
    "identity": [
      {
        "kind": "account",
        "id": "connections",
        "label": "Connections",
        "path": "/apps/connections",
        "description": "Review the destination’s external-account and identity prerequisites."
      }
    ],
    "finance": [
      {
        "kind": "workspace",
        "pack": "experience.finance-workbooks.workspace",
        "label": "Financial workbooks",
        "description": "Continue financial review in its native workbook owner."
      }
    ],
    "funding": [
      {
        "kind": "workspace",
        "pack": "experience.finance-workbooks.workspace",
        "label": "Financial workbooks",
        "description": "Review funding scope and costs without treating a candidate as awarded funding."
      }
    ],
    "commerce": [
      {
        "kind": "workspace",
        "pack": "experience.multi-merchant-catalogue.workspace",
        "label": "Catalogue",
        "description": "Continue product and merchant preparation in Catalogue."
      },
      {
        "kind": "workspace",
        "pack": "experience.retail-procurement.workspace",
        "label": "Room Planning",
        "description": "Continue procurement preparation in its owning editor."
      },
      {
        "kind": "surface",
        "id": "listings",
        "label": "Listings",
        "href": "/listings",
        "description": "Open the native listings surface without claiming a purchase or offer."
      }
    ],
    "catalogue": [
      {
        "kind": "workspace",
        "pack": "experience.multi-merchant-catalogue.workspace",
        "label": "Catalogue",
        "description": "Continue product and merchant preparation in Catalogue."
      }
    ],
    "procurement": [
      {
        "kind": "workspace",
        "pack": "experience.retail-procurement.workspace",
        "label": "Room Planning",
        "description": "Continue procurement preparation in its owning editor."
      }
    ],
    "shipments": [
      {
        "kind": "workspace",
        "pack": "experience.shipment-archive.workspace",
        "label": "Shipment & archive review",
        "description": "Continue logistics and archive review in its own editor."
      }
    ],
    "logistics": [
      {
        "kind": "workspace",
        "pack": "experience.shipment-archive.workspace",
        "label": "Shipment & archive review",
        "description": "Continue logistics and archive review in its own editor."
      }
    ],
    "analysis": [
      {
        "kind": "workspace",
        "pack": "experience.connected-instruments.workspace",
        "label": "Research instruments",
        "description": "Continue research and validation."
      },
      {
        "kind": "workspace",
        "pack": "experience.skill-instruments.workspace",
        "label": "Planning and analysis instruments",
        "description": "Continue analysis in its own editor."
      }
    ],
    "campaigns": [
      {
        "kind": "workspace",
        "pack": "experience.source-campaign.workspace",
        "label": "Source-to-campaign studio",
        "description": "Open the installed campaign editor to review analytical source evidence before building a campaign. No campaign is created by this continuation."
      }
    ],
    "programs": [
      {
        "kind": "workspace",
        "pack": "experience.source-programs.workspace",
        "label": "Sources and recurring programs",
        "description": "Review pinned source evidence in an installed recurring-program editor. No saved program, subscription or schedule is created."
      },
      {
        "kind": "surface",
        "id": "programs",
        "label": "Programs",
        "href": "/programs",
        "description": "Return to ProgramFabric for this destination."
      }
    ]
  }
}

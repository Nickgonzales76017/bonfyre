package control

// What each application kind actually is.
//
// #ApplicationKind declares fifty-nine names and carries nothing else. It says
// a slot holds a "Scoreboard" and cannot say that a scoreboard is a live
// entity, that it belongs on a card rather than a page, or that its audience is
// a community rather than a market. So the facet compiler -- which types four
// thousand external occurrences by phenotype and derives twenty-five relations
// from them -- cannot see a site application at all. Fifty-nine kinds sit
// outside the relation grammar because nobody wrote down what they are.
//
// This is the same shape the phenotype grammar already has. A phenotype is not
// a name: it carries the relations it admits, the carriers that speak it, what
// invalidates it and how it is represented. An application kind should carry at
// least what it is, where it sits and who it is for, and then the relations
// follow from the phenotype rather than being restated per capability program.
//
// Phenotypes are a list because a thing is often more than one. A Marketplace
// is an economic offer and a dataset; a Podcast is a media asset and a
// versioned record. The facet compiler already types occurrences with several
// phenotypes at once and this matches it rather than forcing a single answer.
//
// Cannot infer: a declared phenotype -> a lawful interaction. It says what the
// kind is, which is what the compiler needs to propose a facet. Whether that
// facet may be enacted is AuthorityGraph's question and not this file's.

#ApplicationKindFacts: {
	// The primary phenotype is first; any others follow.
	phenotypes: [#Phenotype, ...#Phenotype]
	// Where the kind naturally sits. A ticker is a chip; a workspace is not.
	extent: #SlotExtent
	// Who it is for by default. A site may narrow this and may not widen it
	// past what InformationPolicy admits.
	audience: #DomainAudience
}

applicationKindFacts: [Name=string]: #ApplicationKindFacts

applicationKindFacts: {
	// ---- publication and documents
	PublicationFeed:    {phenotypes: ["EVENT", "DOCUMENT"], extent: "section", audience: "public"}
	Newsroom:           {phenotypes: ["EVENT", "DOCUMENT"], extent: "page", audience: "public"}
	Article:            {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], extent: "page", audience: "public"}
	Documentation:      {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], extent: "workspace", audience: "public"}
	Thread:             {phenotypes: ["EVENT", "DOCUMENT"], extent: "section", audience: "community"}
	Board:              {phenotypes: ["DOCUMENT", "EVENT"], extent: "page", audience: "community"}
	Community:          {phenotypes: ["IDENTITY_CREDENTIAL", "EVENT"], extent: "page", audience: "community"}

	// ---- media
	Gallery:            {phenotypes: ["MEDIA_ASSET"], extent: "section", audience: "public"}
	Watch:              {phenotypes: ["MEDIA_ASSET"], extent: "immersive", audience: "public"}
	Listen:             {phenotypes: ["MEDIA_ASSET"], extent: "card", audience: "public"}
	Podcast:            {phenotypes: ["MEDIA_ASSET", "VERSIONED_RECORD"], extent: "section", audience: "public"}

	// ---- time and live state
	LiveEvent:          {phenotypes: ["EVENT", "LIVE_ENTITY"], extent: "page", audience: "public"}
	Calendar:           {phenotypes: ["EVENT"], extent: "section", audience: "group"}
	Scoreboard:         {phenotypes: ["LIVE_ENTITY", "EVENT"], extent: "card", audience: "public"}
	Standings:          {phenotypes: ["VERSIONED_RECORD", "LIVE_ENTITY"], extent: "section", audience: "public"}
	Ticker:             {phenotypes: ["LIVE_ENTITY", "MEASUREMENT_STREAM"], extent: "chip", audience: "public"}
	BuildStatus:        {phenotypes: ["LIVE_ENTITY", "EVENT"], extent: "chip", audience: "group"}
	UtilityStatus:      {phenotypes: ["LIVE_ENTITY", "MEASUREMENT_STREAM"], extent: "chip", audience: "public"}
	Tracking:           {phenotypes: ["LIVE_ENTITY", "SUPPLY_CHAIN_OBJECT"], extent: "card", audience: "self"}

	// ---- markets and money
	MarketTape:         {phenotypes: ["MEASUREMENT_STREAM", "LIVE_ENTITY"], extent: "chip", audience: "market"}
	QuoteBook:          {phenotypes: ["ECONOMIC_OFFER", "LIVE_ENTITY"], extent: "section", audience: "market"}
	Product:            {phenotypes: ["ECONOMIC_OFFER"], extent: "card", audience: "market"}
	Marketplace:        {phenotypes: ["ECONOMIC_OFFER", "DATASET"], extent: "page", audience: "market"}
	Checkout:           {phenotypes: ["ECONOMIC_OFFER", "OBLIGATION"], extent: "section", audience: "self"}
	Ticketing:          {phenotypes: ["ECONOMIC_OFFER", "EVENT"], extent: "section", audience: "public"}
	Portfolio:          {phenotypes: ["DATASET", "ECONOMIC_OFFER"], extent: "workspace", audience: "self"}

	// ---- space and place
	Map:                {phenotypes: ["SPATIAL_RESOURCE", "FIELD_COVERAGE"], extent: "page", audience: "public"}
	Route:              {phenotypes: ["SPATIAL_RESOURCE"], extent: "section", audience: "self"}
	Property:           {phenotypes: ["SPATIAL_RESOURCE", "ECONOMIC_OFFER"], extent: "page", audience: "market"}
	HousingSearch:      {phenotypes: ["SPATIAL_RESOURCE", "DATASET"], extent: "workspace", audience: "market"}
	Restaurant:         {phenotypes: ["SPATIAL_RESOURCE", "ECONOMIC_OFFER"], extent: "card", audience: "public"}
	Facility:           {phenotypes: ["SPATIAL_RESOURCE", "COMMANDABLE_RESOURCE"], extent: "page", audience: "institution"}

	// ---- things that move or are commanded
	Ride:               {phenotypes: ["LIVE_ENTITY", "SERVICE_CAPABILITY"], extent: "card", audience: "self"}
	Shipment:           {phenotypes: ["SUPPLY_CHAIN_OBJECT", "LIVE_ENTITY"], extent: "card", audience: "self"}
	Vehicle:            {phenotypes: ["COMMANDABLE_RESOURCE", "LIVE_ENTITY"], extent: "card", audience: "self"}
	Maintenance:        {phenotypes: ["OBLIGATION", "COMMANDABLE_RESOURCE"], extent: "section", audience: "institution"}

	// ---- measurement and hazard
	Weather:            {phenotypes: ["MEASUREMENT_STREAM", "FIELD_COVERAGE"], extent: "card", audience: "public"}
	Incident:           {phenotypes: ["EVENT", "SPATIAL_RESOURCE"], extent: "card", audience: "public"}
	Alert:              {phenotypes: ["EVENT"], extent: "chip", audience: "public"}

	// ---- research and code
	ResearchPaper:      {phenotypes: ["DOCUMENT", "VERSIONED_RECORD"], extent: "page", audience: "public"}
	Dataset:            {phenotypes: ["DATASET"], extent: "section", audience: "public"}
	Experiment:         {phenotypes: ["DATASET", "EVENT"], extent: "workspace", audience: "group"}
	Repository:         {phenotypes: ["DATASET", "VERSIONED_RECORD"], extent: "workspace", audience: "public"}
	IssueTracker:       {phenotypes: ["OBLIGATION", "EVENT"], extent: "workspace", audience: "group"}

	// ---- service and obligation
	SupportCase:        {phenotypes: ["OBLIGATION", "SERVICE_CAPABILITY"], extent: "section", audience: "self"}
	ServiceRequest:     {phenotypes: ["SERVICE_CAPABILITY", "OBLIGATION"], extent: "section", audience: "self"}
	Appointment:        {phenotypes: ["OBLIGATION", "EVENT"], extent: "card", audience: "self"}
	Reservation:        {phenotypes: ["OBLIGATION", "ECONOMIC_OFFER"], extent: "card", audience: "self"}
	Claim:              {phenotypes: ["OBLIGATION", "VERSIONED_RECORD"], extent: "section", audience: "self"}
	Policy:             {phenotypes: ["VERSIONED_RECORD", "OBLIGATION"], extent: "page", audience: "institution"}

	// ---- money in, people in
	Grant:              {phenotypes: ["ECONOMIC_OFFER", "OBLIGATION"], extent: "page", audience: "institution"}
	FundingOpportunity: {phenotypes: ["ECONOMIC_OFFER", "DOCUMENT"], extent: "page", audience: "institution"}
	Job:                {phenotypes: ["ECONOMIC_OFFER", "DOCUMENT"], extent: "card", audience: "market"}
	Applicant:          {phenotypes: ["IDENTITY_CREDENTIAL", "DOCUMENT"], extent: "card", audience: "institution"}
	ProviderDirectory:  {phenotypes: ["IDENTITY_CREDENTIAL", "DATASET"], extent: "page", audience: "public"}

	// ---- learning
	Course:             {phenotypes: ["DOCUMENT", "DATASET"], extent: "workspace", audience: "group"}
	Lesson:             {phenotypes: ["DOCUMENT", "MEDIA_ASSET"], extent: "section", audience: "group"}
	Quiz:               {phenotypes: ["DOCUMENT", "OBLIGATION"], extent: "card", audience: "group"}

	// ---- assets
	Asset:              {phenotypes: ["VERSIONED_RECORD", "ECONOMIC_OFFER"], extent: "card", audience: "institution"}
}

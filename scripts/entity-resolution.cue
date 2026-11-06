entityResolution: {
  endpoint: "https://qlever.dev/api/wikidata"
  user_agent: "Aurekai/1.0 (estate entity resolution; contact via aurekai.live)"
  single_word_sitelink_floor: 60
  pause_ms: 400
  default_limit: 400
  default_min_publishers: 3
  default_batch: 40
  default_sitelink_floor: 5
  ordinary_words: """
    a an the this that these those and or but for with from into over under about
    best better good great new old first last next final full more most less least
    big small long short high low real true free open close early late young
    future past present today week weekend month year years day days time times
    here there now then when what why how who all any each every some many much
    one two three four five ten hundred thousand million billion
    way ways thing things part parts kind kinds sort story stories news update
    top end start begin made make making take taking get getting going know
    look looking see seeing say saying come coming back front side inside outside
    life live living work working home house world people person man woman
    right left north south east west up down out off on in at by to of is are was
    were be been being have has had do does did can could would should will
    available released announced coming launch launched review reviews guide guides
    tips ideas list lists watch listen read season episode series show shows video
    photo photos picture pictures music song songs album feature features special
    state states public private tech technology radio television media press report
    reports data study studies research market markets business company companies
    city cities county town area areas center centre school schools health care
    police court courts government federal national international local global
    south north east west central american united national general service services
    group groups team teams game games play player players season event events
    """
  instance_kinds: ["state", "country", "city", "town", "province", "county", "municipality", "village", "capital", "island", "river", "mountain", "region", "territory", "human", "person", "company", "business", "enterprise", "corporation", "organization", "organisation", "agency", "university", "college", "school", "team", "club", "party", "republic", "kingdom", "nation", "rechtsstaat", "league", "federation", "institution", "newspaper", "broadcaster"]
  cannot_infer: "A resolved name is an identity for the phrase, not a claim that any title carrying it is about that entity. Mention is not aboutness. A name that did not resolve is absent rather than assumed, and the sitelink count is kept so a reader can see how strong each identification was."
}

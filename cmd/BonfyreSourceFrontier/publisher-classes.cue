publisherClasses: {
  endpoint: "https://query.wikidata.org/sparql"
  agent: "BonfyreSourceFrontier/1.0 (estate source census; contact via aurekai.life)"
  accept: "application/sparql-results+json"
  timeout_ms: 180000
  default_per_class: 1500
  default_pause_ms: 2000
  classes: {
    newspaper: "Q11032"
    magazine: "Q41298"
    "news-agency": "Q192283"
    "radio-station": "Q14350"
    "television-station": "Q1616075"
    museum: "Q33506"
    library: "Q7075"
    university: "Q3918"
    "botanical-garden": "Q167346"
    observatory: "Q62832"
    theatre: "Q24354"
    orchestra: "Q42998"
    publisher: "Q2085381"
    "research-institute": "Q31855"
    "national-park": "Q46169"
    zoo: "Q43501"
    archive: "Q166118"
    "concert-hall": "Q1060829"
    "art-gallery": "Q1007870"
    "film-festival": "Q220505"
  }
  query_prefix: "SELECT ?item ?itemLabel ?site WHERE {\n  ?item wdt:P31/wdt:P279* wd:"
  query_suffix: " .\n  ?item wdt:P856 ?site .\n  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\". }\n}\nLIMIT "
  cannot_infer: "a publisher listed here is a host an encyclopedia records as someone's official website; it is not a channel, not a source this estate reads, and nothing here says the host answers or publishes a feed"
}

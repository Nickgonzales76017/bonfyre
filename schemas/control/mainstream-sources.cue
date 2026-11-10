package control

// Mainstream publishers, one family per thing people come to each territory
// for: SHOP deals, picks, retail and recalls; LIVE film, TV, music and sport;
// LIFE local news, food, home, health, travel and books; WORK jobs, running a
// small business and money.  Every feed was read live on 2026-10-18 and had
// items from the last two weeks.

nativeMainstreamSources: [...#CensusSource] & [
    {
        source: "feed-apartment-therapy-home", family: "home", kind: "text"
        label: "Apartment Therapy home", says: "what Apartment Therapy home publishes"
        twin: "feed-apartment-therapy-home", tier: "WARM", cadence_seconds: 3600
        url: "https://www.apartmenttherapy.com/main.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Apartment Therapy home's own feed; each story stays theirs"
    },
    {
        source: "feed-atlas-obscura", family: "travel", kind: "text"
        label: "Atlas Obscura", says: "what Atlas Obscura publishes"
        twin: "feed-atlas-obscura", tier: "WARM", cadence_seconds: 3600
        url: "https://www.atlasobscura.com/feeds/latest"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Atlas Obscura's own feed; each story stays theirs"
    },
    {
        source: "feed-bon-appetit", family: "food", kind: "text"
        label: "Bon App\u00e9tit", says: "what Bon App\u00e9tit publishes"
        twin: "feed-bon-appetit", tier: "WARM", cadence_seconds: 3600
        url: "https://www.bonappetit.com/feed/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Bon App\u00e9tit's own feed; each story stays theirs"
    },
    {
        source: "feed-budget-bytes", family: "food", kind: "text"
        label: "Budget Bytes", says: "what Budget Bytes publishes"
        twin: "feed-budget-bytes", tier: "WARM", cadence_seconds: 3600
        url: "https://www.budgetbytes.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Budget Bytes's own feed; each story stays theirs"
    },
    {
        source: "feed-candysdirt", family: "local-news", kind: "text"
        label: "CandysDirt", says: "what CandysDirt publishes"
        twin: "feed-candysdirt", tier: "WARM", cadence_seconds: 3600
        url: "https://candysdirt.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "CandysDirt's own feed; each story stays theirs"
    },
    {
        source: "feed-gardenista", family: "home", kind: "text"
        label: "Gardenista", says: "what Gardenista publishes"
        twin: "feed-gardenista", tier: "WARM", cadence_seconds: 3600
        url: "https://www.gardenista.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Gardenista's own feed; each story stays theirs"
    },
    {
        source: "feed-nbcdfw", family: "local-news", kind: "text"
        label: "NBC DFW", says: "what NBC DFW publishes"
        twin: "feed-nbcdfw", tier: "WARM", cadence_seconds: 3600
        url: "https://www.nbcdfw.com/?rss=y"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NBC DFW's own feed; each story stays theirs"
    },
    {
        source: "feed-npr-food", family: "food", kind: "text"
        label: "NPR Food", says: "what NPR Food publishes"
        twin: "feed-npr-food", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/1053/rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NPR Food's own feed; each story stays theirs"
    },
    {
        source: "feed-npr-health", family: "health", kind: "text"
        label: "NPR Health", says: "what NPR Health publishes"
        twin: "feed-npr-health", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/1128/rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NPR Health's own feed; each story stays theirs"
    },
    {
        source: "feed-nyt-books", family: "books", kind: "text"
        label: "NYT Books", says: "what NYT Books publishes"
        twin: "feed-nyt-books", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.nytimes.com/services/xml/rss/nyt/Books.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NYT Books's own feed; each story stays theirs"
    },
    {
        source: "feed-nyt-travel", family: "travel", kind: "text"
        label: "NYT Travel", says: "what NYT Travel publishes"
        twin: "feed-nyt-travel", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NYT Travel's own feed; each story stays theirs"
    },
    {
        source: "feed-nyt-well", family: "health", kind: "text"
        label: "NYT Well", says: "what NYT Well publishes"
        twin: "feed-nyt-well", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.nytimes.com/services/xml/rss/nyt/Well.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NYT Well's own feed; each story stays theirs"
    },
    {
        source: "feed-recipetin", family: "food", kind: "text"
        label: "RecipeTin Eats", says: "what RecipeTin Eats publishes"
        twin: "feed-recipetin", tier: "WARM", cadence_seconds: 3600
        url: "https://www.recipetineats.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "RecipeTin Eats's own feed; each story stays theirs"
    },
    {
        source: "feed-sallys-baking", family: "food", kind: "text"
        label: "Sally's Baking Addiction", says: "what Sally's Baking Addiction publishes"
        twin: "feed-sallys-baking", tier: "WARM", cadence_seconds: 3600
        url: "https://sallysbakingaddiction.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Sally's Baking Addiction's own feed; each story stays theirs"
    },
    {
        source: "feed-texas-monthly", family: "local-news", kind: "text"
        label: "Texas Monthly", says: "what Texas Monthly publishes"
        twin: "feed-texas-monthly", tier: "WARM", cadence_seconds: 3600
        url: "https://www.texasmonthly.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Texas Monthly's own feed; each story stays theirs"
    },
    {
        source: "feed-texas-tribune", family: "local-news", kind: "text"
        label: "The Texas Tribune", says: "what The Texas Tribune publishes"
        twin: "feed-texas-tribune", tier: "WARM", cadence_seconds: 3600
        url: "https://www.texastribune.org/feeds/main/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Texas Tribune's own feed; each story stays theirs"
    },
    {
        source: "feed-wfaa", family: "local-news", kind: "text"
        label: "WFAA", says: "what WFAA publishes"
        twin: "feed-wfaa", tier: "WARM", cadence_seconds: 3600
        url: "https://www.wfaa.com/feeds/syndication/rss/news"
        reading: {extract: "rss:item", title_field: "title"}
        note: "WFAA's own feed; each story stays theirs"
    },
    {
        source: "feed-billboard", family: "music", kind: "text"
        label: "Billboard", says: "what Billboard publishes"
        twin: "feed-billboard", tier: "WARM", cadence_seconds: 3600
        url: "https://www.billboard.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Billboard's own feed; each story stays theirs"
    },
    {
        source: "feed-cbs-sports", family: "sports", kind: "text"
        label: "CBS Sports", says: "what CBS Sports publishes"
        twin: "feed-cbs-sports", tier: "WARM", cadence_seconds: 3600
        url: "https://www.cbssports.com/rss/headlines/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "CBS Sports's own feed; each story stays theirs"
    },
    {
        source: "feed-consequence", family: "music", kind: "text"
        label: "Consequence", says: "what Consequence publishes"
        twin: "feed-consequence", tier: "WARM", cadence_seconds: 3600
        url: "https://consequence.net/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Consequence's own feed; each story stays theirs"
    },
    {
        source: "feed-dallas-observer-music", family: "music", kind: "text"
        label: "Dallas Observer music", says: "what Dallas Observer music publishes"
        twin: "feed-dallas-observer-music", tier: "WARM", cadence_seconds: 3600
        url: "https://www.dallasobserver.com/dallas/Rss.xml?section=2111398"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Dallas Observer music's own feed; each story stays theirs"
    },
    {
        source: "feed-deadline", family: "entertainment", kind: "text"
        label: "Deadline", says: "what Deadline publishes"
        twin: "feed-deadline", tier: "WARM", cadence_seconds: 3600
        url: "https://deadline.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Deadline's own feed; each story stays theirs"
    },
    {
        source: "feed-eonline", family: "entertainment", kind: "text"
        label: "E! News", says: "what E! News publishes"
        twin: "feed-eonline", tier: "WARM", cadence_seconds: 3600
        url: "https://www.eonline.com/syndication/feeds/rssfeeds/topstories.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "E! News's own feed; each story stays theirs"
    },
    {
        source: "feed-espn-nba", family: "sports", kind: "text"
        label: "ESPN NBA", says: "what ESPN NBA publishes"
        twin: "feed-espn-nba", tier: "WARM", cadence_seconds: 3600
        url: "https://www.espn.com/espn/rss/nba/news"
        reading: {extract: "rss:item", title_field: "title"}
        note: "ESPN NBA's own feed; each story stays theirs"
    },
    {
        source: "feed-espn-nfl", family: "sports", kind: "text"
        label: "ESPN NFL", says: "what ESPN NFL publishes"
        twin: "feed-espn-nfl", tier: "WARM", cadence_seconds: 3600
        url: "https://www.espn.com/espn/rss/nfl/news"
        reading: {extract: "rss:item", title_field: "title"}
        note: "ESPN NFL's own feed; each story stays theirs"
    },
    {
        source: "feed-espn-top", family: "sports", kind: "text"
        label: "ESPN", says: "what ESPN publishes"
        twin: "feed-espn-top", tier: "WARM", cadence_seconds: 3600
        url: "https://www.espn.com/espn/rss/news"
        reading: {extract: "rss:item", title_field: "title"}
        note: "ESPN's own feed; each story stays theirs"
    },
    {
        source: "feed-hollywood-reporter", family: "entertainment", kind: "text"
        label: "The Hollywood Reporter", says: "what The Hollywood Reporter publishes"
        twin: "feed-hollywood-reporter", tier: "WARM", cadence_seconds: 3600
        url: "https://www.hollywoodreporter.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Hollywood Reporter's own feed; each story stays theirs"
    },
    {
        source: "feed-pitchfork", family: "music", kind: "text"
        label: "Pitchfork", says: "what Pitchfork publishes"
        twin: "feed-pitchfork", tier: "WARM", cadence_seconds: 3600
        url: "https://pitchfork.com/feed/feed-news/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Pitchfork's own feed; each story stays theirs"
    },
    {
        source: "feed-rolling-stone", family: "music", kind: "text"
        label: "Rolling Stone", says: "what Rolling Stone publishes"
        twin: "feed-rolling-stone", tier: "WARM", cadence_seconds: 3600
        url: "https://www.rollingstone.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Rolling Stone's own feed; each story stays theirs"
    },
    {
        source: "feed-stereogum", family: "music", kind: "text"
        label: "Stereogum", says: "what Stereogum publishes"
        twin: "feed-stereogum", tier: "WARM", cadence_seconds: 3600
        url: "https://www.stereogum.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Stereogum's own feed; each story stays theirs"
    },
    {
        source: "feed-variety", family: "entertainment", kind: "text"
        label: "Variety", says: "what Variety publishes"
        twin: "feed-variety", tier: "WARM", cadence_seconds: 3600
        url: "https://variety.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Variety's own feed; each story stays theirs"
    },
    {
        source: "feed-apartment-therapy", family: "shopping", kind: "text"
        label: "Apartment Therapy", says: "what Apartment Therapy publishes"
        twin: "feed-apartment-therapy", tier: "WARM", cadence_seconds: 3600
        url: "https://www.apartmenttherapy.com/main.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Apartment Therapy's own feed; each story stays theirs"
    },
    {
        source: "feed-cpsc-recalls-rss", family: "recalls", kind: "text"
        label: "CPSC recalls", says: "what CPSC recalls publishes"
        twin: "feed-cpsc-recalls-rss", tier: "WARM", cadence_seconds: 3600
        url: "https://www.cpsc.gov/Newsroom/CPSC-RSS-Feed/Recalls-RSS"
        reading: {extract: "rss:item", title_field: "title"}
        note: "CPSC recalls's own feed; each story stays theirs"
    },
    {
        source: "feed-goodhousekeeping-products", family: "shopping", kind: "text"
        label: "Good Housekeeping", says: "what Good Housekeeping publishes"
        twin: "feed-goodhousekeeping-products", tier: "WARM", cadence_seconds: 3600
        url: "https://www.goodhousekeeping.com/rss/all.xml/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Good Housekeeping's own feed; each story stays theirs"
    },
    {
        source: "feed-hypebeast", family: "shopping", kind: "text"
        label: "Hypebeast", says: "what Hypebeast publishes"
        twin: "feed-hypebeast", tier: "WARM", cadence_seconds: 3600
        url: "https://hypebeast.com/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Hypebeast's own feed; each story stays theirs"
    },
    {
        source: "feed-kiplinger-spending", family: "shopping", kind: "text"
        label: "Kiplinger", says: "what Kiplinger publishes"
        twin: "feed-kiplinger-spending", tier: "WARM", cadence_seconds: 3600
        url: "https://www.kiplinger.com/feeds/all"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Kiplinger's own feed; each story stays theirs"
    },
    {
        source: "feed-modern-retail", family: "retail", kind: "text"
        label: "Modern Retail", says: "what Modern Retail publishes"
        twin: "feed-modern-retail", tier: "WARM", cadence_seconds: 3600
        url: "https://www.modernretail.co/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Modern Retail's own feed; each story stays theirs"
    },
    {
        source: "feed-retaildive", family: "retail", kind: "text"
        label: "Retail Dive", says: "what Retail Dive publishes"
        twin: "feed-retaildive", tier: "WARM", cadence_seconds: 3600
        url: "https://www.retaildive.com/feeds/news/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Retail Dive's own feed; each story stays theirs"
    },
    {
        source: "feed-slickdeals-frontpage", family: "deals", kind: "text"
        label: "Slickdeals", says: "what Slickdeals publishes"
        twin: "feed-slickdeals-frontpage", tier: "WARM", cadence_seconds: 3600
        url: "https://slickdeals.net/newsearch.php?mode=frontpage&searcharea=deals&searchin=first&rss=1"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Slickdeals's own feed; each story stays theirs"
    },
    {
        source: "feed-cnbc-personal-finance", family: "money", kind: "text"
        label: "CNBC personal finance", says: "what CNBC personal finance publishes"
        twin: "feed-cnbc-personal-finance", tier: "WARM", cadence_seconds: 3600
        url: "https://www.cnbc.com/id/21324812/device/rss/rss.html"
        reading: {extract: "rss:item", title_field: "title"}
        note: "CNBC personal finance's own feed; each story stays theirs"
    },
    {
        source: "feed-cnbc-small-business", family: "small-business", kind: "text"
        label: "CNBC small business", says: "what CNBC small business publishes"
        twin: "feed-cnbc-small-business", tier: "WARM", cadence_seconds: 3600
        url: "https://www.cnbc.com/id/44877279/device/rss/rss.html"
        reading: {extract: "rss:item", title_field: "title"}
        note: "CNBC small business's own feed; each story stays theirs"
    },
    {
        source: "feed-entrepreneur", family: "small-business", kind: "text"
        label: "Entrepreneur", says: "what Entrepreneur publishes"
        twin: "feed-entrepreneur", tier: "WARM", cadence_seconds: 3600
        url: "https://www.entrepreneur.com/latest.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Entrepreneur's own feed; each story stays theirs"
    },
    {
        source: "feed-fast-company", family: "small-business", kind: "text"
        label: "Fast Company", says: "what Fast Company publishes"
        twin: "feed-fast-company", tier: "WARM", cadence_seconds: 3600
        url: "https://www.fastcompany.com/latest/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Fast Company's own feed; each story stays theirs"
    },
    {
        source: "feed-inc", family: "small-business", kind: "text"
        label: "Inc.", says: "what Inc. publishes"
        twin: "feed-inc", tier: "WARM", cadence_seconds: 3600
        url: "https://www.inc.com/rss/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Inc.'s own feed; each story stays theirs"
    },
    {
        source: "feed-nerdwallet", family: "money", kind: "text"
        label: "NerdWallet", says: "what NerdWallet publishes"
        twin: "feed-nerdwallet", tier: "WARM", cadence_seconds: 3600
        url: "https://www.nerdwallet.com/blog/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NerdWallet's own feed; each story stays theirs"
    },
    {
        source: "feed-npr-business", family: "money", kind: "text"
        label: "NPR Business", says: "what NPR Business publishes"
        twin: "feed-npr-business", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/1006/rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NPR Business's own feed; each story stays theirs"
    },
    {
        source: "feed-remotive", family: "jobs", kind: "text"
        label: "Remotive", says: "what Remotive publishes"
        twin: "feed-remotive", tier: "WARM", cadence_seconds: 3600
        url: "https://remotive.com/remote-jobs/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Remotive's own feed; each story stays theirs"
    },
    {
        source: "feed-sba-news", family: "small-business", kind: "text"
        label: "SBA", says: "what SBA publishes"
        twin: "feed-sba-news", tier: "WARM", cadence_seconds: 3600
        url: "https://www.sba.gov/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "SBA's own feed; each story stays theirs"
    },
    {
        source: "feed-weworkremotely", family: "jobs", kind: "text"
        label: "We Work Remotely", says: "what We Work Remotely publishes"
        twin: "feed-weworkremotely", tier: "WARM", cadence_seconds: 3600
        url: "https://weworkremotely.com/remote-jobs.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "We Work Remotely's own feed; each story stays theirs"
    },
    {
        source: "feed-refinery29", family: "style", kind: "text"
        label: "Refinery29", says: "what Refinery29 publishes"
        twin: "feed-refinery29", tier: "WARM", cadence_seconds: 3600
        url: "https://www.refinery29.com/en-us/rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Refinery29's own feed; each story stays theirs"
    },
    {
        source: "feed-indiewire", family: "film", kind: "text"
        label: "IndieWire", says: "what IndieWire publishes"
        twin: "feed-indiewire", tier: "WARM", cadence_seconds: 3600
        url: "https://www.indiewire.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "IndieWire's own feed; each story stays theirs"
    },
    {
        source: "feed-bon-appetit-recipes", family: "food", kind: "text"
        label: "Bon App\u00e9tit recipes", says: "what Bon App\u00e9tit recipes publishes"
        twin: "feed-bon-appetit-recipes", tier: "WARM", cadence_seconds: 3600
        url: "https://www.bonappetit.com/feed/recipes-rss-feed/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Bon App\u00e9tit recipes's own feed; each story stays theirs"
    },
    {
        source: "feed-letterboxd-journal", family: "film", kind: "text"
        label: "Letterboxd Journal", says: "what Letterboxd Journal publishes"
        twin: "feed-letterboxd-journal", tier: "WARM", cadence_seconds: 3600
        url: "https://letterboxd.com/journal/rss/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Letterboxd Journal's own feed; each story stays theirs"
    },
    {
        source: "feed-slate", family: "culture", kind: "text"
        label: "Slate", says: "what Slate publishes"
        twin: "feed-slate", tier: "WARM", cadence_seconds: 3600
        url: "https://slate.com/feeds/all.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Slate's own feed; each story stays theirs"
    },
    {
        source: "feed-the-atlantic", family: "culture", kind: "text"
        label: "The Atlantic", says: "what The Atlantic publishes"
        twin: "feed-the-atlantic", tier: "WARM", cadence_seconds: 3600
        url: "https://www.theatlantic.com/feed/all/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Atlantic's own feed; each story stays theirs"
    },
    {
        source: "feed-defector", family: "sports", kind: "text"
        label: "Defector", says: "what Defector publishes"
        twin: "feed-defector", tier: "WARM", cadence_seconds: 3600
        url: "https://defector.com/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Defector's own feed; each story stays theirs"
    },
    {
        source: "feed-the-kitchn", family: "food", kind: "text"
        label: "The Kitchn", says: "what The Kitchn publishes"
        twin: "feed-the-kitchn", tier: "WARM", cadence_seconds: 3600
        url: "https://www.thekitchn.com/main.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Kitchn's own feed; each story stays theirs"
    },
    {
        source: "feed-d-magazine", family: "going-out", kind: "text"
        label: "D Magazine", says: "what D Magazine publishes"
        twin: "feed-d-magazine", tier: "WARM", cadence_seconds: 3600
        url: "https://www.dmagazine.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "D Magazine's own feed; each story stays theirs"
    },
    {
        source: "feed-the-free-press", family: "culture", kind: "text"
        label: "The Free Press", says: "what The Free Press publishes"
        twin: "feed-the-free-press", tier: "WARM", cadence_seconds: 3600
        url: "https://www.thefp.com/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Free Press's own feed; each story stays theirs"
    },
    {
        source: "feed-highsnobiety", family: "style", kind: "text"
        label: "Highsnobiety", says: "what Highsnobiety publishes"
        twin: "feed-highsnobiety", tier: "WARM", cadence_seconds: 3600
        url: "https://www.highsnobiety.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Highsnobiety's own feed; each story stays theirs"
    },
    {
        source: "feed-embedded", family: "entertainment", kind: "text"
        label: "Embedded", says: "what Embedded publishes"
        twin: "feed-embedded", tier: "WARM", cadence_seconds: 3600
        url: "https://embedded.substack.com/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Embedded's own feed; each story stays theirs"
    },
    {
        source: "feed-dallas-observer", family: "going-out", kind: "text"
        label: "Dallas Observer", says: "what Dallas Observer publishes"
        twin: "feed-dallas-observer", tier: "WARM", cadence_seconds: 3600
        url: "https://www.dallasobserver.com/dallas/Rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Dallas Observer's own feed; each story stays theirs"
    },
    {
        source: "feed-kottke", family: "culture", kind: "text"
        label: "Kottke", says: "what Kottke publishes"
        twin: "feed-kottke", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.kottke.org/main"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Kottke's own feed; each story stays theirs"
    },
    {
        source: "feed-nme", family: "music", kind: "text"
        label: "NME", says: "what NME publishes"
        twin: "feed-nme", tier: "WARM", cadence_seconds: 3600
        url: "https://www.nme.com/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "NME's own feed; each story stays theirs"
    },
    {
        source: "feed-blackbird-spyplane", family: "style", kind: "text"
        label: "Blackbird Spyplane", says: "what Blackbird Spyplane publishes"
        twin: "feed-blackbird-spyplane", tier: "WARM", cadence_seconds: 3600
        url: "https://www.blackbirdspyplane.com/feed"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Blackbird Spyplane's own feed; each story stays theirs"
    },
    {
        source: "feed-who-what-wear", family: "style", kind: "text"
        label: "Who What Wear", says: "what Who What Wear publishes"
        twin: "feed-who-what-wear", tier: "WARM", cadence_seconds: 3600
        url: "https://www.whowhatwear.com/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Who What Wear's own feed; each story stays theirs"
    },
    {
        source: "feed-axios", family: "today", kind: "text"
        label: "Axios", says: "what Axios publishes"
        twin: "feed-axios", tier: "WARM", cadence_seconds: 3600
        url: "https://api.axios.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Axios's own feed; each story stays theirs"
    },
    {
        source: "feed-polygon", family: "entertainment", kind: "text"
        label: "Polygon", says: "what Polygon publishes"
        twin: "feed-polygon", tier: "WARM", cadence_seconds: 3600
        url: "https://www.polygon.com/rss/index.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Polygon's own feed; each story stays theirs"
    },
    {
        source: "feed-hyperallergic", family: "art", kind: "text"
        label: "Hyperallergic", says: "what Hyperallergic publishes"
        twin: "feed-hyperallergic", tier: "WARM", cadence_seconds: 3600
        url: "https://hyperallergic.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Hyperallergic's own feed; each story stays theirs"
    },
    {
        source: "feed-uproxx", family: "entertainment", kind: "text"
        label: "Uproxx", says: "what Uproxx publishes"
        twin: "feed-uproxx", tier: "WARM", cadence_seconds: 3600
        url: "https://uproxx.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Uproxx's own feed; each story stays theirs"
    },
    {
        source: "feed-vox", family: "culture", kind: "text"
        label: "Vox", says: "what Vox publishes"
        twin: "feed-vox", tier: "WARM", cadence_seconds: 3600
        url: "https://www.vox.com/rss/index.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Vox's own feed; each story stays theirs"
    },
    {
        source: "feed-eater", family: "food", kind: "text"
        label: "Eater", says: "what Eater publishes"
        twin: "feed-eater", tier: "WARM", cadence_seconds: 3600
        url: "https://www.eater.com/rss/index.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Eater's own feed; each story stays theirs"
    },
    {
        source: "feed-colossal", family: "art", kind: "text"
        label: "Colossal", says: "what Colossal publishes"
        twin: "feed-colossal", tier: "WARM", cadence_seconds: 3600
        url: "https://www.thisiscolossal.com/feed/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Colossal's own feed; each story stays theirs"
    },
    {
        source: "feed-semafor", family: "today", kind: "text"
        label: "Semafor", says: "what Semafor publishes"
        twin: "feed-semafor", tier: "WARM", cadence_seconds: 3600
        url: "https://www.semafor.com/rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Semafor's own feed; each story stays theirs"
    },
    {
        source: "feed-the-takeout", family: "food", kind: "text"
        label: "The Takeout", says: "what The Takeout publishes"
        twin: "feed-the-takeout", tier: "WARM", cadence_seconds: 3600
        url: "https://www.thetakeout.com/rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Takeout's own feed; each story stays theirs"
    },
]

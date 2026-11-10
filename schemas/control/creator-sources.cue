package control

// Shows and stores people follow, chosen by what the publications they read
// have in common: a host or a brand they come back to, a recurring format,
// pictures, and something new every week.  General talk shows play on Listen;
// a show about one thing (food, film, money, style, Dallas) reads on that
// thing's page with its episode art.  Stores are the drops and lookbook of
// SHOP: what a brand or a creator just released, with its picture and price.
// Every feed was read live on 2026-10-19 and had items from the last two
// weeks.  YouTube channel feeds (Speeed, Donut, Bon Appetit, Vox, The Ringer
// and others) answered 404 that day, from here and through Firecrawl, so none
// is admitted until they answer again.

nativeCreatorSources: [...#CensusSource] & [
    {
        source: "show-pod-save-america", family: "podcasts", kind: "audio"
        label: "Pod Save America", says: "new episodes of Pod Save America, to play here"
        twin: "show-pod-save-america", tier: "WARM", cadence_seconds: 3600
        url: "https://audioboom.com/channels/5166624.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Pod Save America's own feed, hosted by Pod Save America; each episode stays theirs"
    },
    {
        source: "show-lovett-or-leave-it", family: "podcasts", kind: "audio"
        label: "Lovett or Leave It", says: "new episodes of Lovett or Leave It, to play here"
        twin: "show-lovett-or-leave-it", tier: "WARM", cadence_seconds: 3600
        url: "https://audioboom.com/channels/5166622.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Lovett or Leave It's own feed, hosted by Lovett or Leave It; each episode stays theirs"
    },
    {
        source: "show-smartless", family: "podcasts", kind: "audio"
        label: "SmartLess", says: "new episodes of SmartLess, to play here"
        twin: "show-smartless", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/hNaFxXpO"
        reading: {extract: "rss:item", title_field: "title"}
        note: "SmartLess's own feed, hosted by Jason Bateman, Sean Hayes, Will Arnett; each episode stays theirs"
    },
    {
        source: "show-call-her-daddy", family: "podcasts", kind: "audio"
        label: "Call Her Daddy", says: "new episodes of Call Her Daddy, to play here"
        twin: "show-call-her-daddy", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/mKn_QmLS"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Call Her Daddy's own feed, hosted by Alex Cooper; each episode stays theirs"
    },
    {
        source: "show-armchair-expert", family: "podcasts", kind: "audio"
        label: "Armchair Expert with Dax Shepard", says: "new episodes of Armchair Expert with Dax Shepard, to play here"
        twin: "show-armchair-expert", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.art19.com/armchair-expert"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Armchair Expert with Dax Shepard's own feed, hosted by Armchair Umbrella; each episode stays theirs"
    },
    {
        source: "show-new-heights", family: "podcasts", kind: "audio"
        label: "New Heights with Jason & Travis Kelce", says: "new episodes of New Heights with Jason & Travis Kelce, to play here"
        twin: "show-new-heights", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.art19.com/new-heights"
        reading: {extract: "rss:item", title_field: "title"}
        note: "New Heights with Jason & Travis Kelce's own feed, hosted by Wondery; each episode stays theirs"
    },
    {
        source: "show-conan-obrien-needs-a-friend", family: "podcasts", kind: "audio"
        label: "Conan O’Brien Needs A Friend", says: "new episodes of Conan O’Brien Needs A Friend, to play here"
        twin: "show-conan-obrien-needs-a-friend", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/dHoohVNH"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Conan O’Brien Needs A Friend's own feed, hosted by Team Coco & Earwolf; each episode stays theirs"
    },
    {
        source: "show-las-culturistas", family: "podcasts", kind: "audio"
        label: "Las Culturistas with Matt Rogers and Bowen Yang", says: "new episodes of Las Culturistas with Matt Rogers and Bowen Yang, to play here"
        twin: "show-las-culturistas", tier: "WARM", cadence_seconds: 3600
        url: "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/f6816727-c503-47ac-a7ac-ae2700391b1e/935c500f-8bb0-436b-ba7f-ae2700391b49/podcast.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Las Culturistas with Matt Rogers and Bowen Yang's own feed, hosted by Big Money Players Network and iHeartPodcasts; each episode stays theirs"
    },
    {
        source: "show-good-hang", family: "podcasts", kind: "audio"
        label: "Good Hang with Amy Poehler", says: "new episodes of Good Hang with Amy Poehler, to play here"
        twin: "show-good-hang", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/good-hang-with-amy-poehler"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Good Hang with Amy Poehler's own feed, hosted by The Ringer; each episode stays theirs"
    },
    {
        source: "show-the-moth", family: "podcasts", kind: "audio"
        label: "The Moth", says: "new episodes of The Moth, to play here"
        twin: "show-the-moth", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.amperwave.net/v2/feed/audacynetwork/the-moth"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Moth's own feed, hosted by The Moth; each episode stays theirs"
    },
    {
        source: "show-fresh-air", family: "podcasts", kind: "audio"
        label: "Fresh Air", says: "new episodes of Fresh Air, to play here"
        twin: "show-fresh-air", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/381444908/podcast.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Fresh Air's own feed, hosted by NPR; each episode stays theirs"
    },
    {
        source: "show-radiolab", family: "podcasts", kind: "audio"
        label: "Radiolab", says: "new episodes of Radiolab, to play here"
        twin: "show-radiolab", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/EmVW7VGp"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Radiolab's own feed, hosted by WNYC Studios; each episode stays theirs"
    },
    {
        source: "show-99-invisible", family: "podcasts", kind: "audio"
        label: "99% Invisible", says: "new episodes of 99% Invisible, to play here"
        twin: "show-99-invisible", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/BqbsxVfO"
        reading: {extract: "rss:item", title_field: "title"}
        note: "99% Invisible's own feed, hosted by Roman Mars; each episode stays theirs"
    },
    {
        source: "show-hidden-brain", family: "podcasts", kind: "audio"
        label: "Hidden Brain", says: "new episodes of Hidden Brain, to play here"
        twin: "show-hidden-brain", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/kwWc0lhf"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Hidden Brain's own feed, hosted by Hidden Brain, Shankar Vedantam; each episode stays theirs"
    },
    {
        source: "show-song-exploder", family: "podcasts", kind: "audio"
        label: "Song Exploder", says: "new episodes of Song Exploder, to play here"
        twin: "show-song-exploder", tier: "WARM", cadence_seconds: 3600
        url: "https://feed.songexploder.net/SongExploder"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Song Exploder's own feed, hosted by Hrishikesh Hirway; each episode stays theirs"
    },
    {
        source: "show-switched-on-pop", family: "podcasts", kind: "audio"
        label: "Switched on Pop", says: "new episodes of Switched on Pop, to play here"
        twin: "show-switched-on-pop", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/switchedonpop"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Switched on Pop's own feed, hosted by Vulture; each episode stays theirs"
    },
    {
        source: "show-pop-culture-happy-hour", family: "podcasts", kind: "audio"
        label: "Pop Culture Happy Hour", says: "new episodes of Pop Culture Happy Hour, to play here"
        twin: "show-pop-culture-happy-hour", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/510282/podcast.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Pop Culture Happy Hour's own feed, hosted by NPR; each episode stays theirs"
    },
    {
        source: "show-the-bill-simmons-podcast", family: "podcasts", kind: "audio"
        label: "The Bill Simmons Podcast", says: "new episodes of The Bill Simmons Podcast, to play here"
        twin: "show-the-bill-simmons-podcast", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/the-bill-simmons-podcast"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Bill Simmons Podcast's own feed, hosted by The Ringer; each episode stays theirs"
    },
    {
        source: "show-pardon-my-take", family: "podcasts", kind: "audio"
        label: "Pardon My Take", says: "new episodes of Pardon My Take, to play here"
        twin: "show-pardon-my-take", tier: "WARM", cadence_seconds: 3600
        url: "https://mcsorleys.barstoolsports.com/feed/pardon-my-take"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Pardon My Take's own feed, hosted by Barstool Sports; each episode stays theirs"
    },
    {
        source: "show-the-rewatchables", family: "podcasts", kind: "audio"
        label: "The Rewatchables", says: "new episodes of The Rewatchables, to play here"
        twin: "show-the-rewatchables", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/the-rewatchables"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Rewatchables's own feed, hosted by The Ringer; each episode stays theirs"
    },
    {
        source: "show-spikes-car-radio", family: "podcasts", kind: "audio"
        label: "Spike's Car Radio", says: "new episodes of Spike's Car Radio, to play here"
        twin: "show-spikes-car-radio", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/BVLLC7872108368"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Spike's Car Radio's own feed, hosted by Spike Feresten; each episode stays theirs"
    },
    {
        source: "show-freakonomics-radio", family: "podcasts", kind: "audio"
        label: "Freakonomics Radio", says: "new episodes of Freakonomics Radio, to play here"
        twin: "show-freakonomics-radio", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/Y8lFbOT4"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Freakonomics Radio's own feed, hosted by Freakonomics Radio + Stitcher; each episode stays theirs"
    },
    {
        source: "show-what-a-day", family: "today", kind: "text"
        label: "What A Day", says: "what What A Day is talking about this week"
        twin: "show-what-a-day", tier: "WARM", cadence_seconds: 3600
        url: "https://audioboom.com/channels/5166630.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "What A Day's own feed, hosted by What A Day; each episode stays theirs"
    },
    {
        source: "show-today-explained", family: "today", kind: "text"
        label: "Today, Explained", says: "what Today, Explained is talking about this week"
        twin: "show-today-explained", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/VMP5705694065"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Today, Explained's own feed, hosted by Vox; each episode stays theirs"
    },
    {
        source: "show-the-daily", family: "today", kind: "text"
        label: "The Daily", says: "what The Daily is talking about this week"
        twin: "show-the-daily", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/Sl5CSM3S"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Daily's own feed, hosted by The New York Times; each episode stays theirs"
    },
    {
        source: "show-the-big-picture", family: "film", kind: "text"
        label: "The Big Picture", says: "what The Big Picture is talking about this week"
        twin: "show-the-big-picture", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/the-big-picture"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Big Picture's own feed, hosted by The Ringer; each episode stays theirs"
    },
    {
        source: "show-blank-check", family: "film", kind: "text"
        label: "Blank Check with Griffin & David", says: "what Blank Check with Griffin & David is talking about this week"
        twin: "show-blank-check", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.acast.com/public/shows/6ac4309aaef0446505856133"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Blank Check with Griffin & David's own feed, hosted by Blank Check Productions; each episode stays theirs"
    },
    {
        source: "show-the-town", family: "film", kind: "text"
        label: "The Town with Matthew Belloni", says: "what The Town with Matthew Belloni is talking about this week"
        twin: "show-the-town", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/the-town-with-matthew-belloni"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Town with Matthew Belloni's own feed, hosted by The Ringer; each episode stays theirs"
    },
    {
        source: "show-popcast", family: "music", kind: "text"
        label: "Popcast", says: "what Popcast is talking about this week"
        twin: "show-popcast", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/TzbxCT1l"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Popcast's own feed, hosted by The New York Times; each episode stays theirs"
    },
    {
        source: "show-the-ringer-nba-show", family: "sports", kind: "text"
        label: "The Ringer NBA Show", says: "what The Ringer NBA Show is talking about this week"
        twin: "show-the-ringer-nba-show", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/the-ringer-nba-show"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Ringer NBA Show's own feed, hosted by The Ringer; each episode stays theirs"
    },
    {
        source: "show-the-mina-kimes-show", family: "sports", kind: "text"
        label: "The Mina Kimes Show featuring Lenny", says: "what The Mina Kimes Show featuring Lenny is talking about this week"
        twin: "show-the-mina-kimes-show", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/ESP8957020927"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Mina Kimes Show featuring Lenny's own feed, hosted by ESPN, Omaha Productions, Mina Kimes; each episode stays theirs"
    },
    {
        source: "show-the-ticket-top-10", family: "sports", kind: "text"
        label: "The Ticket Top 10", says: "what The Ticket Top 10 is talking about this week"
        twin: "show-the-ticket-top-10", tier: "WARM", cadence_seconds: 3600
        url: "https://www.omnycontent.com/d/playlist/a7b0bd27-d748-4fbe-ab3b-a6fa0049bcf6/685ad9ad-9963-4aab-a428-a88d0111928c/1784923a-360f-49ea-b992-a88d01119295/podcast.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Ticket Top 10's own feed, hosted by The Ticket | Cumulus Media Dallas; each episode stays theirs"
    },
    {
        source: "show-throwing-fits", family: "style", kind: "text"
        label: "Throwing Fits", says: "what Throwing Fits is talking about this week"
        twin: "show-throwing-fits", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.libsyn.com/shows/519643/destinations/4460808.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Throwing Fits's own feed, hosted by Throwing Fits; each episode stays theirs"
    },
    {
        source: "show-articles-of-interest", family: "style", kind: "text"
        label: "Articles of Interest", says: "what Articles of Interest is talking about this week"
        twin: "show-articles-of-interest", tier: "WARM", cadence_seconds: 3600
        url: "https://feed.articlesofinterest.club/"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Articles of Interest's own feed, hosted by Avery Trufelman; each episode stays theirs"
    },
    {
        source: "show-hodinkee-podcasts", family: "style", kind: "text"
        label: "HODINKEE Podcasts", says: "what HODINKEE Podcasts is talking about this week"
        twin: "show-hodinkee-podcasts", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/OzTmhziA"
        reading: {extract: "rss:item", title_field: "title"}
        note: "HODINKEE Podcasts's own feed, hosted by HODINKEE; each episode stays theirs"
    },
    {
        source: "show-gastropod", family: "food", kind: "text"
        label: "Gastropod", says: "what Gastropod is talking about this week"
        twin: "show-gastropod", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/VMP6255701211"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Gastropod's own feed, hosted by Cynthia Graber and Nicola Twilley; each episode stays theirs"
    },
    {
        source: "show-christopher-kimballs-milk-street-radio", family: "food", kind: "text"
        label: "Christopher Kimball’s Milk Street Radio", says: "what Christopher Kimball’s Milk Street Radio is talking about this week"
        twin: "show-christopher-kimballs-milk-street-radio", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.pdrl.fm/864d83/publicfeeds.net/f/9135/feed-rss.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Christopher Kimball’s Milk Street Radio's own feed, hosted by Milk Street Radio; each episode stays theirs"
    },
    {
        source: "show-the-splendid-table", family: "food", kind: "text"
        label: "The Splendid Table", says: "what The Splendid Table is talking about this week"
        twin: "show-the-splendid-table", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.publicradio.org/public_feeds/the-splendid-table"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Splendid Table's own feed, hosted by American Public Media; each episode stays theirs"
    },
    {
        source: "show-zero-to-travel-podcast", family: "travel", kind: "text"
        label: "Zero To Travel Podcast", says: "what Zero To Travel Podcast is talking about this week"
        twin: "show-zero-to-travel-podcast", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.introcast.io/778339885/feeds.megaphone.fm/ZTTIA6764283121"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Zero To Travel Podcast's own feed, hosted by Jason Moore; each episode stays theirs"
    },
    {
        source: "show-travel", family: "travel", kind: "text"
        label: "Travel with Rick Steves", says: "what Travel with Rick Steves is talking about this week"
        twin: "show-travel", tier: "WARM", cadence_seconds: 3600
        url: "https://www.ricksteves.com/rss/podcast"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Travel with Rick Steves's own feed, hosted by Rick Steves; each episode stays theirs"
    },
    {
        source: "show-huberman-lab", family: "health", kind: "text"
        label: "Huberman Lab", says: "what Huberman Lab is talking about this week"
        twin: "show-huberman-lab", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/hubermanlab"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Huberman Lab's own feed, hosted by Scicomm Media; each episode stays theirs"
    },
    {
        source: "show-we-can-do-hard-things", family: "health", kind: "text"
        label: "We Can Do Hard Things", says: "what We Can Do Hard Things is talking about this week"
        twin: "show-we-can-do-hard-things", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/wcdht"
        reading: {extract: "rss:item", title_field: "title"}
        note: "We Can Do Hard Things's own feed, hosted by Treat Media and Glennon Doyle; each episode stays theirs"
    },
    {
        source: "show-planet-money", family: "money", kind: "text"
        label: "Planet Money", says: "what Planet Money is talking about this week"
        twin: "show-planet-money", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/510289/podcast.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Planet Money's own feed, hosted by NPR; each episode stays theirs"
    },
    {
        source: "show-the-indicator-from-planet-money", family: "money", kind: "text"
        label: "The Indicator from Planet Money", says: "what The Indicator from Planet Money is talking about this week"
        twin: "show-the-indicator-from-planet-money", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.npr.org/510325/podcast.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Indicator from Planet Money's own feed, hosted by NPR; each episode stays theirs"
    },
    {
        source: "show-marketplace", family: "money", kind: "text"
        label: "Marketplace", says: "what Marketplace is talking about this week"
        twin: "show-marketplace", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.publicradio.org/public_feeds/marketplace"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Marketplace's own feed, hosted by Marketplace; each episode stays theirs"
    },
    {
        source: "show-how-i-built-this", family: "small-business", kind: "text"
        label: "How I Built This with Guy Raz", says: "what How I Built This with Guy Raz is talking about this week"
        twin: "show-how-i-built-this", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.art19.com/how-i-built-this"
        reading: {extract: "rss:item", title_field: "title"}
        note: "How I Built This with Guy Raz's own feed, hosted by Guy Raz | Wondery; each episode stays theirs"
    },
    {
        source: "show-my-first-million", family: "small-business", kind: "text"
        label: "My First Million", says: "what My First Million is talking about this week"
        twin: "show-my-first-million", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/HS2300184645"
        reading: {extract: "rss:item", title_field: "title"}
        note: "My First Million's own feed, hosted by Hubspot Media; each episode stays theirs"
    },
    {
        source: "show-the-prof-g-pod", family: "small-business", kind: "text"
        label: "The Prof G Pod with Scott Galloway", says: "what The Prof G Pod with Scott Galloway is talking about this week"
        twin: "show-the-prof-g-pod", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.megaphone.fm/WWO6655869236"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Prof G Pod with Scott Galloway's own feed, hosted by Vox Media Podcast Network; each episode stays theirs"
    },
    {
        source: "show-the-ezra-klein-show", family: "culture", kind: "text"
        label: "The Ezra Klein Show", says: "what The Ezra Klein Show is talking about this week"
        twin: "show-the-ezra-klein-show", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.simplecast.com/kEKXbjuJ"
        reading: {extract: "rss:item", title_field: "title"}
        note: "The Ezra Klein Show's own feed, hosted by New York Times Opinion; each episode stays theirs"
    },
    {
        source: "show-stuff-you-should-know", family: "culture", kind: "text"
        label: "Stuff You Should Know", says: "what Stuff You Should Know is talking about this week"
        twin: "show-stuff-you-should-know", tier: "WARM", cadence_seconds: 3600
        url: "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/a91018a4-ea4f-4130-bf55-ae270180c327/44710ecc-10bb-48d1-93c7-ae270180c33e/podcast.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Stuff You Should Know's own feed, hosted by iHeartPodcasts; each episode stays theirs"
    },
    {
        source: "show-what-should-i-read-next", family: "books", kind: "text"
        label: "What Should I Read Next?", says: "what What Should I Read Next? is talking about this week"
        twin: "show-what-should-i-read-next", tier: "WARM", cadence_seconds: 3600
        url: "https://rss.pdrl.fm/009d1c/feeds.megaphone.fm/ARML2867767043"
        reading: {extract: "rss:item", title_field: "title"}
        note: "What Should I Read Next?'s own feed, hosted by Anne Bogel; each episode stays theirs"
    },
    {
        source: "show-think-from-kera", family: "local-news", kind: "text"
        label: "Think from KERA", says: "what Think from KERA is talking about this week"
        twin: "show-think-from-kera", tier: "WARM", cadence_seconds: 3600
        url: "https://feeds.feedburner.com/KERA-Think"
        reading: {extract: "rss:item", title_field: "title"}
        note: "Think from KERA's own feed, hosted by KERA; each episode stays theirs"
    },
    {
        source: "show-wbaps-morning-news-podcast", family: "local-news", kind: "text"
        label: "WBAP's Morning News Podcast", says: "what WBAP's Morning News Podcast is talking about this week"
        twin: "show-wbaps-morning-news-podcast", tier: "WARM", cadence_seconds: 3600
        url: "https://www.omnycontent.com/d/playlist/a7b0bd27-d748-4fbe-ab3b-a6fa0049bcf6/19758321-d72f-4fbf-b582-aa8d01358236/6cc8a0f9-12d3-44ec-bc50-aa8d01358261/podcast.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "WBAP's Morning News Podcast's own feed, hosted by Cumulus Media Dallas; each episode stays theirs"
    },
    {
        source: "shop-crooked-store", family: "drops", kind: "image"
        label: "Crooked Store", says: "what Crooked Media just put in its store"
        twin: "shop-crooked-store", tier: "WARM", cadence_seconds: 21600
        url: "https://store.crooked.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://store.crooked.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Crooked Store's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-feastables", family: "drops", kind: "image"
        label: "Feastables and MrBeast", says: "what MrBeast's store just released"
        twin: "shop-feastables", tier: "WARM", cadence_seconds: 21600
        url: "https://www.mrbeast.store/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.mrbeast.store/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Feastables and MrBeast's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-heatonist", family: "drops", kind: "image"
        label: "Heatonist", says: "the hot sauces behind Hot Ones, as Heatonist stocks them"
        twin: "shop-heatonist", tier: "WARM", cadence_seconds: 21600
        url: "https://heatonist.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://heatonist.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Heatonist's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-noah", family: "style", kind: "image"
        label: "Noah", says: "what Noah just put out"
        twin: "shop-noah", tier: "WARM", cadence_seconds: 21600
        url: "https://www.noahny.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.noahny.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Noah's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-jjjjound", family: "style", kind: "image"
        label: "JJJJound", says: "what JJJJound just put out"
        twin: "shop-jjjjound", tier: "WARM", cadence_seconds: 21600
        url: "https://jjjjound.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://jjjjound.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "JJJJound's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-taylor-stitch", family: "style", kind: "image"
        label: "Taylor Stitch", says: "what Taylor Stitch just put out"
        twin: "shop-taylor-stitch", tier: "WARM", cadence_seconds: 21600
        url: "https://www.taylorstitch.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.taylorstitch.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Taylor Stitch's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-outdoor-voices", family: "style", kind: "image"
        label: "Outdoor Voices", says: "what Outdoor Voices just put out"
        twin: "shop-outdoor-voices", tier: "WARM", cadence_seconds: 21600
        url: "https://www.outdoorvoices.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.outdoorvoices.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Outdoor Voices's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-graza", family: "shopping", kind: "image"
        label: "Graza", says: "what Graza just put out"
        twin: "shop-graza", tier: "WARM", cadence_seconds: 21600
        url: "https://www.graza.co/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.graza.co/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Graza's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-great-jones", family: "shopping", kind: "image"
        label: "Great Jones", says: "what Great Jones just put out"
        twin: "shop-great-jones", tier: "WARM", cadence_seconds: 21600
        url: "https://www.greatjonesgoods.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.greatjonesgoods.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Great Jones's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-brightland", family: "shopping", kind: "image"
        label: "Brightland", says: "what Brightland just put out"
        twin: "shop-brightland", tier: "WARM", cadence_seconds: 21600
        url: "https://brightland.co/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://brightland.co/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Brightland's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-parachute", family: "shopping", kind: "image"
        label: "Parachute", says: "what Parachute just put out"
        twin: "shop-parachute", tier: "WARM", cadence_seconds: 21600
        url: "https://www.parachutehome.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.parachutehome.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Parachute's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
    {
        source: "shop-brooklinen", family: "shopping", kind: "image"
        label: "Brooklinen", says: "what Brooklinen just put out"
        twin: "shop-brooklinen", tier: "WARM", cadence_seconds: 21600
        url: "https://www.brooklinen.com/products.json?limit=50"
        reading: {extract: "products", title_field: "title", link_field: "handle", link_base: "https://www.brooklinen.com/products/",
            image_field: "images.0.src", meta_field: "published_at", maker_field: "vendor", sub_field: "variants.0.price",
            summary_field: "product_type", key_field: "id"}
        note: "Brooklinen's own store listing; a product shown is theirs, its price is the store's in its own currency when read"
    },
]

// Canonical Aurekai editorial voice. No publication rights or customer claims here.
// Usage: public brand pages, source-owned ServiceSite content and channel copy.
// Do not attach Aurekai signatures to independent customer or member-owned sites.
export const AUREKAI_VOICE = Object.freeze({
  company: "Aurekai",
  engine: "Bonfÿre",
  campaign: "Every brand is built on software. The ones everyone talks about are built on Aurekai.",
  hero: "Every brand runs on software. The ones that lead run on Aurekai.",
  authority: "Software runs every business. Aurekai powers the ones people remember.",
  signature: "Built on software. Driven by Aurekai.",
  promise: "Aurekai builds and operates the connected software behind distinctive digital businesses, products, publishing and commerce.",
  distinction: "Aurekai is the operating business; Bonfÿre is the native software engine. Independently operated brands keep their own names, customers and authority.",
  channels: Object.freeze({
    work: Object.freeze({title:"Build the business people remember.", description:"Websites, CRM, operations, finance and connected business applications.", cta:"Explore business systems"}),
    life: Object.freeze({title:"Make your corner of the internet your own.", description:"People, profiles, communities and the things worth keeping together.", cta:"Explore people and communities"}),
    live: Object.freeze({title:"Give your work more places to go.", description:"Publishing, creators, recordings and channels—with their source and credits intact.", cta:"Explore publishing and media"}),
    shop: Object.freeze({title:"Build the business behind every product.", description:"Catalogues, procurement, offers, inventory and connected commerce.", cta:"Explore products and markets"})
  }),
  fronts: Object.freeze({
    work: Object.freeze({"index":"01 / 04","category":"BUSINESS / WORK","detail":"Your customer experience depends on the systems behind it. Bring websites, customer relationships, finance and operations into useful work.","action":"/apps?context=self","label":"Open your applications"}),
    life: Object.freeze({"index":"02 / 04","category":"PEOPLE / LIFE","detail":"Profiles, communities, collections and the work people share deserve more than a disposable feed. Keep the things that matter connected.","action":"/people","label":"Explore people"}),
    live: Object.freeze({"index":"03 / 04","category":"PUBLISHING / LIVE","detail":"An article, show or recording should find its audience without losing who made it, its source or the story around it.","action":"/channels","label":"Explore channels"}),
    shop: Object.freeze({"index":"04 / 04","category":"COMMERCE / SHOP","detail":"A product moves through a supplier, a catalogue, inventory, a decision and a buyer. The software should support all of that work.","action":"/retail","label":"Explore retail"})
  }),
  forms: Object.freeze({
    pitch:"Every brand runs on software. The ones that lead run on Aurekai.",
    proposal:"Software runs every business. Aurekai powers the ones people remember.",
    signoff:"Built on software. Driven by Aurekai.",
    developer:"Bonfÿre: the native software behind Aurekai.",
    partner:"Your brand stays yours. The software can get stronger together."
  })
});
export const OWNED_BRAND_HOSTS = Object.freeze(["aurekai.work","aurekai.life","aurekai.live","aurekai.shop","aurekai-work-samples.pages.dev","docs.aurekai.work"]);
export function isAurekaiOwnedBrandHost(host) {
  return OWNED_BRAND_HOSTS.includes(String(host||"").toLowerCase());
}
export function channelFor(host) {
  const h=String(host||"").toLowerCase();
  const match=/^aurekai\.(work|life|live|shop)$/.exec(h);
  return match ? AUREKAI_VOICE.channels[match[1]] : null;
}

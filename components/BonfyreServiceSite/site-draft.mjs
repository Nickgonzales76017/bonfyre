// Pure draft factory. No CMS mutation or filesystem publication here.
export function newSiteDraft({site_id,business,contact,phone,email}) {
  if (typeof contact !== "string" || !contact.trim()) throw Error("contact name is required");
  const first=contact.split(/\s+/)[0];
  return {
    schema:"service-site.v1",site_id,pages_project:site_id,preview:true,leads:"mail",
    business:{name:business,page_title:business},contact:{name:contact,phone,email},
    action:"Get in touch",theme:{template:"ghost-ease"},
    sections:[
      {kind:"hero",headline:business,lede:"",form:{id:"request",title:"Tell us what you need",fields:[
        {label:"Your name",icon:"person",wide:true},
        {label:"Phone",type:"tel",icon:"call"},
        {label:"Email",type:"email",icon:"mail"}],
        button:"Send my request",subject:"Request",
        sent:"Your mail app has opened with your details. Press send and it comes straight to us."
      }},
      {kind:"steps",id:"how-it-works",menu:"How it works",title:"How it works",items:[
        {title:"Get in touch",text:"Call or send the form."}]},
      {kind:"reviews",id:"reviews",menu:"Reviews",items:[]},
      {kind:"person",id:"about",menu:"About "+first,text:"You deal with "+first+" directly."},
      {kind:"call",title:"Ready when you are"}
    ],
    footer:{},to_confirm:[
      "Every sentence on this page is a starting draft. Replace each with the business's own words before it goes public."
    ]
  };
}

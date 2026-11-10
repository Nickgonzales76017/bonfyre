// The native Aurekai builder's own reusable presentation vocabulary.
// These are CREATE controls available on an empty canvas, not imported pages.
// Every pattern is pure data; effects require a separately installed destination.
const spec=(name,group,description,payload,children=false,behavior="static")=>
 Object.freeze({kind:"ui:"+name,name,group,description,payload:Object.freeze(payload),
  children,behavior});
export const AUTHORING_PATTERNS=Object.freeze([
 spec("section","Layout","Responsive full-width section",{heading:"",lede:"",tone:"surface"},true),
 spec("container","Layout","Content width and alignment",{width:"wide",alignment:"start"},true),
 spec("columns","Layout","Responsive column layout",{columns:2,gap:20},true),
 spec("grid","Layout","Responsive item grid",{columns:3,gap:20},true),
 spec("stack","Layout","Flowing vertical layout",{gap:20},true),
 spec("hero","Content","Editorial hero",{eyebrow:"",title:"A new experience",lede:"Add a description",image:{src:"",alt:""},actions:[]},true),
 spec("card","Content","Media, text, actions",{title:"Card title",body:"Describe this item",image:{src:"",alt:""},actions:[]},true),
 spec("alert","Components","Status message",{tone:"info",title:"Information",body:"A clear message",dismissible:true},"", "dismiss"),
 spec("badge","Components","Compact status label",{label:"New",tone:"info"}),
 spec("buttons","Components","Primary and secondary actions",{items:[{label:"Explore",href:"#content",tone:"primary"}]}),
 spec("button-group","Components","Clustered actions",{items:[{label:"First",href:"#content"}]}),
 spec("breadcrumb","Navigation","Structured location trail",{items:[{label:"Home",href:"/"},{label:"Current",href:""}]}),
 spec("navbar","Navigation","Responsive navigation",{brand:"Your site",links:[{label:"Home",href:"/"},{label:"Work",href:"/work/"}],expanded:false},true,"menu"),
 spec("nav-tabs","Navigation","Tabbed navigation with content",{tabs:[{label:"Overview",body:"Overview content"},{label:"Details",body:"Details content"}]},"", "tabs"),
 spec("scrollspy","Navigation","Anchored sections and page outline",{links:[{label:"Overview",href:"#overview"}]},true,"scrollspy"),
 spec("pagination","Navigation","Paged navigation",{pages:[{label:"1",href:"/"},{label:"2",href:"/page/2/"}],current:1}),
 spec("accordion","Components","Accessible disclosure groups",{items:[{title:"First question",body:"An answer"},{title:"Second question",body:"Another answer"}]},"", "disclosure"),
 spec("collapse","Components","Disclosure toggle",{label:"Read more",body:"Details to reveal"},"", "disclosure"),
 spec("dropdown","Components","Expandable action menu",{label:"More",items:[{label:"View",href:"#content"}]},"", "disclosure"),
 spec("modal","Overlays","Modal dialog",{label:"Open dialog",title:"Dialog title",body:"Dialog content"},"", "dialog"),
 spec("offcanvas","Overlays","Sliding panel",{label:"Open panel",title:"Panel title",body:"Panel content"},"", "panel"),
 spec("toast","Overlays","Dismissible notice",{title:"Saved draft",body:"Your work is retained",dismissible:true},false,"dismiss"),
 spec("popover","Overlays","Toggleable information",{label:"Learn more",title:"More details",body:"Additional information"},"", "popover"),
 spec("tooltip","Overlays","Accessible tooltip",{label:"Information",hint:"Helpful explanation"},"", "tooltip"),
 spec("carousel","Media","Keyboard-accessible slide collection",{slides:[{title:"Slide one",body:"First slide",src:"",alt:""},{title:"Slide two",body:"Second slide",src:"",alt:""}]},"", "carousel"),
 spec("progress","Data","Measured progress indicator",{label:"Progress",value:45,max:100}),
 spec("spinner","Data","Loading indicator",{label:"Loading"}),
 spec("placeholder","Data","Loading placeholder",{lines:3}),
 spec("list-group","Data","Grouped linked items",{items:[{label:"Overview",href:"/"},{label:"Features",href:"#content"}]}),
 spec("table","Data","Structured responsive table",{headers:["Name","Value"],rows:[["Metric","42"],["Status","Active"]]}),
 spec("form","Forms","Reviewed form fields and explicit destination",{title:"Get in touch",fields:[{label:"Email",name:"email",type:"email",required:true},{label:"Message",name:"message",type:"textarea",required:true}],destination:""},true,"review-required"),
 spec("input","Forms","Labeled input control",{label:"Email",name:"email",type:"email",required:true,help:""},false,"review-required"),
 spec("checks-radios","Forms","Choice controls",{legend:"Select an option",type:"radio",items:[{label:"First",value:"first"},{label:"Second",value:"second"}]},false,"review-required"),
 spec("select","Forms","Labeled choice list",{label:"Choice",name:"choice",options:[{label:"First",value:"first"},{label:"Second",value:"second"}]},false,"review-required"),
 spec("range","Forms","Bounded range input",{label:"Range",min:0,max:100,value:50},false,"review-required"),
 spec("documentation","Editorial","Documentation with navigation and source refs",{title:"Documentation",lede:"Describe this capability",toc:[{label:"Overview",href:"#overview"}]},true)
].map(x=>Object.freeze({...x,children:!!x.children})));
export const PATTERN_BY_KIND=new Map(AUTHORING_PATTERNS.map(x=>[x.kind,x]));
export const PATTERN_GROUPS=Object.freeze([...new Set(AUTHORING_PATTERNS.map(x=>x.group))]);
export function defaultPatternPayload(kind){
 const pattern=PATTERN_BY_KIND.get(kind);
 if(!pattern)throw TypeError("Unknown native pattern "+kind);
 return structuredClone(pattern.payload);
}

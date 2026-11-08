/* Interactive arithmetic under user-defined, hypothetical assumptions. No network requests. */
(function() {
  "use strict";
  var dollar = function (x) { return new Intl.NumberFormat("en-US",{style:"currency",currency:"USD",maximumFractionDigits:2}).format(x); };
  var count = function (x) { return new Intl.NumberFormat("en-US",{maximumFractionDigits:1}).format(x); };
  var percent = function (x) { return new Intl.NumberFormat("en-US",{maximumFractionDigits:1}).format(x)+"%"; };
  var num = function(id) { var el=document.getElementById(id); return el ? Number(el.value) : NaN; };
  var valid = function(ids) { return ids.every(function(id){var e=document.getElementById(id);return e&&e.validity.valid&&Number.isFinite(Number(e.value));}); };
  var out = function(parent,name,value) { var e=parent.querySelector('[data-out="'+name+'"]'); if(e)e.textContent=value; };
  var bar = function(parent,name,value) { var e=parent.querySelector('[data-bar="'+name+'"]'); if(e)e.style.width=Math.max(0,Math.min(100,value))+"%"; };
  var result = function(id) {return document.getElementById(id);};
  var models = {};
  models["base-form"]=function() {
    var ids=["base-total","base-prevalence","base-sensitivity","base-fpr"];
    if(!valid(ids))return;
    var total=num(ids[0]), prev=num(ids[1])/100,sens=num(ids[2])/100,fpr=num(ids[3])/100;
    var tp=total*prev*sens,fp=total*(1-prev)*fpr,den=tp+fp,ppv=den===0?0:100*tp/den;
    var r=result("base-result"); out(r,"precision",den===0?"No alerts":percent(ppv));
    out(r,"tp",count(tp));out(r,"fp",count(fp));bar(r,"true",ppv);bar(r,"false",den===0?0:100-ppv);
    out(r,"base-explain",den===0?"Under these assumptions the detector produces no alerts. Precision is undefined.":"Of "+count(den)+" expected alerts, "+count(tp)+" are true cases and "+count(fp)+" are false alerts. The positive predictive value is "+percent(ppv)+".");
  };
  models["lot-form"]=function() {
    var ids=["lot-retail","lot-salable","lot-recovery","lot-buy","lot-freight","lot-labor","lot-fees","lot-returns"];
    if(!valid(ids))return;var values=ids.map(num);
    var revenue=values[0]*values[1]/100*values[2]/100;
    var costs=values.slice(3).reduce(function(a,b){return a+b;},0),profit=revenue-costs;
    var r=result("lot-result"),big=r.querySelector('[data-out="lot-profit"]');
    out(r,"lot-profit",dollar(profit));big.classList.toggle("negative",profit<0);
    out(r,"lot-revenue",dollar(revenue));out(r,"lot-costs",dollar(costs));
    var total=revenue+costs,share=total?100*revenue/total:0;
    bar(r,"lot-revenue",share);bar(r,"lot-cost",total?100-share:0);
    var breakEven=values[0]*values[2]/100>0?100*costs/(values[0]*values[2]/100):NaN;
    out(r,"lot-explain","Expected gross sales are "+dollar(revenue)+" against "+dollar(costs)+" in estimated costs. "+(Number.isFinite(breakEven)?"At the current resale ratio, break-even requires "+percent(breakEven)+" of the advertised inventory to sell.":"With zero resale recovery, this lot cannot reach break-even under these inputs."));
  };
  models["scope-form"]=function() {
    var ids=["scope-count","scope-independent","scope-platform","scope-custom"];
    if(!valid(ids))return;var n=num(ids[0]),ind=num(ids[1])*n,shared=num(ids[2])+num(ids[3])*n,savings=ind-shared;
    var r=result("scope-result"),big=r.querySelector('[data-out="scope-savings"]');
    out(r,"scope-savings",dollar(Math.abs(savings))+(savings>=0?" saved":" more costly"));big.classList.toggle("negative",savings<0);
    out(r,"scope-independent-cost",dollar(ind));out(r,"scope-shared-cost",dollar(shared));
    var total=ind+shared;bar(r,"scope-independent",total?100*ind/total:0);bar(r,"scope-shared",total?100*shared/total:0);
    var diff=num(ids[1])-num(ids[3]);
    var message=diff>0?"The shared option reaches break-even at "+Math.max(1,Math.ceil(num(ids[2])/diff))+" businesses under these fixed-cost assumptions.":"The per-business adaptation cost is at least the independent cost, so adding more businesses cannot recover the shared platform cost.";
    out(r,"scope-explain",message+" Real costs also include operating, support and coordination work.");
  };
  models["model-form"]=function() {
    var ids=["model-a-price","model-a-failure","model-b-price","model-b-failure","model-recovery"];
    if(!valid(ids))return;var cost=num(ids[4]),a=num(ids[0])+num(ids[1])/100*cost,b=num(ids[2])+num(ids[3])/100*cost;
    var r=result("model-result");out(r,"model-winner",a===b?"Same expected cost":a<b?"Route A":"Route B");
    out(r,"model-a-cost",dollar(a));out(r,"model-b-cost",dollar(b));
    var total=a+b;bar(r,"model-a",total?100*a/total:0);bar(r,"model-b",total?100*b/total:0);
    out(r,"model-explain","The expected-cost difference is "+dollar(Math.abs(a-b))+" per attempt. This includes one hypothetical failure loss; repeated retries, delay and quality constraints would change the result.");
  };
  Object.keys(models).forEach(function(id) {
    var form=document.getElementById(id);
    if(!form)return;
    form.addEventListener("input",models[id]);
    form.addEventListener("submit",function(e){e.preventDefault();models[id]();});
    var reset=document.querySelector('[data-reset="'+id+'"]');
    if(reset)reset.addEventListener("click",function(){form.reset();models[id]();});
    models[id]();
  });
})();
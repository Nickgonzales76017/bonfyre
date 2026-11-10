// The invitation owner still verifies the recipient, expiry and access. A link
// is only a locator to a private review; it never accepts its own invitation.
export function invitationReference(value,origin){
 if(typeof value!=='string'||value.length>2048)throw Error('Paste the complete invitation link you received.');
 let url,base;try{base=new URL(origin);url=new URL(value.trim(),base);}catch{throw Error('Use the original Aurekai invitation link.');}
 const estate=['aurekai.work','aurekai.life','aurekai.live','aurekai.shop'];
 const local=url.origin===base.origin&&['127.0.0.1','localhost'].includes(url.hostname);
 if((!local&&(url.protocol!=='https:'||url.port||!estate.includes(url.hostname)))||url.username||url.password||url.pathname!=='/organizations/join'||url.search||!/^#[a-f0-9]{64}$/.test(url.hash))throw Error('Use an Aurekai organization invitation link, not an account ID or code.');
 return {secret:url.hash.slice(1),service:local?'this Aurekai site':url.hostname};
}
export function mountInvitationChoice(document=globalThis.document,origin=globalThis.location?.origin){
 const form=document?.querySelector('#org-invite-review'),secret=form?.elements.namedItem('secret');
 if(!form||!secret||form.dataset.linkChoice)return null;form.dataset.linkChoice='';
 const oldLabel=secret.closest('label');if(oldLabel)oldLabel.hidden=true;secret.type='hidden';secret.required=false;
 const label=document.createElement('label');label.textContent='Invitation link';const input=document.createElement('input');input.type='text';input.inputMode='url';input.autocomplete='off';input.maxLength=2048;input.placeholder='Paste the private invitation link you received';input.setAttribute('aria-label','Invitation link');label.append(input);
 const help=document.createElement('p');help.textContent='Review the named organization and access before accepting. Opening this link does not join the organization.';
 const status=document.createElement('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 const submit=form.querySelector('button');submit.textContent='Review invitation';form.insertBefore(label,submit);form.insertBefore(help,submit);form.insertBefore(status,submit);
 const parse=()=>{secret.value='';input.setCustomValidity('');status.textContent='';if(!input.value.trim()){submit.disabled=true;return null;}try{const result=invitationReference(input.value,origin);status.textContent='Aurekai invitation ready for private review.';submit.disabled=false;return result;}catch(error){status.textContent=error.message;submit.disabled=true;return null;}};
 const changed=()=>parse();input.addEventListener('input',changed);
 const review=event=>{const selected=parse();if(!selected){event.preventDefault();input.focus();return;}secret.value=selected.secret;};form.addEventListener('submit',review);
 // A native fragment submission may already be in flight before this module
 // mounts. Do not make a second request or move a secret into a query string.
 if(secret.value){status.textContent='Opening your private invitation…';submit.disabled=true;}else parse();
 return {input,destroy(){input.removeEventListener('input',changed);form.removeEventListener('submit',review);}};
}
if(typeof document!=='undefined')mountInvitationChoice();

export function invitationDisplay(value){
 // Human previews deliberately omit the private token.
 const reference=invitationReference(value,'https://aurekai.work');
 return {title:'Organization invitation',service:reference.service,action:'Review invitation'};
}

// No directory of other people's accounts is queried by this component.
// The existing Auth invitation owner performs all reads and acceptance writes.

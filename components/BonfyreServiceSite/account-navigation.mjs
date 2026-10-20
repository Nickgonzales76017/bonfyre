/** View only: the existing Auth/CMS owners resolve identity, profile and installed work. */
export function sameOriginPath(value, origin) {
  if (typeof value !== 'string' || value.length > 2048 || /[\\\u0000-\u001f\u007f]/.test(value)) return null;
  try {const url = new URL(value,origin); return url.origin===new URL(origin).origin && !url.username && !url.password ? url.pathname+url.search+url.hash : null;} catch {return null;}
}
export function mountAccountNavigation({root,session,origin=root?.ownerDocument.defaultView.location.origin,links={},anonymousNodes=[]}={}) {
  if(!root?.ownerDocument || typeof session?.subscribe!=='function')throw Error('A root and the shared native session are required.');
  const document=root.ownerDocument,window=document.defaultView;
  root.dataset.akSession='';root.setAttribute('aria-label','Your account');
  const original=root.cloneNode(true), originalVisibility=anonymousNodes.map(node=>[node,node.hidden]);
  // A published profile/destination from a previous actor must not survive an account switch.
  const currentLinks=state=>{const value=typeof links==='function'?links(state):links;return value?.userId===state.user?.id?value:{};};
  const node=(tag,text,cls)=>{const e=document.createElement(tag);if(text)e.textContent=text;if(cls)e.className=cls;return e;};
  const anchor=(text,href)=>{const a=node('a',text);a.href=href;return a;};
  let menu=null,stopped=false;
  const check=()=>session.refresh({force:true}).catch(()=>{});
  const render=state=>{
    if(stopped)return;
    const focused=root.contains(document.activeElement);
    // Registration is only shown on confirmed anonymity, never a failed identity read.
    for(const el of anonymousNodes)el.hidden=state.kind!=='anonymous';
    root.replaceChildren();menu=null;root.dataset.state=state.kind;
    if(state.kind==='authenticated'){
      const routes=currentLinks(state),path=(key,fallback)=>sameOriginPath(routes[key],origin)||fallback;
      menu=node('details',null,'ak-account-menu');
      const summary=node('summary',null,'ak-account-trigger');
      summary.append(node('span',state.user.name.trim().slice(0,1).toLocaleUpperCase()||'A','ak-account-avatar'),node('span',state.user.name||'Your account','ak-account-name'),node('span','⌄','ak-account-caret'));
      const panel=node('nav',null,'ak-account-panel');panel.setAttribute('aria-label','Personal navigation');
      const profile=sameOriginPath(routes.profile,origin);
      panel.append(node('div','YOUR SPACE','ak-account-eyebrow'),anchor('Your work',path('work','/apps?context=self')),
        anchor(profile?'My profile':'Profile & publications',profile||'/account#presence'),anchor('Saved material',path('saved','/saved')),
        anchor('Connections',path('connections','/account#connections')),anchor('Account settings',path('account','/account')));
      if(routes.destinations){const p=sameOriginPath(routes.destinations,origin);if(p)panel.append(anchor('Switch workspace',p));}
      const logout=node('button','Sign out');logout.type='button';logout.className='ak-account-signout';
      const status=node('p',null,'ak-account-status');status.setAttribute('role','status');
      logout.onclick=async()=>{logout.disabled=true;try{const result=await session.logout();if(!result.confirmed){const target=root.querySelector('.ak-account-status')||root.appendChild(node('p',null,'ak-account-status'));target.setAttribute('role','alert');target.textContent='Sign-out was not confirmed. Check your account before trying again.';}}catch{status.textContent='Sign-out could not be confirmed.';}finally{if(logout.isConnected)logout.disabled=false;}};
      panel.append(logout,status);menu.append(summary,panel);root.append(menu);
      if(focused)summary.focus();
    }else if(state.kind==='anonymous'){
      const next=sameOriginPath(window.location.pathname+window.location.search+window.location.hash,origin)||'/';
      root.append(anchor('Sign in','/login?next='+encodeURIComponent(next)),anchor('Create account','/signup?next='+encodeURIComponent(next)));
    }else{
      const button=node('button',state.kind==='checking'?'Checking account…':'Check account');button.type='button';button.disabled=state.kind==='checking';button.onclick=check;root.append(button);
      if(state.kind==='unavailable'){const status=node('span','Account check unavailable','ak-account-status');status.setAttribute('role','status');root.append(status);}
    }
  };
  const unsubscribe=session.subscribe(render);
  const key=event=>{if(event.key==='Escape'&&menu?.open){menu.open=false;menu.querySelector('summary').focus();event.stopPropagation();}};
  const outside=event=>{if(menu?.open&&!root.contains(event.target))menu.open=false;};
  root.addEventListener('keydown',key);document.addEventListener('pointerdown',outside);
  return Object.freeze({dispose(){stopped=true;unsubscribe();root.removeEventListener('keydown',key);document.removeEventListener('pointerdown',outside);root.replaceChildren(...[...original.childNodes].map(n=>n.cloneNode(true)));for(const [node,hidden]of originalVisibility)node.hidden=hidden;delete root.dataset.akSession;delete root.dataset.state;}});
}

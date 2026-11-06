import {test,expect} from 'bun:test';
import {providerAuthCapability,authMethod,PROVIDER_AUTH_STRATEGIES,validProviderAuthCapability} from '../provider-auth-capability.mjs';
const entry=(id='200',patch={})=>({id,name:'Fixture Provider',url:'https://provider.example.invalid/setup',gate:'portal',note:'Fixture',...patch});
const fields=(...rows)=>rows.map(([field,kind])=>({field,kind}));
test('strategy vocabulary covers materially different authentication families',()=>{
 for(const strategy of ['atproto_oauth','oauth_pkce','oauth_confidential','oidc_pkce','oauth1','device_authorization','client_credentials','token_exchange','managed_installation','browser_signer','remote_signer','bot_token','personal_access_token','app_password','api_key','webhook','instance','approval_application','public_protocol','closed','portal'])expect(PROVIDER_AUTH_STRATEGIES).toContain(strategy);
 expect(new Set(PROVIDER_AUTH_STRATEGIES).size).toBe(PROVIDER_AUTH_STRATEGIES.length);
});
test('Bluesky separates OAuth, app password and public protocol without inventing OAuth readiness',()=>{
 const e=entry('001',{name:'Bluesky / AT Protocol',url:'https://bsky.app/',gate:'instance'});
 const value=providerAuthCapability(e);expect(validProviderAuthCapability(value)).toBe(true);expect(value.version).toBe(2);expect(value.recommended).toBe('atproto_oauth');
 expect(authMethod(value,'atproto_oauth')).toMatchObject({available:false,owner:'missing',input:'handle_or_did'});
 expect(authMethod(value,'atproto_oauth').features).toContain('dpop');expect(authMethod(value,'public_protocol')).toMatchObject({available:true,owner:'public'});
 expect(authMethod(value,'app_password').available).toBe(false);
 const declared=providerAuthCapability(e,{credentialFields:fields(['app_password','app_password'])});expect(authMethod(declared,'app_password')).toMatchObject({available:true,owner:'BonfyreCredentials',fields:['app_password']});
});
test('instance-first and signer protocols expose their actual prerequisites',()=>{
 const mastodon=providerAuthCapability(entry('002',{name:'Mastodon / ActivityPub',url:'https://joinmastodon.org/servers',gate:'instance'}));
 expect(mastodon.methods.map(x=>x.strategy)).toEqual(['instance','oauth_pkce','public_protocol']);expect(authMethod(mastodon,'instance')).toMatchObject({available:true,input:'instance_domain'});expect(authMethod(mastodon,'oauth_pkce').available).toBe(false);
 const nostr=providerAuthCapability(entry('003',{name:'Nostr Networks',url:'https://nostr.com/clients',gate:'instance'}));
 expect(nostr.methods.map(x=>x.strategy)).toEqual(['browser_signer','remote_signer','public_protocol']);expect(authMethod(nostr,'browser_signer')).toMatchObject({available:true,owner:'browser'});expect(authMethod(nostr,'remote_signer').features).toContain('nip46');
});
test('GitHub, Twitch and Spotify do not collapse their multiple grant types',()=>{
 const github=providerAuthCapability(entry('009',{name:'GitHub Pages / Releases',url:'https://github.com/login'}),{credentialFields:fields(['token','token'])});
 expect(github.methods.map(x=>x.strategy)).toEqual(['oauth_pkce','device_authorization','personal_access_token']);expect(authMethod(github,'personal_access_token')).toMatchObject({available:true,owner:'BonfyreCredentials'});
 const twitch=providerAuthCapability(entry('012',{name:'Twitch',url:'https://dev.twitch.tv/console/apps'}));expect(twitch.methods.map(x=>x.strategy)).toEqual(['oauth_pkce','device_authorization','client_credentials']);
 const spotify=providerAuthCapability(entry('017',{name:'Spotify (Web API)',url:'https://developer.spotify.com/dashboard'}));expect(spotify.methods.map(x=>x.strategy)).toEqual(['oauth_pkce','oauth_confidential','client_credentials']);
});
test('Shopify, Slack and Patreon keep install, token, webhook and OAuth paths distinct',()=>{
 const shopify=providerAuthCapability(entry('051',{name:'Shopify',url:'https://accounts.shopify.com/store-login'}));expect(shopify.methods.map(x=>x.strategy)).toEqual(['managed_installation','token_exchange','oauth_confidential','client_credentials']);
 const slack=providerAuthCapability(entry('056',{name:'Slack',url:'https://api.slack.com/apps'}));expect(slack.methods.map(x=>x.strategy)).toEqual(['oauth_confidential','webhook']);
 const patreon=providerAuthCapability(entry('028',{name:'Patreon',url:'https://www.patreon.com/portal/registration/register-clients'}));expect(patreon.methods.map(x=>x.strategy)).toEqual(['oauth_confidential','webhook']);
});
test('consumer, approval, public and credential providers get different surfaces',()=>{
 expect(providerAuthCapability(entry('085',{name:'Netflix',url:'https://www.netflix.com/login'})).recommended).toBe('closed');
 expect(providerAuthCapability(entry('082',{name:'Mediavine',url:'https://www.mediavine.com/apply/'})).recommended).toBe('approval_application');
 expect(providerAuthCapability(entry('008',{name:'RSS / Open Podcast Feeds',url:'https://www.rssboard.org/rss-specification',gate:'public'})).recommended).toBe('public_protocol');
 const api=providerAuthCapability(entry(),{credentialFields:fields(['api_key','api_key'])});expect(api.recommended).toBe('api_key');expect(authMethod(api,'api_key')).toMatchObject({available:true,owner:'BonfyreCredentials'});
 const fallback=providerAuthCapability(entry());expect(fallback.recommended).toBe('portal');expect(authMethod(fallback,'portal')).toMatchObject({available:true,owner:'provider'});
});
test('unsafe provider coordinates are rejected',()=>{for(const bad of [entry('x'),entry('200',{url:'http://provider.example.invalid'}),entry('200',{url:'https://user:pass@provider.example.invalid'})])expect(()=>providerAuthCapability(bad)).toThrow();});

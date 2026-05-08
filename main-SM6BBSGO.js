var XE=Object.defineProperty,qE=Object.defineProperties;var YE=Object.getOwnPropertyDescriptors;var e0=Object.getOwnPropertySymbols;var ZE=Object.prototype.hasOwnProperty,KE=Object.prototype.propertyIsEnumerable;var t0=(n,e,t)=>e in n?XE(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,he=(n,e)=>{for(var t in e||={})ZE.call(e,t)&&t0(n,t,e[t]);if(e0)for(var t of e0(e))KE.call(e,t)&&t0(n,t,e[t]);return n},st=(n,e)=>qE(n,YE(e));var ht=(n,e,t)=>new Promise((i,r)=>{var s=c=>{try{a(t.next(c))}catch(l){r(l)}},o=c=>{try{a(t.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(s,o);a((t=t.apply(n,e)).next())});var mn=null,Tl=!1,Jh=1,JE=null,Ln=Symbol("SIGNAL");function He(n){let e=mn;return mn=n,e}function Dl(){return mn}var Ea={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function wa(n){if(Tl)throw new Error("");if(mn===null)return;mn.consumerOnSignalRead(n);let e=mn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=mn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:mn.producers,t!==void 0&&t.producer===n)){mn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===mn&&(!i||ew(r,mn)))return;let s=ro(mn),o={producer:n,consumer:mn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};mn.producersTail=o,e!==void 0?e.nextProducer=o:mn.producers=o,s&&s0(n,o)}function n0(){Jh++}function Qh(n){if(!(ro(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Jh)){if(!n.producerMustRecompute(n)&&!Al(n)){Cl(n);return}n.producerRecomputeValue(n),Cl(n)}}function ep(n){if(n.consumers===void 0)return;let e=Tl;Tl=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||QE(i)}}finally{Tl=e}}function tp(){return mn?.consumerAllowSignalWrites!==!1}function QE(n){n.dirty=!0,ep(n),n.consumerMarkedDirty?.(n)}function Cl(n){n.dirty=!1,n.lastCleanEpoch=Jh}function Ta(n){return n&&i0(n),He(n)}function i0(n){n.producersTail=void 0,n.recomputing=!0}function Il(n,e){He(e),n&&r0(n)}function r0(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(ro(n))do t=np(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Al(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(Qh(t),i!==t.version))return!0}return!1}function Ca(n){if(ro(n)){let e=n.producers;for(;e!==void 0;)e=np(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function s0(n,e){let t=n.consumersTail,i=ro(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)s0(r.producer,r)}function np(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!ro(e)){let s=e.producers;for(;s!==void 0;)s=np(s)}return t}function ro(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function ip(n){JE?.(n)}function ew(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function rp(n,e){return Object.is(n,e)}function tw(){throw new Error}var o0=tw;function a0(n){o0(n)}function sp(n){o0=n}var nw=null;function op(n,e){let t=Object.create(Rl);t.value=n,e!==void 0&&(t.equal=e);let i=()=>c0(t);return i[Ln]=t,ip(t),[i,o=>so(t,o),o=>ap(t,o)]}function c0(n){return wa(n),n.value}function so(n,e){tp()||a0(n),n.equal(n.value,e)||(n.value=e,iw(n))}function ap(n,e){tp()||a0(n),so(n,e(n.value))}var Rl=st(he({},Ea),{equal:rp,value:void 0,kind:"signal"});function iw(n){n.version++,n0(),ep(n),nw?.(n)}function Ue(n){return typeof n=="function"}function oo(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Nl=oo(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Da(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var $t=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Ue(i))try{i()}catch(s){e=s instanceof Nl?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{l0(s)}catch(o){e=e??[],o instanceof Nl?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Nl(e)}}add(e){var t;if(e&&e!==this)if(this.closed)l0(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Da(t,e)}remove(e){let{_finalizers:t}=this;t&&Da(t,e),e instanceof n&&e._removeParent(this)}};$t.EMPTY=(()=>{let n=new $t;return n.closed=!0,n})();var cp=$t.EMPTY;function Pl(n){return n instanceof $t||n&&"closed"in n&&Ue(n.remove)&&Ue(n.add)&&Ue(n.unsubscribe)}function l0(n){Ue(n)?n():n.unsubscribe()}var ci={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ao={setTimeout(n,e,...t){let{delegate:i}=ao;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=ao;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Ol(n){ao.setTimeout(()=>{let{onUnhandledError:e}=ci;if(e)e(n);else throw n})}function Ia(){}var u0=lp("C",void 0,void 0);function d0(n){return lp("E",void 0,n)}function f0(n){return lp("N",n,void 0)}function lp(n,e,t){return{kind:n,value:e,error:t}}var cs=null;function co(n){if(ci.useDeprecatedSynchronousErrorHandling){let e=!cs;if(e&&(cs={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=cs;if(cs=null,t)throw i}}else n()}function h0(n){ci.useDeprecatedSynchronousErrorHandling&&cs&&(cs.errorThrown=!0,cs.error=n)}var ls=class extends $t{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Pl(e)&&e.add(this)):this.destination=ow}static create(e,t,i){return new lo(e,t,i)}next(e){this.isStopped?dp(f0(e),this):this._next(e)}error(e){this.isStopped?dp(d0(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?dp(u0,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},rw=Function.prototype.bind;function up(n,e){return rw.call(n,e)}var fp=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Ll(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Ll(i)}else Ll(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Ll(t)}}},lo=class extends ls{constructor(e,t,i){super();let r;if(Ue(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&ci.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&up(e.next,s),error:e.error&&up(e.error,s),complete:e.complete&&up(e.complete,s)}):r=e}this.destination=new fp(r)}};function Ll(n){ci.useDeprecatedSynchronousErrorHandling?h0(n):Ol(n)}function sw(n){throw n}function dp(n,e){let{onStoppedNotification:t}=ci;t&&ao.setTimeout(()=>t(n,e))}var ow={closed:!0,next:Ia,error:sw,complete:Ia};var uo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function li(n){return n}function hp(...n){return pp(n)}function pp(n){return n.length===0?li:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var at=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=cw(t)?t:new lo(t,i,r);return co(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=p0(i),new i((r,s)=>{let o=new lo({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[uo](){return this}pipe(...t){return pp(t)(this)}toPromise(t){return t=p0(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function p0(n){var e;return(e=n??ci.Promise)!==null&&e!==void 0?e:Promise}function aw(n){return n&&Ue(n.next)&&Ue(n.error)&&Ue(n.complete)}function cw(n){return n&&n instanceof ls||aw(n)&&Pl(n)}function lw(n){return Ue(n?.lift)}function ut(n){return e=>{if(lw(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function _t(n,e,t,i,r){return new mp(n,e,t,i,r)}var mp=class extends ls{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var m0=oo(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var on=(()=>{class n extends at{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new Fl(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new m0}next(t){co(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){co(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){co(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?cp:(this.currentObservers=null,s.push(t),new $t(()=>{this.currentObservers=null,Da(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new at;return t.source=this,t}}return n.create=(e,t)=>new Fl(e,t),n})(),Fl=class extends on{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:cp}};var an=class extends on{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var cn=new at(n=>n.complete());function g0(n){return n&&Ue(n.schedule)}function v0(n){return n[n.length-1]}function y0(n){return Ue(v0(n))?n.pop():void 0}function Sr(n){return g0(v0(n))?n.pop():void 0}function x0(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function _0(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function us(n){return this instanceof us?(this.v=n,this):new us(n)}function b0(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(h){return function(g){return Promise.resolve(g).then(h,d)}}function a(h,g){i[h]&&(r[h]=function(y){return new Promise(function(m,p){s.push([h,y,m,p])>1||c(h,y)})},g&&(r[h]=g(r[h])))}function c(h,g){try{l(i[h](g))}catch(y){f(s[0][3],y)}}function l(h){h.value instanceof us?Promise.resolve(h.value.v).then(u,d):f(s[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,g){h(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function S0(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof _0=="function"?_0(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var kl=n=>n&&typeof n.length=="number"&&typeof n!="function";function Ul(n){return Ue(n?.then)}function Bl(n){return Ue(n[uo])}function Vl(n){return Symbol.asyncIterator&&Ue(n?.[Symbol.asyncIterator])}function Hl(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function uw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var zl=uw();function Gl(n){return Ue(n?.[zl])}function jl(n){return b0(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield us(t.read());if(r)return yield us(void 0);yield yield us(i)}}finally{t.releaseLock()}})}function Wl(n){return Ue(n?.getReader)}function Qt(n){if(n instanceof at)return n;if(n!=null){if(Bl(n))return dw(n);if(kl(n))return fw(n);if(Ul(n))return hw(n);if(Vl(n))return M0(n);if(Gl(n))return pw(n);if(Wl(n))return mw(n)}throw Hl(n)}function dw(n){return new at(e=>{let t=n[uo]();if(Ue(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function fw(n){return new at(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function hw(n){return new at(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,Ol)})}function pw(n){return new at(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function M0(n){return new at(e=>{gw(n,e).catch(t=>e.error(t))})}function mw(n){return M0(jl(n))}function gw(n,e){var t,i,r,s;return x0(this,void 0,void 0,function*(){try{for(t=S0(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function In(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function $l(n,e=0){return ut((t,i)=>{t.subscribe(_t(i,r=>In(i,n,()=>i.next(r),e),()=>In(i,n,()=>i.complete(),e),r=>In(i,n,()=>i.error(r),e)))})}function Xl(n,e=0){return ut((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function E0(n,e){return Qt(n).pipe(Xl(e),$l(e))}function w0(n,e){return Qt(n).pipe(Xl(e),$l(e))}function T0(n,e){return new at(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function C0(n,e){return new at(t=>{let i;return In(t,e,()=>{i=n[zl](),In(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>Ue(i?.return)&&i.return()})}function ql(n,e){if(!n)throw new Error("Iterable cannot be null");return new at(t=>{In(t,e,()=>{let i=n[Symbol.asyncIterator]();In(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function D0(n,e){return ql(jl(n),e)}function I0(n,e){if(n!=null){if(Bl(n))return E0(n,e);if(kl(n))return T0(n,e);if(Ul(n))return w0(n,e);if(Vl(n))return ql(n,e);if(Gl(n))return C0(n,e);if(Wl(n))return D0(n,e)}throw Hl(n)}function zt(n,e){return e?I0(n,e):Qt(n)}function Je(...n){let e=Sr(n);return zt(n,e)}function gp(n,e){let t=Ue(n)?n:()=>n,i=r=>r.error(t());return new at(e?r=>e.schedule(i,0,r):i)}function Yl(n){return!!n&&(n instanceof at||Ue(n.lift)&&Ue(n.subscribe))}var ds=oo(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function Pt(n,e){return ut((t,i)=>{let r=0;t.subscribe(_t(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:vw}=Array;function yw(n,e){return vw(e)?n(...e):n(e)}function A0(n){return Pt(e=>yw(n,e))}var{isArray:_w}=Array,{getPrototypeOf:xw,prototype:bw,keys:Sw}=Object;function R0(n){if(n.length===1){let e=n[0];if(_w(e))return{args:e,keys:null};if(Mw(e)){let t=Sw(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Mw(n){return n&&typeof n=="object"&&xw(n)===bw}function N0(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function vp(...n){let e=Sr(n),t=y0(n),{args:i,keys:r}=R0(n);if(i.length===0)return zt([],e);let s=new at(Ew(i,e,r?o=>N0(r,o):li));return t?s.pipe(A0(t)):s}function Ew(n,e,t=li){return i=>{P0(e,()=>{let{length:r}=n,s=new Array(r),o=r,a=r;for(let c=0;c<r;c++)P0(e,()=>{let l=zt(n[c],e),u=!1;l.subscribe(_t(i,d=>{s[c]=d,u||(u=!0,a--),a||i.next(t(s.slice()))},()=>{--o||i.complete()}))},i)},i)}}function P0(n,e,t){n?In(t,n,e):e()}function O0(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,d=!1,f=()=>{d&&!c.length&&!l&&e.complete()},h=y=>l<i?g(y):c.push(y),g=y=>{s&&e.next(y),l++;let m=!1;Qt(t(y,u++)).subscribe(_t(e,p=>{r?.(p),s?h(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(l--;c.length&&l<i;){let p=c.shift();o?In(e,o,()=>g(p)):g(p)}f()}catch(p){e.error(p)}}))};return n.subscribe(_t(e,h,()=>{d=!0,f()})),()=>{a?.()}}function Mn(n,e,t=1/0){return Ue(e)?Mn((i,r)=>Pt((s,o)=>e(i,s,r,o))(Qt(n(i,r))),t):(typeof e=="number"&&(t=e),ut((i,r)=>O0(i,r,n,t)))}function L0(n=1/0){return Mn(li,n)}function F0(){return L0(1)}function fo(...n){return F0()(zt(n,Sr(n)))}function Aa(n){return new at(e=>{Qt(n()).subscribe(e)})}function Xi(n,e){return ut((t,i)=>{let r=0;t.subscribe(_t(i,s=>n.call(e,s,r++)&&i.next(s)))})}function Ra(n){return ut((e,t)=>{let i=null,r=!1,s;i=e.subscribe(_t(t,void 0,void 0,o=>{s=Qt(n(o,Ra(n)(e))),i?(i.unsubscribe(),i=null,s.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,s.subscribe(t))})}function Zl(n,e){return Ue(e)?Mn(n,e,1):Mn(n,1)}function k0(n){return ut((e,t)=>{let i=!1;e.subscribe(_t(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function qi(n){return n<=0?()=>cn:ut((e,t)=>{let i=0;e.subscribe(_t(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function U0(n=ww){return ut((e,t)=>{let i=!1;e.subscribe(_t(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function ww(){return new ds}function yp(n){return ut((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function Yi(n,e){let t=arguments.length>=2;return i=>i.pipe(n?Xi((r,s)=>n(r,s,i)):li,qi(1),t?k0(e):U0(()=>new ds))}function Kl(n){return n<=0?()=>cn:ut((e,t)=>{let i=[];e.subscribe(_t(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function _p(...n){let e=Sr(n);return ut((t,i)=>{(e?fo(n,t,e):fo(n,t)).subscribe(i)})}function ui(n,e){return ut((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(_t(i,c=>{r?.unsubscribe();let l=0,u=s++;Qt(n(c,u)).subscribe(r=_t(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}function Na(n){return ut((e,t)=>{Qt(n).subscribe(_t(t,()=>t.complete(),Ia)),!t.closed&&e.subscribe(t)})}function $n(n,e,t){let i=Ue(n)||e||t?{next:n,error:e,complete:t}:n;return i?ut((r,s)=>{var o;(o=i.subscribe)===null||o===void 0||o.call(i);let a=!0;r.subscribe(_t(s,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),s.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),s.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),s.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):li}var xp;function Jl(){return xp}function Ti(n){let e=xp;return xp=n,e}var B0=Symbol("NotFound");function ho(n){return n===B0||n?.name==="\u0275NotFound"}function V0(n){let e=He(null);try{return n()}finally{He(e)}}var Me=class extends Error{code;constructor(e,t){super(mo(e,t)),this.code=e}};function Iw(n){return`NG0${Math.abs(n)}`}function mo(n,e){return`${Iw(n)}${e?": "+e:""}`}var ys=globalThis;function dt(n){for(let e in n)if(n[e]===dt)return e;throw Error("")}function iu(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(iu).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function Lp(n,e){return n?e?`${n} ${e}`:n:e||""}var Aw=dt({__forward_ref__:dt});function ru(n){return n.__forward_ref__=ru,n}function ln(n){return Fp(n)?n():n}function Fp(n){return typeof n=="function"&&n.hasOwnProperty(Aw)&&n.__forward_ref__===ru}function Ee(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Ua(n){return Rw(n,su)}function kp(n){return Ua(n)!==null}function Rw(n,e){return n.hasOwnProperty(e)&&n[e]||null}function Nw(n){let e=n?.[su]??null;return e||null}function Sp(n){return n&&n.hasOwnProperty(eu)?n[eu]:null}var su=dt({\u0275prov:dt}),eu=dt({\u0275inj:dt}),Ie=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Ee({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Up(n){return n&&!!n.\u0275providers}var Bp=dt({\u0275cmp:dt}),Vp=dt({\u0275dir:dt}),Hp=dt({\u0275pipe:dt}),zp=dt({\u0275mod:dt}),Oa=dt({\u0275fac:dt}),_s=dt({__NG_ELEMENT_ID__:dt}),H0=dt({__NG_ENV_ID__:dt});function Gp(n){return ou(n,"@NgModule"),n[zp]||null}function wr(n){return ou(n,"@Component"),n[Bp]||null}function jp(n){return ou(n,"@Directive"),n[Vp]||null}function W0(n){return ou(n,"@Pipe"),n[Hp]||null}function ou(n,e){if(n==null)throw new Me(-919,!1)}function $0(n){return typeof n=="string"?n:n==null?"":String(n)}var X0=dt({ngErrorCode:dt}),Pw=dt({ngErrorMessage:dt}),Ow=dt({ngTokenPath:dt});function Wp(n,e){return q0("",-200,e)}function au(n,e){throw new Me(-201,!1)}function q0(n,e,t){let i=new Me(e,n);return i[X0]=e,i[Pw]=n,t&&(i[Ow]=t),i}function Lw(n){return n[X0]}var Mp;function Y0(){return Mp}function En(n){let e=Mp;return Mp=n,e}function $p(n,e,t){let i=Ua(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;au(n,"")}var Fw={},fs=Fw,kw="__NG_DI_FLAG__",Ep=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=hs(t)||0;try{return this.injector.get(e,i&8?null:fs,i)}catch(r){if(ho(r))return r;throw r}}};function Uw(n,e=0){let t=Jl();if(t===void 0)throw new Me(-203,!1);if(t===null)return $p(n,void 0,e);{let i=Bw(e),r=t.retrieve(n,i);if(ho(r)){if(i.optional)return null;throw r}return r}}function We(n,e=0){return(Y0()||Uw)(ln(n),e)}function J(n,e){return We(n,hs(e))}function hs(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Bw(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function wp(n){let e=[];for(let t=0;t<n.length;t++){let i=ln(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Me(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=Vw(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(We(r,s))}else e.push(We(i))}return e}function Vw(n){return n[kw]}function Mr(n,e){let t=n.hasOwnProperty(Oa);return t?n[Oa]:null}function Z0(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function K0(n){return n.flat(Number.POSITIVE_INFINITY)}function cu(n,e){n.forEach(t=>Array.isArray(t)?cu(t,e):e(t))}function Xp(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Ba(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}var xs={},ps=[],Ji=new Ie(""),qp=new Ie("",-1),Yp=new Ie(""),La=class{get(e,t=fs){if(t===fs){let r=q0("",-201);throw r.name="\u0275NotFound",r}return t}};function bs(n){return{\u0275providers:n}}function J0(n){return bs([{provide:Ji,multi:!0,useValue:n}])}function Q0(...n){return{\u0275providers:Zp(!0,n),\u0275fromNgModule:!0}}function Zp(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return cu(e,o=>{let a=o;tu(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&e_(r,s),t}function e_(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Kp(r,s=>{e(s,i)})}}function tu(n,e,t,i){if(n=ln(n),!n)return!1;let r=null,s=Sp(n),o=!s&&wr(n);if(!s&&!o){let c=n.ngModule;if(s=Sp(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)tu(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;cu(s.imports,u=>{tu(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&e_(l,e)}if(!a){let l=Mr(r)||(()=>new r);e({provide:r,useFactory:l,deps:ps},r),e({provide:Yp,useValue:r,multi:!0},r),e({provide:Ji,useValue:()=>We(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;Kp(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Kp(n,e){for(let t of n)Up(t)&&(t=t.\u0275providers),Array.isArray(t)?Kp(t,e):e(t)}var Hw=dt({provide:String,useValue:dt});function t_(n){return n!==null&&typeof n=="object"&&Hw in n}function zw(n){return!!(n&&n.useExisting)}function Gw(n){return!!(n&&n.useFactory)}function ms(n){return typeof n=="function"}function n_(n){return!!n.useClass}var Va=new Ie(""),Ql={},z0={},bp;function Ha(){return bp===void 0&&(bp=new La),bp}var Xt=class{},gs=class extends Xt{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Cp(e,o=>this.processProvider(o)),this.records.set(qp,po(void 0,this)),r.has("environment")&&this.records.set(Xt,po(void 0,this));let s=this.records.get(Va);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(Yp,ps,{self:!0}))}retrieve(e,t){let i=hs(t)||0;try{return this.get(e,fs,i)}catch(r){if(ho(r))return r;throw r}}destroy(){Pa(this),this._destroyed=!0;let e=He(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),He(e)}}onDestroy(e){return Pa(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Pa(this);let t=Ti(this),i=En(void 0),r;try{return e()}finally{Ti(t),En(i)}}get(e,t=fs,i){if(Pa(this),e.hasOwnProperty(H0))return e[H0](this);let r=hs(i),s,o=Ti(this),a=En(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=qw(e)&&Ua(e);u&&this.injectableDefInScope(u)?l=po(Tp(e),Ql):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Ha():this.parent;return t=r&8&&t===fs?null:t,c.get(e,t)}catch(c){let l=Lw(c);throw l===-200||l===-201?new Me(l,null):c}finally{En(a),Ti(o)}}resolveInjectorInitializers(){let e=He(null),t=Ti(this),i=En(void 0),r;try{let s=this.get(Ji,ps,{self:!0});for(let o of s)o()}finally{Ti(t),En(i),He(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=ln(e);let t=ms(e)?e:ln(e&&e.provide),i=Ww(e);if(!ms(e)&&e.multi===!0){let r=this.records.get(t);r||(r=po(void 0,Ql,!0),r.factory=()=>wp(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=He(null);try{if(t.value===z0)throw Wp("");return t.value===Ql&&(t.value=z0,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Xw(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{He(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=ln(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Tp(n){let e=Ua(n),t=e!==null?e.factory:Mr(n);if(t!==null)return t;if(n instanceof Ie)throw new Me(-204,!1);if(n instanceof Function)return jw(n);throw new Me(-204,!1)}function jw(n){if(n.length>0)throw new Me(-204,!1);let t=Nw(n);return t!==null?()=>t.factory(n):()=>new n}function Ww(n){if(t_(n))return po(void 0,n.useValue);{let e=Jp(n);return po(e,Ql)}}function Jp(n,e,t){let i;if(ms(n)){let r=ln(n);return Mr(r)||Tp(r)}else if(t_(n))i=()=>ln(n.useValue);else if(Gw(n))i=()=>n.useFactory(...wp(n.deps||[]));else if(zw(n))i=(r,s)=>We(ln(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=ln(n&&(n.useClass||n.provide));if($w(n))i=()=>new r(...wp(n.deps));else return Mr(r)||Tp(r)}return i}function Pa(n){if(n.destroyed)throw new Me(-205,!1)}function po(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function $w(n){return!!n.deps}function Xw(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function qw(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Cp(n,e){for(let t of n)Array.isArray(t)?Cp(t,e):t&&Up(t)?Cp(t.\u0275providers,e):e(t)}function gn(n,e){let t;n instanceof gs?(Pa(n),t=n):t=new Ep(n);let i,r=Ti(t),s=En(void 0);try{return e()}finally{Ti(r),En(s)}}function i_(){return Y0()!==void 0||Jl()!=null}var fi=0,Pe=1,ze=2,en=3,Xn=4,qn=5,za=6,go=7,un=8,Tr=9,Ci=10,vn=11,vo=12,Qp=13,Ss=14,Yn=15,Cr=16,Ms=17,Di=18,Dr=19,em=20,Zi=21,lu=22,Ga=23,Fn=24,uu=25,yo=26,tn=27,r_=1;var Ir=7,ja=8,Es=9,yn=10;function Ar(n){return Array.isArray(n)&&typeof n[r_]=="object"}function hi(n){return Array.isArray(n)&&n[r_]===!0}function tm(n){return(n.flags&4)!==0}function Rr(n){return n.componentOffset>-1}function du(n){return(n.flags&1)===1}function Nr(n){return!!n.template}function _o(n){return(n[ze]&512)!==0}function ws(n){return(n[ze]&256)===256}var s_="svg",o_="math";function Zn(n){for(;Array.isArray(n);)n=n[fi];return n}function a_(n,e){return Zn(e[n])}function Qi(n,e){return Zn(e[n.index])}function nm(n,e){return n.data[e]}function c_(n,e){return n[e]}function im(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function Ii(n,e){let t=e[n];return Ar(t)?t:t[fi]}function l_(n){return(n[ze]&4)===4}function fu(n){return(n[ze]&128)===128}function u_(n){return hi(n[en])}function Pr(n,e){return e==null?null:n[e]}function rm(n){n[Ms]=0}function sm(n){n[ze]&1024||(n[ze]|=1024,fu(n)&&$a(n))}function d_(n,e){for(;n>0;)e=e[Ss],n--;return e}function Wa(n){return!!(n[ze]&9216||n[Fn]?.dirty)}function hu(n){n[Ci].changeDetectionScheduler?.notify(8),n[ze]&64&&(n[ze]|=1024),Wa(n)&&$a(n)}function $a(n){n[Ci].changeDetectionScheduler?.notify(0);let e=Er(n);for(;e!==null&&!(e[ze]&8192||(e[ze]|=8192,!fu(e)));)e=Er(e)}function om(n,e){if(ws(n))throw new Me(911,!1);n[Zi]===null&&(n[Zi]=[]),n[Zi].push(e)}function f_(n,e){if(n[Zi]===null)return;let t=n[Zi].indexOf(e);t!==-1&&n[Zi].splice(t,1)}function Er(n){let e=n[en];return hi(e)?e[en]:e}function am(n){return n[go]??=[]}function cm(n){return n.cleanup??=[]}function h_(n,e,t,i){let r=am(e);r.push(t),n.firstCreatePass&&cm(n).push(i,r.length-1)}var tt={lFrame:T_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Dp=!1;function p_(){return tt.lFrame.elementDepthCount}function m_(){tt.lFrame.elementDepthCount++}function lm(){tt.lFrame.elementDepthCount--}function g_(){return tt.bindingsEnabled}function v_(){return tt.skipHydrationRootTNode!==null}function um(n){return tt.skipHydrationRootTNode===n}function dm(){tt.skipHydrationRootTNode=null}function Mt(){return tt.lFrame.lView}function Kn(){return tt.lFrame.tView}function pu(n){return tt.lFrame.contextLView=n,n[un]}function mu(n){return tt.lFrame.contextLView=null,n}function Jn(){let n=fm();for(;n!==null&&n.type===64;)n=n.parent;return n}function fm(){return tt.lFrame.currentTNode}function y_(){let n=tt.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function xo(n,e){let t=tt.lFrame;t.currentTNode=n,t.isParent=e}function hm(){return tt.lFrame.isParent}function __(){tt.lFrame.isParent=!1}function pm(){return Dp}function mm(n){let e=Dp;return Dp=n,e}function x_(){let n=tt.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function b_(n){return tt.lFrame.bindingIndex=n}function gm(){return tt.lFrame.bindingIndex++}function S_(){return tt.lFrame.inI18n}function M_(n,e){let t=tt.lFrame;t.bindingIndex=t.bindingRootIndex=n,gu(e)}function E_(){return tt.lFrame.currentDirectiveIndex}function gu(n){tt.lFrame.currentDirectiveIndex=n}function vm(){return tt.lFrame.currentQueryIndex}function vu(n){tt.lFrame.currentQueryIndex=n}function Yw(n){let e=n[Pe];return e.type===2?e.declTNode:e.type===1?n[qn]:null}function ym(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=Yw(s),r===null||(s=s[Ss],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=tt.lFrame=w_();return i.currentTNode=e,i.lView=n,!0}function yu(n){let e=w_(),t=n[Pe];tt.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function w_(){let n=tt.lFrame,e=n===null?null:n.child;return e===null?T_(n):e}function T_(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function C_(){let n=tt.lFrame;return tt.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var _m=C_;function _u(){let n=C_();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function D_(n){return(tt.lFrame.contextLView=d_(n,tt.lFrame.contextLView))[un]}function xu(){return tt.lFrame.selectedIndex}function Or(n){tt.lFrame.selectedIndex=n}function I_(){return tt.lFrame.currentNamespace}var A_=!0;function bu(){return A_}function Su(n){A_=n}function Ip(n,e=null,t=null,i){let r=xm(n,e,t,i);return r.resolveInjectorInitializers(),r}function xm(n,e=null,t=null,i,r=new Set){let s=[t||ps,Q0(n)],o;return new gs(s,e||Ha(),o||null,r)}var di=class n{static THROW_IF_NOT_FOUND=fs;static NULL=new La;static create(e,t){if(Array.isArray(e))return Ip({name:""},t,e,"");{let i=e.name??"";return Ip({name:i},e.parent,e.providers,i)}}static \u0275prov=Ee({token:n,providedIn:"any",factory:()=>We(qp)});static __NG_ELEMENT_ID__=-1},qt=new Ie(""),Lr=(()=>{class n{static __NG_ELEMENT_ID__=Zw;static __NG_ENV_ID__=t=>t}return n})(),Ap=class extends Lr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return ws(this._lView)}onDestroy(e){let t=this._lView;return om(t,e),()=>f_(t,e)}};function Zw(){return new Ap(Mt())}var bm=!1,R_=new Ie(""),er=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new an(!1);debugTaskTracker=J(R_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new at(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>new n})}return n})(),Rp=class extends on{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,i_()&&(this.destroyRef=J(Lr,{optional:!0})??void 0,this.pendingTasks=J(er,{optional:!0})??void 0)}emit(e){let t=He(null);try{super.next(e)}finally{He(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof $t&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},wn=Rp;function nu(...n){}function Sm(n){let e,t;function i(){n=nu;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch(r){}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function N_(n){return queueMicrotask(()=>n()),()=>{n=nu}}var Mm="isAngularZone",Fa=Mm+"_ID",Kw=0,Ot=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new wn(!1);onMicrotaskEmpty=new wn(!1);onStable=new wn(!1);onError=new wn(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=bm}=e;if(typeof Zone>"u")throw new Me(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,eT(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Mm)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Me(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Me(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,Jw,nu,nu);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},Jw={};function Em(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function Qw(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Sm(()=>{n.callbackScheduled=!1,Np(n),n.isCheckStableRunning=!0,Em(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Np(n)}function eT(n){let e=()=>{Qw(n)},t=Kw++;n._inner=n._inner.fork({name:"angular",properties:{[Mm]:!0,[Fa]:t,[Fa+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(tT(c))return i.invokeTask(s,o,a,c);try{return G0(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),j0(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return G0(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!nT(c)&&e(),j0(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Np(n),Em(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Np(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function G0(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function j0(n){n._nesting--,Em(n)}var ka=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new wn;onMicrotaskEmpty=new wn;onStable=new wn;onError=new wn;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function tT(n){return P_(n,"__ignore_ng_zone__")}function nT(n){return P_(n,"__scheduler_tick__")}function P_(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Ki=class{_console=console;handleError(e){this._console.error("ERROR",e)}},Qn=new Ie("",{factory:()=>{let n=J(Ot),e=J(Xt),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(Ki),t.handleError(i))})}}}),O_={provide:Ji,useValue:()=>{let n=J(Ki,{optional:!0})},multi:!0},iT=new Ie("",{factory:()=>{let n=J(qt).defaultView;if(!n)return;let e=J(Qn),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),J(Lr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function wm(){return bs([J0(()=>{J(iT)})])}function ei(n,e){let[t,i,r]=op(n,e?.equal),s=t,o=s[Ln];return s.set=i,s.update=r,s.asReadonly=L_.bind(s),s}function L_(){let n=this[Ln];if(n.readonlyFn===void 0){let e=()=>this();e[Ln]=n,n.readonlyFn=e}return n.readonlyFn}var vs=class{},bo=new Ie("",{factory:()=>!0});var Mu=new Ie("");var Tm=(()=>{class n{static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>new Pp})}return n})(),Pp=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Op=class{[Ln];constructor(e){this[Ln]=e}destroy(){this[Ln].destroy()}};function Wu(n){return{toString:n}.toString()}function mT(n){return typeof n=="function"}function lx(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var Du=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},$u=(()=>{let n=()=>ux;return n.ngInherit=!0,n})();function ux(n){return n.type.prototype.ngOnChanges&&(n.setInput=vT),gT}function gT(){let n=fx(this),e=n?.current;if(e){let t=n.previous;if(t===xs)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function vT(n,e,t,i,r){let s=this.declaredInputs[i],o=fx(n)||yT(n,{previous:xs,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Du(l&&l.currentValue,t,c===xs),lx(n,e,r,t)}var dx="__ngSimpleChanges__";function fx(n){return n[dx]||null}function yT(n,e){return n[dx]=e}var F_=[];var xt=function(n,e=null,t){for(let i=0;i<F_.length;i++){let r=F_[i];r(n,e,t)}},ct=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(ct||{});function _T(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=ux(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function xT(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function Eu(n,e,t){hx(n,e,3,t)}function wu(n,e,t,i){(n[ze]&3)===t&&hx(n,e,t,i)}function Cm(n,e){let t=n[ze];(t&3)===e&&(t&=16383,t+=1,n[ze]=t)}function hx(n,e,t,i){let r=i!==void 0?n[Ms]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Ms]+=65536),(a<s||s==-1)&&(bT(n,t,e,c),n[Ms]=(n[Ms]&4294901760)+c+2),c++}function k_(n,e){xt(ct.LifecycleHookStart,n,e);let t=He(null);try{e.call(n)}finally{He(t),xt(ct.LifecycleHookEnd,n,e)}}function bT(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[ze]>>14<n[Ms]>>16&&(n[ze]&3)===e&&(n[ze]+=16384,k_(a,s)):k_(a,s)}var Mo=-1,Cs=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function ST(n){return(n.flags&8)!==0}function MT(n){return(n.flags&16)!==0}function ET(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];TT(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function wT(n){return n===3||n===4||n===6}function TT(n){return n.charCodeAt(0)===64}function Xu(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?U_(n,t,r,null,e[++i]):U_(n,t,r,null,null))}}return n}function U_(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function px(n){return n!==Mo}function Iu(n){return n&32767}function CT(n){return n>>16}function Au(n,e){let t=CT(n),i=e;for(;t>0;)i=i[Ss],t--;return i}var Om=!0;function Ru(n){let e=Om;return Om=n,e}var DT=256,mx=DT-1,gx=5,IT=0,Ai={};function AT(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(_s)&&(i=t[_s]),i==null&&(i=t[_s]=IT++);let r=i&mx,s=1<<r;e.data[n+(r>>gx)]|=s}function Nu(n,e){let t=vx(n,e);if(t!==-1)return t;let i=e[Pe];i.firstCreatePass&&(n.injectorIndex=e.length,Dm(i.data,n),Dm(e,null),Dm(i.blueprint,null));let r=tg(n,e),s=n.injectorIndex;if(px(r)){let o=Iu(r),a=Au(r,e),c=a[Pe].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Dm(n,e){n.push(0,0,0,0,0,0,0,0,e)}function vx(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function tg(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Sx(r),i===null)return Mo;if(t++,r=r[Ss],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Mo}function Lm(n,e,t){AT(n,e,t)}function yx(n,e,t){if(t&8||n!==void 0)return n;au(e,"NodeInjector")}function _x(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[Tr],s=En(void 0);try{return r?r.get(e,i,t&8):$p(e,i,t&8)}finally{En(s)}}return yx(i,e,t)}function xx(n,e,t,i=0,r){if(n!==null){if(e[ze]&2048&&!(i&2)){let o=OT(n,e,t,i,Ai);if(o!==Ai)return o}let s=bx(n,e,t,i,Ai);if(s!==Ai)return s}return _x(e,t,i,r)}function bx(n,e,t,i,r){let s=NT(t);if(typeof s=="function"){if(!ym(e,n,i))return i&1?yx(r,t,i):_x(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))au(t);else return o}finally{_m()}}else if(typeof s=="number"){let o=null,a=vx(n,e),c=Mo,l=i&1?e[Yn][qn]:null;for((a===-1||i&4)&&(c=a===-1?tg(n,e):e[a+8],c===Mo||!V_(i,!1)?a=-1:(o=e[Pe],a=Iu(c),e=Au(c,e)));a!==-1;){let u=e[Pe];if(B_(s,a,u.data)){let d=RT(a,e,t,o,i,l);if(d!==Ai)return d}c=e[a+8],c!==Mo&&V_(i,e[Pe].data[a+8]===l)&&B_(s,a,e)?(o=u,a=Iu(c),e=Au(c,e)):a=-1}}return r}function RT(n,e,t,i,r,s){let o=e[Pe],a=o.data[n+8],c=i==null?Rr(a)&&Om:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=Tu(a,o,t,c,l);return u!==null?Ka(e,o,u,a,r):Ai}function Tu(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let g=o[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(r){let h=o[c];if(h&&Nr(h)&&h.type===t)return c}return null}function Ka(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Cs){let a=s;if(a.resolving)throw Wp("");let c=Ru(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,d=a.injectImpl?En(a.injectImpl):null,f=ym(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&_T(t,o[t],e)}finally{d!==null&&En(d),Ru(c),a.resolving=!1,_m()}}return s}function NT(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(_s)?n[_s]:void 0;return typeof e=="number"?e>=0?e&mx:PT:e}function B_(n,e,t){let i=1<<n;return!!(t[e+(n>>gx)]&i)}function V_(n,e){return!(n&2)&&!(n&1&&e)}var Ts=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return xx(this._tNode,this._lView,e,hs(i),t)}};function PT(){return new Ts(Jn(),Mt())}function ec(n){return Wu(()=>{let e=n.prototype.constructor,t=e[Oa]||Fm(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[Oa]||Fm(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function Fm(n){return Fp(n)?()=>{let e=Fm(ln(n));return e&&e()}:Mr(n)}function OT(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[ze]&2048&&!_o(o);){let a=bx(s,o,t,i|2,Ai);if(a!==Ai)return a;let c=s.parent;if(!c){let l=o[em];if(l){let u=l.get(t,Ai,i&-5);if(u!==Ai)return u}c=Sx(o),o=o[Ss]}s=c}return r}function Sx(n){let e=n[Pe],t=e.type;return t===2?e.declTNode:t===1?n[qn]:null}function LT(){return Do(Jn(),Mt())}function Do(n,e){return new kr(Qi(n,e))}var kr=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=LT}return n})();function FT(n){return n instanceof kr?n.nativeElement:n}function kT(){return this._results[Symbol.iterator]()}var Pu=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new on}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=K0(e);(this._changesDetected=!Z0(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=kT};function Mx(n){return(n.flags&128)===128}var ng=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(ng||{}),Ex=new Map,UT=0;function BT(){return UT++}function VT(n){Ex.set(n[Dr],n)}function km(n){Ex.delete(n[Dr])}var H_="__ngContext__";function Eo(n,e){Ar(e)?(n[H_]=e[Dr],VT(e)):n[H_]=e}function wx(n){return Cx(n[vo])}function Tx(n){return Cx(n[Xn])}function Cx(n){for(;n!==null&&!hi(n);)n=n[Xn];return n}var HT;function ig(n){HT=n}var qu=new Ie("",{factory:()=>zT}),zT="ng";var Yu=new Ie(""),tc=new Ie("",{providedIn:"platform",factory:()=>"unknown"});var Zu=new Ie("",{factory:()=>J(qt).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Dx=!1,Ix=new Ie("",{factory:()=>Dx});var z_=new WeakMap;function GT(n,e){if(n==null||typeof n!="object")return;let t=z_.get(n);t||(t=new WeakSet,z_.set(n,t)),t.add(e)}var jT=(n,e,t,i)=>{};function WT(n,e,t,i){jT(n,e,t,i)}function rg(n){return(n.flags&32)===32}var $T=()=>null;function Ax(n,e,t=!1){return $T(n,e,t)}function Rx(n,e){let t=n.contentQueries;if(t!==null){let i=He(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];vu(s),a.contentQueries(2,e[o],o)}}}finally{He(i)}}}function Um(n,e,t){vu(0);let i=He(null);try{e(n,t)}finally{He(i)}}function Nx(n,e,t){if(tm(e)){let i=He(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{He(i)}}}var mi=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(mi||{});function XT(n,e){return n.createText(e)}function qT(n,e,t){n.setValue(e,t)}function Px(n,e,t){return n.createElement(e,t)}function Ou(n,e,t,i,r){n.insertBefore(e,t,i,r)}function Ox(n,e,t){n.appendChild(e,t)}function G_(n,e,t,i,r){i!==null?Ou(n,e,t,i,r):Ox(n,e,t)}function YT(n,e,t,i){n.removeChild(null,e,t,i)}function ZT(n,e,t){n.setAttribute(e,"style",t)}function KT(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Lx(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&ET(n,e,i),r!==null&&KT(n,e,r),s!==null&&ZT(n,e,s)}function Fx(n){return n instanceof Function?n():n}function JT(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var kx="ng-template";function QT(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&JT(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(sg(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function sg(n){return n.type===4&&n.value!==kx}function eC(n,e,t){let i=n.type===4&&!t?kx:n.value;return e===i}function tC(n,e,t){let i=4,r=n.attrs,s=r!==null?rC(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!pi(i)&&!pi(c))return!1;if(o&&pi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!eC(n,c,t)||c===""&&e.length===1){if(pi(i))return!1;o=!0}}else if(i&8){if(r===null||!QT(n,r,c,t)){if(pi(i))return!1;o=!0}}else{let l=e[++a],u=nC(c,r,sg(n),t);if(u===-1){if(pi(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(pi(i))return!1;o=!0}}}}return pi(i)||o}function pi(n){return(n&1)===0}function nC(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return sC(e,n)}function iC(n,e,t=!1){for(let i=0;i<e.length;i++)if(tC(n,e[i],t))return!0;return!1}function rC(n){for(let e=0;e<n.length;e++){let t=n[e];if(wT(t))return e}return n.length}function sC(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function j_(n,e){return n?":not("+e.trim()+")":e}function oC(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!pi(o)&&(e+=j_(s,r),r=""),i=o,s=s||!pi(i);t++}return r!==""&&(e+=j_(s,r)),e}function aC(n){return n.map(oC).join(",")}function cC(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!pi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var Ur={};function og(n,e,t,i,r,s,o,a,c,l,u){let d=tn+i,f=d+r,h=lC(d,f),g=typeof l=="function"?l():l;return h[Pe]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function lC(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Ur);return t}function uC(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=og(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function ag(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[fi]=r,d[ze]=i|4|128|8|64|1024,(l!==null||n&&n[ze]&2048)&&(d[ze]|=2048),rm(d),d[en]=d[Ss]=n,d[un]=t,d[Ci]=o||n&&n[Ci],d[vn]=a||n&&n[vn],d[Tr]=c||n&&n[Tr]||null,d[qn]=s,d[Dr]=BT(),d[za]=u,d[em]=l,d[Yn]=e.type==2?n[Yn]:d,d}function dC(n,e,t){let i=Qi(e,n),r=uC(t),s=n[Ci].rendererFactory,o=cg(n,ag(n,r,null,Ux(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function Ux(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function Bx(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function cg(n,e){return n[vo]?n[Qp][Xn]=e:n[vo]=e,n[Qp]=e,e}function Br(n=1){Vx(Kn(),Mt(),xu()+n,!1)}function Vx(n,e,t,i){if(!i)if((e[ze]&3)===3){let s=n.preOrderCheckHooks;s!==null&&Eu(e,s,t)}else{let s=n.preOrderHooks;s!==null&&wu(e,s,0,t)}Or(t)}var Ku=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(Ku||{});function Bm(n,e,t,i){let r=He(null);try{let[s,o,a]=n.inputs[t],c=null;(o&Ku.SignalBased)!==0&&(c=e[s][Ln]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):lx(e,c,s,i)}finally{He(r)}}var Vr=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Vr||{}),fC;function lg(n,e){return fC(n,e)}var nz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Vm=new WeakMap,qa=new WeakSet;function hC(n,e){let t=Vm.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),qa.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function pC(n,e){let t=Vm.get(n);t?t.includes(e)||t.push(e):Vm.set(n,[e])}var wo=new Set,ug=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(ug||{}),Io=new Ie(""),W_=new Set;function As(n){W_.has(n)||(W_.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var Hx=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>new n})}return n})();var mC=new Ie("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:J(Xt)})});function zx(n,e,t){let i=n.get(mC);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function gC(n,e){for(let[t,i]of e)zx(n,i.animateFns)}function $_(n,e,t,i){let r=n?.[yo]?.enter;e!==null&&r&&r.has(t.index)&&gC(i,r)}function So(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;hi(r)?c=r:Ar(r)&&(l=!0,r=r[fi]);let u=Zn(r);n===0&&i!==null?($_(a,i,s,t),o==null?Ox(e,i,u):Ou(e,i,u,o||null,!0)):n===1&&i!==null?($_(a,i,s,t),Ou(e,i,u,o||null,!0),hC(s,u)):n===2?(a?.[yo]?.leave?.has(s.index)&&pC(s,u),qa.delete(u),X_(a,s,t,d=>{if(qa.has(u)){qa.delete(u);return}YT(e,u,l,d)})):n===3&&(qa.delete(u),X_(a,s,t,()=>{e.destroyNode(u)})),c!=null&&IC(e,n,t,c,s,i,o)}}function vC(n,e){Gx(n,e),e[fi]=null,e[qn]=null}function yC(n,e,t,i,r,s){i[fi]=r,i[qn]=e,Ju(n,i,t,1,r,s)}function Gx(n,e){e[Ci].changeDetectionScheduler?.notify(9),Ju(n,e,e[vn],2,null,null)}function _C(n){let e=n[vo];if(!e)return Im(n[Pe],n);for(;e;){let t=null;if(Ar(e))t=e[vo];else{let i=e[yn];i&&(t=i)}if(!t){for(;e&&!e[Xn]&&e!==n;)Ar(e)&&Im(e[Pe],e),e=e[en];e===null&&(e=n),Ar(e)&&Im(e[Pe],e),t=e&&e[Xn]}e=t}}function dg(n,e){let t=n[Es],i=t.indexOf(e);t.splice(i,1)}function fg(n,e){if(ws(e))return;let t=e[vn];t.destroyNode&&Ju(n,e,t,3,null,null),_C(e)}function Im(n,e){if(ws(e))return;let t=He(null);try{e[ze]&=-129,e[ze]|=256,e[Fn]&&Ca(e[Fn]),SC(n,e),bC(n,e),e[Pe].type===1&&e[vn].destroy();let i=e[Cr];if(i!==null&&hi(e[en])){i!==e[en]&&dg(i,e);let r=e[Di];r!==null&&r.detachView(n)}km(e)}finally{He(t)}}function X_(n,e,t,i){let r=n?.[yo];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&wo.add(n[Dr]),zx(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),xC(n,i)}else n&&wo.delete(n[Dr]),i(!1)},r)}function xC(n,e){let t=n[yo]?.running;if(t){t.then(()=>{n[yo].running=void 0,wo.delete(n[Dr]),e(!0)});return}e(!1)}function bC(n,e){let t=n.cleanup,i=e[go];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[go]=null);let r=e[Zi];if(r!==null){e[Zi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Ga];if(s!==null){e[Ga]=null;for(let o of s)o.destroy()}}function SC(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Cs)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];xt(ct.LifecycleHookStart,a,c);try{c.call(a)}finally{xt(ct.LifecycleHookEnd,a,c)}}else{xt(ct.LifecycleHookStart,r,s);try{s.call(r)}finally{xt(ct.LifecycleHookEnd,r,s)}}}}}function MC(n,e,t){return EC(n,e.parent,t)}function EC(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[fi];if(Rr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===mi.None||r===mi.Emulated)return null}return Qi(i,t)}function wC(n,e,t){return CC(n,e,t)}function TC(n,e,t){return n.type&40?Qi(n,t):null}var CC=TC,q_;function hg(n,e,t,i){let r=MC(n,i,e),s=e[vn],o=i.parent||e[qn],a=wC(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)G_(s,r,t[c],a,!1);else G_(s,r,t,a,!1);q_!==void 0&&q_(s,i,e,t,r)}function Ya(n,e){if(e!==null){let t=e.type;if(t&3)return Qi(e,n);if(t&4)return Hm(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Ya(n,i);{let r=n[e.index];return hi(r)?Hm(-1,r):Zn(r)}}else{if(t&128)return Ya(n,e.next);if(t&32)return lg(e,n)()||Zn(n[e.index]);{let i=jx(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Er(n[Yn]);return Ya(r,i)}else return Ya(n,e.next)}}}return null}function jx(n,e){if(e!==null){let i=n[Yn][qn],r=e.projection;return i.projection[r]}return null}function Hm(n,e){let t=yn+n+1;if(t<e.length){let i=e[t],r=i[Pe].firstChild;if(r!==null)return Ya(i,r)}return e[Ir]}function pg(n,e,t,i,r,s,o){for(;t!=null;){let a=i[Tr];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Eo(Zn(c),i),t.flags|=2),!rg(t))if(l&8)pg(n,e,t.child,i,r,s,!1),So(e,n,a,r,c,t,s,i);else if(l&32){let u=lg(t,i),d;for(;d=u();)So(e,n,a,r,d,t,s,i);So(e,n,a,r,c,t,s,i)}else l&16?DC(n,e,i,t,r,s):So(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function Ju(n,e,t,i,r,s){pg(t,i,n.firstChild,e,r,s,!1)}function DC(n,e,t,i,r,s){let o=t[Yn],c=o[qn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];So(e,n,t[Tr],r,u,i,s,t)}else{let l=c,u=o[en];Mx(i)&&(l.flags|=128),pg(n,e,l,u,r,s,!0)}}function IC(n,e,t,i,r,s,o){let a=i[Ir],c=Zn(i);a!==c&&So(e,n,t,s,a,r,o);for(let l=yn;l<i.length;l++){let u=i[l];Ju(u[Pe],u,n,e,s,a)}}function Wx(n,e,t,i,r){let s=xu(),o=i&2;try{Or(-1),o&&e.length>tn&&Vx(n,e,tn,!1);let a=o?ct.TemplateUpdateStart:ct.TemplateCreateStart;xt(a,r,t),t(i,r)}finally{Or(s);let a=o?ct.TemplateUpdateEnd:ct.TemplateCreateEnd;xt(a,r,t)}}function $x(n,e,t){PC(n,e,t),(t.flags&64)===64&&OC(n,e,t)}function mg(n,e,t=Qi){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function AC(n,e,t,i){let s=i.get(Ix,Dx)||t===mi.ShadowDom||t===mi.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return RC(o),o}function RC(n){NC(n)}var NC=()=>null;function PC(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Rr(t)&&dC(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Nu(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=Ka(e,n,o,t);if(Eo(c,e),s!==null&&kC(e,o-i,c,a,t,s),Nr(a)){let l=Ii(t.index,e);l[un]=Ka(e,n,o,t)}}}function OC(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=E_();try{Or(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];gu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&LC(c,l)}}finally{Or(-1),gu(o)}}function LC(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function FC(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];iC(e,s.selectors,!1)&&(i??=[],Nr(s)?i.unshift(s):i.push(s))}return i}function kC(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Bm(i,t,c,l)}}function Xx(n,e,t,i,r){let s=tn+t,o=e[Pe],a=r(o,e,n,i,t);e[s]=a,xo(n,!0);let c=n.type===2;return c?(Lx(e[vn],a,n),(p_()===0||du(n))&&Eo(a,e),m_()):Eo(a,e),bu()&&(!c||!rg(n))&&hg(o,e,a,n),n}function qx(n){let e=n;return hm()?__():(e=e.parent,xo(e,!1)),e}function UC(n,e){let t=n[Tr];if(!t)return;let i;try{i=t.get(Qn,null)}catch(r){i=null}i?.(e)}function Yx(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];Bm(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Bm(u,l,i,r),a=!0}return a}function BC(n,e){let t=Ii(e,n),i=t[Pe];VC(i,t);let r=t[fi];r!==null&&t[za]===null&&(t[za]=Ax(r,t[Tr])),xt(ct.ComponentStart);try{gg(i,t,t[un])}finally{xt(ct.ComponentEnd,t[un])}}function VC(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function gg(n,e,t){yu(e);try{let i=n.viewQuery;i!==null&&Um(1,i,t);let r=n.template;r!==null&&Wx(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Di]?.finishViewCreation(n),n.staticContentQueries&&Rx(n,e),n.staticViewQueries&&Um(2,n.viewQuery,t);let s=n.components;s!==null&&HC(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[ze]&=-5,_u()}}function HC(n,e){for(let t=0;t<e.length;t++)BC(n,e[t])}function Zx(n,e,t,i){let r=He(null);try{let s=e.tView,a=n[ze]&4096?4096:16,c=ag(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Cr]=l;let u=n[Di];return u!==null&&(c[Di]=u.createEmbeddedView(s)),gg(s,c,t),c}finally{He(r)}}function zm(n,e){return!e||e.firstChild===null||Mx(n)}function Ja(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Zn(s)),hi(s)&&Kx(s,i);let o=t.type;if(o&8)Ja(n,e,t.child,i);else if(o&32){let a=lg(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=jx(e,t);if(Array.isArray(a))i.push(...a);else{let c=Er(e[Yn]);Ja(c[Pe],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function Kx(n,e){for(let t=yn;t<n.length;t++){let i=n[t],r=i[Pe].firstChild;r!==null&&Ja(i[Pe],i,r,e)}n[Ir]!==n[fi]&&e.push(n[Ir])}function Jx(n){if(n[uu]!==null){for(let e of n[uu])e.impl.addSequence(e);n[uu].length=0}}var Qx=[];function zC(n){return n[Fn]??GC(n)}function GC(n){let e=Qx.pop()??Object.create(WC);return e.lView=n,e}function jC(n){n.lView[Fn]!==n&&(n.lView=null,Qx.push(n))}var WC=st(he({},Ea),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{$a(n.lView)},consumerOnSignalRead(){this.lView[Fn]=this}});function $C(n){let e=n[Fn]??Object.create(XC);return e.lView=n,e}var XC=st(he({},Ea),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Er(n.lView);for(;e&&!eb(e[Pe]);)e=Er(e);e&&sm(e)},consumerOnSignalRead(){this.lView[Fn]=this}});function eb(n){return n.type!==2}function tb(n){if(n[Ga]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Ga])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[ze]&8192)}}var qC=100;function nb(n,e=0){let i=n[Ci].rendererFactory,r=!1;r||i.begin?.();try{YC(n,e)}finally{r||i.end?.()}}function YC(n,e){let t=pm();try{mm(!0),Gm(n,e);let i=0;for(;Wa(n);){if(i===qC)throw new Me(103,!1);i++,Gm(n,1)}}finally{mm(t)}}function ZC(n,e,t,i){if(ws(e))return;let r=e[ze],s=!1,o=!1;yu(e);let a=!0,c=null,l=null;s||(eb(n)?(l=zC(e),c=Ta(l)):Dl()===null?(a=!1,l=$C(e),c=Ta(l)):e[Fn]&&(Ca(e[Fn]),e[Fn]=null));try{rm(e),b_(n.bindingStartIndex),t!==null&&Wx(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&Eu(e,h,null)}else{let h=n.preOrderHooks;h!==null&&wu(e,h,0,null),Cm(e,0)}if(o||KC(e),tb(e),ib(e,0),n.contentQueries!==null&&Rx(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&Eu(e,h)}else{let h=n.contentHooks;h!==null&&wu(e,h,1),Cm(e,1)}QC(n,e);let d=n.components;d!==null&&sb(e,d,0);let f=n.viewQuery;if(f!==null&&Um(2,f,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&Eu(e,h)}else{let h=n.viewHooks;h!==null&&wu(e,h,2),Cm(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[lu]){for(let h of e[lu])h();e[lu]=null}s||(Jx(e),e[ze]&=-73)}catch(u){throw s||$a(e),u}finally{l!==null&&(Il(l,c),a&&jC(l)),_u()}}function ib(n,e){for(let t=wx(n);t!==null;t=Tx(t))for(let i=yn;i<t.length;i++){let r=t[i];rb(r,e)}}function KC(n){for(let e=wx(n);e!==null;e=Tx(e)){if(!(e[ze]&2))continue;let t=e[Es];for(let i=0;i<t.length;i++){let r=t[i];sm(r)}}}function JC(n,e,t){xt(ct.ComponentStart);let i=Ii(e,n);try{rb(i,t)}finally{xt(ct.ComponentEnd,i[un])}}function rb(n,e){fu(n)&&Gm(n,e)}function Gm(n,e){let i=n[Pe],r=n[ze],s=n[Fn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Al(s)),o||=!1,s&&(s.dirty=!1),n[ze]&=-9217,o)ZC(i,n,i.template,n[un]);else if(r&8192){let a=He(null);try{tb(n),ib(n,1);let c=i.components;c!==null&&sb(n,c,1),Jx(n)}finally{He(a)}}}function sb(n,e,t){for(let i=0;i<e.length;i++)JC(n,e[i],t)}function QC(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Or(~r);else{let s=r,o=t[++i],a=t[++i];M_(o,s);let c=e[s];xt(ct.HostBindingsUpdateStart,c);try{a(2,c)}finally{xt(ct.HostBindingsUpdateEnd,c)}}}}finally{Or(-1)}}function vg(n,e){let t=pm()?64:1088;for(n[Ci].changeDetectionScheduler?.notify(e);n;){n[ze]|=t;let i=Er(n);if(_o(n)&&!i)return n;n=i}return null}function ob(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function eD(n,e){let t=yn+e;if(t<n.length)return n[t]}function ab(n,e,t,i=!0){let r=e[Pe];if(nD(r,e,n,t),i){let o=Hm(t,n),a=e[vn],c=a.parentNode(n[Ir]);c!==null&&yC(r,n[qn],a,e,c,o)}let s=e[za];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function tD(n,e){let t=Lu(n,e);return t!==void 0&&fg(t[Pe],t),t}function Lu(n,e){if(n.length<=yn)return;let t=yn+e,i=n[t];if(i){let r=i[Cr];r!==null&&r!==n&&dg(r,i),e>0&&(n[t-1][Xn]=i[Xn]);let s=Ba(n,yn+e);vC(i[Pe],i);let o=s[Di];o!==null&&o.detachView(s[Pe]),i[en]=null,i[Xn]=null,i[ze]&=-129}return i}function nD(n,e,t,i){let r=yn+i,s=t.length;i>0&&(t[r-1][Xn]=e),i<s-yn?(e[Xn]=t[r],Xp(t,yn+i,e)):(t.push(e),e[Xn]=null),e[en]=t;let o=e[Cr];o!==null&&t!==o&&cb(o,e);let a=e[Di];a!==null&&a.insertView(n),hu(e),e[ze]|=128}function cb(n,e){let t=n[Es],i=e[en];if(Ar(i))n[ze]|=2;else{let r=i[en][Yn];e[Yn]!==r&&(n[ze]|=2)}t===null?n[Es]=[e]:t.push(e)}var Fr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Pe];return Ja(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[un]}set context(e){this._lView[un]=e}get destroyed(){return ws(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[en];if(hi(e)){let t=e[ja],i=t?t.indexOf(this):-1;i>-1&&(Lu(e,i),Ba(t,i))}this._attachedToViewContainer=!1}fg(this._lView[Pe],this._lView)}onDestroy(e){om(this._lView,e)}markForCheck(){vg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[ze]&=-129}reattach(){hu(this._lView),this._lView[ze]|=128}detectChanges(){this._lView[ze]|=1024,nb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Me(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=_o(this._lView),t=this._lView[Cr];t!==null&&!e&&dg(t,this._lView),Gx(this._lView[Pe],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Me(902,!1);this._appRef=e;let t=_o(this._lView),i=this._lView[Cr];i!==null&&!t&&cb(i,this._lView),hu(this._lView)}};var To=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=iD;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=Zx(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Fr(s)}}return n})();function iD(){return yg(Jn(),Mt())}function yg(n,e){return n.type&4?new To(e,n,Do(n,e)):null}function Qu(n,e,t,i,r){let s=n.data[e];if(s===null)s=rD(n,e,t,i,r),S_()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=y_();s.injectorIndex=o===null?-1:o.injectorIndex}return xo(s,!0),s}function rD(n,e,t,i,r){let s=fm(),o=hm(),a=o?s:s&&s.parent,c=n.data[e]=oD(n,a,t,e,i,r);return sD(n,c,s,o),c}function sD(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function oD(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return v_()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var aD=()=>null,cD=()=>null;function Y_(n,e){return aD(n,e)}function lD(n,e,t){return cD(n,e,t)}var lb=class{},ed=class{},jm=class{resolveComponentFactory(e){throw new Me(917,!1)}},nc=class{static NULL=new jm},Ds=class{};var ub=(()=>{class n{static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>null})}return n})();var Cu={},Wm=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Cu,i);return r!==Cu||t===Cu?r:this.parentInjector.get(e,t,i)}};function Fu(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Lp(r,a);else if(s==2){let c=a,l=e[++o];i=Lp(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Rs(n,e=0){let t=Mt();if(t===null)return We(n,e);let i=Jn();return xx(i,t,ln(n),e)}function uD(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}hD(n,e,t,a,s,c,l)}s!==null&&i!==null&&dD(t,i,s)}function dD(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Me(-301,!1);i.push(e[r],s)}}function fD(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function hD(n,e,t,i,r,s,o){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&Nr(h)&&(c=h,fD(n,t,f)),Lm(Nu(t,e),n,h.type)}_D(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=Bx(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=Xu(t.mergedAttrs,h.hostAttrs),mD(n,t,e,d,h),yD(d,h,r),o!==null&&o.has(h)){let[y,m]=o.get(h);t.directiveToIndex.set(h.type,[d,y+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}pD(n,t,s)}function pD(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Z_(0,e,r,i),Z_(1,e,r,i),J_(e,i,!1);else{let s=t.get(r);K_(0,e,s,i),K_(1,e,s,i),J_(e,i,!0)}}}function Z_(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),db(e,s)}}function K_(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),db(e,o)}}function db(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function J_(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||sg(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function mD(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Mr(r.type,!0)),o=new Cs(s,Nr(r),Rs,null);n.blueprint[i]=o,t[i]=o,gD(n,e,i,Bx(n,t,r.hostVars,Ur),r)}function gD(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;vD(o)!=a&&o.push(a),o.push(t,i,s)}}function vD(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function yD(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Nr(e)&&(t[""]=n)}}function _D(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function fb(n,e,t,i,r,s,o,a){let c=e[Pe],l=c.consts,u=Pr(l,o),d=Qu(c,n,t,i,u);return s&&uD(c,e,d,Pr(l,a),r),d.mergedAttrs=Xu(d.mergedAttrs,d.attrs),d.attrs!==null&&Fu(d,d.attrs,!1),d.mergedAttrs!==null&&Fu(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function hb(n,e){xT(n,e),tm(e)&&n.queries.elementEnd(e)}function xD(n,e,t,i,r,s){let o=e.consts,a=Pr(o,r),c=Qu(e,n,t,i,a);if(c.mergedAttrs=Xu(c.mergedAttrs,c.attrs),s!=null){let l=Pr(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&Fu(c,c.attrs,!1),c.mergedAttrs!==null&&Fu(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function bD(n,e,t){return n[e]=t}function ku(n,e,t){if(t===Ur)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function SD(n,e,t,i){let r=ku(n,e,t);return ku(n,e+1,i)||r}function MD(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&GT(r,s);let o=Rr(n)?Ii(n.index,e):e;vg(o,5);let a=e[un],c=Q_(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=Q_(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Q_(n,e,t,i){let r=He(null);try{return xt(ct.OutputStart,e,t),t(i)!==!1}catch(s){return UC(n,s),!1}finally{xt(ct.OutputEnd,e,t),He(r)}}function ED(n,e,t,i,r,s,o,a){let c=du(n),l=!1,u=null;if(!i&&c&&(u=TD(e,t,s,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let d=Qi(n,t),f=i?i(d):d;WT(t,f,s,a),i||(a.__ngNativeEl__=d);let h=r.listen(f,s,a);if(!wD(s)){let g=i?y=>i(Zn(y[n.index])):n.index;CD(g,e,t,s,a,h,!1)}}return l}function wD(n){return n.startsWith("animation")||n.startsWith("transition")}function TD(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[go],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function CD(n,e,t,i,r,s,o){let a=e.firstCreatePass?cm(e):null,c=am(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}var $m=Symbol("BINDING");function pb(n){return n.debugInfo?.className||n.type.name||null}var Uu=class extends nc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=wr(e);return new Co(t,this.ngModule)}};function DD(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&Ku.SignalBased)!==0};return r&&(s.transform=r),s})}function ID(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function AD(n,e,t){let i=e instanceof Xt?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Wm(t,i):t}function RD(n){let e=n.get(Ds,null);if(e===null)throw new Me(407,!1);let t=n.get(ub,null),i=n.get(vs,null),r=n.get(Io,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function ND(n,e){let t=mb(n);return Px(e,t,t==="svg"?s_:t==="math"?o_:null)}function mb(n){return(n.selectors[0][0]||"div").toLowerCase()}var Co=class extends ed{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=DD(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=ID(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=aC(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){xt(ct.DynamicComponentStart);let a=He(null);try{let c=this.componentDef,l=AD(c,r||this.ngModule,e),u=RD(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(pb(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{He(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=PD(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?AC(l,r,a.encapsulation,t):ND(a,l),d=o?.some(ex)||s?.some(g=>typeof g!="function"&&g.bindings.some(ex)),f=ag(null,c,null,512|Ux(a),null,null,e,l,t,null,Ax(u,t,!0));f[tn]=u,yu(f);let h=null;try{let g=fb(tn,f,2,"#host",()=>c.directiveRegistry,!0,0);Lx(l,u,g),Eo(u,f),$x(c,f,g),Nx(c,g,f),hb(c,g),i!==void 0&&LD(g,this.ngContentSelectors,i),h=Ii(g.index,f),f[un]=h[un],gg(c,f,null)}catch(g){throw h!==null&&km(h),km(f),g}finally{xt(ct.DynamicComponentEnd),_u()}return new Bu(this.componentType,f,!!d)}};function PD(n,e,t,i){let r=n?["ng-version","21.2.12"]:cC(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[$m].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let f of d.bindings){a+=f[$m].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(s??=[]).push(f)),f.update&&(f.targetIdx=h,(o??=[]).push(f))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,f=jp(d);c.push(f)}return og(0,null,OD(s,o),1,a,c,null,null,null,[r],null)}function OD(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function ex(n){let e=n[$m].kind;return e==="input"||e==="twoWay"}var Bu=class extends lb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=nm(t[Pe],tn),this.location=Do(this._tNode,t),this.instance=Ii(this._tNode.index,t)[un],this.hostView=this.changeDetectorRef=new Fr(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Yx(i,r[Pe],r,e,t);this.previousInputValues.set(e,t);let o=Ii(i.index,r);vg(o,1)}get injector(){return new Ts(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function LD(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var Ns=(()=>{class n{static __NG_ELEMENT_ID__=FD}return n})();function FD(){let n=Jn();return gb(n,Mt())}var Xm=class n extends Ns{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Do(this._hostTNode,this._hostLView)}get injector(){return new Ts(this._hostTNode,this._hostLView)}get parentInjector(){let e=tg(this._hostTNode,this._hostLView);if(px(e)){let t=Au(e,this._hostLView),i=Iu(e),r=t[Pe].data[i+8];return new Ts(r,t)}else return new Ts(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=tx(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-yn}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Y_(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,zm(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!mT(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new Co(wr(e)),d=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?d:this.parentInjector).get(Xt,null);p&&(s=p)}let f=wr(u.componentType??{}),h=Y_(this._lContainer,f?.id??null),g=h?.firstChild??null,y=u.create(d,r,g,s,o,a);return this.insertImpl(y.hostView,l,zm(this._hostTNode,h)),y}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(u_(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[en],l=new n(c,c[qn],c[en]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return ab(o,r,s,i),e.attachToViewContainerRef(),Xp(Am(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=tx(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Lu(this._lContainer,t);i&&(Ba(Am(this._lContainer),t),fg(i[Pe],i))}detach(e){let t=this._adjustIndex(e,-1),i=Lu(this._lContainer,t);return i&&Ba(Am(this._lContainer),t)!=null?new Fr(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function tx(n){return n[ja]}function Am(n){return n[ja]||(n[ja]=[])}function gb(n,e){let t,i=e[n.index];return hi(i)?t=i:(t=ob(i,e,null,n),e[n.index]=t,cg(e,t)),UD(t,e,n,i),new Xm(t,n,e)}function kD(n,e){let t=n[vn],i=t.createComment(""),r=Qi(e,n),s=t.parentNode(r);return Ou(t,s,i,t.nextSibling(r),!1),i}var UD=HD,BD=()=>!1;function VD(n,e,t){return BD(n,e,t)}function HD(n,e,t,i){if(n[Ir])return;let r;t.type&8?r=Zn(i):r=kD(e,t),n[Ir]=r}var qm=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Ym=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)_g(e,t).matches!==null&&this.queries[t].setDirty()}},Zm=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=YD(e):this.predicate=e}},Km=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Jm=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,zD(t,s)),this.matchTNodeWithReadOption(e,t,Tu(t,e,s,!1,!1))}else i===To?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Tu(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===kr||r===Ns||r===To&&t.type&4)this.addMatch(t.index,-2);else{let s=Tu(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function zD(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function GD(n,e){return n.type&11?Do(n,e):n.type&4?yg(n,e):null}function jD(n,e,t,i){return t===-1?GD(e,n):t===-2?WD(n,e,i):Ka(n,n[Pe],t,e)}function WD(n,e,t){if(t===kr)return Do(e,n);if(t===To)return yg(e,n);if(t===Ns)return gb(e,n)}function vb(n,e,t,i){let r=e[Di].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(jD(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Qm(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=vb(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=yn;d<u.length;d++){let f=u[d];f[Cr]===f[en]&&Qm(f[Pe],f,l,i)}if(u[Es]!==null){let d=u[Es];for(let f=0;f<d.length;f++){let h=d[f];Qm(h[Pe],h,l,i)}}}}}return i}function $D(n,e){return n[Di].queries[e].queryList}function XD(n,e,t){let i=new Pu((t&4)===4);return h_(n,e,i,i.destroy),(e[Di]??=new Ym).queries.push(new qm(i))-1}function qD(n,e,t){let i=Kn();return i.firstCreatePass&&(ZD(i,new Zm(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),XD(i,Mt(),e)}function YD(n){return n.split(",").map(e=>e.trim())}function ZD(n,e,t){n.queries===null&&(n.queries=new Km),n.queries.track(new Jm(e,t))}function _g(n,e){return n.queries.getByIndex(e)}function KD(n,e){let t=n[Pe],i=_g(t,e);return i.crossesNgTemplate?Qm(t,n,e,[]):vb(t,n,i,e)}var Is=class{},td=class{};var Vu=class extends Is{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Uu(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let s=Gp(e);this._bootstrapComponents=Fx(s.bootstrap),this._r3Injector=xm(e,t,[{provide:Is,useValue:this},{provide:nc,useValue:this.componentFactoryResolver},...i],iu(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},Hu=class extends td{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new Vu(this.moduleType,e,[])}};var Qa=class extends Is{injector;componentFactoryResolver=new Uu(this);instance=null;constructor(e){super();let t=new gs([...e.providers,{provide:Is,useValue:this},{provide:nc,useValue:this.componentFactoryResolver}],e.parent||Ha(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function ic(n,e,t=null){return new Qa({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var JD=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Zp(!1,t.type),r=i.length>0?ic([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Ee({token:n,providedIn:"environment",factory:()=>new n(We(Xt))})}return n})();function Ps(n){return Wu(()=>{let e=yb(n),t=st(he({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===ng.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(JD).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||mi.Emulated,styles:n.styles||ps,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&As("NgStandalone"),_b(t);let i=n.dependencies;return t.directiveDefs=nx(i,QD),t.pipeDefs=nx(i,W0),t.id=nI(t),t})}function QD(n){return wr(n)||jp(n)}function eI(n,e){if(n==null)return xs;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=Ku.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function tI(n){if(n==null)return xs;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function nd(n){return Wu(()=>{let e=yb(n);return _b(e),e})}function xg(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function yb(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||xs,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||ps,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:eI(n.inputs,e),outputs:tI(n.outputs),debugInfo:null}}function _b(n){n.features?.forEach(e=>e(n))}function nx(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function nI(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function iI(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=Xu(n.mergedAttrs,n.attrs);let u=n.tView=og(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),xo(n,!1);let c=rI(t,e,n,i);bu()&&hg(t,e,c,n),Eo(c,e);let l=ob(c,e,c,n);e[i+tn]=l,cg(e,l),VD(l,n,e)}function xb(n,e,t,i,r,s,o,a,c,l,u){let d=t+tn,f;if(e.firstCreatePass){if(f=Qu(e,d,4,o||null,a||null),l!=null){let h=Pr(e.consts,l);f.localNames=[];for(let g=0;g<h.length;g+=2)f.localNames.push(h[g],-1)}}else f=e.data[d];return iI(f,n,e,t,i,r,s,c),l!=null&&mg(n,f,u),f}var rI=sI;function sI(n,e,t,i){return Su(!0),e[vn].createComment("")}var bg=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();var Sg=new Ie("");function Ao(n){return!!n&&typeof n.then=="function"}function Mg(n){return!!n&&typeof n.subscribe=="function"}var bb=new Ie("");var Eg=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=J(bb,{optional:!0})??[];injector=J(di);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=gn(this.injector,r);if(Ao(s))t.push(s);else if(Mg(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),id=new Ie("");function Sb(){sp(()=>{let n="";throw new Me(600,n)})}function Mb(n){return n.isBoundToModule}var oI=10;var Os=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=J(Qn);afterRenderManager=J(Hx);zonelessEnabled=J(bo);rootEffectScheduler=J(Tm);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new on;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=J(er);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Pt(t=>!t))}constructor(){J(Io,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=J(Xt);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=di.NULL){return this._injector.get(Ot).run(()=>{xt(ct.BootstrapComponentStart);let o=t instanceof ed;if(!this._injector.get(Eg).done){let g="";throw new Me(405,g)}let c;o?c=t:c=this._injector.get(nc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=Mb(c)?void 0:this._injector.get(Is),u=i||c.selector,d=c.create(r,[],u,l),f=d.location.nativeElement,h=d.injector.get(Sg,null);return h?.registerApplication(f),d.onDestroy(()=>{this.detachView(d.hostView),Za(this.components,d),h?.unregisterApplication(f)}),this._loadComponent(d),xt(ct.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){xt(ct.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(ug.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw xt(ct.ChangeDetectionEnd),new Me(101,!1);let t=He(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,He(t),this.afterTick.next(),xt(ct.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ds,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<oI;){xt(ct.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{xt(ct.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Wa(r))continue;let s=i&&!this.zonelessEnabled?0:1;nb(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Wa(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Za(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(id,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Za(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Me(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Za(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function wg(n,e,t,i,r,s,o,a){As("NgControlFlow");let c=Mt(),l=Kn(),u=Pr(l.consts,s);return xb(c,l,n,e,t,i,r,u,256,o,a),Tg}function Tg(n,e,t,i,r,s,o,a){As("NgControlFlow");let c=Mt(),l=Kn(),u=Pr(l.consts,s);return xb(c,l,n,e,t,i,r,u,512,o,a),Tg}function Cg(n,e){As("NgControlFlow");let t=Mt(),i=gm(),r=t[i]!==Ur?t[i]:-1,s=r!==-1?ix(t,tn+r):void 0,o=0;if(ku(t,i,n)){let a=He(null);try{if(s!==void 0&&tD(s,o),n!==-1){let c=tn+n,l=ix(t,c),u=aI(t[Pe],c),d=lD(l,u,t),f=Zx(t,u,e,{dehydratedView:d});ab(l,f,o,zm(u,d))}}finally{He(a)}}else if(s!==void 0){let a=eD(s,o);a!==void 0&&(a[un]=e)}}function ix(n,e){return n[e]}function aI(n,e){return nm(n,e)}function rx(n,e,t,i,r){Yx(e,n,t,r?"class":"style",i)}function zu(n,e,t,i){let r=Mt(),s=r[Pe],o=n+tn,a=s.firstCreatePass?fb(o,r,2,e,FC,g_(),t,i):s.data[o];if(Rr(a)){let c=r[Ci].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(pb(l),()=>(sx(n,e,r,a,i),zu))}}return sx(n,e,r,a,i),zu}function sx(n,e,t,i,r){if(Xx(i,t,n,e,Eb),du(i)){let s=t[Pe];$x(s,t,i),Nx(s,i,t)}r!=null&&mg(t,i)}function Dg(){let n=Kn(),e=Jn(),t=qx(e);return n.firstCreatePass&&hb(n,t),um(t)&&dm(),lm(),t.classesWithoutHost!=null&&ST(t)&&rx(n,t,Mt(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&MT(t)&&rx(n,t,Mt(),t.stylesWithoutHost,!1),Dg}function Ro(n,e,t,i){return zu(n,e,t,i),Dg(),Ro}function Lt(n,e,t,i){let r=Mt(),s=r[Pe],o=n+tn,a=s.firstCreatePass?xD(o,s,2,e,t,i):s.data[o];return Xx(a,r,n,e,Eb),i!=null&&mg(r,a),Lt}function Yt(){let n=Jn(),e=qx(n);return um(e)&&dm(),lm(),Yt}function rd(n,e,t,i){return Lt(n,e,t,i),Yt(),rd}var Eb=(n,e,t,i,r)=>(Su(!0),Px(e[vn],i,I_()));function Ig(){return Mt()}var Xa=void 0;function cI(n){let e=Math.floor(Math.abs(n)),t=n.toString().replace(/^[^.]*\.?/,"").length;return e===1&&t===0?1:5}var lI=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Xa,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Xa,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Xa,Xa,Xa],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",cI],Rm={};function sd(n){let e=uI(n),t=ox(e);if(t)return t;let i=e.split("-")[0];if(t=ox(i),t)return t;if(i==="en")return lI;throw new Me(701,!1)}function ox(n){return n in Rm||(Rm[n]=ys.ng&&ys.ng.common&&ys.ng.common.locales&&ys.ng.common.locales[n]),Rm[n]}var Ls=(function(n){return n[n.LocaleId=0]="LocaleId",n[n.DayPeriodsFormat=1]="DayPeriodsFormat",n[n.DayPeriodsStandalone=2]="DayPeriodsStandalone",n[n.DaysFormat=3]="DaysFormat",n[n.DaysStandalone=4]="DaysStandalone",n[n.MonthsFormat=5]="MonthsFormat",n[n.MonthsStandalone=6]="MonthsStandalone",n[n.Eras=7]="Eras",n[n.FirstDayOfWeek=8]="FirstDayOfWeek",n[n.WeekendRange=9]="WeekendRange",n[n.DateFormat=10]="DateFormat",n[n.TimeFormat=11]="TimeFormat",n[n.DateTimeFormat=12]="DateTimeFormat",n[n.NumberSymbols=13]="NumberSymbols",n[n.NumberFormats=14]="NumberFormats",n[n.CurrencyCode=15]="CurrencyCode",n[n.CurrencySymbol=16]="CurrencySymbol",n[n.CurrencyName=17]="CurrencyName",n[n.Currencies=18]="Currencies",n[n.Directionality=19]="Directionality",n[n.PluralCase=20]="PluralCase",n[n.ExtraData=21]="ExtraData",n})(Ls||{});function uI(n){return n.toLowerCase().replace(/_/g,"-")}var rc="en-US";var dI=rc;function wb(n){typeof n=="string"&&(dI=n.toLowerCase().replace(/_/g,"-"))}function No(n,e,t){let i=Mt(),r=Kn(),s=Jn();return(s.type&3||t)&&ED(s,r,i,t,i[vn],n,e,MD(s,i,e)),No}function od(n=1){return D_(n)}function ad(n,e,t){return qD(n,e,t),ad}function Ag(n){let e=Mt(),t=Kn(),i=vm();vu(i+1);let r=_g(t,i);if(n.dirty&&l_(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=KD(e,i);n.reset(s,FT),n.notifyOnChanges()}return!0}return!1}function Rg(){return $D(Mt(),vm())}function dn(n,e=""){let t=Mt(),i=Kn(),r=n+tn,s=i.firstCreatePass?Qu(i,r,1,e,null):i.data[r],o=fI(i,t,s,e);t[r]=o,bu()&&hg(i,t,o,s),xo(s,!1)}var fI=(n,e,t,i)=>(Su(!0),XT(e[vn],i));function hI(n,e,t,i=""){return ku(n,gm(),t)?e+$0(t)+i:Ur}function Hr(n){return Ng("",n),Hr}function Ng(n,e,t){let i=Mt(),r=hI(i,n,e,t);return r!==Ur&&pI(i,xu(),r),Ng}function pI(n,e,t){let i=a_(e,n);qT(n[vn],i,t)}function ax(n,e,t){let i=Kn();i.firstCreatePass&&Tb(e,i.data,i.blueprint,Nr(n),t)}function Tb(n,e,t,i,r){if(n=ln(n),Array.isArray(n))for(let s=0;s<n.length;s++)Tb(n[s],e,t,i,r);else{let s=Kn(),o=Mt(),a=Jn(),c=ms(n)?n:ln(n.provide),l=Jp(n),u=a.providerIndexes&1048575,d=a.directiveStart,f=a.providerIndexes>>20;if(ms(n)||!n.multi){let h=new Cs(l,r,Rs,null),g=Pm(c,e,r?u:u+f,d);g===-1?(Lm(Nu(a,o),s,c),Nm(s,n,e.length),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(h),o.push(h)):(t[g]=h,o[g]=h)}else{let h=Pm(c,e,u+f,d),g=Pm(c,e,u,u+f),y=h>=0&&t[h],m=g>=0&&t[g];if(r&&!m||!r&&!y){Lm(Nu(a,o),s,c);let p=vI(r?gI:mI,t.length,r,i,l,n);!r&&m&&(t[g].providerFactory=p),Nm(s,n,e.length,0),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=Cb(t[r?g:h],l,!r&&i);Nm(s,n,h>-1?h:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function Nm(n,e,t,i){let r=ms(e),s=n_(e);if(r||s){let c=(s?ln(e.useClass):e).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=l.indexOf(t);u===-1?l.push(t,[i,c]):l[u+1].push(i,c)}else l.push(t,c)}}}function Cb(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function Pm(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function mI(n,e,t,i,r){return eg(this.multi,[])}function gI(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Ka(i,i[Pe],this.providerFactory.index,r);o=c.slice(0,a),eg(s,o);for(let l=a;l<c.length;l++)o.push(c[l])}else o=[],eg(s,o);return o}function eg(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function vI(n,e,t,i,r,s){let o=new Cs(n,t,Rs,null);return o.multi=[],o.index=e,o.componentProviders=0,Cb(o,r,i&&!t),o}function Pg(n,e){return t=>{t.providersResolver=(i,r)=>ax(i,r?r(n):n,!1),e&&(t.viewProvidersResolver=(i,r)=>ax(i,r?r(e):e,!0))}}function yI(n,e){let t=n[e];return t===Ur?void 0:t}function _I(n,e,t,i,r,s,o){let a=e+t;return SD(n,a,r,s)?bD(n,a+2,o?i.call(o,r,s):i(r,s)):yI(n,a+2)}function Fs(n,e){let t=Kn(),i,r=n+tn;t.firstCreatePass?(i=xI(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=Mr(i.type,!0)),o,a=En(Rs);try{let c=Ru(!1),l=s();return Ru(c),im(t,Mt(),r,l),l}finally{En(a)}}function xI(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function ks(n,e,t,i){let r=n+tn,s=Mt(),o=c_(s,r);return bI(s,r)?_I(s,x_(),e,o.transform,t,i,o):o.transform(t,i)}function bI(n,e){return n[Pe].data[e].pure}var Gu=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},Og=(()=>{class n{compileModuleSync(t){return new Hu(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=Gp(t),s=Fx(r.declarations).reduce((o,a)=>{let c=wr(a);return c&&o.push(new Co(c)),o},[]);return new Gu(i,s)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Db=(()=>{class n{applicationErrorHandler=J(Qn);appRef=J(Os);taskService=J(er);ngZone=J(Ot);zonelessEnabled=J(bo);tracing=J(Io,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new $t;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Fa):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(J(Mu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?N_:Sm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Fa+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Ib(){return[{provide:vs,useExisting:Db},{provide:Ot,useClass:ka},{provide:bo,useValue:!0}]}function SI(){return typeof $localize<"u"&&$localize.locale||rc}var sc=new Ie("",{factory:()=>J(sc,{optional:!0,skipSelf:!0})||SI()});function tr(n){return V0(n)}var Pb=Symbol("InputSignalNode#UNSET"),BI=st(he({},Rl),{transformFn:void 0,applyValueToInputSignal(n,e){so(n,e)}});function Ob(n,e){let t=Object.create(BI);t.value=n,t.transformFn=e?.transform;function i(){if(wa(t),t.value===Pb){let r=null;throw new Me(-950,r)}return t.value}return i[Ln]=t,i}function Ab(n,e){return Ob(n,e)}function VI(n){return Ob(Pb,n)}var Lb=(Ab.required=VI,Ab);var HI=(()=>{class n{zone=J(Ot);changeDetectionScheduler=J(vs);applicationRef=J(Os);applicationErrorHandler=J(Qn);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{try{this.applicationRef.dirtyFlags|=1,this.applicationRef._tick()}catch(t){this.applicationErrorHandler(t)}})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),zI=new Ie("",{factory:()=>!1});function GI({ngZoneFactory:n,scheduleInRootZone:e}){return n??=()=>new Ot(st(he({},kb()),{scheduleInRootZone:e})),[{provide:bo,useValue:!1},{provide:Ot,useFactory:n},{provide:Ji,multi:!0,useFactory:()=>{let t=J(HI,{optional:!0});return()=>t.initialize()}},{provide:Ji,multi:!0,useFactory:()=>{let t=J(jI);return()=>{t.initialize()}}},{provide:Mu,useValue:e??bm}]}function Fb(n){let e=n?.scheduleInRootZone,t=GI({ngZoneFactory:()=>{let i=kb(n);return i.scheduleInRootZone=e,i.shouldCoalesceEventChangeDetection&&As("NgZone_CoalesceEvent"),new Ot(i)},scheduleInRootZone:e});return bs([{provide:zI,useValue:!0},t])}function kb(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var jI=(()=>{class n{subscription=new $t;initialized=!1;zone=J(Ot);pendingTasks=J(er);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{Ot.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{Ot.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Lg=new Ie(""),WI=new Ie("");function oc(n){return!n.moduleRef}function $I(n){let e=oc(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Ot);return t.run(()=>{oc(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(Qn),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),oc(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Lg);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Lg);o.add(s),n.moduleRef.onDestroy(()=>{Za(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return qI(i,t,()=>{let s=e.get(er),o=s.add(),a=e.get(Eg);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(sc,rc);if(wb(c||rc),!e.get(WI,!0))return oc(n)?e.get(Os):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(oc(n)){let u=e.get(Os);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return XI?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var XI;function qI(n,e,t){try{let i=t();return Ao(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var cd=null;function YI(n=[],e){return di.create({name:e,providers:[{provide:Va,useValue:"platform"},{provide:Lg,useValue:new Set([()=>cd=null])},...n]})}function ZI(n=[]){if(cd)return cd;let e=YI(n);return cd=e,Sb(),KI(e),e}function KI(n){let e=n.get(Yu,null);gn(n,()=>{e?.forEach(t=>t())})}var JI=1e4;var e6=JI-1e3;var ac=(()=>{class n{static __NG_ELEMENT_ID__=QI}return n})();function QI(n){return eA(Jn(),Mt(),(n&16)===16)}function eA(n,e,t){if(Rr(n)&&!t){let i=Ii(n.index,e);return new Fr(i,i)}else if(n.type&175){let i=e[Yn];return new Fr(i,e)}return null}function Ub(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;xt(ct.BootstrapApplicationStart);try{let s=r?.injector??ZI(i),o=[Ib(),O_,...t||[]],a=new Qa({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return $I({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{xt(ct.BootstrapApplicationEnd)}}var Bb=null;function nr(){return Bb}function kg(n){Bb??=n}var cc=class{},ld=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(Vb),providedIn:"platform"})}return n})();var Vb=(()=>{class n extends ld{_location;_history;_doc=J(qt);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return nr().getBaseHref(this._doc)}onPopState(t){let i=nr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=nr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function Gb(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function Hb(n){let e=n.search(/#|\?|$/);return n[e-1]==="/"?n.slice(0,e-1)+n.slice(e):n}function zr(n){return n&&n[0]!=="?"?`?${n}`:n}var ud=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(nA),providedIn:"root"})}return n})(),tA=new Ie(""),nA=(()=>{class n extends ud{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??J(qt).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Gb(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+zr(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,s){let o=this.prepareExternalUrl(r+zr(s));this._platformLocation.pushState(t,i,o)}replaceState(t,i,r,s){let o=this.prepareExternalUrl(r+zr(s));this._platformLocation.replaceState(t,i,o)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(We(ld),We(tA,8))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Po=(()=>{class n{_subject=new on;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=sA(Hb(zb(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+zr(i))}normalize(t){return n.stripTrailingSlash(rA(this._basePath,zb(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+zr(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+zr(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=zr;static joinWithSlash=Gb;static stripTrailingSlash=Hb;static \u0275fac=function(i){return new(i||n)(We(ud))};static \u0275prov=Ee({token:n,factory:()=>iA(),providedIn:"root"})}return n})();function iA(){return new Po(We(ud))}function rA(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function zb(n){return n.replace(/\/index.html$/,"")}function sA(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}var Vg=(function(n){return n[n.Decimal=0]="Decimal",n[n.Percent=1]="Percent",n[n.Currency=2]="Currency",n[n.Scientific=3]="Scientific",n})(Vg||{});var Ri={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Oo(n,e){let t=sd(n),i=t[Ls.NumberSymbols][e];if(typeof i>"u"){if(e===Ri.CurrencyDecimal)return t[Ls.NumberSymbols][Ri.Decimal];if(e===Ri.CurrencyGroup)return t[Ls.NumberSymbols][Ri.Group]}return i}function Wb(n,e){return sd(n)[Ls.NumberFormats][e]}var oA=/^(\d+)?\.((\d+)(-(\d+))?)?$/,jb=22,dd=".",lc="0",aA=";",cA=",",Ug="#";function lA(n,e,t,i,r,s,o=!1){let a="",c=!1;if(!isFinite(n))a=Oo(t,Ri.Infinity);else{let l=fA(n);o&&(l=dA(l));let u=e.minInt,d=e.minFrac,f=e.maxFrac;if(s){let b=s.match(oA);if(b===null)throw new Me(2306,!1);let M=b[1],E=b[3],D=b[5];M!=null&&(u=Bg(M)),E!=null&&(d=Bg(E)),D!=null?f=Bg(D):E!=null&&d>f&&(f=d)}hA(l,d,f);let h=l.digits,g=l.integerLen,y=l.exponent,m=[];for(c=h.every(b=>!b);g<u;g++)h.unshift(0);for(;g<0;g++)h.unshift(0);g>0?m=h.splice(g,h.length):(m=h,h=[0]);let p=[];for(h.length>=e.lgSize&&p.unshift(h.splice(-e.lgSize,h.length).join(""));h.length>e.gSize;)p.unshift(h.splice(-e.gSize,h.length).join(""));h.length&&p.unshift(h.join("")),a=p.join(Oo(t,i)),m.length&&(a+=Oo(t,r)+m.join("")),y&&(a+=Oo(t,Ri.Exponential)+"+"+y)}return n<0&&!c?a=e.negPre+a+e.negSuf:a=e.posPre+a+e.posSuf,a}function $b(n,e,t){let i=Wb(e,Vg.Decimal),r=uA(i,Oo(e,Ri.MinusSign));return lA(n,r,e,Ri.Group,Ri.Decimal,t)}function uA(n,e="-"){let t={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=n.split(aA),r=i[0],s=i[1],o=r.indexOf(dd)!==-1?r.split(dd):[r.substring(0,r.lastIndexOf(lc)+1),r.substring(r.lastIndexOf(lc)+1)],a=o[0],c=o[1]||"";t.posPre=a.substring(0,a.indexOf(Ug));for(let u=0;u<c.length;u++){let d=c.charAt(u);d===lc?t.minFrac=t.maxFrac=u+1:d===Ug?t.maxFrac=u+1:t.posSuf+=d}let l=a.split(cA);if(t.gSize=l[1]?l[1].length:0,t.lgSize=l[2]||l[1]?(l[2]||l[1]).length:0,s){let u=r.length-t.posPre.length-t.posSuf.length,d=s.indexOf(Ug);t.negPre=s.substring(0,d).replace(/'/g,""),t.negSuf=s.slice(d+u).replace(/'/g,"")}else t.negPre=e+t.posPre,t.negSuf=t.posSuf;return t}function dA(n){if(n.digits[0]===0)return n;let e=n.digits.length-n.integerLen;return n.exponent?n.exponent+=2:(e===0?n.digits.push(0,0):e===1&&n.digits.push(0),n.integerLen+=2),n}function fA(n){let e=Math.abs(n)+"",t=0,i,r,s,o,a;for((r=e.indexOf(dd))>-1&&(e=e.replace(dd,"")),(s=e.search(/e/i))>0?(r<0&&(r=s),r+=+e.slice(s+1),e=e.substring(0,s)):r<0&&(r=e.length),s=0;e.charAt(s)===lc;s++);if(s===(a=e.length))i=[0],r=1;else{for(a--;e.charAt(a)===lc;)a--;for(r-=s,i=[],o=0;s<=a;s++,o++)i[o]=Number(e.charAt(s))}return r>jb&&(i=i.splice(0,jb-1),t=r-1,r=1),{digits:i,exponent:t,integerLen:r}}function hA(n,e,t){if(e>t)throw new Me(2307,!1);let i=n.digits,r=i.length-n.integerLen,s=Math.min(Math.max(e,r),t),o=s+n.integerLen,a=i[o];if(o>0){i.splice(Math.max(n.integerLen,o));for(let d=o;d<i.length;d++)i[d]=0}else{r=Math.max(0,r),n.integerLen=1,i.length=Math.max(1,o=s+1),i[0]=0;for(let d=1;d<o;d++)i[d]=0}if(a>=5)if(o-1<0){for(let d=0;d>o;d--)i.unshift(0),n.integerLen++;i.unshift(1),n.integerLen++}else i[o-1]++;for(;r<Math.max(0,s);r++)i.push(0);let c=s!==0,l=e+n.integerLen,u=i.reduceRight(function(d,f,h,g){return f=f+d,g[h]=f<10?f:f-10,c&&(g[h]===0&&h>=l?g.pop():c=!1),f>=10?1:0},0);u&&(i.unshift(u),n.integerLen++)}function Bg(n){let e=parseInt(n);if(isNaN(e))throw new Me(2305,!1);return e}function pA(n,e){return new Me(2100,!1)}var Hg=(()=>{class n{_locale;constructor(t){this._locale=t}transform(t,i,r){if(!mA(t))return null;r||=this._locale;try{let s=gA(t);return $b(s,r,i)}catch(s){throw pA(n,s.message)}}static \u0275fac=function(i){return new(i||n)(Rs(sc,16))};static \u0275pipe=xg({name:"number",type:n,pure:!0})}return n})();function mA(n){return!(n==null||n===""||n!==n)}function gA(n){if(typeof n=="string"&&!isNaN(Number(n)-parseFloat(n)))return Number(n);if(typeof n!="number")throw new Me(2309,!1);return n}function zg(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var uc=class{};var Xb="browser";var dc=class{_doc;constructor(e){this._doc=e}manager},fd=(()=>{class n extends dc{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(We(qt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),md=new Ie(""),$g=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof fd));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof fd);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Me(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(We(md),We(Ot))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),Gg="ng-app-id";function qb(n){for(let e of n)e.remove()}function Yb(n,e){let t=e.createElement("style");return t.textContent=n,t}function _A(n,e,t,i){let r=n.head?.querySelectorAll(`style[${Gg}="${e}"],link[${Gg}="${e}"]`);if(r)for(let s of r)s.removeAttribute(Gg),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function Wg(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var Xg=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,_A(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Yb);i?.forEach(r=>this.addUsage(r,this.external,Wg))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(qb(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])qb(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Yb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,Wg(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(We(qt),We(qu),We(Zu,8),We(tc))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),jg={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},qg=/%COMP%/g;var Kb="%COMP%",xA=`_nghost-${Kb}`,bA=`_ngcontent-${Kb}`,SA=!0,MA=new Ie("",{factory:()=>SA});function EA(n){return bA.replace(qg,n)}function wA(n){return xA.replace(qg,n)}function Jb(n,e){return e.map(t=>t.replace(qg,n))}var Yg=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new fc(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof pd?r.applyToHost(t):r instanceof hc&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case mi.Emulated:s=new pd(c,l,i,this.appId,u,o,a,d);break;case mi.ShadowDom:return new hd(c,t,i,o,a,this.nonce,d,l);case mi.ExperimentalIsolatedShadowDom:return new hd(c,t,i,o,a,this.nonce,d);default:s=new hc(c,l,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(We($g),We(Xg),We(qu),We(MA),We(qt),We(Ot),We(Zu),We(Io,8))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),fc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(jg[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(Zb(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(Zb(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Me(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=jg[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=jg[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(Vr.DashCase|Vr.Important)?e.style.setProperty(t,i,r&Vr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&Vr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=nr().getGlobalEventTarget(this.doc,e),!e))throw new Me(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function Zb(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var hd=class extends fc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=Jb(i.id,l);for(let d of l){let f=document.createElement("style");o&&f.setAttribute("nonce",o),f.textContent=d,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let d of u){let f=Wg(d,r);o&&f.setAttribute("nonce",o),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},hc=class extends fc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?Jb(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&wo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},pd=class extends hc{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=EA(l),this.hostAttr=wA(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var gd=class n extends cc{supportsDOMEvents=!0;static makeCurrent(){kg(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=TA();return t==null?null:CA(t)}resetBaseElement(){pc=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return zg(document.cookie,e)}},pc=null;function TA(){return pc=pc||document.head.querySelector("base"),pc?pc.getAttribute("href"):null}function CA(n){return new URL(n,document.baseURI).pathname}var DA=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),Qb=["alt","control","meta","shift"],IA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},AA={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},eS=(()=>{class n extends dc{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>nr().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),Qb.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=IA[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),Qb.forEach(o=>{if(o!==r){let a=AA[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(We(qt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})();function Zg(n,e,t){return ht(this,null,function*(){let i=he({rootComponent:n},RA(e,t));return Ub(i)})}function RA(n,e){return{platformRef:e?.platformRef,appProviders:[...FA,...n?.providers??[]],platformProviders:LA}}function NA(){gd.makeCurrent()}function PA(){return new Ki}function OA(){return ig(document),document}var LA=[{provide:tc,useValue:Xb},{provide:Yu,useValue:NA,multi:!0},{provide:qt,useFactory:OA}];var FA=[{provide:Va,useValue:"root"},{provide:Ki,useFactory:PA},{provide:md,useClass:fd,multi:!0},{provide:md,useClass:eS,multi:!0},Yg,Xg,$g,{provide:Ds,useExisting:Yg},{provide:uc,useClass:DA},[]];var tS=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(We(qt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Le="primary",Dc=Symbol("RouteTitle"),tv=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function Bs(n){return new tv(n)}function Kg(n,e,t){for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(r[0]===":")t[r.substring(1)]=s;else if(r!==s.path)return!1}return!0}function lS(n,e,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let c={},l=n.slice(0,i.length);return Kg(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let s=i.slice(0,r),o=i.slice(r+1);if(s.length+o.length>n.length||t.pathMatch==="full"&&e.hasChildren()&&t.path!=="**")return null;let a={};return!Kg(s,n.slice(0,s.length),a)||!Kg(o,n.slice(n.length-o.length),a)?null:{consumed:n,posParams:a}}function Sd(n){return new Promise((e,t)=>{n.pipe(Yi()).subscribe({next:i=>e(i),error:i=>t(i)})})}function BA(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!Ni(n[t],e[t]))return!1;return!0}function Ni(n,e){let t=n?nv(n):void 0,i=e?nv(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let s=0;s<t.length;s++)if(r=t[s],!uS(n[r],e[r]))return!1;return!0}function nv(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function uS(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,s)=>i[s]===r)}else return n===e}function VA(n){return n.length>0?n[n.length-1]:null}function zs(n){return Yl(n)?n:Ao(n)?zt(Promise.resolve(n)):Je(n)}function dS(n){return Yl(n)?Sd(n):Promise.resolve(n)}var HA={exact:pS,subset:mS},fS={exact:zA,subset:GA,ignored:()=>!0},hS={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},iv={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function nS(n,e,t){return HA[t.paths](n.root,e.root,t.matrixParams)&&fS[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function zA(n,e){return Ni(n,e)}function pS(n,e,t){if(!Us(n.segments,e.segments)||!_d(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!pS(n.children[i],e.children[i],t))return!1;return!0}function GA(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>uS(n[t],e[t]))}function mS(n,e,t){return gS(n,e,e.segments,t)}function gS(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!Us(r,t)||e.hasChildren()||!_d(r,t,i))}else if(n.segments.length===t.length){if(!Us(n.segments,t)||!_d(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!mS(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),s=t.slice(n.segments.length);return!Us(n.segments,r)||!_d(n.segments,r,i)||!n.children[Le]?!1:gS(n.children[Le],e,s,i)}}function _d(n,e,t){return e.every((i,r)=>fS[t](n[r].parameters,i.parameters))}var ni=class{root;queryParams;fragment;_queryParamMap;constructor(e=new lt([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Bs(this.queryParams),this._queryParamMap}toString(){return $A.serialize(this)}},lt=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return xd(this)}},Gr=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=Bs(this.parameters),this._parameterMap}toString(){return yS(this)}};function jA(n,e){return Us(n,e)&&n.every((t,i)=>Ni(t.parameters,e[i].parameters))}function Us(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function WA(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===Le&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==Le&&(t=t.concat(e(r,i)))}),t}var Ic=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>new jr,providedIn:"root"})}return n})(),jr=class{parse(e){let t=new sv(e);return new ni(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${mc(e.root,!0)}`,i=YA(e.queryParams),r=typeof e.fragment=="string"?`#${XA(e.fragment)}`:"";return`${t}${i}${r}`}},$A=new jr;function xd(n){return n.segments.map(e=>yS(e)).join("/")}function mc(n,e){if(!n.hasChildren())return xd(n);if(e){let t=n.children[Le]?mc(n.children[Le],!1):"",i=[];return Object.entries(n.children).forEach(([r,s])=>{r!==Le&&i.push(`${r}:${mc(s,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=WA(n,(i,r)=>r===Le?[mc(n.children[Le],!1)]:[`${r}:${mc(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[Le]!=null?`${xd(n)}/${t[0]}`:`${xd(n)}/(${t.join("//")})`}}function vS(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function vd(n){return vS(n).replace(/%3B/gi,";")}function XA(n){return encodeURI(n)}function rv(n){return vS(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function bd(n){return decodeURIComponent(n)}function iS(n){return bd(n.replace(/\+/g,"%20"))}function yS(n){return`${rv(n.path)}${qA(n.parameters)}`}function qA(n){return Object.entries(n).map(([e,t])=>`;${rv(e)}=${rv(t)}`).join("")}function YA(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${vd(t)}=${vd(r)}`).join("&"):`${vd(t)}=${vd(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var ZA=/^[^\/()?;#]+/;function Jg(n){let e=n.match(ZA);return e?e[0]:""}var KA=/^[^\/()?;=#]+/;function JA(n){let e=n.match(KA);return e?e[0]:""}var QA=/^[^=?&#]+/;function eR(n){let e=n.match(QA);return e?e[0]:""}var tR=/^[^&#]+/;function nR(n){let e=n.match(tR);return e?e[0]:""}var sv=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new lt([],{}):new lt([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new Me(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,e));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,e)),(t.length>0||Object.keys(i).length>0)&&(r[Le]=new lt(t,i)),r}parseSegment(){let e=Jg(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Me(4009,!1);return this.capture(e),new Gr(bd(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=JA(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=Jg(this.remaining);r&&(i=r,this.capture(i))}e[bd(t)]=bd(i)}parseQueryParam(e){let t=eR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let o=nR(this.remaining);o&&(i=o,this.capture(i))}let r=iS(t),s=iS(i);if(e.hasOwnProperty(r)){let o=e[r];Array.isArray(o)||(o=[o],e[r]=o),o.push(s)}else e[r]=s}parseParens(e,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Jg(this.remaining),s=this.remaining[r.length];if(s!=="/"&&s!==")"&&s!==";")throw new Me(4010,!1);let o;r.indexOf(":")>-1?(o=r.slice(0,r.indexOf(":")),this.capture(o),this.capture(":")):e&&(o=Le);let a=this.parseChildren(t+1);i[o??Le]=Object.keys(a).length===1&&a[Le]?a[Le]:new lt([],a),this.consumeOptional("//")}return i}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Me(4011,!1)}};function _S(n){return n.segments.length>0?new lt([],{[Le]:n}):n}function xS(n){let e={};for(let[i,r]of Object.entries(n.children)){let s=xS(r);if(i===Le&&s.segments.length===0&&s.hasChildren())for(let[o,a]of Object.entries(s.children))e[o]=a;else(s.segments.length>0||s.hasChildren())&&(e[i]=s)}let t=new lt(n.segments,e);return iR(t)}function iR(n){if(n.numberOfChildren===1&&n.children[Le]){let e=n.children[Le];return new lt(n.segments.concat(e.segments),e.children)}return n}function Uo(n){return n instanceof ni}function bS(n,e,t=null,i=null,r=new jr){let s=SS(n);return MS(s,e,t,i,r)}function SS(n){let e;function t(s){let o={};for(let c of s.children){let l=t(c);o[c.outlet]=l}let a=new lt(s.url,o);return s===n&&(e=a),a}let i=t(n.root),r=_S(i);return e??r}function MS(n,e,t,i,r){let s=n;for(;s.parent;)s=s.parent;if(e.length===0)return Qg(s,s,s,t,i,r);let o=rR(e);if(o.toRoot())return Qg(s,s,new lt([],{}),t,i,r);let a=sR(o,s,n),c=a.processChildren?vc(a.segmentGroup,a.index,o.commands):wS(a.segmentGroup,a.index,o.commands);return Qg(s,a.segmentGroup,c,t,i,r)}function Md(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function xc(n){return typeof n=="object"&&n!=null&&n.outlets}function rS(n,e,t){n||="\u0275";let i=new ni;return i.queryParams={[n]:e},t.parse(t.serialize(i)).queryParams[n]}function Qg(n,e,t,i,r,s){let o={};for(let[l,u]of Object.entries(i??{}))o[l]=Array.isArray(u)?u.map(d=>rS(l,d,s)):rS(l,u,s);let a;n===e?a=t:a=ES(n,e,t);let c=_S(xS(a));return new ni(c,o,r)}function ES(n,e,t){let i={};return Object.entries(n.children).forEach(([r,s])=>{s===e?i[r]=t:i[r]=ES(s,e,t)}),new lt(n.segments,i)}var Ed=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&Md(i[0]))throw new Me(4003,!1);let r=i.find(xc);if(r&&r!==VA(i))throw new Me(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function rR(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new Ed(!0,0,n);let e=0,t=!1,i=n.reduce((r,s,o)=>{if(typeof s=="object"&&s!=null){if(s.outlets){let a={};return Object.entries(s.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(s.segmentPath)return[...r,s.segmentPath]}return typeof s!="string"?[...r,s]:o===0?(s.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,s]},[]);return new Ed(t,e,i)}var Fo=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function sR(n,e,t){if(n.isAbsolute)return new Fo(e,!0,0);if(!t)return new Fo(e,!1,NaN);if(t.parent===null)return new Fo(t,!0,0);let i=Md(n.commands[0])?0:1,r=t.segments.length-1+i;return oR(t,r,n.numberOfDoubleDots)}function oR(n,e,t){let i=n,r=e,s=t;for(;s>r;){if(s-=r,i=i.parent,!i)throw new Me(4005,!1);r=i.segments.length}return new Fo(i,!1,r-s)}function aR(n){return xc(n[0])?n[0].outlets:{[Le]:n}}function wS(n,e,t){if(n??=new lt([],{}),n.segments.length===0&&n.hasChildren())return vc(n,e,t);let i=cR(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let s=new lt(n.segments.slice(0,i.pathIndex),{});return s.children[Le]=new lt(n.segments.slice(i.pathIndex),n.children),vc(s,0,r)}else return i.match&&r.length===0?new lt(n.segments,{}):i.match&&!n.hasChildren()?ov(n,e,t):i.match?vc(n,0,r):ov(n,e,t)}function vc(n,e,t){if(t.length===0)return new lt(n.segments,{});{let i=aR(t),r={};if(Object.keys(i).some(s=>s!==Le)&&n.children[Le]&&n.numberOfChildren===1&&n.children[Le].segments.length===0){let s=vc(n.children[Le],e,t);return new lt(n.segments,s.children)}return Object.entries(i).forEach(([s,o])=>{typeof o=="string"&&(o=[o]),o!==null&&(r[s]=wS(n.children[s],e,o))}),Object.entries(n.children).forEach(([s,o])=>{i[s]===void 0&&(r[s]=o)}),new lt(n.segments,r)}}function cR(n,e,t){let i=0,r=e,s={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return s;let o=n.segments[r],a=t[i];if(xc(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!oS(c,l,o))return s;i+=2}else{if(!oS(c,{},o))return s;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function ov(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let s=t[r];if(xc(s)){let c=lR(s.outlets);return new lt(i,c)}if(r===0&&Md(t[0])){let c=n.segments[e];i.push(new Gr(c.path,sS(t[0]))),r++;continue}let o=xc(s)?s.outlets[Le]:`${s}`,a=r<t.length-1?t[r+1]:null;o&&a&&Md(a)?(i.push(new Gr(o,sS(a))),r+=2):(i.push(new Gr(o,{})),r++)}return new lt(i,{})}function lR(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=ov(new lt([],{}),0,i))}),e}function sS(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function oS(n,e,t){return n==t.path&&Ni(e,t.parameters)}var yc="imperative",nn=(function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n})(nn||{}),Un=class{id;url;constructor(e,t){this.id=e,this.url=t}},Vs=class extends Un{type=nn.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},rr=class extends Un{urlAfterRedirects;type=nn.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},_n=(function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n[n.Aborted=4]="Aborted",n})(_n||{}),bc=(function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n})(bc||{}),ti=class extends Un{reason;code;type=nn.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function TS(n){return n instanceof ti&&(n.code===_n.Redirect||n.code===_n.SupersededByNewNavigation)}var sr=class extends Un{reason;code;type=nn.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},Hs=class extends Un{error;target;type=nn.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Sc=class extends Un{urlAfterRedirects;state;type=nn.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},wd=class extends Un{urlAfterRedirects;state;type=nn.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Td=class extends Un{urlAfterRedirects;state;shouldActivate;type=nn.GuardsCheckEnd;constructor(e,t,i,r,s){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=s}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Cd=class extends Un{urlAfterRedirects;state;type=nn.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Dd=class extends Un{urlAfterRedirects;state;type=nn.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Id=class{route;type=nn.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Ad=class{route;type=nn.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Rd=class{snapshot;type=nn.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Nd=class{snapshot;type=nn.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Pd=class{snapshot;type=nn.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Od=class{snapshot;type=nn.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Bo=class{},Mc=class{},Vo=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function uR(n){return!(n instanceof Bo)&&!(n instanceof Vo)&&!(n instanceof Mc)}var Ld=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new jo(this.rootInjector)}},jo=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new Ld(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(We(Xt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Fd=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=av(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=av(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=cv(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return cv(e,this._root).map(t=>t.value)}};function av(n,e){if(n===e.value)return e;for(let t of e.children){let i=av(n,t);if(i)return i}return null}function cv(n,e){if(n===e.value)return[e];for(let t of e.children){let i=cv(n,t);if(i.length)return i.unshift(e),i}return[]}var kn=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function Lo(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var Ec=class extends Fd{snapshot;constructor(e,t){super(e),this.snapshot=t,vv(this,e)}toString(){return this.snapshot.toString()}};function CS(n,e){let t=dR(n,e),i=new an([new Gr("",{})]),r=new an({}),s=new an({}),o=new an({}),a=new an(""),c=new Wr(i,r,o,a,s,Le,n,t.root);return c.snapshot=t.root,new Ec(new kn(c,[]),t)}function dR(n,e){let t={},i={},r={},o=new Ho([],t,r,"",i,Le,n,null,{},e);return new wc("",new kn(o,[]))}var Wr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,s,o,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=s,this.outlet=o,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Pt(l=>l[Dc]))??Je(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Pt(e=>Bs(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Pt(e=>Bs(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function gv(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:he(he({},e.params),n.params),data:he(he({},e.data),n.data),resolve:he(he(he(he({},n.data),e.data),r?.data),n._resolvedData)}:i={params:he({},n.params),data:he({},n.data),resolve:he(he({},n.data),n._resolvedData??{})},r&&IS(r)&&(i.resolve[Dc]=r.title),i}var Ho=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Dc]}constructor(e,t,i,r,s,o,a,c,l,u){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s,this.outlet=o,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Bs(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Bs(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},wc=class extends Fd{url;constructor(e,t){super(t),this.url=e,vv(this,t)}toString(){return DS(this._root)}};function vv(n,e){e.value._routerState=n,e.children.forEach(t=>vv(n,t))}function DS(n){let e=n.children.length>0?` { ${n.children.map(DS).join(", ")} } `:"";return`${n.value}${e}`}function ev(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,Ni(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),Ni(e.params,t.params)||n.paramsSubject.next(t.params),BA(e.url,t.url)||n.urlSubject.next(t.url),Ni(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function lv(n,e){let t=Ni(n.params,e.params)&&jA(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||lv(n.parent,e.parent))}function IS(n){return typeof n.title=="string"||n.title===null}var AS=new Ie(""),Ac=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Le;activateEvents=new wn;deactivateEvents=new wn;attachEvents=new wn;detachEvents=new wn;routerOutletData=Lb();parentContexts=J(jo);location=J(Ns);changeDetector=J(ac);inputBinder=J(Vd,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Me(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Me(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Me(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Me(4013,!1);this._activatedRoute=t;let r=this.location,o=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new uv(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(o,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=nd({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[$u]})}return n})(),uv=class{route;childContexts;parent;outletData;constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===Wr?this.route:e===jo?this.childContexts:e===AS?this.outletData:this.parent.get(e,t)}},Vd=new Ie("");var yv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=Ps({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&Ro(0,"router-outlet")},dependencies:[Ac],encapsulation:2})}return n})();function _v(n){let e=n.children&&n.children.map(_v),t=e?st(he({},n),{children:e}):he({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==Le&&(t.component=yv),t}function fR(n,e,t){let i=Tc(n,e._root,t?t._root:void 0);return new Ec(i,e)}function Tc(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=hR(n,e,t);return new kn(i,r)}else{if(n.shouldAttach(e.value)){let s=n.retrieve(e.value);if(s!==null){let o=s.route;return o.value._futureSnapshot=e.value,o.children=e.children.map(a=>Tc(n,a)),o}}let i=pR(e.value),r=e.children.map(s=>Tc(n,s));return new kn(i,r)}}function hR(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return Tc(n,i,r);return Tc(n,i)})}function pR(n){return new Wr(new an(n.url),new an(n.params),new an(n.queryParams),new an(n.fragment),new an(n.data),n.outlet,n.component,n)}var zo=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},RS="ngNavigationCancelingError";function kd(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=Uo(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=NS(!1,_n.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function NS(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[RS]=!0,t.cancellationCode=e,t}function mR(n){return PS(n)&&Uo(n.url)}function PS(n){return!!n&&n[RS]}var dv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,s){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=s}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),ev(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=Lo(t);e.children.forEach(s=>{let o=s.value.outlet;this.deactivateRoutes(s,r[o],i),delete r[o]}),Object.values(r).forEach(s=>{this.deactivateRouteAndItsChildren(s,i)})}deactivateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(r===s)if(r.component){let o=i.getContext(r.outlet);o&&this.deactivateChildRoutes(e,t,o.children)}else this.deactivateChildRoutes(e,t,i);else s&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=Lo(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);if(i&&i.outlet){let o=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:o,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=Lo(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=Lo(t);e.children.forEach(s=>{this.activateRoutes(s,r[s.value.outlet],i),this.forwardEvent(new Od(s.value.snapshot))}),e.children.length&&this.forwardEvent(new Nd(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(ev(r),r===s)if(r.component){let o=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,o.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let o=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),o.children.onOutletReAttached(a.contexts),o.attachRef=a.componentRef,o.route=a.route.value,o.outlet&&o.outlet.attach(a.componentRef,a.route.value),ev(a.route.value),this.activateChildRoutes(e,null,o.children)}else o.attachRef=null,o.route=r,o.outlet&&o.outlet.activateWith(r,o.injector),this.activateChildRoutes(e,null,o.children)}else this.activateChildRoutes(e,null,i)}},Ud=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},ko=class{component;route;constructor(e,t){this.component=e,this.route=t}};function gR(n,e,t){let i=n._root,r=e?e._root:null;return gc(i,r,t,[i.value])}function vR(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function Wo(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!kp(n)?n:e.get(n):i}function gc(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=Lo(e);return n.children.forEach(o=>{yR(o,s[o.value.outlet],t,i.concat([o.value]),r),delete s[o.value.outlet]}),Object.entries(s).forEach(([o,a])=>_c(a,t.getContext(o),r)),r}function yR(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=n.value,o=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(o&&s.routeConfig===o.routeConfig){let c=_R(o,s,s.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Ud(i)):(s.data=o.data,s._resolvedData=o._resolvedData),s.component?gc(n,e,a?a.children:null,i,r):gc(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ko(a.outlet.component,o))}else o&&_c(e,a,r),r.canActivateChecks.push(new Ud(i)),s.component?gc(n,null,a?a.children:null,i,r):gc(n,null,t,i,r);return r}function _R(n,e,t){if(typeof t=="function")return gn(e._environmentInjector,()=>t(n,e));switch(t){case"pathParamsChange":return!Us(n.url,e.url);case"pathParamsOrQueryParamsChange":return!Us(n.url,e.url)||!Ni(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!lv(n,e)||!Ni(n.queryParams,e.queryParams);default:return!lv(n,e)}}function _c(n,e,t){let i=Lo(n),r=n.value;Object.entries(i).forEach(([s,o])=>{r.component?e?_c(o,e.children.getContext(s),t):_c(o,null,t):_c(o,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new ko(e.outlet.component,r)):t.canDeactivateChecks.push(new ko(null,r)):t.canDeactivateChecks.push(new ko(null,r))}function Rc(n){return typeof n=="function"}function xR(n){return typeof n=="boolean"}function bR(n){return n&&Rc(n.canLoad)}function SR(n){return n&&Rc(n.canActivate)}function MR(n){return n&&Rc(n.canActivateChild)}function ER(n){return n&&Rc(n.canDeactivate)}function wR(n){return n&&Rc(n.canMatch)}function OS(n){return n instanceof ds||n?.name==="EmptyError"}var yd=Symbol("INITIAL_VALUE");function Go(){return ui(n=>vp(n.map(e=>e.pipe(qi(1),_p(yd)))).pipe(Pt(e=>{for(let t of e)if(t!==!0){if(t===yd)return yd;if(t===!1||TR(t))return t}return!0}),Xi(e=>e!==yd),qi(1)))}function TR(n){return Uo(n)||n instanceof zo}function LS(n){return n.aborted?Je(void 0).pipe(qi(1)):new at(e=>{let t=()=>{e.next(),e.complete()};return n.addEventListener("abort",t),()=>n.removeEventListener("abort",t)})}function FS(n){return Na(LS(n))}function CR(n){return Mn(e=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:s}}=e;return s.length===0&&r.length===0?Je(st(he({},e),{guardsResult:!0})):DR(s,t,i).pipe(Mn(o=>o&&xR(o)?IR(t,r,n):Je(o)),Pt(o=>st(he({},e),{guardsResult:o})))})}function DR(n,e,t){return zt(n).pipe(Mn(i=>OR(i.component,i.route,t,e)),Yi(i=>i!==!0,!0))}function IR(n,e,t){return zt(e).pipe(Zl(i=>fo(RR(i.route.parent,t),AR(i.route,t),PR(n,i.path),NR(n,i.route))),Yi(i=>i!==!0,!0))}function AR(n,e){return n!==null&&e&&e(new Pd(n)),Je(!0)}function RR(n,e){return n!==null&&e&&e(new Rd(n)),Je(!0)}function NR(n,e){let t=e.routeConfig?e.routeConfig.canActivate:null;if(!t||t.length===0)return Je(!0);let i=t.map(r=>Aa(()=>{let s=e._environmentInjector,o=Wo(r,s),a=SR(o)?o.canActivate(e,n):gn(s,()=>o(e,n));return zs(a).pipe(Yi())}));return Je(i).pipe(Go())}function PR(n,e){let t=e[e.length-1],r=e.slice(0,e.length-1).reverse().map(s=>vR(s)).filter(s=>s!==null).map(s=>Aa(()=>{let o=s.guards.map(a=>{let c=s.node._environmentInjector,l=Wo(a,c),u=MR(l)?l.canActivateChild(t,n):gn(c,()=>l(t,n));return zs(u).pipe(Yi())});return Je(o).pipe(Go())}));return Je(r).pipe(Go())}function OR(n,e,t,i){let r=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!r||r.length===0)return Je(!0);let s=r.map(o=>{let a=e._environmentInjector,c=Wo(o,a),l=ER(c)?c.canDeactivate(n,e,t,i):gn(a,()=>c(n,e,t,i));return zs(l).pipe(Yi())});return Je(s).pipe(Go())}function LR(n,e,t,i,r){let s=e.canLoad;if(s===void 0||s.length===0)return Je(!0);let o=s.map(a=>{let c=Wo(a,n),l=bR(c)?c.canLoad(e,t):gn(n,()=>c(e,t)),u=zs(l);return r?u.pipe(FS(r)):u});return Je(o).pipe(Go(),kS(i))}function kS(n){return hp($n(e=>{if(typeof e!="boolean")throw kd(n,e)}),Pt(e=>e===!0))}function FR(n,e,t,i,r,s){let o=e.canMatch;if(!o||o.length===0)return Je(!0);let a=o.map(c=>{let l=Wo(c,n),u=wR(l)?l.canMatch(e,t,r):gn(n,()=>l(e,t,r));return zs(u).pipe(FS(s))});return Je(a).pipe(Go(),kS(i))}var ir=class n extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,n.prototype)}},Cc=class n extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,n.prototype)}};function kR(n){throw new Me(4e3,!1)}function UR(n){throw NS(!1,_n.GuardRejected)}var fv=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}lineralizeSegments(e,t){return ht(this,null,function*(){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Le])throw kR(`${e.redirectTo}`);r=r.children[Le]}})}applyRedirectCommands(e,t,i,r,s){return ht(this,null,function*(){let o=yield BR(t,r,s);if(o instanceof ni)throw new Cc(o);let a=this.applyRedirectCreateUrlTree(o,this.urlSerializer.parse(o),e,i);if(o[0]==="/")throw new Cc(a);return a})}applyRedirectCreateUrlTree(e,t,i,r){let s=this.createSegmentGroup(e,t.root,i,r);return new ni(s,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,s])=>{if(typeof s=="string"&&s[0]===":"){let a=s.substring(1);i[r]=t[a]}else i[r]=s}),i}createSegmentGroup(e,t,i,r){let s=this.createSegments(e,t.segments,i,r),o={};return Object.entries(t.children).forEach(([a,c])=>{o[a]=this.createSegmentGroup(e,c,i,r)}),new lt(s,o)}createSegments(e,t,i,r){return t.map(s=>s.path[0]===":"?this.findPosParam(e,s,r):this.findOrReturn(s,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Me(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}};function BR(n,e,t){if(typeof n=="string")return Promise.resolve(n);let i=n;return Sd(zs(gn(t,()=>i(e))))}function VR(n,e){return n.providers&&!n._injector&&(n._injector=ic(n.providers,e,`Route: ${n.path}`)),n._injector??e}function gi(n){return n.outlet||Le}function HR(n,e){let t=n.filter(i=>gi(i)===e);return t.push(...n.filter(i=>gi(i)!==e)),t}var hv={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function US(n){return{routeConfig:n.routeConfig,url:n.url,params:n.params,queryParams:n.queryParams,fragment:n.fragment,data:n.data,outlet:n.outlet,title:n.title,paramMap:n.paramMap,queryParamMap:n.queryParamMap}}function zR(n,e,t,i,r,s,o){let a=BS(n,e,t);if(!a.matched)return Je(a);let c=US(s(a));return i=VR(e,i),FR(i,e,t,r,c,o).pipe(Pt(l=>l===!0?a:he({},hv)))}function BS(n,e,t){if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?he({},hv):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||lS)(t,n,e);if(!r)return he({},hv);let s={};Object.entries(r.posParams??{}).forEach(([a,c])=>{s[a]=c.path});let o=r.consumed.length>0?he(he({},s),r.consumed[r.consumed.length-1].parameters):s;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:o,positionalParamSegments:r.posParams??{}}}function aS(n,e,t,i,r){return t.length>0&&WR(n,t,i,r)?{segmentGroup:new lt(e,jR(i,new lt(t,n.children))),slicedSegments:[]}:t.length===0&&$R(n,t,i)?{segmentGroup:new lt(n.segments,GR(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new lt(n.segments,n.children),slicedSegments:t}}function GR(n,e,t,i){let r={};for(let s of t)if(Hd(n,e,s)&&!i[gi(s)]){let o=new lt([],{});r[gi(s)]=o}return he(he({},i),r)}function jR(n,e){let t={};t[Le]=e;for(let i of n)if(i.path===""&&gi(i)!==Le){let r=new lt([],{});t[gi(i)]=r}return t}function WR(n,e,t,i){return t.some(r=>!Hd(n,e,r)||!(gi(r)!==Le)?!1:!(i!==void 0&&gi(r)===i))}function $R(n,e,t){return t.some(i=>Hd(n,e,i))}function Hd(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function XR(n,e,t){return e.length===0&&!n.children[t]}var pv=class{};function qR(n,e,t,i,r,s,o="emptyOnly",a){return ht(this,null,function*(){return new mv(n,e,t,i,r,o,s,a).recognize()})}var YR=31,mv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,t,i,r,s,o,a,c){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=s,this.paramsInheritanceStrategy=o,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new fv(this.urlSerializer,this.urlTree)}noMatchError(e){return new Me(4002,`'${e.segmentGroup}'`)}recognize(){return ht(this,null,function*(){let e=aS(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=yield this.match(e),r=new kn(i,t),s=new wc("",r),o=bS(i,[],this.urlTree.queryParams,this.urlTree.fragment);return o.queryParams=this.urlTree.queryParams,s.url=this.urlSerializer.serialize(o),{state:s,tree:o}})}match(e){return ht(this,null,function*(){let t=new Ho([],Object.freeze({}),Object.freeze(he({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Le,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,e,Le,t),rootSnapshot:t}}catch(i){if(i instanceof Cc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof ir?this.noMatchError(i):i}})}processSegmentGroup(e,t,i,r,s){return ht(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(e,t,i,s);let o=yield this.processSegment(e,t,i,i.segments,r,!0,s);return o instanceof kn?[o]:[]})}processChildren(e,t,i,r){return ht(this,null,function*(){let s=[];for(let c of Object.keys(i.children))c==="primary"?s.unshift(c):s.push(c);let o=[];for(let c of s){let l=i.children[c],u=HR(t,c),d=yield this.processSegmentGroup(e,u,l,c,r);o.push(...d)}let a=VS(o);return ZR(a),a})}processSegment(e,t,i,r,s,o,a){return ht(this,null,function*(){for(let c of t)try{return yield this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,s,o,a)}catch(l){if(l instanceof ir||OS(l))continue;throw l}if(XR(i,r,s))return new pv;throw new ir(i)})}processSegmentAgainstRoute(e,t,i,r,s,o,a,c){return ht(this,null,function*(){if(gi(i)!==o&&(o===Le||!Hd(r,s,i)))throw new ir(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,r,i,s,o,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,s,o,c);throw new ir(r)})}expandSegmentAgainstRouteUsingRedirect(e,t,i,r,s,o,a){return ht(this,null,function*(){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:f}=BS(t,r,s);if(!c)throw new ir(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>YR&&(this.allowRedirects=!1));let h=this.createSnapshot(e,r,s,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let g=yield this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,US(h),e),y=yield this.applyRedirects.lineralizeSegments(r,g);return this.processSegment(e,i,t,y.concat(f),o,!1,a)})}createSnapshot(e,t,i,r,s){let o=new Ho(i,r,Object.freeze(he({},this.urlTree.queryParams)),this.urlTree.fragment,JR(t),gi(t),t.component??t._loadedComponent??null,t,QR(t),e),a=gv(o,s,this.paramsInheritanceStrategy);return o.params=Object.freeze(a.params),o.data=Object.freeze(a.data),o}matchSegmentAgainstRoute(e,t,i,r,s,o){return ht(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=M=>this.createSnapshot(e,i,M.consumedSegments,M.parameters,o),c=yield Sd(zR(t,i,r,e,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new ir(t);e=i._injector??e;let{routes:l}=yield this.getChildConfig(e,i,r),u=i._loadedInjector??e,{parameters:d,consumedSegments:f,remainingSegments:h}=c,g=this.createSnapshot(e,i,f,d,o),{segmentGroup:y,slicedSegments:m}=aS(t,f,h,l,s);if(m.length===0&&y.hasChildren()){let M=yield this.processChildren(u,l,y,g);return new kn(g,M)}if(l.length===0&&m.length===0)return new kn(g,[]);let p=gi(i)===s,b=yield this.processSegment(u,l,y,m,p?Le:s,!0,g);return new kn(g,b instanceof kn?[b]:[])})}getChildConfig(e,t,i){return ht(this,null,function*(){if(t.children)return{routes:t.children,injector:e};if(t.loadChildren){if(t._loadedRoutes!==void 0){let s=t._loadedNgModuleFactory;return s&&!t._loadedInjector&&(t._loadedInjector=s.create(e).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield Sd(LR(e,t,i,this.urlSerializer,this.abortSignal))){let s=yield this.configLoader.loadChildren(e,t);return t._loadedRoutes=s.routes,t._loadedInjector=s.injector,t._loadedNgModuleFactory=s.factory,s}throw UR(t)}return{routes:[],injector:e}})}};function ZR(n){n.sort((e,t)=>e.value.outlet===Le?-1:t.value.outlet===Le?1:e.value.outlet.localeCompare(t.value.outlet))}function KR(n){let e=n.value.routeConfig;return e&&e.path===""}function VS(n){let e=[],t=new Set;for(let i of n){if(!KR(i)){e.push(i);continue}let r=e.find(s=>i.value.routeConfig===s.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):e.push(i)}for(let i of t){let r=VS(i.children);e.push(new kn(i.value,r))}return e.filter(i=>!t.has(i))}function JR(n){return n.data||{}}function QR(n){return n.resolve||{}}function e1(n,e,t,i,r,s,o){return Mn(a=>ht(null,null,function*(){let{state:c,tree:l}=yield qR(n,e,t,i,a.extractedUrl,r,s,o);return st(he({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function t1(n){return Mn(e=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=e;if(!i.length)return Je(e);let r=new Set(i.map(a=>a.route)),s=new Set;for(let a of r)if(!s.has(a))for(let c of HS(a))s.add(c);let o=0;return zt(s).pipe(Zl(a=>r.has(a)?n1(a,t,n):(a.data=gv(a,a.parent,n).resolve,Je(void 0))),$n(()=>o++),Kl(1),Mn(a=>o===s.size?Je(e):cn))})}function HS(n){let e=n.children.map(t=>HS(t)).flat();return[n,...e]}function n1(n,e,t){let i=n.routeConfig,r=n._resolve;return i?.title!==void 0&&!IS(i)&&(r[Dc]=i.title),Aa(()=>(n.data=gv(n,n.parent,t).resolve,i1(r,n,e).pipe(Pt(s=>(n._resolvedData=s,n.data=he(he({},n.data),s),null)))))}function i1(n,e,t){let i=nv(n);if(i.length===0)return Je({});let r={};return zt(i).pipe(Mn(s=>r1(n[s],e,t).pipe(Yi(),$n(o=>{if(o instanceof zo)throw kd(new jr,o);r[s]=o}))),Kl(1),Pt(()=>r),Ra(s=>OS(s)?cn:gp(s)))}function r1(n,e,t){let i=e._environmentInjector,r=Wo(n,i),s=r.resolve?r.resolve(e,t):gn(i,()=>r(e,t));return zs(s)}function cS(n){return ui(e=>{let t=n(e);return t?zt(t).pipe(Pt(()=>e)):Je(e)})}var xv=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(s=>s.outlet===Le);return i}getResolvedTitleForRoute(t){return t.data[Dc]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(zS),providedIn:"root"})}return n})(),zS=(()=>{class n extends xv{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(We(tS))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Nc=new Ie("",{factory:()=>({})}),Pc=new Ie(""),GS=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=J(Og);loadComponent(t,i){return ht(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=ht(this,null,function*(){try{let s=yield dS(gn(t,()=>i.loadComponent())),o=yield $S(WS(s));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=o,o}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=ht(this,null,function*(){try{let s=yield jS(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=s.routes,i._loadedInjector=s.injector,i._loadedNgModuleFactory=s.factory,s}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function jS(n,e,t,i){return ht(this,null,function*(){let r=yield dS(gn(t,()=>n.loadChildren())),s=yield $S(WS(r)),o;s instanceof td||Array.isArray(s)?o=s:o=yield e.compileModuleAsync(s),i&&i(n);let a,c,l=!1,u;return Array.isArray(o)?(c=o,l=!0):(a=o.create(t).injector,u=o,c=a.get(Pc,[],{optional:!0,self:!0}).flat()),{routes:c.map(_v),injector:a,factory:u}})}function s1(n){return n&&typeof n=="object"&&"default"in n}function WS(n){return s1(n)?n.default:n}function $S(n){return ht(this,null,function*(){return n})}var zd=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(o1),providedIn:"root"})}return n})(),o1=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),XS=new Ie("");var a1=()=>{},qS=new Ie(""),YS=(()=>{class n{currentNavigation=ei(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=ei(null);events=new on;transitionAbortWithErrorSubject=new on;configLoader=J(GS);environmentInjector=J(Xt);destroyRef=J(Lr);urlSerializer=J(Ic);rootContexts=J(jo);location=J(Po);inputBindingEnabled=J(Vd,{optional:!0})!==null;titleStrategy=J(xv);options=J(Nc,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=J(zd);createViewTransition=J(XS,{optional:!0});navigationErrorHandler=J(qS,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Je(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Id(r)),i=r=>this.events.next(new Ad(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;tr(()=>{this.transitions?.next(st(he({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new an(null),this.transitions.pipe(Xi(i=>i!==null),ui(i=>{let r=!1,s=new AbortController,o=()=>!r&&this.currentTransition?.id===i.id;return Je(i).pipe(ui(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",_n.SupersededByNewNavigation),cn;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?st(he({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new sr(a.id,this.urlSerializer.serialize(a.rawUrl),"",bc.IgnoredSameUrlNavigation)),a.resolve(!1),cn;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return Je(a).pipe(ui(d=>(this.events.next(new Vs(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?cn:Promise.resolve(d))),e1(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),$n(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(f=>(f.finalUrl=d.urlAfterRedirects,f)),this.events.next(new Mc)}),ui(d=>zt(i.routesRecognizeHandler.deferredHandle??Je(void 0)).pipe(Pt(()=>d))),$n(()=>{let d=new Sc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:f,source:h,restoredState:g,extras:y}=a,m=new Vs(d,this.urlSerializer.serialize(f),h,g);this.events.next(m);let p=CS(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=st(he({},a),{targetSnapshot:p,urlAfterRedirects:f,extras:st(he({},y),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(b=>(b.finalUrl=f,b)),Je(i)}else return this.events.next(new sr(a.id,this.urlSerializer.serialize(a.extractedUrl),"",bc.IgnoredByUrlHandlingStrategy)),a.resolve(!1),cn}),Pt(a=>{let c=new wd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=st(he({},a),{guards:gR(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),CR(a=>this.events.next(a)),ui(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw kd(this.urlSerializer,a.guardsResult);let c=new Td(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!o())return cn;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",_n.GuardRejected),cn;if(a.guards.canActivateChecks.length===0)return Je(a);let l=new Cd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!o())return cn;let u=!1;return Je(a).pipe(t1(this.paramsInheritanceStrategy),$n({next:()=>{u=!0;let d=new Dd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",_n.NoDataFromResolver)}}))}),cS(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let f=u._environmentInjector;d.push(this.configLoader.loadComponent(f,u.routeConfig).then(h=>{u.component=h}))}for(let f of u.children)d.push(...c(f));return d},l=c(a.targetSnapshot.root);return l.length===0?Je(a):zt(Promise.all(l).then(()=>a))}),cS(()=>this.afterPreactivation()),ui(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?zt(l).pipe(Pt(()=>i)):Je(i)}),qi(1),ui(a=>{let c=fR(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=st(he({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new Bo);let l=i.beforeActivateHandler.deferredHandle;return l?zt(l.then(()=>a)):Je(a)}),$n(a=>{new dv(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),o()&&(r=!0,this.currentNavigation.update(c=>(c.abort=a1,c)),this.lastSuccessfulNavigation.set(tr(this.currentNavigation)),this.events.next(new rr(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Na(LS(s.signal).pipe(Xi(()=>!r&&!i.targetRouterState),$n(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",_n.Aborted)}))),$n({complete:()=>{r=!0}}),Na(this.transitionAbortWithErrorSubject.pipe($n(a=>{throw a}))),yp(()=>{s.abort(),r||this.cancelNavigationTransition(i,"",_n.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Ra(a=>{if(r=!0,this.destroyed)return i.resolve(!1),cn;if(PS(a))this.events.next(new ti(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),mR(a)?this.events.next(new Vo(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new Hs(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=gn(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof zo){let{message:u,cancellationCode:d}=kd(this.urlSerializer,l);this.events.next(new ti(i.id,this.urlSerializer.serialize(i.extractedUrl),u,d)),this.events.next(new Vo(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return cn}))}))}cancelNavigationTransition(t,i,r){let s=new ti(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(s),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=tr(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function c1(n){return n!==yc}var ZS=new Ie("");var KS=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(l1),providedIn:"root"})}return n})(),Bd=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}shouldDestroyInjector(e){return!0}},l1=(()=>{class n extends Bd{static \u0275fac=(()=>{let t;return function(r){return(t||(t=ec(n)))(r||n)}})();static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),bv=(()=>{class n{urlSerializer=J(Ic);options=J(Nc,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=J(Po);urlHandlingStrategy=J(zd);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new ni;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let s=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,o=r??s;return o instanceof ni?this.urlSerializer.serialize(o):o}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=CS(null,J(Xt));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(u1),providedIn:"root"})}return n})(),u1=(()=>{class n extends bv{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof Vs?this.updateStateMemento():t instanceof sr?this.commitTransition(i):t instanceof Sc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Bo?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof ti&&!TS(t)?this.restoreHistory(i):t instanceof Hs?this.restoreHistory(i,!0):t instanceof rr&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let{extras:r,id:s}=i,{replaceUrl:o,state:a}=r;if(this.location.isCurrentPathEqualTo(t)||o){let c=this.browserPageId,l=he(he({},a),this.generateNgRouterState(s,c,i));this.location.replaceState(t,"",l)}else{let c=he(he({},a),this.generateNgRouterState(s,this.browserPageId+1,i));this.location.go(t,"",c)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,s=this.currentPageId-r;s!==0?this.location.historyGo(s):this.getCurrentUrlTree()===t.finalUrl&&s===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i,r){return this.canceledNavigationResolution==="computed"?he({navigationId:t,\u0275routerPageId:i},this.routerUrlState(r)):he({navigationId:t},this.routerUrlState(r))}static \u0275fac=(()=>{let t;return function(r){return(t||(t=ec(n)))(r||n)}})();static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Sv(n,e){n.events.pipe(Xi(t=>t instanceof rr||t instanceof ti||t instanceof Hs||t instanceof sr),Pt(t=>t instanceof rr||t instanceof sr?0:(t instanceof ti?t.code===_n.Redirect||t.code===_n.SupersededByNewNavigation:!1)?2:1),Xi(t=>t!==2),qi(1)).subscribe(()=>{e()})}var Gd=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=J(bg);stateManager=J(bv);options=J(Nc,{optional:!0})||{};pendingTasks=J(er);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=J(YS);urlSerializer=J(Ic);location=J(Po);urlHandlingStrategy=J(zd);injector=J(Xt);_events=new on;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=J(KS);injectorCleanup=J(ZS,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=J(Pc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!J(Vd,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new $t;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,s=tr(this.navigationTransitions.currentNavigation);if(r!==null&&s!==null){if(this.stateManager.handleRouterEvent(i,s),i instanceof ti&&i.code!==_n.Redirect&&i.code!==_n.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof rr)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof Vo){let o=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=he({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||c1(r.source)},o);this.scheduleNavigation(a,yc,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}uR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),yc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,s)=>{this.navigateToSyncWithBrowser(t,r,i,s)})}navigateToSyncWithBrowser(t,i,r,s){let o=r?.navigationId?r:null,a=r?.\u0275routerUrl??t;if(r?.\u0275routerUrl&&(s=st(he({},s),{browserUrl:t})),r){let l=he({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(s.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,o,s).catch(l=>{this.disposed||this.injector.get(Qn)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return tr(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(_v),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:s,fragment:o,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:o,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=he(he({},this.currentUrlTree.queryParams),s);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=s||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let f=r?r.snapshot:this.routerState.snapshot.root;d=SS(f)}catch(f){(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return MS(d,t,u,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=Uo(t)?t:this.parseUrl(t),s=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(s,yc,null,i)}navigate(t,i={skipLocationChange:!1}){return d1(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch(i){return this.console.warn(mo(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=he({},hS):i===!1?r=he({},iv):r=he(he({},iv),i),Uo(t))return nS(this.currentUrlTree,t,r);let s=this.parseUrl(t);return nS(this.currentUrlTree,s,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,s])=>(s!=null&&(i[r]=s),i),{})}scheduleNavigation(t,i,r,s,o){if(this.disposed)return Promise.resolve(!1);let a,c,l;o?(a=o.resolve,c=o.reject,l=o.promise):l=new Promise((d,f)=>{a=d,c=f});let u=this.pendingTasks.add();return Sv(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:s,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function d1(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Me(4008,!1)}var p1=new Ie("");function Mv(n,...e){return bs([{provide:Pc,multi:!0,useValue:n},[],{provide:Wr,useFactory:m1},{provide:id,multi:!0,useFactory:g1},e.map(t=>t.\u0275providers)])}function m1(){return J(Gd).routerState.root}function g1(){let n=J(di);return e=>{let t=n.get(Os);if(e!==t.components[0])return;let i=n.get(Gd),r=n.get(v1);n.get(y1)===1&&i.initialNavigation(),n.get(_1,null,{optional:!0})?.setUpPreloading(),n.get(p1,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var v1=new Ie("",{factory:()=>new on}),y1=new Ie("",{factory:()=>1});var _1=new Ie("");var EM=0,sy=1,wM=2;var ol=1,Jf=2,va=3,hr=0,Dn=1,Hi=2,zi=0,qs=1,oy=2,ay=3,cy=4,TM=5;var Jr=100,CM=101,DM=102,IM=103,AM=104,RM=200,NM=201,PM=202,OM=203,vf=204,yf=205,LM=206,FM=207,kM=208,UM=209,BM=210,VM=211,HM=212,zM=213,GM=214,_f=0,xf=1,bf=2,Ys=3,Sf=4,Mf=5,Ef=6,wf=7,ly=0,jM=1,WM=2,bi=0,uy=1,dy=2,fy=3,al=4,hy=5,py=6,my=7;var qv=300,ns=301,Qs=302,Qf=303,eh=304,cl=306,Tf=1e3,Li=1001,Cf=1002,sn=1003,$M=1004;var ll=1005;var hn=1006,th=1007;var is=1008;var Nn=1009,gy=1010,vy=1011,ya=1012,nh=1013,Si=1014,Mi=1015,Gi=1016,ih=1017,rh=1018,_a=1020,yy=35902,_y=35899,xy=1021,by=1022,ai=1023,Fi=1026,rs=1027,Sy=1028,sh=1029,ss=1030,oh=1031;var ah=1033,ul=33776,dl=33777,fl=33778,hl=33779,ch=35840,lh=35841,uh=35842,dh=35843,fh=36196,hh=37492,ph=37496,mh=37488,gh=37489,pl=37490,vh=37491,yh=37808,_h=37809,xh=37810,bh=37811,Sh=37812,Mh=37813,Eh=37814,wh=37815,Th=37816,Ch=37817,Dh=37818,Ih=37819,Ah=37820,Rh=37821,Nh=36492,Ph=36494,Oh=36495,Lh=36283,Fh=36284,ml=36285,kh=36286;var Vc=2300,Df=2301,gf=2302,Yv=2303,Zv=2400,Kv=2401,Jv=2402;var XM=3200;var Uh=0,qM=1,vr="",Cn="srgb",Hc="srgb-linear",zc="linear",ft="srgb";var Xs=7680;var Qv=519,YM=512,ZM=513,KM=514,Bh=515,JM=516,QM=517,Vh=518,eE=519,ey=35044;var My="300 es",xi=2e3,aa=2001;function b1(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function S1(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Gc(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function tE(){let n=Gc("canvas");return n.style.display="block",n}var JS={},ca=null;function Ey(...n){let e="THREE."+n.shift();ca?ca("log",e,...n):console.log(e,...n)}function nE(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function De(...n){n=nE(n);let e="THREE."+n.shift();if(ca)ca("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Re(...n){n=nE(n);let e="THREE."+n.shift();if(ca)ca("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function If(...n){let e=n.join(" ");e in JS||(JS[e]=!0,De(...n))}function iE(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var rE={[_f]:xf,[bf]:Ef,[Sf]:wf,[Ys]:Mf,[xf]:_f,[Ef]:bf,[wf]:Sf,[Mf]:Ys},ki=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},xn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],QS=1234567,sa=Math.PI/180,la=180/Math.PI;function xa(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(xn[n&255]+xn[n>>8&255]+xn[n>>16&255]+xn[n>>24&255]+"-"+xn[e&255]+xn[e>>8&255]+"-"+xn[e>>16&15|64]+xn[e>>24&255]+"-"+xn[t&63|128]+xn[t>>8&255]+"-"+xn[t>>16&255]+xn[t>>24&255]+xn[i&255]+xn[i>>8&255]+xn[i>>16&255]+xn[i>>24&255]).toLowerCase()}function et(n,e,t){return Math.max(e,Math.min(t,n))}function wy(n,e){return(n%e+e)%e}function M1(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function E1(n,e,t){return n!==e?(t-n)/(e-n):0}function Bc(n,e,t){return(1-t)*n+t*e}function w1(n,e,t,i){return Bc(n,e,1-Math.exp(-t*i))}function T1(n,e=1){return e-Math.abs(wy(n,e*2)-e)}function C1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function D1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function I1(n,e){return n+Math.floor(Math.random()*(e-n+1))}function A1(n,e){return n+Math.random()*(e-n)}function R1(n){return n*(.5-Math.random())}function N1(n){n!==void 0&&(QS=n);let e=QS+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function P1(n){return n*sa}function O1(n){return n*la}function L1(n){return(n&n-1)===0&&n!==0}function F1(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function k1(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function U1(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),f=o((e-i)/2),h=s((i-e)/2),g=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*f,a*l);break;case"YZY":n.set(c*f,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*f,a*u,a*l);break;case"XZX":n.set(a*u,c*g,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*g,a*l);break;case"ZYZ":n.set(c*g,c*h,a*u,a*l);break;default:De("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function ra(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function Tn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var Ut={DEG2RAD:sa,RAD2DEG:la,generateUUID:xa,clamp:et,euclideanModulo:wy,mapLinear:M1,inverseLerp:E1,lerp:Bc,damp:w1,pingpong:T1,smoothstep:C1,smootherstep:D1,randInt:I1,randFloat:A1,randFloatSpread:R1,seededRandom:N1,degToRad:P1,radToDeg:O1,isPowerOfTwo:L1,ceilPowerOfTwo:F1,floorPowerOfTwo:k1,setQuaternionFromProperEuler:U1,normalize:Tn,denormalize:ra},ot=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=et(this.x,e.x,t.x),this.y=et(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=et(this.x,e,t),this.y=et(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(et(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(et(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Ui=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=s[o+0],h=s[o+1],g=s[o+2],y=s[o+3];if(d!==y||c!==f||l!==h||u!==g){let m=c*f+l*h+u*g+d*y;m<0&&(f=-f,h=-h,g=-g,y=-y,m=-m);let p=1-a;if(m<.9995){let b=Math.acos(m),M=Math.sin(b);p=Math.sin(p*b)/M,a=Math.sin(a*b)/M,c=c*p+f*a,l=l*p+h*a,u=u*p+g*a,d=d*p+y*a}else{c=c*p+f*a,l=l*p+h*a,u=u*p+g*a,d=d*p+y*a;let b=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=b,l*=b,u*=b,d*=b}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],f=s[o+1],h=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*h-l*f,e[t+1]=c*g+u*f+l*d-a*h,e[t+2]=l*g+u*h+a*f-c*d,e[t+3]=u*g-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),f=c(i/2),h=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"YXZ":this._x=f*u*d+l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"ZXY":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d-f*h*g;break;case"ZYX":this._x=f*u*d-l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d+f*h*g;break;case"YZX":this._x=f*u*d+l*h*g,this._y=l*h*d+f*u*g,this._z=l*u*g-f*h*d,this._w=l*u*d-f*h*g;break;case"XZY":this._x=f*u*d-l*h*g,this._y=l*h*d-f*u*g,this._z=l*u*g+f*h*d,this._w=l*u*d+f*h*g;break;default:De("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(et(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},O=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(eM.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(eM.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=et(this.x,e.x,t.x),this.y=et(this.y,e.y,t.y),this.z=et(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=et(this.x,e,t),this.y=et(this.y,e,t),this.z=et(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(et(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Ev.copy(this).projectOnVector(e),this.sub(Ev)}reflect(e){return this.sub(Ev.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(et(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Ev=new O,eM=new Ui,Fe=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],g=i[8],y=r[0],m=r[3],p=r[6],b=r[1],M=r[4],E=r[7],D=r[2],w=r[5],I=r[8];return s[0]=o*y+a*b+c*D,s[3]=o*m+a*M+c*w,s[6]=o*p+a*E+c*I,s[1]=l*y+u*b+d*D,s[4]=l*m+u*M+d*w,s[7]=l*p+u*E+d*I,s[2]=f*y+h*b+g*D,s[5]=f*m+h*M+g*w,s[8]=f*p+h*E+g*I,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,f=a*c-u*s,h=l*s-o*c,g=t*d+i*f+r*h;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let y=1/g;return e[0]=d*y,e[1]=(r*l-u*i)*y,e[2]=(a*i-r*o)*y,e[3]=f*y,e[4]=(u*t-r*c)*y,e[5]=(r*s-a*t)*y,e[6]=h*y,e[7]=(i*c-l*t)*y,e[8]=(o*t-i*s)*y,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(wv.makeScale(e,t)),this}rotate(e){return this.premultiply(wv.makeRotation(-e)),this}translate(e,t){return this.premultiply(wv.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},wv=new Fe,tM=new Fe().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),nM=new Fe().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function B1(){let n={enabled:!0,workingColorSpace:Hc,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===ft&&(r.r=fr(r.r),r.g=fr(r.g),r.b=fr(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===ft&&(r.r=oa(r.r),r.g=oa(r.g),r.b=oa(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===vr?zc:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return If("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return If("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Hc]:{primaries:e,whitePoint:i,transfer:zc,toXYZ:tM,fromXYZ:nM,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Cn},outputColorSpaceConfig:{drawingBufferColorSpace:Cn}},[Cn]:{primaries:e,whitePoint:i,transfer:ft,toXYZ:tM,fromXYZ:nM,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Cn}}}),n}var Qe=B1();function fr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function oa(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var $o,Af=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{$o===void 0&&($o=Gc("canvas")),$o.width=e.width,$o.height=e.height;let r=$o.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=$o}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Gc("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=fr(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(fr(t[i]/255)*255):t[i]=fr(t[i]);return{data:t,width:e.width,height:e.height}}else return De("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},V1=0,ua=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:V1++}),this.uuid=xa(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Tv(r[o].image)):s.push(Tv(r[o]))}else s=Tv(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Tv(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Af.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(De("Texture: Unable to serialize Texture."),{})}var H1=0,Cv=new O,yr=(()=>{class n extends ki{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Li,s=Li,o=hn,a=is,c=ai,l=Nn,u=n.DEFAULT_ANISOTROPY,d=vr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:H1++}),this.uuid=xa(),this.name="",this.source=new ua(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new ot(0,0),this.repeat=new ot(1,1),this.center=new ot(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Fe,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(Cv).x}get height(){return this.source.getSize(Cv).y}get depth(){return this.source.getSize(Cv).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){De(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){De(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==qv)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Tf:t.x=t.x-Math.floor(t.x);break;case Li:t.x=t.x<0?0:1;break;case Cf:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Tf:t.y=t.y-Math.floor(t.y);break;case Li:t.y=t.y<0?0:1;break;case Cf:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=qv,n.DEFAULT_ANISOTROPY=1,n})(),At=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],g=c[9],y=c[2],m=c[6],p=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-y)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+y)<.1&&Math.abs(g+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let M=(l+1)/2,E=(h+1)/2,D=(p+1)/2,w=(u+f)/4,I=(d+y)/4,_=(g+m)/4;return M>E&&M>D?M<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(M),r=w/i,s=I/i):E>D?E<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(E),i=w/r,s=_/r):D<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(D),i=I/s,r=_/s),this.set(i,r,s,t),this}let b=Math.sqrt((m-g)*(m-g)+(d-y)*(d-y)+(f-u)*(f-u));return Math.abs(b)<.001&&(b=1),this.x=(m-g)/b,this.y=(d-y)/b,this.z=(f-u)/b,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=et(this.x,e.x,t.x),this.y=et(this.y,e.y,t.y),this.z=et(this.z,e.z,t.z),this.w=et(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=et(this.x,e,t),this.y=et(this.y,e,t),this.z=et(this.z,e,t),this.w=et(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(et(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},Rf=class extends ki{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:hn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new At(0,0,e,t),this.scissorTest=!1,this.viewport=new At(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new yr(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:hn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new ua(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this.multiview=e.multiview,this}dispose(){this.dispatchEvent({type:"dispose"})}},zn=class extends Rf{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},jc=class extends yr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=sn,this.minFilter=sn,this.wrapR=Li,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Nf=class extends yr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=sn,this.minFilter=sn,this.wrapR=Li,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var It=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,d,f,h,g,y,m){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,y,m)}set(e,t,i,r,s,o,a,c,l,u,d,f,h,g,y,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=g,p[11]=y,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/Xo.setFromMatrixColumn(e,0).length(),s=1/Xo.setFromMatrixColumn(e,1).length(),o=1/Xo.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let f=o*u,h=o*d,g=a*u,y=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+g*l,t[5]=f-y*l,t[9]=-a*c,t[2]=y-f*l,t[6]=g+h*l,t[10]=o*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,g=l*u,y=l*d;t[0]=f+y*a,t[4]=g*a-h,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=h*a-g,t[6]=y+f*a,t[10]=o*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,g=l*u,y=l*d;t[0]=f-y*a,t[4]=-o*d,t[8]=g+h*a,t[1]=h+g*a,t[5]=o*u,t[9]=y-f*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let f=o*u,h=o*d,g=a*u,y=a*d;t[0]=c*u,t[4]=g*l-h,t[8]=f*l+y,t[1]=c*d,t[5]=y*l+f,t[9]=h*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let f=o*c,h=o*l,g=a*c,y=a*l;t[0]=c*u,t[4]=y-f*d,t[8]=g*d+h,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+g,t[10]=f-y*d}else if(e.order==="XZY"){let f=o*c,h=o*l,g=a*c,y=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+y,t[5]=o*u,t[9]=h*d-g,t[2]=g*d-h,t[6]=a*u,t[10]=y*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(z1,e,G1)}lookAt(e,t,i){let r=this.elements;return Bn.subVectors(e,t),Bn.lengthSq()===0&&(Bn.z=1),Bn.normalize(),$r.crossVectors(i,Bn),$r.lengthSq()===0&&(Math.abs(i.z)===1?Bn.x+=1e-4:Bn.z+=1e-4,Bn.normalize(),$r.crossVectors(i,Bn)),$r.normalize(),jd.crossVectors(Bn,$r),r[0]=$r.x,r[4]=jd.x,r[8]=Bn.x,r[1]=$r.y,r[5]=jd.y,r[9]=Bn.y,r[2]=$r.z,r[6]=jd.z,r[10]=Bn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],g=i[2],y=i[6],m=i[10],p=i[14],b=i[3],M=i[7],E=i[11],D=i[15],w=r[0],I=r[4],_=r[8],T=r[12],L=r[1],C=r[5],U=r[9],j=r[13],q=r[2],P=r[6],z=r[10],k=r[14],Q=r[3],ee=r[7],ue=r[11],xe=r[15];return s[0]=o*w+a*L+c*q+l*Q,s[4]=o*I+a*C+c*P+l*ee,s[8]=o*_+a*U+c*z+l*ue,s[12]=o*T+a*j+c*k+l*xe,s[1]=u*w+d*L+f*q+h*Q,s[5]=u*I+d*C+f*P+h*ee,s[9]=u*_+d*U+f*z+h*ue,s[13]=u*T+d*j+f*k+h*xe,s[2]=g*w+y*L+m*q+p*Q,s[6]=g*I+y*C+m*P+p*ee,s[10]=g*_+y*U+m*z+p*ue,s[14]=g*T+y*j+m*k+p*xe,s[3]=b*w+M*L+E*q+D*Q,s[7]=b*I+M*C+E*P+D*ee,s[11]=b*_+M*U+E*z+D*ue,s[15]=b*T+M*j+E*k+D*xe,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],g=e[3],y=e[7],m=e[11],p=e[15],b=c*h-l*f,M=a*h-l*d,E=a*f-c*d,D=o*h-l*u,w=o*f-c*u,I=o*d-a*u;return t*(y*b-m*M+p*E)-i*(g*b-m*D+p*w)+r*(g*M-y*D+p*I)-s*(g*E-y*w+m*I)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],g=e[12],y=e[13],m=e[14],p=e[15],b=t*a-i*o,M=t*c-r*o,E=t*l-s*o,D=i*c-r*a,w=i*l-s*a,I=r*l-s*c,_=u*y-d*g,T=u*m-f*g,L=u*p-h*g,C=d*m-f*y,U=d*p-h*y,j=f*p-h*m,q=b*j-M*U+E*C+D*L-w*T+I*_;if(q===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let P=1/q;return e[0]=(a*j-c*U+l*C)*P,e[1]=(r*U-i*j-s*C)*P,e[2]=(y*I-m*w+p*D)*P,e[3]=(f*w-d*I-h*D)*P,e[4]=(c*L-o*j-l*T)*P,e[5]=(t*j-r*L+s*T)*P,e[6]=(m*E-g*I-p*M)*P,e[7]=(u*I-f*E+h*M)*P,e[8]=(o*U-a*L+l*_)*P,e[9]=(i*L-t*U-s*_)*P,e[10]=(g*w-y*E+p*b)*P,e[11]=(d*E-u*w-h*b)*P,e[12]=(a*T-o*C-c*_)*P,e[13]=(t*C-i*T+r*_)*P,e[14]=(y*M-g*D-m*b)*P,e[15]=(u*D-d*M+f*b)*P,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,f=s*l,h=s*u,g=s*d,y=o*u,m=o*d,p=a*d,b=c*l,M=c*u,E=c*d,D=i.x,w=i.y,I=i.z;return r[0]=(1-(y+p))*D,r[1]=(h+E)*D,r[2]=(g-M)*D,r[3]=0,r[4]=(h-E)*w,r[5]=(1-(f+p))*w,r[6]=(m+b)*w,r[7]=0,r[8]=(g+M)*I,r[9]=(m-b)*I,r[10]=(1-(f+y))*I,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=Xo.set(r[0],r[1],r[2]).length(),a=Xo.set(r[4],r[5],r[6]).length(),c=Xo.set(r[8],r[9],r[10]).length();s<0&&(o=-o),vi.copy(this);let l=1/o,u=1/a,d=1/c;return vi.elements[0]*=l,vi.elements[1]*=l,vi.elements[2]*=l,vi.elements[4]*=u,vi.elements[5]*=u,vi.elements[6]*=u,vi.elements[8]*=d,vi.elements[9]*=d,vi.elements[10]*=d,t.setFromRotationMatrix(vi),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=xi,c=!1){let l=this.elements,u=2*s/(t-e),d=2*s/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),g,y;if(c)g=s/(o-s),y=o*s/(o-s);else if(a===xi)g=-(o+s)/(o-s),y=-2*o*s/(o-s);else if(a===aa)g=-o/(o-s),y=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=y,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=xi,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),g,y;if(c)g=1/(o-s),y=o/(o-s);else if(a===xi)g=-2/(o-s),y=-(o+s)/(o-s);else if(a===aa)g=-1/(o-s),y=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=g,l[14]=y,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Xo=new O,vi=new It,z1=new O(0,0,0),G1=new O(1,1,1),$r=new O,jd=new O,Bn=new O,iM=new It,rM=new Ui,da=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],f=s[2],h=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(et(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-et(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,o),this._z=0);break;case"ZXY":this._x=Math.asin(et(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-et(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(et(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-et(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:De("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return iM.makeRotationFromQuaternion(t),this.setFromRotationMatrix(iM,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return rM.setFromEuler(this),this.setFromQuaternion(rM,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Wc=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},j1=0,sM=new O,qo=new Ui,or=new It,Wd=new O,Oc=new O,W1=new O,$1=new Ui,oM=new O(1,0,0),aM=new O(0,1,0),cM=new O(0,0,1),lM={type:"added"},X1={type:"removed"},Yo={type:"childadded",child:null},Dv={type:"childremoved",child:null},si=(()=>{class n extends ki{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:j1++}),this.uuid=xa(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new O,i=new da,r=new Ui,s=new O(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new It},normalMatrix:{value:new Fe}}),this.matrix=new It,this.matrixWorld=new It,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Wc,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return qo.setFromAxisAngle(t,i),this.quaternion.multiply(qo),this}rotateOnWorldAxis(t,i){return qo.setFromAxisAngle(t,i),this.quaternion.premultiply(qo),this}rotateX(t){return this.rotateOnAxis(oM,t)}rotateY(t){return this.rotateOnAxis(aM,t)}rotateZ(t){return this.rotateOnAxis(cM,t)}translateOnAxis(t,i){return sM.copy(t).applyQuaternion(this.quaternion),this.position.add(sM.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(oM,t)}translateY(t){return this.translateOnAxis(aM,t)}translateZ(t){return this.translateOnAxis(cM,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(or.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Wd.copy(t):Wd.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Oc.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?or.lookAt(Oc,Wd,this.up):or.lookAt(Wd,Oc,this.up),this.quaternion.setFromRotationMatrix(or),s&&(or.extractRotation(s.matrixWorld),qo.setFromRotationMatrix(or),this.quaternion.premultiply(qo.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Re("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(lM),Yo.child=t,this.dispatchEvent(Yo),Yo.child=null):Re("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(X1),Dv.child=t,this.dispatchEvent(Dv),Dv.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),or.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),or.multiply(t.parent.matrixWorld)),t.applyMatrix4(or),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(lM),Yo.child=t,this.dispatchEvent(Yo),Yo.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Oc,t,W1),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Oc,$1,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>st(he({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>he({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];o(t.shapes,f)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),g=a(t.animations),y=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),g.length>0&&(r.animations=g),y.length>0&&(r.nodes=y)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new O(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),ri=class extends si{constructor(){super(),this.isGroup=!0,this.type="Group"}},q1={type:"move"},fa=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new ri,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new ri,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new O,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new O),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new ri,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new O,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new O,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let y of e.hand.values()){let m=t.getJointPose(y,i),p=this._getHandJoint(l,y);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,g=.005;l.inputState.pinching&&f>h+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(q1)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new ri;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},sE={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Xr={h:0,s:0,l:0},$d={h:0,s:0,l:0};function Iv(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var qe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Cn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,Qe.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=Qe.workingColorSpace){return this.r=e,this.g=t,this.b=i,Qe.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=Qe.workingColorSpace){if(e=wy(e,1),t=et(t,0,1),i=et(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Iv(o,s,e+1/3),this.g=Iv(o,s,e),this.b=Iv(o,s,e-1/3)}return Qe.colorSpaceToWorking(this,r),this}setStyle(e,t=Cn){function i(s){s!==void 0&&parseFloat(s)<1&&De("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:De("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);De("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Cn){let i=sE[e.toLowerCase()];return i!==void 0?this.setHex(i,t):De("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=fr(e.r),this.g=fr(e.g),this.b=fr(e.b),this}copyLinearToSRGB(e){return this.r=oa(e.r),this.g=oa(e.g),this.b=oa(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Cn){return Qe.workingToColorSpace(bn.copy(this),e),Math.round(et(bn.r*255,0,255))*65536+Math.round(et(bn.g*255,0,255))*256+Math.round(et(bn.b*255,0,255))}getHexString(e=Cn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=Qe.workingColorSpace){Qe.workingToColorSpace(bn.copy(this),t);let i=bn.r,r=bn.g,s=bn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=Qe.workingColorSpace){return Qe.workingToColorSpace(bn.copy(this),t),e.r=bn.r,e.g=bn.g,e.b=bn.b,e}getStyle(e=Cn){Qe.workingToColorSpace(bn.copy(this),e);let t=bn.r,i=bn.g,r=bn.b;return e!==Cn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Xr),this.setHSL(Xr.h+e,Xr.s+t,Xr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Xr),e.getHSL($d);let i=Bc(Xr.h,$d.h,t),r=Bc(Xr.s,$d.s,t),s=Bc(Xr.l,$d.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},bn=new qe;qe.NAMES=sE;var $c=class n{constructor(e,t=25e-5){this.isFogExp2=!0,this.name="",this.color=new qe(e),this.density=t}clone(){return new n(this.color,this.density)}toJSON(){return{type:"FogExp2",name:this.name,color:this.color.getHex(),density:this.density}}};var Xc=class extends si{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new da,this.environmentIntensity=1,this.environmentRotation=new da,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},yi=new O,ar=new O,Av=new O,cr=new O,Zo=new O,Ko=new O,uM=new O,Rv=new O,Nv=new O,Pv=new O,Ov=new At,Lv=new At,Fv=new At,dr=class n{constructor(e=new O,t=new O,i=new O){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),yi.subVectors(e,t),r.cross(yi);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){yi.subVectors(r,t),ar.subVectors(i,t),Av.subVectors(e,t);let o=yi.dot(yi),a=yi.dot(ar),c=yi.dot(Av),l=ar.dot(ar),u=ar.dot(Av),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,g=(o*u-a*c)*f;return s.set(1-h-g,g,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,cr)===null?!1:cr.x>=0&&cr.y>=0&&cr.x+cr.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,cr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,cr.x),c.addScaledVector(o,cr.y),c.addScaledVector(a,cr.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return Ov.setScalar(0),Lv.setScalar(0),Fv.setScalar(0),Ov.fromBufferAttribute(e,t),Lv.fromBufferAttribute(e,i),Fv.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Ov,s.x),o.addScaledVector(Lv,s.y),o.addScaledVector(Fv,s.z),o}static isFrontFacing(e,t,i,r){return yi.subVectors(i,t),ar.subVectors(e,t),yi.cross(ar).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return yi.subVectors(this.c,this.b),ar.subVectors(this.a,this.b),yi.cross(ar).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;Zo.subVectors(r,i),Ko.subVectors(s,i),Rv.subVectors(e,i);let c=Zo.dot(Rv),l=Ko.dot(Rv);if(c<=0&&l<=0)return t.copy(i);Nv.subVectors(e,r);let u=Zo.dot(Nv),d=Ko.dot(Nv);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(Zo,o);Pv.subVectors(e,s);let h=Zo.dot(Pv),g=Ko.dot(Pv);if(g>=0&&h<=g)return t.copy(s);let y=h*l-c*g;if(y<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(Ko,a);let m=u*g-h*d;if(m<=0&&d-u>=0&&h-g>=0)return uM.subVectors(s,r),a=(d-u)/(d-u+(h-g)),t.copy(r).addScaledVector(uM,a);let p=1/(m+y+f);return o=y*p,a=f*p,t.copy(i).addScaledVector(Zo,o).addScaledVector(Ko,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Bi=class{constructor(e=new O(1/0,1/0,1/0),t=new O(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(_i.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(_i.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=_i.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,_i):_i.fromBufferAttribute(s,o),_i.applyMatrix4(e.matrixWorld),this.expandByPoint(_i);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),Xd.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),Xd.copy(i.boundingBox)),Xd.applyMatrix4(e.matrixWorld),this.union(Xd)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,_i),_i.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Lc),qd.subVectors(this.max,Lc),Jo.subVectors(e.a,Lc),Qo.subVectors(e.b,Lc),ea.subVectors(e.c,Lc),qr.subVectors(Qo,Jo),Yr.subVectors(ea,Qo),Gs.subVectors(Jo,ea);let t=[0,-qr.z,qr.y,0,-Yr.z,Yr.y,0,-Gs.z,Gs.y,qr.z,0,-qr.x,Yr.z,0,-Yr.x,Gs.z,0,-Gs.x,-qr.y,qr.x,0,-Yr.y,Yr.x,0,-Gs.y,Gs.x,0];return!kv(t,Jo,Qo,ea,qd)||(t=[1,0,0,0,1,0,0,0,1],!kv(t,Jo,Qo,ea,qd))?!1:(Yd.crossVectors(qr,Yr),t=[Yd.x,Yd.y,Yd.z],kv(t,Jo,Qo,ea,qd))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,_i).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(_i).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(lr[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),lr[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),lr[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),lr[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),lr[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),lr[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),lr[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),lr[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(lr),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},lr=[new O,new O,new O,new O,new O,new O,new O,new O],_i=new O,Xd=new Bi,Jo=new O,Qo=new O,ea=new O,qr=new O,Yr=new O,Gs=new O,Lc=new O,qd=new O,Yd=new O,js=new O;function kv(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){js.fromArray(n,s);let a=r.x*Math.abs(js.x)+r.y*Math.abs(js.y)+r.z*Math.abs(js.z),c=e.dot(js),l=t.dot(js),u=i.dot(js);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Gt=new O,Zd=new ot,Y1=0,Hn=class extends ki{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:Y1++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=ey,this.updateRanges=[],this.gpuType=Mi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Zd.fromBufferAttribute(this,t),Zd.applyMatrix3(e),this.setXY(t,Zd.x,Zd.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyMatrix3(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyMatrix4(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.applyNormalMatrix(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Gt.fromBufferAttribute(this,t),Gt.transformDirection(e),this.setXYZ(t,Gt.x,Gt.y,Gt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=ra(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=Tn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=ra(t,this.array)),t}setX(e,t){return this.normalized&&(t=Tn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=ra(t,this.array)),t}setY(e,t){return this.normalized&&(t=Tn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=ra(t,this.array)),t}setZ(e,t){return this.normalized&&(t=Tn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=ra(t,this.array)),t}setW(e,t){return this.normalized&&(t=Tn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=Tn(t,this.array),i=Tn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=Tn(t,this.array),i=Tn(i,this.array),r=Tn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=Tn(t,this.array),i=Tn(i,this.array),r=Tn(r,this.array),s=Tn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==ey&&(e.usage=this.usage),e}dispose(){this.dispatchEvent({type:"dispose"})}};var qc=class extends Hn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Yc=class extends Hn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Zt=class extends Hn{constructor(e,t,i){super(new Float32Array(e),t,i)}},Z1=new Bi,Fc=new O,Uv=new O,Zs=class{constructor(e=new O,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):Z1.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Fc.subVectors(e,this.center);let t=Fc.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Fc,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Uv.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Fc.copy(e.center).add(Uv)),this.expandByPoint(Fc.copy(e.center).sub(Uv))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},K1=0,ii=new It,Bv=new si,ta=new O,Vn=new Bi,kc=new Bi,rn=new O,Rn=class n extends ki{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:K1++}),this.uuid=xa(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(b1(e)?Yc:qc)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Fe().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return ii.makeRotationFromQuaternion(e),this.applyMatrix4(ii),this}rotateX(e){return ii.makeRotationX(e),this.applyMatrix4(ii),this}rotateY(e){return ii.makeRotationY(e),this.applyMatrix4(ii),this}rotateZ(e){return ii.makeRotationZ(e),this.applyMatrix4(ii),this}translate(e,t,i){return ii.makeTranslation(e,t,i),this.applyMatrix4(ii),this}scale(e,t,i){return ii.makeScale(e,t,i),this.applyMatrix4(ii),this}lookAt(e){return Bv.lookAt(e),Bv.updateMatrix(),this.applyMatrix4(Bv.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(ta).negate(),this.translate(ta.x,ta.y,ta.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new Zt(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&De("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Bi);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new O(-1/0,-1/0,-1/0),new O(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];Vn.setFromBufferAttribute(s),this.morphTargetsRelative?(rn.addVectors(this.boundingBox.min,Vn.min),this.boundingBox.expandByPoint(rn),rn.addVectors(this.boundingBox.max,Vn.max),this.boundingBox.expandByPoint(rn)):(this.boundingBox.expandByPoint(Vn.min),this.boundingBox.expandByPoint(Vn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Re('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Zs);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new O,1/0);return}if(e){let i=this.boundingSphere.center;if(Vn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];kc.setFromBufferAttribute(a),this.morphTargetsRelative?(rn.addVectors(Vn.min,kc.min),Vn.expandByPoint(rn),rn.addVectors(Vn.max,kc.max),Vn.expandByPoint(rn)):(Vn.expandByPoint(kc.min),Vn.expandByPoint(kc.max))}Vn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)rn.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(rn));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)rn.fromBufferAttribute(a,l),c&&(ta.fromBufferAttribute(e,l),rn.add(ta)),r=Math.max(r,i.distanceToSquared(rn))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Re('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Re("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new Hn(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let _=0;_<i.count;_++)a[_]=new O,c[_]=new O;let l=new O,u=new O,d=new O,f=new ot,h=new ot,g=new ot,y=new O,m=new O;function p(_,T,L){l.fromBufferAttribute(i,_),u.fromBufferAttribute(i,T),d.fromBufferAttribute(i,L),f.fromBufferAttribute(s,_),h.fromBufferAttribute(s,T),g.fromBufferAttribute(s,L),u.sub(l),d.sub(l),h.sub(f),g.sub(f);let C=1/(h.x*g.y-g.x*h.y);isFinite(C)&&(y.copy(u).multiplyScalar(g.y).addScaledVector(d,-h.y).multiplyScalar(C),m.copy(d).multiplyScalar(h.x).addScaledVector(u,-g.x).multiplyScalar(C),a[_].add(y),a[T].add(y),a[L].add(y),c[_].add(m),c[T].add(m),c[L].add(m))}let b=this.groups;b.length===0&&(b=[{start:0,count:e.count}]);for(let _=0,T=b.length;_<T;++_){let L=b[_],C=L.start,U=L.count;for(let j=C,q=C+U;j<q;j+=3)p(e.getX(j+0),e.getX(j+1),e.getX(j+2))}let M=new O,E=new O,D=new O,w=new O;function I(_){D.fromBufferAttribute(r,_),w.copy(D);let T=a[_];M.copy(T),M.sub(D.multiplyScalar(D.dot(T))).normalize(),E.crossVectors(w,T);let C=E.dot(c[_])<0?-1:1;o.setXYZW(_,M.x,M.y,M.z,C)}for(let _=0,T=b.length;_<T;++_){let L=b[_],C=L.start,U=L.count;for(let j=C,q=C+U;j<q;j+=3)I(e.getX(j+0)),I(e.getX(j+1)),I(e.getX(j+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new Hn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new O,s=new O,o=new O,a=new O,c=new O,l=new O,u=new O,d=new O;if(e)for(let f=0,h=e.count;f<h;f+=3){let g=e.getX(f+0),y=e.getX(f+1),m=e.getX(f+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,y),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,y),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(y,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),s.fromBufferAttribute(t,f+1),o.fromBufferAttribute(t,f+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)rn.fromBufferAttribute(e,t),rn.normalize(),e.setXYZ(t,rn.x,rn.y,rn.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,g=0;for(let y=0,m=c.length;y<m;y++){a.isInterleavedBufferAttribute?h=c[y]*a.data.stride+a.offset:h=c[y]*u;for(let p=0;p<u;p++)f[g++]=l[h++]}return new Hn(f,u,d)}if(this.index===null)return De("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var J1=0,pr=class extends ki{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:J1++}),this.uuid=xa(),this.name="",this.type="Material",this.blending=qs,this.side=hr,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=vf,this.blendDst=yf,this.blendEquation=Jr,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new qe(0,0,0),this.blendAlpha=0,this.depthFunc=Ys,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Qv,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Xs,this.stencilZFail=Xs,this.stencilZPass=Xs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){De(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){De(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==qs&&(i.blending=this.blending),this.side!==hr&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==vf&&(i.blendSrc=this.blendSrc),this.blendDst!==yf&&(i.blendDst=this.blendDst),this.blendEquation!==Jr&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Ys&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Qv&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Xs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Xs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Xs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var ur=new O,Vv=new O,Kd=new O,Zr=new O,Hv=new O,Jd=new O,zv=new O,Zc=class{constructor(e=new O,t=new O(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,ur)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=ur.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(ur.copy(this.origin).addScaledVector(this.direction,t),ur.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Vv.copy(e).add(t).multiplyScalar(.5),Kd.copy(t).sub(e).normalize(),Zr.copy(this.origin).sub(Vv);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Kd),a=Zr.dot(this.direction),c=-Zr.dot(Kd),l=Zr.lengthSq(),u=Math.abs(1-o*o),d,f,h,g;if(u>0)if(d=o*c-a,f=o*a-c,g=s*u,d>=0)if(f>=-g)if(f<=g){let y=1/u;d*=y,f*=y,h=d*(d+o*f+2*a)+f*(o*d+f+2*c)+l}else f=s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f=-s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-g?(d=Math.max(0,-(-o*s+a)),f=d>0?-s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l):f<=g?(d=0,f=Math.min(Math.max(-s,-c),s),h=f*(f+2*c)+l):(d=Math.max(0,-(o*s+a)),f=d>0?s:Math.min(Math.max(-s,-c),s),h=-d*d+f*(f+2*c)+l);else f=o>0?-s:s,d=Math.max(0,-(o*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Vv).addScaledVector(Kd,f),h}intersectSphere(e,t){ur.subVectors(e.center,this.origin);let i=ur.dot(this.direction),r=ur.dot(ur)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(s=(e.min.y-f.y)*u,o=(e.max.y-f.y)*u):(s=(e.max.y-f.y)*u,o=(e.min.y-f.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,ur)!==null}intersectTriangle(e,t,i,r,s){Hv.subVectors(t,e),Jd.subVectors(i,e),zv.crossVectors(Hv,Jd);let o=this.direction.dot(zv),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;Zr.subVectors(this.origin,e);let c=a*this.direction.dot(Jd.crossVectors(Zr,Jd));if(c<0)return null;let l=a*this.direction.dot(Hv.cross(Zr));if(l<0||c+l>o)return null;let u=-a*Zr.dot(zv);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Ks=class extends pr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new qe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new da,this.combine=ly,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},dM=new It,Ws=new Zc,Qd=new Zs,fM=new O,ef=new O,tf=new O,nf=new O,Gv=new O,rf=new O,hM=new O,sf=new O,pt=class extends si{constructor(e=new Rn,t=new Ks){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){rf.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(Gv.fromBufferAttribute(d,e),o?rf.addScaledVector(Gv,u):rf.addScaledVector(Gv.sub(t),u))}t.add(rf)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Qd.copy(i.boundingSphere),Qd.applyMatrix4(s),Ws.copy(e.ray).recast(e.near),!(Qd.containsPoint(Ws.origin)===!1&&(Ws.intersectSphere(Qd,fM)===null||Ws.origin.distanceToSquared(fM)>(e.far-e.near)**2))&&(dM.copy(s).invert(),Ws.copy(e.ray).applyMatrix4(dM),!(i.boundingBox!==null&&Ws.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Ws)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,f=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,y=f.length;g<y;g++){let m=f[g],p=o[m.materialIndex],b=Math.max(m.start,h.start),M=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let E=b,D=M;E<D;E+=3){let w=a.getX(E),I=a.getX(E+1),_=a.getX(E+2);r=of(this,p,e,i,l,u,d,w,I,_),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),y=Math.min(a.count,h.start+h.count);for(let m=g,p=y;m<p;m+=3){let b=a.getX(m),M=a.getX(m+1),E=a.getX(m+2);r=of(this,o,e,i,l,u,d,b,M,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,y=f.length;g<y;g++){let m=f[g],p=o[m.materialIndex],b=Math.max(m.start,h.start),M=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let E=b,D=M;E<D;E+=3){let w=E,I=E+1,_=E+2;r=of(this,p,e,i,l,u,d,w,I,_),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,h.start),y=Math.min(c.count,h.start+h.count);for(let m=g,p=y;m<p;m+=3){let b=m,M=m+1,E=m+2;r=of(this,o,e,i,l,u,d,b,M,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function Q1(n,e,t,i,r,s,o,a){let c;if(e.side===Dn?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===hr,a),c===null)return null;sf.copy(a),sf.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(sf);return l<t.near||l>t.far?null:{distance:l,point:sf.clone(),object:n}}function of(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,ef),n.getVertexPosition(c,tf),n.getVertexPosition(l,nf);let u=Q1(n,e,t,i,ef,tf,nf,hM);if(u){let d=new O;dr.getBarycoord(hM,ef,tf,nf,d),r&&(u.uv=dr.getInterpolatedAttribute(r,a,c,l,d,new ot)),s&&(u.uv1=dr.getInterpolatedAttribute(s,a,c,l,d,new ot)),o&&(u.normal=dr.getInterpolatedAttribute(o,a,c,l,d,new O),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new O,materialIndex:0};dr.getNormal(ef,tf,nf,f.normal),u.face=f,u.barycoord=d}return u}var Pf=class extends yr{constructor(e=null,t=1,i=1,r,s,o,a,c,l=sn,u=sn,d,f){super(null,o,a,c,l,u,r,s,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var jv=new O,eN=new O,tN=new Fe,Oi=class{constructor(e=new O(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=jv.subVectors(i,t).cross(eN.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(jv),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||tN.getNormalMatrix(e),r=this.coplanarPoint(jv).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},$s=new Zs,nN=new ot(.5,.5),af=new O,ha=class{constructor(e=new Oi,t=new Oi,i=new Oi,r=new Oi,s=new Oi,o=new Oi){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=xi,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],d=s[5],f=s[6],h=s[7],g=s[8],y=s[9],m=s[10],p=s[11],b=s[12],M=s[13],E=s[14],D=s[15];if(r[0].setComponents(l-o,h-u,p-g,D-b).normalize(),r[1].setComponents(l+o,h+u,p+g,D+b).normalize(),r[2].setComponents(l+a,h+d,p+y,D+M).normalize(),r[3].setComponents(l-a,h-d,p-y,D-M).normalize(),i)r[4].setComponents(c,f,m,E).normalize(),r[5].setComponents(l-c,h-f,p-m,D-E).normalize();else if(r[4].setComponents(l-c,h-f,p-m,D-E).normalize(),t===xi)r[5].setComponents(l+c,h+f,p+m,D+E).normalize();else if(t===aa)r[5].setComponents(c,f,m,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),$s.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),$s.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere($s)}intersectsSprite(e){$s.center.set(0,0,0);let t=nN.distanceTo(e.center);return $s.radius=.7071067811865476+t,$s.applyMatrix4(e.matrixWorld),this.intersectsSphere($s)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(af.x=r.normal.x>0?e.max.x:e.min.x,af.y=r.normal.y>0?e.max.y:e.min.y,af.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(af)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var Vi=class extends pr{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new qe(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},Of=new O,Lf=new O,pM=new It,Uc=new Zc,cf=new Zs,Wv=new O,mM=new O,Ff=class extends si{constructor(e=new Rn,t=new Vi){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)Of.fromBufferAttribute(t,r-1),Lf.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=Of.distanceTo(Lf);e.setAttribute("lineDistance",new Zt(i,1))}else De("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),cf.copy(i.boundingSphere),cf.applyMatrix4(r),cf.radius+=s,e.ray.intersectsSphere(cf)===!1)return;pM.copy(r).invert(),Uc.copy(e.ray).applyMatrix4(pM);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,o.start),g=Math.min(u.count,o.start+o.count);for(let y=h,m=g-1;y<m;y+=l){let p=u.getX(y),b=u.getX(y+1),M=lf(this,e,Uc,c,p,b,y);M&&t.push(M)}if(this.isLineLoop){let y=u.getX(g-1),m=u.getX(h),p=lf(this,e,Uc,c,y,m,g-1);p&&t.push(p)}}else{let h=Math.max(0,o.start),g=Math.min(f.count,o.start+o.count);for(let y=h,m=g-1;y<m;y+=l){let p=lf(this,e,Uc,c,y,y+1,y);p&&t.push(p)}if(this.isLineLoop){let y=lf(this,e,Uc,c,g-1,h,g-1);y&&t.push(y)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function lf(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(Of.fromBufferAttribute(a,r),Lf.fromBufferAttribute(a,s),t.distanceSqToSegment(Of,Lf,Wv,mM)>i)return;Wv.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(Wv);if(!(l<e.near||l>e.far))return{distance:l,point:mM.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var gM=new O,vM=new O,mr=class extends Ff{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)gM.fromBufferAttribute(t,r),vM.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+gM.distanceTo(vM);e.setAttribute("lineDistance",new Zt(i,1))}else De("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var Kc=class extends yr{constructor(e=[],t=ns,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var gr=class extends yr{constructor(e,t,i=Si,r,s,o,a=sn,c=sn,l,u=Fi,d=1){if(u!==Fi&&u!==rs)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new ua(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},kf=class extends gr{constructor(e,t=Si,i=ns,r,s,o=sn,a=sn,c,l=Fi){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Jc=class extends yr{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},kt=class n extends Rn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],f=0,h=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new Zt(l,3)),this.setAttribute("normal",new Zt(u,3)),this.setAttribute("uv",new Zt(d,2));function g(y,m,p,b,M,E,D,w,I,_,T){let L=E/I,C=D/_,U=E/2,j=D/2,q=w/2,P=I+1,z=_+1,k=0,Q=0,ee=new O;for(let ue=0;ue<z;ue++){let xe=ue*C-j;for(let Te=0;Te<P;Te++){let nt=Te*L-U;ee[y]=nt*b,ee[m]=xe*M,ee[p]=q,l.push(ee.x,ee.y,ee.z),ee[y]=0,ee[m]=0,ee[p]=w>0?1:-1,u.push(ee.x,ee.y,ee.z),d.push(Te/I),d.push(1-ue/_),k+=1}}for(let ue=0;ue<_;ue++)for(let xe=0;xe<I;xe++){let Te=f+xe+P*ue,nt=f+xe+P*(ue+1),mt=f+(xe+1)+P*(ue+1),Ge=f+(xe+1)+P*ue;c.push(Te,nt,Ge),c.push(nt,mt,Ge),Q+=6}a.addGroup(h,Q,T),h+=Q,f+=k}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var uf=new O,df=new O,$v=new O,ff=new dr,Js=class extends Rn{constructor(e=null,t=1){if(super(),this.type="EdgesGeometry",this.parameters={geometry:e,thresholdAngle:t},e!==null){let r=Math.pow(10,4),s=Math.cos(sa*t),o=e.getIndex(),a=e.getAttribute("position"),c=o?o.count:a.count,l=[0,0,0],u=["a","b","c"],d=new Array(3),f={},h=[];for(let g=0;g<c;g+=3){o?(l[0]=o.getX(g),l[1]=o.getX(g+1),l[2]=o.getX(g+2)):(l[0]=g,l[1]=g+1,l[2]=g+2);let{a:y,b:m,c:p}=ff;if(y.fromBufferAttribute(a,l[0]),m.fromBufferAttribute(a,l[1]),p.fromBufferAttribute(a,l[2]),ff.getNormal($v),d[0]=`${Math.round(y.x*r)},${Math.round(y.y*r)},${Math.round(y.z*r)}`,d[1]=`${Math.round(m.x*r)},${Math.round(m.y*r)},${Math.round(m.z*r)}`,d[2]=`${Math.round(p.x*r)},${Math.round(p.y*r)},${Math.round(p.z*r)}`,!(d[0]===d[1]||d[1]===d[2]||d[2]===d[0]))for(let b=0;b<3;b++){let M=(b+1)%3,E=d[b],D=d[M],w=ff[u[b]],I=ff[u[M]],_=`${E}_${D}`,T=`${D}_${E}`;T in f&&f[T]?($v.dot(f[T].normal)<=s&&(h.push(w.x,w.y,w.z),h.push(I.x,I.y,I.z)),f[T]=null):_ in f||(f[_]={index0:l[b],index1:l[M],normal:$v.clone()})}}for(let g in f)if(f[g]){let{index0:y,index1:m}=f[g];uf.fromBufferAttribute(a,y),df.fromBufferAttribute(a,m),h.push(uf.x,uf.y,uf.z),h.push(df.x,df.y,df.z)}this.setAttribute("position",new Zt(h,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};var Qc=class n extends Rn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],g=[],y=[],m=[];for(let p=0;p<u;p++){let b=p*f-o;for(let M=0;M<l;M++){let E=M*d-s;g.push(E,-b,0),y.push(0,0,1),m.push(M/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let b=0;b<a;b++){let M=b+l*p,E=b+l*(p+1),D=b+1+l*(p+1),w=b+1+l*p;h.push(M,E,w),h.push(E,D,w)}this.setIndex(h),this.setAttribute("position",new Zt(g,3)),this.setAttribute("normal",new Zt(y,3)),this.setAttribute("uv",new Zt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var pa=class n extends Rn{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new O,f=new O,h=[],g=[],y=[],m=[];for(let p=0;p<=i;p++){let b=[],M=p/i,E=0;p===0&&o===0?E=.5/t:p===i&&c===Math.PI&&(E=-.5/t);for(let D=0;D<=t;D++){let w=D/t;d.x=-e*Math.cos(r+w*s)*Math.sin(o+M*a),d.y=e*Math.cos(o+M*a),d.z=e*Math.sin(r+w*s)*Math.sin(o+M*a),g.push(d.x,d.y,d.z),f.copy(d).normalize(),y.push(f.x,f.y,f.z),m.push(w+E,1-M),b.push(l++)}u.push(b)}for(let p=0;p<i;p++)for(let b=0;b<t;b++){let M=u[p][b+1],E=u[p][b],D=u[p+1][b],w=u[p+1][b+1];(p!==0||o>0)&&h.push(M,E,w),(p!==i-1||c<Math.PI)&&h.push(E,D,w)}this.setIndex(h),this.setAttribute("position",new Zt(g,3)),this.setAttribute("normal",new Zt(y,3)),this.setAttribute("uv",new Zt(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};var el=class extends Rn{constructor(e=null){if(super(),this.type="WireframeGeometry",this.parameters={geometry:e},e!==null){let t=[],i=new Set,r=new O,s=new O;if(e.index!==null){let o=e.attributes.position,a=e.index,c=e.groups;c.length===0&&(c=[{start:0,count:a.count,materialIndex:0}]);for(let l=0,u=c.length;l<u;++l){let d=c[l],f=d.start,h=d.count;for(let g=f,y=f+h;g<y;g+=3)for(let m=0;m<3;m++){let p=a.getX(g+m),b=a.getX(g+(m+1)%3);r.fromBufferAttribute(o,p),s.fromBufferAttribute(o,b),yM(r,s,i)===!0&&(t.push(r.x,r.y,r.z),t.push(s.x,s.y,s.z))}}}else{let o=e.attributes.position;for(let a=0,c=o.count/3;a<c;a++)for(let l=0;l<3;l++){let u=3*a+l,d=3*a+(l+1)%3;r.fromBufferAttribute(o,u),s.fromBufferAttribute(o,d),yM(r,s,i)===!0&&(t.push(r.x,r.y,r.z),t.push(s.x,s.y,s.z))}}this.setAttribute("position",new Zt(t,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};function yM(n,e,t){let i=`${n.x},${n.y},${n.z}-${e.x},${e.y},${e.z}`,r=`${e.x},${e.y},${e.z}-${n.x},${n.y},${n.z}`;return t.has(i)===!0||t.has(r)===!0?!1:(t.add(i),t.add(r),!0)}function eo(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(_M(r))r.isRenderTargetTexture?(De("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(_M(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function Sn(n){let e={};for(let t=0;t<n.length;t++){let i=eo(n[t]);for(let r in i)e[r]=i[r]}return e}function _M(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function iN(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function Ty(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:Qe.workingColorSpace}var oE={clone:eo,merge:Sn},rN=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,sN=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,Gn=class extends pr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=rN,this.fragmentShader=sN,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=eo(e.uniforms),this.uniformsGroups=iN(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Uf=class extends Gn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},oi=class extends pr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new qe(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new qe(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Uh,this.normalScale=new ot(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new da,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};var Bf=class extends pr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=XM,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},Vf=class extends pr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function hf(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var Qr=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Hf=class extends Qr{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Zv,endingEnd:Zv}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Kv:s=e,a=2*t-i;break;case Jv:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Kv:o=e,c=2*i-t;break;case Jv:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,g=(i-t)/(r-t),y=g*g,m=y*g,p=-f*m+2*f*y-f*g,b=(1+f)*m+(-1.5-2*f)*y+(-.5+f)*g+1,M=(-1-h)*m+(1.5+h)*y+.5*g,E=h*m-h*y;for(let D=0;D!==a;++D)s[D]=p*o[u+D]+b*o[l+D]+M*o[c+D]+E*o[d+D];return s}},zf=class extends Qr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)s[f]=o[l+f]*d+o[c+f]*u;return s}},Gf=class extends Qr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},jf=class extends Qr{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,d=u.inTangents,f=u.outTangents;if(!d||!f){let y=(i-t)/(r-t),m=1-y;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*y;return s}let h=a*2,g=e-1;for(let y=0;y!==a;++y){let m=o[l+y],p=o[c+y],b=g*h+y*2,M=f[b],E=f[b+1],D=e*h+y*2,w=d[D],I=d[D+1],_=(i-t)/(r-t),T,L,C,U,j;for(let q=0;q<8;q++){T=_*_,L=T*_,C=1-_,U=C*C,j=U*C;let z=j*t+3*U*_*M+3*C*T*w+L*r-i;if(Math.abs(z)<1e-10)break;let k=3*U*(M-t)+6*C*_*(w-M)+3*T*(r-w);if(Math.abs(k)<1e-10)break;_=_-z/k,_=Math.max(0,Math.min(1,_))}s[y]=j*m+3*U*_*E+3*C*T*I+L*p}return s}},jn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=hf(t,this.TimeBufferType),this.values=hf(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:hf(e.times,Array),values:hf(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Gf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new zf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Hf(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new jf(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case Vc:t=this.InterpolantFactoryMethodDiscrete;break;case Df:t=this.InterpolantFactoryMethodLinear;break;case gf:t=this.InterpolantFactoryMethodSmooth;break;case Yv:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return De("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Vc;case this.InterpolantFactoryMethodLinear:return Df;case this.InterpolantFactoryMethodSmooth:return gf;case this.InterpolantFactoryMethodBezier:return Yv}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Re("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Re("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Re("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Re("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&S1(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Re("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===gf,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let g=0;g!==i;++g){let y=t[d+g];if(y!==t[f+g]||y!==t[h+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,f=o*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};jn.prototype.ValueTypeName="";jn.prototype.TimeBufferType=Float32Array;jn.prototype.ValueBufferType=Float32Array;jn.prototype.DefaultInterpolation=Df;var es=class extends jn{constructor(e,t,i){super(e,t,i)}};es.prototype.ValueTypeName="bool";es.prototype.ValueBufferType=Array;es.prototype.DefaultInterpolation=Vc;es.prototype.InterpolantFactoryMethodLinear=void 0;es.prototype.InterpolantFactoryMethodSmooth=void 0;var Wf=class extends jn{constructor(e,t,i,r){super(e,t,i,r)}};Wf.prototype.ValueTypeName="color";var $f=class extends jn{constructor(e,t,i,r){super(e,t,i,r)}};$f.prototype.ValueTypeName="number";var Xf=class extends Qr{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Ui.slerpFlat(s,0,o,l-a,o,l,c);return s}},tl=class extends jn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new Xf(this.times,this.values,this.getValueSize(),e)}};tl.prototype.ValueTypeName="quaternion";tl.prototype.InterpolantFactoryMethodSmooth=void 0;var ts=class extends jn{constructor(e,t,i){super(e,t,i)}};ts.prototype.ValueTypeName="string";ts.prototype.ValueBufferType=Array;ts.prototype.DefaultInterpolation=Vc;ts.prototype.InterpolantFactoryMethodLinear=void 0;ts.prototype.InterpolantFactoryMethodSmooth=void 0;var qf=class extends jn{constructor(e,t,i,r){super(e,t,i,r)}};qf.prototype.ValueTypeName="vector";var ma=class extends si{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new qe(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}},nl=class extends ma{constructor(e,t,i){super(e,i),this.isHemisphereLight=!0,this.type="HemisphereLight",this.position.copy(si.DEFAULT_UP),this.updateMatrix(),this.groundColor=new qe(t)}copy(e,t){return super.copy(e,t),this.groundColor.copy(e.groundColor),this}toJSON(e){let t=super.toJSON(e);return t.object.groundColor=this.groundColor.getHex(),t}},Xv=new It,xM=new O,bM=new O,Yf=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.biasNode=null,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new ot(512,512),this.mapType=Nn,this.map=null,this.mapPass=null,this.matrix=new It,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new ha,this._frameExtents=new ot(1,1),this._viewportCount=1,this._viewports=[new At(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;xM.setFromMatrixPosition(e.matrixWorld),t.position.copy(xM),bM.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(bM),t.updateMatrixWorld(),Xv.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Xv,t.coordinateSystem,t.reversedDepth),t.coordinateSystem===aa||t.reversedDepth?i.set(.5,0,0,.5,0,.5,0,.5,0,0,1,0,0,0,0,1):i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Xv)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.autoUpdate=e.autoUpdate,this.needsUpdate=e.needsUpdate,this.normalBias=e.normalBias,this.blurSamples=e.blurSamples,this.mapSize.copy(e.mapSize),this.biasNode=e.biasNode,this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}},pf=new O,mf=new Ui,Pi=new O,il=class extends si{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new It,this.projectionMatrix=new It,this.projectionMatrixInverse=new It,this.coordinateSystem=xi,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(pf,mf,Pi),Pi.x===1&&Pi.y===1&&Pi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(pf,mf,Pi.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(pf,mf,Pi),Pi.x===1&&Pi.y===1&&Pi.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(pf,mf,Pi.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},Kr=new O,SM=new ot,MM=new ot,fn=class extends il{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=la*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(sa*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return la*2*Math.atan(Math.tan(sa*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Kr.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Kr.x,Kr.y).multiplyScalar(-e/Kr.z),Kr.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Kr.x,Kr.y).multiplyScalar(-e/Kr.z)}getViewSize(e,t){return this.getViewBounds(e,SM,MM),t.subVectors(MM,SM)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(sa*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var ty=class extends Yf{constructor(){super(new fn(90,1,.5,500)),this.isPointLightShadow=!0}},rl=class extends ma{constructor(e,t,i=0,r=2){super(e,t),this.isPointLight=!0,this.type="PointLight",this.distance=i,this.decay=r,this.shadow=new ty}get power(){return this.intensity*4*Math.PI}set power(e){this.intensity=e/(4*Math.PI)}dispose(){super.dispose(),this.shadow.dispose()}copy(e,t){return super.copy(e,t),this.distance=e.distance,this.decay=e.decay,this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.distance=this.distance,t.object.decay=this.decay,t.object.shadow=this.shadow.toJSON(),t}},ga=class extends il{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},ny=class extends Yf{constructor(){super(new ga(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},sl=class extends ma{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(si.DEFAULT_UP),this.updateMatrix(),this.target=new si,this.shadow=new ny}dispose(){super.dispose(),this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.shadow=this.shadow.toJSON(),t.object.target=this.target.uuid,t}};var na=-90,ia=1,Zf=class extends si{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new fn(na,ia,e,t);r.layers=this.layers,this.add(r);let s=new fn(na,ia,e,t);s.layers=this.layers,this.add(s);let o=new fn(na,ia,e,t);o.layers=this.layers,this.add(o);let a=new fn(na,ia,e,t);a.layers=this.layers,this.add(a);let c=new fn(na,ia,e,t);c.layers=this.layers,this.add(c);let l=new fn(na,ia,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===xi)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===aa)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let y=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=y,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},Kf=class extends fn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var Cy="\\[\\]\\.:\\/",oN=new RegExp("["+Cy+"]","g"),Dy="[^"+Cy+"]",aN="[^"+Cy.replace("\\.","")+"]",cN=/((?:WC+[\/:])*)/.source.replace("WC",Dy),lN=/(WCOD+)?/.source.replace("WCOD",aN),uN=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Dy),dN=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Dy),fN=new RegExp("^"+cN+lN+uN+dN+"$"),hN=["material","materials","bones","map"],iy=class{constructor(e,t,i){let r=i||Ft.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Ft=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(oN,"")}static parseTrackName(t){let i=fN.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);hN.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){De("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Re("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Re("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Re("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Re("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Re("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Re("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=iy,n})();Ft.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Ft.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Ft.prototype.GetterByBindingType=[Ft.prototype._getValue_direct,Ft.prototype._getValue_array,Ft.prototype._getValue_arrayElement,Ft.prototype._getValue_toArray];Ft.prototype.SetterByBindingTypeAndVersioning=[[Ft.prototype._setValue_direct,Ft.prototype._setValue_direct_setNeedsUpdate,Ft.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Ft.prototype._setValue_array,Ft.prototype._setValue_array_setNeedsUpdate,Ft.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Ft.prototype._setValue_arrayElement,Ft.prototype._setValue_arrayElement_setNeedsUpdate,Ft.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Ft.prototype._setValue_fromArray,Ft.prototype._setValue_fromArray_setNeedsUpdate,Ft.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var xX=new Float32Array(1);var ry=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};function Iy(n,e,t,i){let r=pN(i);switch(t){case xy:return n*e;case Sy:return n*e/r.components*r.byteLength;case sh:return n*e/r.components*r.byteLength;case ss:return n*e*2/r.components*r.byteLength;case oh:return n*e*2/r.components*r.byteLength;case by:return n*e*3/r.components*r.byteLength;case ai:return n*e*4/r.components*r.byteLength;case ah:return n*e*4/r.components*r.byteLength;case ul:case dl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case fl:case hl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case lh:case dh:return Math.max(n,16)*Math.max(e,8)/4;case ch:case uh:return Math.max(n,8)*Math.max(e,8)/2;case fh:case hh:case mh:case gh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case ph:case pl:case vh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case yh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case _h:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case xh:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case bh:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Sh:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Mh:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Eh:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case wh:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Th:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Ch:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Dh:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Ih:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Ah:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Rh:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Nh:case Ph:case Oh:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Lh:case Fh:return Math.ceil(n/4)*Math.ceil(e/4)*8;case ml:case kh:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function pN(n){switch(n){case Nn:case gy:return{byteLength:1,components:1};case ya:case vy:case Gi:return{byteLength:2,components:1};case ih:case rh:return{byteLength:2,components:4};case Si:case nh:case Mi:return{byteLength:4,components:1};case yy:case _y:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"184"}}));typeof window<"u"&&(window.__THREE__?De("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="184");function IE(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function gN(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,g)=>h.start-g.start);let f=0;for(let h=1;h<d.length;h++){let g=d[f],y=d[h];y.start<=g.start+g.count+1?g.count=Math.max(g.count,y.start+y.count-g.start):(++f,d[f]=y)}d.length=f+1;for(let h=0,g=d.length;h<g;h++){let y=d[h];n.bufferSubData(l,y.start*u.BYTES_PER_ELEMENT,u,y.start,y.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var vN=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,yN=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,_N=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,xN=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,bN=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,SN=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,MN=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,EN=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,wN=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,TN=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,CN=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,DN=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,IN=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,AN=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,RN=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,NN=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,PN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,ON=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,LN=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,FN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,kN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,UN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,BN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,VN=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,HN=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,zN=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,GN=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,jN=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,WN=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,$N=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,XN="gl_FragColor = linearToOutputTexel( gl_FragColor );",qN=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,YN=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,ZN=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,KN=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,JN=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,QN=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,eP=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,tP=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,nP=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,iP=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,rP=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,sP=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,oP=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,aP=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,cP=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,lP=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,uP=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,dP=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,fP=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,hP=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,pP=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,mP=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,gP=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#ifdef USE_LIGHT_PROBES_GRID
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = inverseTransformDirection( geometryNormal, viewMatrix );
		irradiance += getLightProbeGridIrradiance( probeWorldPos, probeWorldNormal );
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,vP=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,yP=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,_P=`#ifdef USE_LIGHT_PROBES_GRID
uniform highp sampler3D probesSH;
uniform vec3 probesMin;
uniform vec3 probesMax;
uniform vec3 probesResolution;
vec3 getLightProbeGridIrradiance( vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 resMinusOne = res - 1.0;
	vec3 probeSpacing = gridRange / resMinusOne;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.0, 1.0 );
	uvw = uvw * resMinusOne / res + 0.5 / res;
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,xP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,bP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,SP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,MP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,EP=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,wP=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,TP=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,CP=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,DP=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,IP=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,AP=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,RP=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,NP=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,PP=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,OP=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,LP=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,FP=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,kP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,UP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,BP=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,VP=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,HP=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,zP=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,GP=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,jP=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,WP=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,$P=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,XP=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,qP=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,YP=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,ZP=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,KP=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,JP=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,QP=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,eO=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,tO=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,nO=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,iO=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,rO=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,sO=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,oO=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,aO=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,cO=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,lO=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,uO=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,dO=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,fO=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,hO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,pO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,mO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,gO=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,vO=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,yO=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,_O=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,xO=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,bO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,SO=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,MO=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,EO=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,wO=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,TO=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,CO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,DO=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,IO=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,AO=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,RO=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,NO=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,PO=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,OO=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,LO=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,FO=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,kO=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,UO=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,BO=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,VO=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,HO=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,zO=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,GO=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,jO=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,WO=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,$O=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,XO=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,qO=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,YO=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,ZO=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,$e={alphahash_fragment:vN,alphahash_pars_fragment:yN,alphamap_fragment:_N,alphamap_pars_fragment:xN,alphatest_fragment:bN,alphatest_pars_fragment:SN,aomap_fragment:MN,aomap_pars_fragment:EN,batching_pars_vertex:wN,batching_vertex:TN,begin_vertex:CN,beginnormal_vertex:DN,bsdfs:IN,iridescence_fragment:AN,bumpmap_pars_fragment:RN,clipping_planes_fragment:NN,clipping_planes_pars_fragment:PN,clipping_planes_pars_vertex:ON,clipping_planes_vertex:LN,color_fragment:FN,color_pars_fragment:kN,color_pars_vertex:UN,color_vertex:BN,common:VN,cube_uv_reflection_fragment:HN,defaultnormal_vertex:zN,displacementmap_pars_vertex:GN,displacementmap_vertex:jN,emissivemap_fragment:WN,emissivemap_pars_fragment:$N,colorspace_fragment:XN,colorspace_pars_fragment:qN,envmap_fragment:YN,envmap_common_pars_fragment:ZN,envmap_pars_fragment:KN,envmap_pars_vertex:JN,envmap_physical_pars_fragment:lP,envmap_vertex:QN,fog_vertex:eP,fog_pars_vertex:tP,fog_fragment:nP,fog_pars_fragment:iP,gradientmap_pars_fragment:rP,lightmap_pars_fragment:sP,lights_lambert_fragment:oP,lights_lambert_pars_fragment:aP,lights_pars_begin:cP,lights_toon_fragment:uP,lights_toon_pars_fragment:dP,lights_phong_fragment:fP,lights_phong_pars_fragment:hP,lights_physical_fragment:pP,lights_physical_pars_fragment:mP,lights_fragment_begin:gP,lights_fragment_maps:vP,lights_fragment_end:yP,lightprobes_pars_fragment:_P,logdepthbuf_fragment:xP,logdepthbuf_pars_fragment:bP,logdepthbuf_pars_vertex:SP,logdepthbuf_vertex:MP,map_fragment:EP,map_pars_fragment:wP,map_particle_fragment:TP,map_particle_pars_fragment:CP,metalnessmap_fragment:DP,metalnessmap_pars_fragment:IP,morphinstance_vertex:AP,morphcolor_vertex:RP,morphnormal_vertex:NP,morphtarget_pars_vertex:PP,morphtarget_vertex:OP,normal_fragment_begin:LP,normal_fragment_maps:FP,normal_pars_fragment:kP,normal_pars_vertex:UP,normal_vertex:BP,normalmap_pars_fragment:VP,clearcoat_normal_fragment_begin:HP,clearcoat_normal_fragment_maps:zP,clearcoat_pars_fragment:GP,iridescence_pars_fragment:jP,opaque_fragment:WP,packing:$P,premultiplied_alpha_fragment:XP,project_vertex:qP,dithering_fragment:YP,dithering_pars_fragment:ZP,roughnessmap_fragment:KP,roughnessmap_pars_fragment:JP,shadowmap_pars_fragment:QP,shadowmap_pars_vertex:eO,shadowmap_vertex:tO,shadowmask_pars_fragment:nO,skinbase_vertex:iO,skinning_pars_vertex:rO,skinning_vertex:sO,skinnormal_vertex:oO,specularmap_fragment:aO,specularmap_pars_fragment:cO,tonemapping_fragment:lO,tonemapping_pars_fragment:uO,transmission_fragment:dO,transmission_pars_fragment:fO,uv_pars_fragment:hO,uv_pars_vertex:pO,uv_vertex:mO,worldpos_vertex:gO,background_vert:vO,background_frag:yO,backgroundCube_vert:_O,backgroundCube_frag:xO,cube_vert:bO,cube_frag:SO,depth_vert:MO,depth_frag:EO,distance_vert:wO,distance_frag:TO,equirect_vert:CO,equirect_frag:DO,linedashed_vert:IO,linedashed_frag:AO,meshbasic_vert:RO,meshbasic_frag:NO,meshlambert_vert:PO,meshlambert_frag:OO,meshmatcap_vert:LO,meshmatcap_frag:FO,meshnormal_vert:kO,meshnormal_frag:UO,meshphong_vert:BO,meshphong_frag:VO,meshphysical_vert:HO,meshphysical_frag:zO,meshtoon_vert:GO,meshtoon_frag:jO,points_vert:WO,points_frag:$O,shadow_vert:XO,shadow_frag:qO,sprite_vert:YO,sprite_frag:ZO},le={common:{diffuse:{value:new qe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Fe},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Fe}},envmap:{envMap:{value:null},envMapRotation:{value:new Fe},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Fe}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Fe}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Fe},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Fe},normalScale:{value:new ot(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Fe},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Fe}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Fe}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Fe}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new qe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new O},probesMax:{value:new O},probesResolution:{value:new O}},points:{diffuse:{value:new qe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0},uvTransform:{value:new Fe}},sprite:{diffuse:{value:new qe(16777215)},opacity:{value:1},center:{value:new ot(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Fe},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0}}},Wi={basic:{uniforms:Sn([le.common,le.specularmap,le.envmap,le.aomap,le.lightmap,le.fog]),vertexShader:$e.meshbasic_vert,fragmentShader:$e.meshbasic_frag},lambert:{uniforms:Sn([le.common,le.specularmap,le.envmap,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.fog,le.lights,{emissive:{value:new qe(0)},envMapIntensity:{value:1}}]),vertexShader:$e.meshlambert_vert,fragmentShader:$e.meshlambert_frag},phong:{uniforms:Sn([le.common,le.specularmap,le.envmap,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.fog,le.lights,{emissive:{value:new qe(0)},specular:{value:new qe(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:$e.meshphong_vert,fragmentShader:$e.meshphong_frag},standard:{uniforms:Sn([le.common,le.envmap,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.roughnessmap,le.metalnessmap,le.fog,le.lights,{emissive:{value:new qe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:$e.meshphysical_vert,fragmentShader:$e.meshphysical_frag},toon:{uniforms:Sn([le.common,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.gradientmap,le.fog,le.lights,{emissive:{value:new qe(0)}}]),vertexShader:$e.meshtoon_vert,fragmentShader:$e.meshtoon_frag},matcap:{uniforms:Sn([le.common,le.bumpmap,le.normalmap,le.displacementmap,le.fog,{matcap:{value:null}}]),vertexShader:$e.meshmatcap_vert,fragmentShader:$e.meshmatcap_frag},points:{uniforms:Sn([le.points,le.fog]),vertexShader:$e.points_vert,fragmentShader:$e.points_frag},dashed:{uniforms:Sn([le.common,le.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:$e.linedashed_vert,fragmentShader:$e.linedashed_frag},depth:{uniforms:Sn([le.common,le.displacementmap]),vertexShader:$e.depth_vert,fragmentShader:$e.depth_frag},normal:{uniforms:Sn([le.common,le.bumpmap,le.normalmap,le.displacementmap,{opacity:{value:1}}]),vertexShader:$e.meshnormal_vert,fragmentShader:$e.meshnormal_frag},sprite:{uniforms:Sn([le.sprite,le.fog]),vertexShader:$e.sprite_vert,fragmentShader:$e.sprite_frag},background:{uniforms:{uvTransform:{value:new Fe},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:$e.background_vert,fragmentShader:$e.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Fe}},vertexShader:$e.backgroundCube_vert,fragmentShader:$e.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:$e.cube_vert,fragmentShader:$e.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:$e.equirect_vert,fragmentShader:$e.equirect_frag},distance:{uniforms:Sn([le.common,le.displacementmap,{referencePosition:{value:new O},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:$e.distance_vert,fragmentShader:$e.distance_frag},shadow:{uniforms:Sn([le.lights,le.fog,{color:{value:new qe(0)},opacity:{value:1}}]),vertexShader:$e.shadow_vert,fragmentShader:$e.shadow_frag}};Wi.physical={uniforms:Sn([Wi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Fe},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Fe},clearcoatNormalScale:{value:new ot(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Fe},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Fe},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Fe},sheen:{value:0},sheenColor:{value:new qe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Fe},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Fe},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Fe},transmissionSamplerSize:{value:new ot},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Fe},attenuationDistance:{value:0},attenuationColor:{value:new qe(0)},specularColor:{value:new qe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Fe},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Fe},anisotropyVector:{value:new ot},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Fe}}]),vertexShader:$e.meshphysical_vert,fragmentShader:$e.meshphysical_frag};var Hh={r:0,b:0,g:0},KO=new It,AE=new Fe;AE.set(-1,0,0,0,1,0,0,0,1);function JO(n,e,t,i,r,s){let o=new qe(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(b){let M=b.isScene===!0?b.background:null;if(M&&M.isTexture){let E=b.backgroundBlurriness>0;M=e.get(M,E)}return M}function g(b){let M=!1,E=h(b);E===null?m(o,a):E&&E.isColor&&(m(E,1),M=!0);let D=n.xr.getEnvironmentBlendMode();D==="additive"?t.buffers.color.setClear(0,0,0,1,s):D==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||M)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function y(b,M){let E=h(M);E&&(E.isCubeTexture||E.mapping===cl)?(l===void 0&&(l=new pt(new kt(1,1,1),new Gn({name:"BackgroundCubeMaterial",uniforms:eo(Wi.backgroundCube.uniforms),vertexShader:Wi.backgroundCube.vertexShader,fragmentShader:Wi.backgroundCube.fragmentShader,side:Dn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(D,w,I){this.matrixWorld.copyPosition(I.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=E,l.material.uniforms.backgroundBlurriness.value=M.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=M.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(KO.makeRotationFromEuler(M.backgroundRotation)).transpose(),E.isCubeTexture&&E.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(AE),l.material.toneMapped=Qe.getTransfer(E.colorSpace)!==ft,(u!==E||d!==E.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=E,d=E.version,f=n.toneMapping),l.layers.enableAll(),b.unshift(l,l.geometry,l.material,0,0,null)):E&&E.isTexture&&(c===void 0&&(c=new pt(new Qc(2,2),new Gn({name:"BackgroundMaterial",uniforms:eo(Wi.background.uniforms),vertexShader:Wi.background.vertexShader,fragmentShader:Wi.background.fragmentShader,side:hr,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=E,c.material.uniforms.backgroundIntensity.value=M.backgroundIntensity,c.material.toneMapped=Qe.getTransfer(E.colorSpace)!==ft,E.matrixAutoUpdate===!0&&E.updateMatrix(),c.material.uniforms.uvTransform.value.copy(E.matrix),(u!==E||d!==E.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=E,d=E.version,f=n.toneMapping),c.layers.enableAll(),b.unshift(c,c.geometry,c.material,0,0,null))}function m(b,M){b.getRGB(Hh,Ty(n)),t.buffers.color.setClear(Hh.r,Hh.g,Hh.b,M,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(b,M=1){o.set(b),a=M,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(b){a=b,m(o,a)},render:g,addToRenderList:y,dispose:p}}function QO(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),s=r,o=!1;function a(C,U,j,q,P){let z=!1,k=d(C,q,j,U);s!==k&&(s=k,l(s.object)),z=h(C,q,j,P),z&&g(C,q,j,P),P!==null&&e.update(P,n.ELEMENT_ARRAY_BUFFER),(z||o)&&(o=!1,E(C,U,j,q),P!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(P).buffer))}function c(){return n.createVertexArray()}function l(C){return n.bindVertexArray(C)}function u(C){return n.deleteVertexArray(C)}function d(C,U,j,q){let P=q.wireframe===!0,z=i[U.id];z===void 0&&(z={},i[U.id]=z);let k=C.isInstancedMesh===!0?C.id:0,Q=z[k];Q===void 0&&(Q={},z[k]=Q);let ee=Q[j.id];ee===void 0&&(ee={},Q[j.id]=ee);let ue=ee[P];return ue===void 0&&(ue=f(c()),ee[P]=ue),ue}function f(C){let U=[],j=[],q=[];for(let P=0;P<t;P++)U[P]=0,j[P]=0,q[P]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:U,enabledAttributes:j,attributeDivisors:q,object:C,attributes:{},index:null}}function h(C,U,j,q){let P=s.attributes,z=U.attributes,k=0,Q=j.getAttributes();for(let ee in Q)if(Q[ee].location>=0){let xe=P[ee],Te=z[ee];if(Te===void 0&&(ee==="instanceMatrix"&&C.instanceMatrix&&(Te=C.instanceMatrix),ee==="instanceColor"&&C.instanceColor&&(Te=C.instanceColor)),xe===void 0||xe.attribute!==Te||Te&&xe.data!==Te.data)return!0;k++}return s.attributesNum!==k||s.index!==q}function g(C,U,j,q){let P={},z=U.attributes,k=0,Q=j.getAttributes();for(let ee in Q)if(Q[ee].location>=0){let xe=z[ee];xe===void 0&&(ee==="instanceMatrix"&&C.instanceMatrix&&(xe=C.instanceMatrix),ee==="instanceColor"&&C.instanceColor&&(xe=C.instanceColor));let Te={};Te.attribute=xe,xe&&xe.data&&(Te.data=xe.data),P[ee]=Te,k++}s.attributes=P,s.attributesNum=k,s.index=q}function y(){let C=s.newAttributes;for(let U=0,j=C.length;U<j;U++)C[U]=0}function m(C){p(C,0)}function p(C,U){let j=s.newAttributes,q=s.enabledAttributes,P=s.attributeDivisors;j[C]=1,q[C]===0&&(n.enableVertexAttribArray(C),q[C]=1),P[C]!==U&&(n.vertexAttribDivisor(C,U),P[C]=U)}function b(){let C=s.newAttributes,U=s.enabledAttributes;for(let j=0,q=U.length;j<q;j++)U[j]!==C[j]&&(n.disableVertexAttribArray(j),U[j]=0)}function M(C,U,j,q,P,z,k){k===!0?n.vertexAttribIPointer(C,U,j,P,z):n.vertexAttribPointer(C,U,j,q,P,z)}function E(C,U,j,q){y();let P=q.attributes,z=j.getAttributes(),k=U.defaultAttributeValues;for(let Q in z){let ee=z[Q];if(ee.location>=0){let ue=P[Q];if(ue===void 0&&(Q==="instanceMatrix"&&C.instanceMatrix&&(ue=C.instanceMatrix),Q==="instanceColor"&&C.instanceColor&&(ue=C.instanceColor)),ue!==void 0){let xe=ue.normalized,Te=ue.itemSize,nt=e.get(ue);if(nt===void 0)continue;let mt=nt.buffer,Ge=nt.type,Y=nt.bytesPerElement,pe=Ge===n.INT||Ge===n.UNSIGNED_INT||ue.gpuType===nh;if(ue.isInterleavedBufferAttribute){let re=ue.data,Ae=re.stride,ke=ue.offset;if(re.isInstancedInterleavedBuffer){for(let Ne=0;Ne<ee.locationSize;Ne++)p(ee.location+Ne,re.meshPerAttribute);C.isInstancedMesh!==!0&&q._maxInstanceCount===void 0&&(q._maxInstanceCount=re.meshPerAttribute*re.count)}else for(let Ne=0;Ne<ee.locationSize;Ne++)m(ee.location+Ne);n.bindBuffer(n.ARRAY_BUFFER,mt);for(let Ne=0;Ne<ee.locationSize;Ne++)M(ee.location+Ne,Te/ee.locationSize,Ge,xe,Ae*Y,(ke+Te/ee.locationSize*Ne)*Y,pe)}else{if(ue.isInstancedBufferAttribute){for(let re=0;re<ee.locationSize;re++)p(ee.location+re,ue.meshPerAttribute);C.isInstancedMesh!==!0&&q._maxInstanceCount===void 0&&(q._maxInstanceCount=ue.meshPerAttribute*ue.count)}else for(let re=0;re<ee.locationSize;re++)m(ee.location+re);n.bindBuffer(n.ARRAY_BUFFER,mt);for(let re=0;re<ee.locationSize;re++)M(ee.location+re,Te/ee.locationSize,Ge,xe,Te*Y,Te/ee.locationSize*re*Y,pe)}}else if(k!==void 0){let xe=k[Q];if(xe!==void 0)switch(xe.length){case 2:n.vertexAttrib2fv(ee.location,xe);break;case 3:n.vertexAttrib3fv(ee.location,xe);break;case 4:n.vertexAttrib4fv(ee.location,xe);break;default:n.vertexAttrib1fv(ee.location,xe)}}}}b()}function D(){T();for(let C in i){let U=i[C];for(let j in U){let q=U[j];for(let P in q){let z=q[P];for(let k in z)u(z[k].object),delete z[k];delete q[P]}}delete i[C]}}function w(C){if(i[C.id]===void 0)return;let U=i[C.id];for(let j in U){let q=U[j];for(let P in q){let z=q[P];for(let k in z)u(z[k].object),delete z[k];delete q[P]}}delete i[C.id]}function I(C){for(let U in i){let j=i[U];for(let q in j){let P=j[q];if(P[C.id]===void 0)continue;let z=P[C.id];for(let k in z)u(z[k].object),delete z[k];delete P[C.id]}}}function _(C){for(let U in i){let j=i[U],q=C.isInstancedMesh===!0?C.id:0,P=j[q];if(P!==void 0){for(let z in P){let k=P[z];for(let Q in k)u(k[Q].object),delete k[Q];delete P[z]}delete j[q],Object.keys(j).length===0&&delete i[U]}}}function T(){L(),o=!0,s!==r&&(s=r,l(s.object))}function L(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:T,resetDefaultState:L,dispose:D,releaseStatesOfGeometry:w,releaseStatesOfObject:_,releaseStatesOfProgram:I,initAttributes:y,enableAttribute:m,disableUnusedAttributes:b}}function eL(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let f=0;for(let h=0;h<u;h++)f+=l[h];t.update(f,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function tL(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let I=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(I.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(I){return!(I!==ai&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(I){let _=I===Gi&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(I!==Nn&&i.convert(I)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&I!==Mi&&!_)}function c(I){if(I==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";I="mediump"}return I==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(De("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&f===!1&&De("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),y=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),b=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),M=n.getParameter(n.MAX_VARYING_VECTORS),E=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),D=n.getParameter(n.MAX_SAMPLES),w=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:f,maxTextures:h,maxVertexTextures:g,maxTextureSize:y,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:b,maxVaryings:M,maxFragmentUniforms:E,maxSamples:D,samples:w}}function nL(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Oi,a=new Fe,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let g=d.clippingPlanes,y=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let b=s?0:i,M=b*4,E=p.clippingState||null;c.value=E,E=u(g,f,M,h);for(let D=0;D!==M;++D)E[D]=t[D];p.clippingState=E,this.numIntersection=y?this.numPlanes:0,this.numPlanes+=b}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,g){let y=d!==null?d.length:0,m=null;if(y!==0){if(m=c.value,g!==!0||m===null){let p=h+y*4,b=f.matrixWorldInverse;a.getNormalMatrix(b),(m===null||m.length<p)&&(m=new Float32Array(p));for(let M=0,E=h;M!==y;++M,E+=4)o.copy(d[M]).applyMatrix4(b,a),o.normal.toArray(m,E),m[E+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=y,e.numIntersection=0,m}}var os=4,aE=[.125,.215,.35,.446,.526,.582],to=20,iL=256,gl=new ga,cE=new qe,Ay=null,Ry=0,Ny=0,Py=!1,rL=new O,Gh=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=rL}=s;Ay=this._renderer.getRenderTarget(),Ry=this._renderer.getActiveCubeFace(),Ny=this._renderer.getActiveMipmapLevel(),Py=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=dE(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=uE(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(Ay,Ry,Ny),this._renderer.xr.enabled=Py,e.scissorTest=!1,ba(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===ns||e.mapping===Qs?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Ay=this._renderer.getRenderTarget(),Ry=this._renderer.getActiveCubeFace(),Ny=this._renderer.getActiveMipmapLevel(),Py=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:hn,minFilter:hn,generateMipmaps:!1,type:Gi,format:ai,colorSpace:Hc,depthBuffer:!1},r=lE(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=lE(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=sL(s)),this._blurMaterial=aL(s,e,t),this._ggxMaterial=oL(s,e,t)}return r}_compileMaterial(e){let t=new pt(new Rn,e);this._renderer.compile(t,gl)}_sceneToCubeUV(e,t,i,r,s){let c=new fn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,f=d.autoClear,h=d.toneMapping;d.getClearColor(cE),d.toneMapping=bi,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new pt(new kt,new Ks({name:"PMREM.Background",side:Dn,depthWrite:!1,depthTest:!1})));let y=this._backgroundBox,m=y.material,p=!1,b=e.background;b?b.isColor&&(m.color.copy(b),e.background=null,p=!0):(m.color.copy(cE),p=!0);for(let M=0;M<6;M++){let E=M%3;E===0?(c.up.set(0,l[M],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[M],s.y,s.z)):E===1?(c.up.set(0,0,l[M]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[M],s.z)):(c.up.set(0,l[M],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[M]));let D=this._cubeSize;ba(r,E*D,M>2?D:0,D,D),d.setRenderTarget(r),p&&d.render(y,c),d.render(e,c)}d.toneMapping=h,d.autoClear=f,e.background=b}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===ns||e.mapping===Qs;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=dE()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=uE());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;ba(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,gl)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),f=0+l*1.25,h=d*f,{_lodMax:g}=this,y=this._sizeLods[i],m=3*y*(i>g-os?i-g+os:0),p=4*(this._cubeSize-y);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=g-t,ba(s,m,p,3*y,2*y),r.setRenderTarget(s),r.render(a,gl),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,ba(e,m,p,3*y,2*y),r.setRenderTarget(e),r.render(a,gl)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Re("blur direction must be either latitudinal or longitudinal!");let u=3,d=this._lodMeshes[r];d.material=l;let f=l.uniforms,h=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*to-1),y=s/g,m=isFinite(s)?1+Math.floor(u*y):to;m>to&&De(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${to}`);let p=[],b=0;for(let I=0;I<to;++I){let _=I/y,T=Math.exp(-_*_/2);p.push(T),I===0?b+=T:I<m&&(b+=2*T)}for(let I=0;I<p.length;I++)p[I]=p[I]/b;f.envMap.value=e.texture,f.samples.value=m,f.weights.value=p,f.latitudinal.value=o==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:M}=this;f.dTheta.value=g,f.mipInt.value=M-i;let E=this._sizeLods[r],D=3*E*(r>M-os?r-M+os:0),w=4*(this._cubeSize-E);ba(t,D,w,3*E,2*E),c.setRenderTarget(t),c.render(d,gl)}};function sL(n){let e=[],t=[],i=[],r=n,s=n-os+1+aE.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-os?c=aE[o-n+os-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,d=1+l,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,g=6,y=3,m=2,p=1,b=new Float32Array(y*g*h),M=new Float32Array(m*g*h),E=new Float32Array(p*g*h);for(let w=0;w<h;w++){let I=w%3*2/3-1,_=w>2?0:-1,T=[I,_,0,I+2/3,_,0,I+2/3,_+1,0,I,_,0,I+2/3,_+1,0,I,_+1,0];b.set(T,y*g*w),M.set(f,m*g*w);let L=[w,w,w,w,w,w];E.set(L,p*g*w)}let D=new Rn;D.setAttribute("position",new Hn(b,y)),D.setAttribute("uv",new Hn(M,m)),D.setAttribute("faceIndex",new Hn(E,p)),i.push(new pt(D,null)),r>os&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function lE(n,e,t){let i=new zn(n,e,t);return i.texture.mapping=cl,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function ba(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function oL(n,e,t){return new Gn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:iL,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:$h(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:zi,depthTest:!1,depthWrite:!1})}function aL(n,e,t){let i=new Float32Array(to),r=new O(0,1,0);return new Gn({name:"SphericalGaussianBlur",defines:{n:to,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:$h(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:zi,depthTest:!1,depthWrite:!1})}function uE(){return new Gn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:$h(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:zi,depthTest:!1,depthWrite:!1})}function dE(){return new Gn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:$h(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:zi,depthTest:!1,depthWrite:!1})}function $h(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var jh=class extends zn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Kc(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new kt(5,5,5),s=new Gn({name:"CubemapFromEquirect",uniforms:eo(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:Dn,blending:zi});s.uniforms.tEquirect.value=t;let o=new pt(r,s),a=t.minFilter;return t.minFilter===is&&(t.minFilter=hn),new Zf(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function cL(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?o(f):s(f)}function s(f){if(f&&f.isTexture){let h=f.mapping;if(h===Qf||h===eh)if(e.has(f)){let g=e.get(f).texture;return a(g,f.mapping)}else{let g=f.image;if(g&&g.height>0){let y=new jh(g.height);return y.fromEquirectangularTexture(n,f),e.set(f,y),f.addEventListener("dispose",l),a(y.texture,f.mapping)}else return null}}return f}function o(f){if(f&&f.isTexture){let h=f.mapping,g=h===Qf||h===eh,y=h===ns||h===Qs;if(g||y){let m=t.get(f),p=m!==void 0?m.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==p)return i===null&&(i=new Gh(n)),m=g?i.fromEquirectangular(f,m):i.fromCubemap(f,m),m.texture.pmremVersion=f.pmremVersion,t.set(f,m),m.texture;if(m!==void 0)return m.texture;{let b=f.image;return g&&b&&b.height>0||y&&b&&c(b)?(i===null&&(i=new Gh(n)),m=g?i.fromEquirectangular(f):i.fromCubemap(f),m.texture.pmremVersion=f.pmremVersion,t.set(f,m),f.addEventListener("dispose",u),m.texture):null}}}return f}function a(f,h){return h===Qf?f.mapping=ns:h===eh&&(f.mapping=Qs),f}function c(f){let h=0,g=6;for(let y=0;y<g;y++)f[y]!==void 0&&h++;return h===g}function l(f){let h=f.target;h.removeEventListener("dispose",l);let g=e.get(h);g!==void 0&&(e.delete(h),g.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let g=t.get(h);g!==void 0&&(t.delete(h),g.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function lL(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&If("WebGLRenderer: "+i+" extension not supported."),r}}}function uL(n,e,t,i){let r={},s=new WeakMap;function o(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let g in f.attributes)e.remove(f.attributes[g]);f.removeEventListener("dispose",o),delete r[f.id];let h=s.get(f);h&&(e.remove(h),s.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",o),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,g=d.attributes.position,y=0;if(g===void 0)return;if(h!==null){let b=h.array;y=h.version;for(let M=0,E=b.length;M<E;M+=3){let D=b[M+0],w=b[M+1],I=b[M+2];f.push(D,w,w,I,I,D)}}else{let b=g.array;y=g.version;for(let M=0,E=b.length/3-1;M<E;M+=3){let D=M+0,w=M+1,I=M+2;f.push(D,w,w,I,I,D)}}let m=new(g.count>=65535?Yc:qc)(f,1);m.version=y;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let f=s.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function dL(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,f){n.drawElements(i,f,s,d*o),t.update(f,i,1)}function l(d,f,h){h!==0&&(n.drawElementsInstanced(i,f,s,d*o,h),t.update(f,i,h))}function u(d,f,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,f,0,s,d,0,h);let y=0;for(let m=0;m<h;m++)y+=f[m];t.update(y,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function fL(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Re("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function hL(n,e,t){let i=new WeakMap,r=new At;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let L=function(){_.dispose(),i.delete(a),a.removeEventListener("dispose",L)};var h=L;f!==void 0&&f.texture.dispose();let g=a.morphAttributes.position!==void 0,y=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],b=a.morphAttributes.normal||[],M=a.morphAttributes.color||[],E=0;g===!0&&(E=1),y===!0&&(E=2),m===!0&&(E=3);let D=a.attributes.position.count*E,w=1;D>e.maxTextureSize&&(w=Math.ceil(D/e.maxTextureSize),D=e.maxTextureSize);let I=new Float32Array(D*w*4*d),_=new jc(I,D,w,d);_.type=Mi,_.needsUpdate=!0;let T=E*4;for(let C=0;C<d;C++){let U=p[C],j=b[C],q=M[C],P=D*w*4*C;for(let z=0;z<U.count;z++){let k=z*T;g===!0&&(r.fromBufferAttribute(U,z),I[P+k+0]=r.x,I[P+k+1]=r.y,I[P+k+2]=r.z,I[P+k+3]=0),y===!0&&(r.fromBufferAttribute(j,z),I[P+k+4]=r.x,I[P+k+5]=r.y,I[P+k+6]=r.z,I[P+k+7]=0),m===!0&&(r.fromBufferAttribute(q,z),I[P+k+8]=r.x,I[P+k+9]=r.y,I[P+k+10]=r.z,I[P+k+11]=q.itemSize===4?r.w:1)}}f={count:d,texture:_,size:new ot(D,w)},i.set(a,f),a.addEventListener("dispose",L)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let y=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",y),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:s}}function pL(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(s.get(f)!==u&&(e.update(f),s.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return f}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var mL={[uy]:"LINEAR_TONE_MAPPING",[dy]:"REINHARD_TONE_MAPPING",[fy]:"CINEON_TONE_MAPPING",[al]:"ACES_FILMIC_TONE_MAPPING",[py]:"AGX_TONE_MAPPING",[my]:"NEUTRAL_TONE_MAPPING",[hy]:"CUSTOM_TONE_MAPPING"};function gL(n,e,t,i,r){let s=new zn(e,t,{type:n,depthBuffer:i,stencilBuffer:r,depthTexture:i?new gr(e,t):void 0}),o=new zn(e,t,{type:Gi,depthBuffer:!1,stencilBuffer:!1}),a=new Rn;a.setAttribute("position",new Zt([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new Zt([0,2,0,0,2,0],2));let c=new Uf({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new pt(a,c),u=new ga(-1,1,1,-1,0,1),d=null,f=null,h=!1,g,y=null,m=[],p=!1;this.setSize=function(b,M){s.setSize(b,M),o.setSize(b,M);for(let E=0;E<m.length;E++){let D=m[E];D.setSize&&D.setSize(b,M)}},this.setEffects=function(b){m=b,p=m.length>0&&m[0].isRenderPass===!0;let M=s.width,E=s.height;for(let D=0;D<m.length;D++){let w=m[D];w.setSize&&w.setSize(M,E)}},this.begin=function(b,M){if(h||b.toneMapping===bi&&m.length===0)return!1;if(y=M,M!==null){let E=M.width,D=M.height;(s.width!==E||s.height!==D)&&this.setSize(E,D)}return p===!1&&b.setRenderTarget(s),g=b.toneMapping,b.toneMapping=bi,!0},this.hasRenderPass=function(){return p},this.end=function(b,M){b.toneMapping=g,h=!0;let E=s,D=o;for(let w=0;w<m.length;w++){let I=m[w];if(I.enabled!==!1&&(I.render(b,D,E,M),I.needsSwap!==!1)){let _=E;E=D,D=_}}if(d!==b.outputColorSpace||f!==b.toneMapping){d=b.outputColorSpace,f=b.toneMapping,c.defines={},Qe.getTransfer(d)===ft&&(c.defines.SRGB_TRANSFER="");let w=mL[f];w&&(c.defines[w]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=E.texture,b.setRenderTarget(y),b.render(l,u),y=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.depthTexture&&s.depthTexture.dispose(),s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var RE=new yr,Fy=new gr(1,1),NE=new jc,PE=new Nf,OE=new Kc,fE=[],hE=[],pE=new Float32Array(16),mE=new Float32Array(9),gE=new Float32Array(4);function Ma(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=fE[r];if(s===void 0&&(s=new Float32Array(r),fE[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Kt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Jt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function Xh(n,e){let t=hE[e];t===void 0&&(t=new Int32Array(e),hE[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function vL(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function yL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2fv(this.addr,e),Jt(t,e)}}function _L(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Kt(t,e))return;n.uniform3fv(this.addr,e),Jt(t,e)}}function xL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4fv(this.addr,e),Jt(t,e)}}function bL(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Jt(t,e)}else{if(Kt(t,i))return;gE.set(i),n.uniformMatrix2fv(this.addr,!1,gE),Jt(t,i)}}function SL(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Jt(t,e)}else{if(Kt(t,i))return;mE.set(i),n.uniformMatrix3fv(this.addr,!1,mE),Jt(t,i)}}function ML(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Kt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Jt(t,e)}else{if(Kt(t,i))return;pE.set(i),n.uniformMatrix4fv(this.addr,!1,pE),Jt(t,i)}}function EL(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function wL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2iv(this.addr,e),Jt(t,e)}}function TL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Kt(t,e))return;n.uniform3iv(this.addr,e),Jt(t,e)}}function CL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4iv(this.addr,e),Jt(t,e)}}function DL(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function IL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Kt(t,e))return;n.uniform2uiv(this.addr,e),Jt(t,e)}}function AL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Kt(t,e))return;n.uniform3uiv(this.addr,e),Jt(t,e)}}function RL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Kt(t,e))return;n.uniform4uiv(this.addr,e),Jt(t,e)}}function NL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(Fy.compareFunction=t.isReversedDepthBuffer()?Vh:Bh,s=Fy):s=RE,t.setTexture2D(e||s,r)}function PL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||PE,r)}function OL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||OE,r)}function LL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||NE,r)}function FL(n){switch(n){case 5126:return vL;case 35664:return yL;case 35665:return _L;case 35666:return xL;case 35674:return bL;case 35675:return SL;case 35676:return ML;case 5124:case 35670:return EL;case 35667:case 35671:return wL;case 35668:case 35672:return TL;case 35669:case 35673:return CL;case 5125:return DL;case 36294:return IL;case 36295:return AL;case 36296:return RL;case 35678:case 36198:case 36298:case 36306:case 35682:return NL;case 35679:case 36299:case 36307:return PL;case 35680:case 36300:case 36308:case 36293:return OL;case 36289:case 36303:case 36311:case 36292:return LL}}function kL(n,e){n.uniform1fv(this.addr,e)}function UL(n,e){let t=Ma(e,this.size,2);n.uniform2fv(this.addr,t)}function BL(n,e){let t=Ma(e,this.size,3);n.uniform3fv(this.addr,t)}function VL(n,e){let t=Ma(e,this.size,4);n.uniform4fv(this.addr,t)}function HL(n,e){let t=Ma(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function zL(n,e){let t=Ma(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function GL(n,e){let t=Ma(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function jL(n,e){n.uniform1iv(this.addr,e)}function WL(n,e){n.uniform2iv(this.addr,e)}function $L(n,e){n.uniform3iv(this.addr,e)}function XL(n,e){n.uniform4iv(this.addr,e)}function qL(n,e){n.uniform1uiv(this.addr,e)}function YL(n,e){n.uniform2uiv(this.addr,e)}function ZL(n,e){n.uniform3uiv(this.addr,e)}function KL(n,e){n.uniform4uiv(this.addr,e)}function JL(n,e,t){let i=this.cache,r=e.length,s=Xh(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=Fy:o=RE;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function QL(n,e,t){let i=this.cache,r=e.length,s=Xh(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||PE,s[o])}function eF(n,e,t){let i=this.cache,r=e.length,s=Xh(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||OE,s[o])}function tF(n,e,t){let i=this.cache,r=e.length,s=Xh(t,r);Kt(i,s)||(n.uniform1iv(this.addr,s),Jt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||NE,s[o])}function nF(n){switch(n){case 5126:return kL;case 35664:return UL;case 35665:return BL;case 35666:return VL;case 35674:return HL;case 35675:return zL;case 35676:return GL;case 5124:case 35670:return jL;case 35667:case 35671:return WL;case 35668:case 35672:return $L;case 35669:case 35673:return XL;case 5125:return qL;case 36294:return YL;case 36295:return ZL;case 36296:return KL;case 35678:case 36198:case 36298:case 36306:case 35682:return JL;case 35679:case 36299:case 36307:return QL;case 35680:case 36300:case 36308:case 36293:return eF;case 36289:case 36303:case 36311:case 36292:return tF}}var ky=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=FL(t.type)}},Uy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=nF(t.type)}},By=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},Oy=/(\w+)(\])?(\[|\.)?/g;function vE(n,e){n.seq.push(e),n.map[e.id]=e}function iF(n,e,t){let i=n.name,r=i.length;for(Oy.lastIndex=0;;){let s=Oy.exec(i),o=Oy.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){vE(t,l===void 0?new ky(a,n,e):new Uy(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new By(a),vE(t,d)),t=d}}}var Sa=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);iF(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function yE(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var rF=37297,sF=0;function oF(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var _E=new Fe;function aF(n){Qe._getMatrix(_E,Qe.workingColorSpace,n);let e=`mat3( ${_E.elements.map(t=>t.toFixed(4))} )`;switch(Qe.getTransfer(n)){case zc:return[e,"LinearTransferOETF"];case ft:return[e,"sRGBTransferOETF"];default:return De("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function xE(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+oF(n.getShaderSource(e),a)}else return s}function cF(n,e){let t=aF(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var lF={[uy]:"Linear",[dy]:"Reinhard",[fy]:"Cineon",[al]:"ACESFilmic",[py]:"AgX",[my]:"Neutral",[hy]:"Custom"};function uF(n,e){let t=lF[e];return t===void 0?(De("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var zh=new O;function dF(){Qe.getLuminanceCoefficients(zh);let n=zh.x.toFixed(4),e=zh.y.toFixed(4),t=zh.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function fF(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(yl).join(`
`)}function hF(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function pF(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function yl(n){return n!==""}function bE(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function SE(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var mF=/^[ \t]*#include +<([\w\d./]+)>/gm;function Vy(n){return n.replace(mF,vF)}var gF=new Map;function vF(n,e){let t=$e[e];if(t===void 0){let i=gF.get(e);if(i!==void 0)t=$e[i],De('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Vy(t)}var yF=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function ME(n){return n.replace(yF,_F)}function _F(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function EE(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var xF={[ol]:"SHADOWMAP_TYPE_PCF",[va]:"SHADOWMAP_TYPE_VSM"};function bF(n){return xF[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var SF={[ns]:"ENVMAP_TYPE_CUBE",[Qs]:"ENVMAP_TYPE_CUBE",[cl]:"ENVMAP_TYPE_CUBE_UV"};function MF(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":SF[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var EF={[Qs]:"ENVMAP_MODE_REFRACTION"};function wF(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":EF[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var TF={[ly]:"ENVMAP_BLENDING_MULTIPLY",[jM]:"ENVMAP_BLENDING_MIX",[WM]:"ENVMAP_BLENDING_ADD"};function CF(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":TF[n.combine]||"ENVMAP_BLENDING_NONE"}function DF(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function IF(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=bF(t),l=MF(t),u=wF(t),d=CF(t),f=DF(t),h=fF(t),g=hF(s),y=r.createProgram(),m,p,b=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(yl).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(yl).join(`
`),p.length>0&&(p+=`
`)):(m=[EE(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(yl).join(`
`),p=[EE(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==bi?"#define TONE_MAPPING":"",t.toneMapping!==bi?$e.tonemapping_pars_fragment:"",t.toneMapping!==bi?uF("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",$e.colorspace_pars_fragment,cF("linearToOutputTexel",t.outputColorSpace),dF(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(yl).join(`
`)),o=Vy(o),o=bE(o,t),o=SE(o,t),a=Vy(a),a=bE(a,t),a=SE(a,t),o=ME(o),a=ME(a),t.isRawShaderMaterial!==!0&&(b=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===My?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===My?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let M=b+m+o,E=b+p+a,D=yE(r,r.VERTEX_SHADER,M),w=yE(r,r.FRAGMENT_SHADER,E);r.attachShader(y,D),r.attachShader(y,w),t.index0AttributeName!==void 0?r.bindAttribLocation(y,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(y,0,"position"),r.linkProgram(y);function I(C){if(n.debug.checkShaderErrors){let U=r.getProgramInfoLog(y)||"",j=r.getShaderInfoLog(D)||"",q=r.getShaderInfoLog(w)||"",P=U.trim(),z=j.trim(),k=q.trim(),Q=!0,ee=!0;if(r.getProgramParameter(y,r.LINK_STATUS)===!1)if(Q=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,y,D,w);else{let ue=xE(r,D,"vertex"),xe=xE(r,w,"fragment");Re("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(y,r.VALIDATE_STATUS)+`

Material Name: `+C.name+`
Material Type: `+C.type+`

Program Info Log: `+P+`
`+ue+`
`+xe)}else P!==""?De("WebGLProgram: Program Info Log:",P):(z===""||k==="")&&(ee=!1);ee&&(C.diagnostics={runnable:Q,programLog:P,vertexShader:{log:z,prefix:m},fragmentShader:{log:k,prefix:p}})}r.deleteShader(D),r.deleteShader(w),_=new Sa(r,y),T=pF(r,y)}let _;this.getUniforms=function(){return _===void 0&&I(this),_};let T;this.getAttributes=function(){return T===void 0&&I(this),T};let L=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return L===!1&&(L=r.getProgramParameter(y,rF)),L},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(y),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=sF++,this.cacheKey=e,this.usedTimes=1,this.program=y,this.vertexShader=D,this.fragmentShader=w,this}var AF=0,Hy=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new zy(e),t.set(e,i)),i}},zy=class{constructor(e){this.id=AF++,this.code=e,this.usedTimes=0}};function RF(n){return n===ss||n===pl||n===ml}function NF(n,e,t,i,r,s){let o=new Wc,a=new Hy,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(_){return c.add(_),_===0?"uv":`uv${_}`}function y(_,T,L,C,U,j){let q=C.fog,P=U.geometry,z=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?C.environment:null,k=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap,Q=e.get(_.envMap||z,k),ee=Q&&Q.mapping===cl?Q.image.height:null,ue=h[_.type];_.precision!==null&&(f=i.getMaxPrecision(_.precision),f!==_.precision&&De("WebGLProgram.getParameters:",_.precision,"not supported, using",f,"instead."));let xe=P.morphAttributes.position||P.morphAttributes.normal||P.morphAttributes.color,Te=xe!==void 0?xe.length:0,nt=0;P.morphAttributes.position!==void 0&&(nt=1),P.morphAttributes.normal!==void 0&&(nt=2),P.morphAttributes.color!==void 0&&(nt=3);let mt,Ge,Y,pe;if(ue){let Be=Wi[ue];mt=Be.vertexShader,Ge=Be.fragmentShader}else mt=_.vertexShader,Ge=_.fragmentShader,a.update(_),Y=a.getVertexShaderID(_),pe=a.getFragmentShaderID(_);let re=n.getRenderTarget(),Ae=n.state.buffers.depth.getReversed(),ke=U.isInstancedMesh===!0,Ne=U.isBatchedMesh===!0,Ct=!!_.map,Ze=!!_.matcap,gt=!!Q,Tt=!!_.aoMap,Ye=!!_.lightMap,jt=!!_.bumpMap,Dt=!!_.normalMap,Pn=!!_.displacementMap,R=!!_.emissiveMap,Wt=!!_.metalnessMap,Ke=!!_.roughnessMap,Et=_.anisotropy>0,ce=_.clearcoat>0,Rt=_.dispersion>0,S=_.iridescence>0,v=_.sheen>0,F=_.transmission>0,$=Et&&!!_.anisotropyMap,K=ce&&!!_.clearcoatMap,te=ce&&!!_.clearcoatNormalMap,ae=ce&&!!_.clearcoatRoughnessMap,G=S&&!!_.iridescenceMap,X=S&&!!_.iridescenceThicknessMap,me=v&&!!_.sheenColorMap,ye=v&&!!_.sheenRoughnessMap,se=!!_.specularMap,ne=!!_.specularColorMap,Oe=!!_.specularIntensityMap,je=F&&!!_.transmissionMap,rt=F&&!!_.thicknessMap,A=!!_.gradientMap,ie=!!_.alphaMap,W=_.alphaTest>0,ge=!!_.alphaHash,oe=!!_.extensions,Z=bi;_.toneMapped&&(re===null||re.isXRRenderTarget===!0)&&(Z=n.toneMapping);let Se={shaderID:ue,shaderType:_.type,shaderName:_.name,vertexShader:mt,fragmentShader:Ge,defines:_.defines,customVertexShaderID:Y,customFragmentShaderID:pe,isRawShaderMaterial:_.isRawShaderMaterial===!0,glslVersion:_.glslVersion,precision:f,batching:Ne,batchingColor:Ne&&U._colorsTexture!==null,instancing:ke,instancingColor:ke&&U.instanceColor!==null,instancingMorph:ke&&U.morphTexture!==null,outputColorSpace:re===null?n.outputColorSpace:re.isXRRenderTarget===!0?re.texture.colorSpace:Qe.workingColorSpace,alphaToCoverage:!!_.alphaToCoverage,map:Ct,matcap:Ze,envMap:gt,envMapMode:gt&&Q.mapping,envMapCubeUVHeight:ee,aoMap:Tt,lightMap:Ye,bumpMap:jt,normalMap:Dt,displacementMap:Pn,emissiveMap:R,normalMapObjectSpace:Dt&&_.normalMapType===qM,normalMapTangentSpace:Dt&&_.normalMapType===Uh,packedNormalMap:Dt&&_.normalMapType===Uh&&RF(_.normalMap.format),metalnessMap:Wt,roughnessMap:Ke,anisotropy:Et,anisotropyMap:$,clearcoat:ce,clearcoatMap:K,clearcoatNormalMap:te,clearcoatRoughnessMap:ae,dispersion:Rt,iridescence:S,iridescenceMap:G,iridescenceThicknessMap:X,sheen:v,sheenColorMap:me,sheenRoughnessMap:ye,specularMap:se,specularColorMap:ne,specularIntensityMap:Oe,transmission:F,transmissionMap:je,thicknessMap:rt,gradientMap:A,opaque:_.transparent===!1&&_.blending===qs&&_.alphaToCoverage===!1,alphaMap:ie,alphaTest:W,alphaHash:ge,combine:_.combine,mapUv:Ct&&g(_.map.channel),aoMapUv:Tt&&g(_.aoMap.channel),lightMapUv:Ye&&g(_.lightMap.channel),bumpMapUv:jt&&g(_.bumpMap.channel),normalMapUv:Dt&&g(_.normalMap.channel),displacementMapUv:Pn&&g(_.displacementMap.channel),emissiveMapUv:R&&g(_.emissiveMap.channel),metalnessMapUv:Wt&&g(_.metalnessMap.channel),roughnessMapUv:Ke&&g(_.roughnessMap.channel),anisotropyMapUv:$&&g(_.anisotropyMap.channel),clearcoatMapUv:K&&g(_.clearcoatMap.channel),clearcoatNormalMapUv:te&&g(_.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:ae&&g(_.clearcoatRoughnessMap.channel),iridescenceMapUv:G&&g(_.iridescenceMap.channel),iridescenceThicknessMapUv:X&&g(_.iridescenceThicknessMap.channel),sheenColorMapUv:me&&g(_.sheenColorMap.channel),sheenRoughnessMapUv:ye&&g(_.sheenRoughnessMap.channel),specularMapUv:se&&g(_.specularMap.channel),specularColorMapUv:ne&&g(_.specularColorMap.channel),specularIntensityMapUv:Oe&&g(_.specularIntensityMap.channel),transmissionMapUv:je&&g(_.transmissionMap.channel),thicknessMapUv:rt&&g(_.thicknessMap.channel),alphaMapUv:ie&&g(_.alphaMap.channel),vertexTangents:!!P.attributes.tangent&&(Dt||Et),vertexNormals:!!P.attributes.normal,vertexColors:_.vertexColors,vertexAlphas:_.vertexColors===!0&&!!P.attributes.color&&P.attributes.color.itemSize===4,pointsUvs:U.isPoints===!0&&!!P.attributes.uv&&(Ct||ie),fog:!!q,useFog:_.fog===!0,fogExp2:!!q&&q.isFogExp2,flatShading:_.wireframe===!1&&(_.flatShading===!0||P.attributes.normal===void 0&&Dt===!1&&(_.isMeshLambertMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isMeshPhysicalMaterial)),sizeAttenuation:_.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:Ae,skinning:U.isSkinnedMesh===!0,morphTargets:P.morphAttributes.position!==void 0,morphNormals:P.morphAttributes.normal!==void 0,morphColors:P.morphAttributes.color!==void 0,morphTargetsCount:Te,morphTextureStride:nt,numDirLights:T.directional.length,numPointLights:T.point.length,numSpotLights:T.spot.length,numSpotLightMaps:T.spotLightMap.length,numRectAreaLights:T.rectArea.length,numHemiLights:T.hemi.length,numDirLightShadows:T.directionalShadowMap.length,numPointLightShadows:T.pointShadowMap.length,numSpotLightShadows:T.spotShadowMap.length,numSpotLightShadowsWithMaps:T.numSpotLightShadowsWithMaps,numLightProbes:T.numLightProbes,numLightProbeGrids:j.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:_.dithering,shadowMapEnabled:n.shadowMap.enabled&&L.length>0,shadowMapType:n.shadowMap.type,toneMapping:Z,decodeVideoTexture:Ct&&_.map.isVideoTexture===!0&&Qe.getTransfer(_.map.colorSpace)===ft,decodeVideoTextureEmissive:R&&_.emissiveMap.isVideoTexture===!0&&Qe.getTransfer(_.emissiveMap.colorSpace)===ft,premultipliedAlpha:_.premultipliedAlpha,doubleSided:_.side===Hi,flipSided:_.side===Dn,useDepthPacking:_.depthPacking>=0,depthPacking:_.depthPacking||0,index0AttributeName:_.index0AttributeName,extensionClipCullDistance:oe&&_.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(oe&&_.extensions.multiDraw===!0||Ne)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:_.customProgramCacheKey()};return Se.vertexUv1s=c.has(1),Se.vertexUv2s=c.has(2),Se.vertexUv3s=c.has(3),c.clear(),Se}function m(_){let T=[];if(_.shaderID?T.push(_.shaderID):(T.push(_.customVertexShaderID),T.push(_.customFragmentShaderID)),_.defines!==void 0)for(let L in _.defines)T.push(L),T.push(_.defines[L]);return _.isRawShaderMaterial===!1&&(p(T,_),b(T,_),T.push(n.outputColorSpace)),T.push(_.customProgramCacheKey),T.join()}function p(_,T){_.push(T.precision),_.push(T.outputColorSpace),_.push(T.envMapMode),_.push(T.envMapCubeUVHeight),_.push(T.mapUv),_.push(T.alphaMapUv),_.push(T.lightMapUv),_.push(T.aoMapUv),_.push(T.bumpMapUv),_.push(T.normalMapUv),_.push(T.displacementMapUv),_.push(T.emissiveMapUv),_.push(T.metalnessMapUv),_.push(T.roughnessMapUv),_.push(T.anisotropyMapUv),_.push(T.clearcoatMapUv),_.push(T.clearcoatNormalMapUv),_.push(T.clearcoatRoughnessMapUv),_.push(T.iridescenceMapUv),_.push(T.iridescenceThicknessMapUv),_.push(T.sheenColorMapUv),_.push(T.sheenRoughnessMapUv),_.push(T.specularMapUv),_.push(T.specularColorMapUv),_.push(T.specularIntensityMapUv),_.push(T.transmissionMapUv),_.push(T.thicknessMapUv),_.push(T.combine),_.push(T.fogExp2),_.push(T.sizeAttenuation),_.push(T.morphTargetsCount),_.push(T.morphAttributeCount),_.push(T.numDirLights),_.push(T.numPointLights),_.push(T.numSpotLights),_.push(T.numSpotLightMaps),_.push(T.numHemiLights),_.push(T.numRectAreaLights),_.push(T.numDirLightShadows),_.push(T.numPointLightShadows),_.push(T.numSpotLightShadows),_.push(T.numSpotLightShadowsWithMaps),_.push(T.numLightProbes),_.push(T.shadowMapType),_.push(T.toneMapping),_.push(T.numClippingPlanes),_.push(T.numClipIntersection),_.push(T.depthPacking)}function b(_,T){o.disableAll(),T.instancing&&o.enable(0),T.instancingColor&&o.enable(1),T.instancingMorph&&o.enable(2),T.matcap&&o.enable(3),T.envMap&&o.enable(4),T.normalMapObjectSpace&&o.enable(5),T.normalMapTangentSpace&&o.enable(6),T.clearcoat&&o.enable(7),T.iridescence&&o.enable(8),T.alphaTest&&o.enable(9),T.vertexColors&&o.enable(10),T.vertexAlphas&&o.enable(11),T.vertexUv1s&&o.enable(12),T.vertexUv2s&&o.enable(13),T.vertexUv3s&&o.enable(14),T.vertexTangents&&o.enable(15),T.anisotropy&&o.enable(16),T.alphaHash&&o.enable(17),T.batching&&o.enable(18),T.dispersion&&o.enable(19),T.batchingColor&&o.enable(20),T.gradientMap&&o.enable(21),T.packedNormalMap&&o.enable(22),T.vertexNormals&&o.enable(23),_.push(o.mask),o.disableAll(),T.fog&&o.enable(0),T.useFog&&o.enable(1),T.flatShading&&o.enable(2),T.logarithmicDepthBuffer&&o.enable(3),T.reversedDepthBuffer&&o.enable(4),T.skinning&&o.enable(5),T.morphTargets&&o.enable(6),T.morphNormals&&o.enable(7),T.morphColors&&o.enable(8),T.premultipliedAlpha&&o.enable(9),T.shadowMapEnabled&&o.enable(10),T.doubleSided&&o.enable(11),T.flipSided&&o.enable(12),T.useDepthPacking&&o.enable(13),T.dithering&&o.enable(14),T.transmission&&o.enable(15),T.sheen&&o.enable(16),T.opaque&&o.enable(17),T.pointsUvs&&o.enable(18),T.decodeVideoTexture&&o.enable(19),T.decodeVideoTextureEmissive&&o.enable(20),T.alphaToCoverage&&o.enable(21),T.numLightProbeGrids>0&&o.enable(22),_.push(o.mask)}function M(_){let T=h[_.type],L;if(T){let C=Wi[T];L=oE.clone(C.uniforms)}else L=_.uniforms;return L}function E(_,T){let L=u.get(T);return L!==void 0?++L.usedTimes:(L=new IF(n,T,_,r),l.push(L),u.set(T,L)),L}function D(_){if(--_.usedTimes===0){let T=l.indexOf(_);l[T]=l[l.length-1],l.pop(),u.delete(_.cacheKey),_.destroy()}}function w(_){a.remove(_)}function I(){a.dispose()}return{getParameters:y,getProgramCacheKey:m,getUniforms:M,acquireProgram:E,releaseProgram:D,releaseShaderCache:w,programs:l,dispose:I}}function PF(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function OF(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function wE(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function TE(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,g,y,m,p){let b=n[e];return b===void 0?(b={id:f.id,object:f,geometry:h,material:g,materialVariant:o(f),groupOrder:y,renderOrder:f.renderOrder,z:m,group:p},n[e]=b):(b.id=f.id,b.object=f,b.geometry=h,b.material=g,b.materialVariant=o(f),b.groupOrder=y,b.renderOrder=f.renderOrder,b.z=m,b.group=p),e++,b}function c(f,h,g,y,m,p){let b=a(f,h,g,y,m,p);g.transmission>0?i.push(b):g.transparent===!0?r.push(b):t.push(b)}function l(f,h,g,y,m,p){let b=a(f,h,g,y,m,p);g.transmission>0?i.unshift(b):g.transparent===!0?r.unshift(b):t.unshift(b)}function u(f,h){t.length>1&&t.sort(f||OF),i.length>1&&i.sort(h||wE),r.length>1&&r.sort(h||wE)}function d(){for(let f=e,h=n.length;f<h;f++){let g=n[f];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:d,sort:u}}function LF(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new TE,n.set(i,[o])):r>=s.length?(o=new TE,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function FF(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new O,color:new qe};break;case"SpotLight":t={position:new O,direction:new O,color:new qe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new O,color:new qe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new O,skyColor:new qe,groundColor:new qe};break;case"RectAreaLight":t={color:new qe,position:new O,halfWidth:new O,halfHeight:new O};break}return n[e.id]=t,t}}}function kF(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ot};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ot};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new ot,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var UF=0;function BF(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function VF(n){let e=new FF,t=kF(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new O);let r=new O,s=new It,o=new It;function a(l){let u=0,d=0,f=0;for(let T=0;T<9;T++)i.probe[T].set(0,0,0);let h=0,g=0,y=0,m=0,p=0,b=0,M=0,E=0,D=0,w=0,I=0;l.sort(BF);for(let T=0,L=l.length;T<L;T++){let C=l[T],U=C.color,j=C.intensity,q=C.distance,P=null;if(C.shadow&&C.shadow.map&&(C.shadow.map.texture.format===ss?P=C.shadow.map.texture:P=C.shadow.map.depthTexture||C.shadow.map.texture),C.isAmbientLight)u+=U.r*j,d+=U.g*j,f+=U.b*j;else if(C.isLightProbe){for(let z=0;z<9;z++)i.probe[z].addScaledVector(C.sh.coefficients[z],j);I++}else if(C.isDirectionalLight){let z=e.get(C);if(z.color.copy(C.color).multiplyScalar(C.intensity),C.castShadow){let k=C.shadow,Q=t.get(C);Q.shadowIntensity=k.intensity,Q.shadowBias=k.bias,Q.shadowNormalBias=k.normalBias,Q.shadowRadius=k.radius,Q.shadowMapSize=k.mapSize,i.directionalShadow[h]=Q,i.directionalShadowMap[h]=P,i.directionalShadowMatrix[h]=C.shadow.matrix,b++}i.directional[h]=z,h++}else if(C.isSpotLight){let z=e.get(C);z.position.setFromMatrixPosition(C.matrixWorld),z.color.copy(U).multiplyScalar(j),z.distance=q,z.coneCos=Math.cos(C.angle),z.penumbraCos=Math.cos(C.angle*(1-C.penumbra)),z.decay=C.decay,i.spot[y]=z;let k=C.shadow;if(C.map&&(i.spotLightMap[D]=C.map,D++,k.updateMatrices(C),C.castShadow&&w++),i.spotLightMatrix[y]=k.matrix,C.castShadow){let Q=t.get(C);Q.shadowIntensity=k.intensity,Q.shadowBias=k.bias,Q.shadowNormalBias=k.normalBias,Q.shadowRadius=k.radius,Q.shadowMapSize=k.mapSize,i.spotShadow[y]=Q,i.spotShadowMap[y]=P,E++}y++}else if(C.isRectAreaLight){let z=e.get(C);z.color.copy(U).multiplyScalar(j),z.halfWidth.set(C.width*.5,0,0),z.halfHeight.set(0,C.height*.5,0),i.rectArea[m]=z,m++}else if(C.isPointLight){let z=e.get(C);if(z.color.copy(C.color).multiplyScalar(C.intensity),z.distance=C.distance,z.decay=C.decay,C.castShadow){let k=C.shadow,Q=t.get(C);Q.shadowIntensity=k.intensity,Q.shadowBias=k.bias,Q.shadowNormalBias=k.normalBias,Q.shadowRadius=k.radius,Q.shadowMapSize=k.mapSize,Q.shadowCameraNear=k.camera.near,Q.shadowCameraFar=k.camera.far,i.pointShadow[g]=Q,i.pointShadowMap[g]=P,i.pointShadowMatrix[g]=C.shadow.matrix,M++}i.point[g]=z,g++}else if(C.isHemisphereLight){let z=e.get(C);z.skyColor.copy(C.color).multiplyScalar(j),z.groundColor.copy(C.groundColor).multiplyScalar(j),i.hemi[p]=z,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=le.LTC_FLOAT_1,i.rectAreaLTC2=le.LTC_FLOAT_2):(i.rectAreaLTC1=le.LTC_HALF_1,i.rectAreaLTC2=le.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let _=i.hash;(_.directionalLength!==h||_.pointLength!==g||_.spotLength!==y||_.rectAreaLength!==m||_.hemiLength!==p||_.numDirectionalShadows!==b||_.numPointShadows!==M||_.numSpotShadows!==E||_.numSpotMaps!==D||_.numLightProbes!==I)&&(i.directional.length=h,i.spot.length=y,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=b,i.directionalShadowMap.length=b,i.pointShadow.length=M,i.pointShadowMap.length=M,i.spotShadow.length=E,i.spotShadowMap.length=E,i.directionalShadowMatrix.length=b,i.pointShadowMatrix.length=M,i.spotLightMatrix.length=E+D-w,i.spotLightMap.length=D,i.numSpotLightShadowsWithMaps=w,i.numLightProbes=I,_.directionalLength=h,_.pointLength=g,_.spotLength=y,_.rectAreaLength=m,_.hemiLength=p,_.numDirectionalShadows=b,_.numPointShadows=M,_.numSpotShadows=E,_.numSpotMaps=D,_.numLightProbes=I,i.version=UF++)}function c(l,u){let d=0,f=0,h=0,g=0,y=0,m=u.matrixWorldInverse;for(let p=0,b=l.length;p<b;p++){let M=l[p];if(M.isDirectionalLight){let E=i.directional[d];E.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),d++}else if(M.isSpotLight){let E=i.spot[h];E.position.setFromMatrixPosition(M.matrixWorld),E.position.applyMatrix4(m),E.direction.setFromMatrixPosition(M.matrixWorld),r.setFromMatrixPosition(M.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),h++}else if(M.isRectAreaLight){let E=i.rectArea[g];E.position.setFromMatrixPosition(M.matrixWorld),E.position.applyMatrix4(m),o.identity(),s.copy(M.matrixWorld),s.premultiply(m),o.extractRotation(s),E.halfWidth.set(M.width*.5,0,0),E.halfHeight.set(0,M.height*.5,0),E.halfWidth.applyMatrix4(o),E.halfHeight.applyMatrix4(o),g++}else if(M.isPointLight){let E=i.point[f];E.position.setFromMatrixPosition(M.matrixWorld),E.position.applyMatrix4(m),f++}else if(M.isHemisphereLight){let E=i.hemi[y];E.direction.setFromMatrixPosition(M.matrixWorld),E.direction.transformDirection(m),y++}}}return{setup:a,setupView:c,state:i}}function CE(n){let e=new VF(n),t=[],i=[],r=[];function s(f){d.camera=f,t.length=0,i.length=0,r.length=0}function o(f){t.push(f)}function a(f){i.push(f)}function c(f){r.push(f)}function l(){e.setup(t)}function u(f){e.setupView(t,f)}let d={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:d,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function HF(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new CE(n),e.set(r,[a])):s>=o.length?(a=new CE(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var zF=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,GF=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,jF=[new O(1,0,0),new O(-1,0,0),new O(0,1,0),new O(0,-1,0),new O(0,0,1),new O(0,0,-1)],WF=[new O(0,-1,0),new O(0,-1,0),new O(0,0,1),new O(0,0,-1),new O(0,-1,0),new O(0,-1,0)],DE=new It,vl=new O,Ly=new O;function $F(n,e,t){let i=new ha,r=new ot,s=new ot,o=new At,a=new Bf,c=new Vf,l={},u=t.maxTextureSize,d={[hr]:Dn,[Dn]:hr,[Hi]:Hi},f=new Gn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new ot},radius:{value:4}},vertexShader:zF,fragmentShader:GF}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let g=new Rn;g.setAttribute("position",new Hn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let y=new pt(g,f),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=ol;let p=this.type;this.render=function(w,I,_){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||w.length===0)return;this.type===Jf&&(De("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=ol);let T=n.getRenderTarget(),L=n.getActiveCubeFace(),C=n.getActiveMipmapLevel(),U=n.state;U.setBlending(zi),U.buffers.depth.getReversed()===!0?U.buffers.color.setClear(0,0,0,0):U.buffers.color.setClear(1,1,1,1),U.buffers.depth.setTest(!0),U.setScissorTest(!1);let j=p!==this.type;j&&I.traverse(function(q){q.material&&(Array.isArray(q.material)?q.material.forEach(P=>P.needsUpdate=!0):q.material.needsUpdate=!0)});for(let q=0,P=w.length;q<P;q++){let z=w[q],k=z.shadow;if(k===void 0){De("WebGLShadowMap:",z,"has no shadow.");continue}if(k.autoUpdate===!1&&k.needsUpdate===!1)continue;r.copy(k.mapSize);let Q=k.getFrameExtents();r.multiply(Q),s.copy(k.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/Q.x),r.x=s.x*Q.x,k.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/Q.y),r.y=s.y*Q.y,k.mapSize.y=s.y));let ee=n.state.buffers.depth.getReversed();if(k.camera._reversedDepth=ee,k.map===null||j===!0){if(k.map!==null&&(k.map.depthTexture!==null&&(k.map.depthTexture.dispose(),k.map.depthTexture=null),k.map.dispose()),this.type===va){if(z.isPointLight){De("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}k.map=new zn(r.x,r.y,{format:ss,type:Gi,minFilter:hn,magFilter:hn,generateMipmaps:!1}),k.map.texture.name=z.name+".shadowMap",k.map.depthTexture=new gr(r.x,r.y,Mi),k.map.depthTexture.name=z.name+".shadowMapDepth",k.map.depthTexture.format=Fi,k.map.depthTexture.compareFunction=null,k.map.depthTexture.minFilter=sn,k.map.depthTexture.magFilter=sn}else z.isPointLight?(k.map=new jh(r.x),k.map.depthTexture=new kf(r.x,Si)):(k.map=new zn(r.x,r.y),k.map.depthTexture=new gr(r.x,r.y,Si)),k.map.depthTexture.name=z.name+".shadowMap",k.map.depthTexture.format=Fi,this.type===ol?(k.map.depthTexture.compareFunction=ee?Vh:Bh,k.map.depthTexture.minFilter=hn,k.map.depthTexture.magFilter=hn):(k.map.depthTexture.compareFunction=null,k.map.depthTexture.minFilter=sn,k.map.depthTexture.magFilter=sn);k.camera.updateProjectionMatrix()}let ue=k.map.isWebGLCubeRenderTarget?6:1;for(let xe=0;xe<ue;xe++){if(k.map.isWebGLCubeRenderTarget)n.setRenderTarget(k.map,xe),n.clear();else{xe===0&&(n.setRenderTarget(k.map),n.clear());let Te=k.getViewport(xe);o.set(s.x*Te.x,s.y*Te.y,s.x*Te.z,s.y*Te.w),U.viewport(o)}if(z.isPointLight){let Te=k.camera,nt=k.matrix,mt=z.distance||Te.far;mt!==Te.far&&(Te.far=mt,Te.updateProjectionMatrix()),vl.setFromMatrixPosition(z.matrixWorld),Te.position.copy(vl),Ly.copy(Te.position),Ly.add(jF[xe]),Te.up.copy(WF[xe]),Te.lookAt(Ly),Te.updateMatrixWorld(),nt.makeTranslation(-vl.x,-vl.y,-vl.z),DE.multiplyMatrices(Te.projectionMatrix,Te.matrixWorldInverse),k._frustum.setFromProjectionMatrix(DE,Te.coordinateSystem,Te.reversedDepth)}else k.updateMatrices(z);i=k.getFrustum(),E(I,_,k.camera,z,this.type)}k.isPointLightShadow!==!0&&this.type===va&&b(k,_),k.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(T,L,C)};function b(w,I){let _=e.update(y);f.defines.VSM_SAMPLES!==w.blurSamples&&(f.defines.VSM_SAMPLES=w.blurSamples,h.defines.VSM_SAMPLES=w.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),w.mapPass===null&&(w.mapPass=new zn(r.x,r.y,{format:ss,type:Gi})),f.uniforms.shadow_pass.value=w.map.depthTexture,f.uniforms.resolution.value=w.mapSize,f.uniforms.radius.value=w.radius,n.setRenderTarget(w.mapPass),n.clear(),n.renderBufferDirect(I,null,_,f,y,null),h.uniforms.shadow_pass.value=w.mapPass.texture,h.uniforms.resolution.value=w.mapSize,h.uniforms.radius.value=w.radius,n.setRenderTarget(w.map),n.clear(),n.renderBufferDirect(I,null,_,h,y,null)}function M(w,I,_,T){let L=null,C=_.isPointLight===!0?w.customDistanceMaterial:w.customDepthMaterial;if(C!==void 0)L=C;else if(L=_.isPointLight===!0?c:a,n.localClippingEnabled&&I.clipShadows===!0&&Array.isArray(I.clippingPlanes)&&I.clippingPlanes.length!==0||I.displacementMap&&I.displacementScale!==0||I.alphaMap&&I.alphaTest>0||I.map&&I.alphaTest>0||I.alphaToCoverage===!0){let U=L.uuid,j=I.uuid,q=l[U];q===void 0&&(q={},l[U]=q);let P=q[j];P===void 0&&(P=L.clone(),q[j]=P,I.addEventListener("dispose",D)),L=P}if(L.visible=I.visible,L.wireframe=I.wireframe,T===va?L.side=I.shadowSide!==null?I.shadowSide:I.side:L.side=I.shadowSide!==null?I.shadowSide:d[I.side],L.alphaMap=I.alphaMap,L.alphaTest=I.alphaToCoverage===!0?.5:I.alphaTest,L.map=I.map,L.clipShadows=I.clipShadows,L.clippingPlanes=I.clippingPlanes,L.clipIntersection=I.clipIntersection,L.displacementMap=I.displacementMap,L.displacementScale=I.displacementScale,L.displacementBias=I.displacementBias,L.wireframeLinewidth=I.wireframeLinewidth,L.linewidth=I.linewidth,_.isPointLight===!0&&L.isMeshDistanceMaterial===!0){let U=n.properties.get(L);U.light=_}return L}function E(w,I,_,T,L){if(w.visible===!1)return;if(w.layers.test(I.layers)&&(w.isMesh||w.isLine||w.isPoints)&&(w.castShadow||w.receiveShadow&&L===va)&&(!w.frustumCulled||i.intersectsObject(w))){w.modelViewMatrix.multiplyMatrices(_.matrixWorldInverse,w.matrixWorld);let j=e.update(w),q=w.material;if(Array.isArray(q)){let P=j.groups;for(let z=0,k=P.length;z<k;z++){let Q=P[z],ee=q[Q.materialIndex];if(ee&&ee.visible){let ue=M(w,ee,T,L);w.onBeforeShadow(n,w,I,_,j,ue,Q),n.renderBufferDirect(_,null,j,ue,w,Q),w.onAfterShadow(n,w,I,_,j,ue,Q)}}}else if(q.visible){let P=M(w,q,T,L);w.onBeforeShadow(n,w,I,_,j,P,null),n.renderBufferDirect(_,null,j,P,w,null),w.onAfterShadow(n,w,I,_,j,P,null)}}let U=w.children;for(let j=0,q=U.length;j<q;j++)E(U[j],I,_,T,L)}function D(w){w.target.removeEventListener("dispose",D);for(let _ in l){let T=l[_],L=w.target.uuid;L in T&&(T[L].dispose(),delete T[L])}}}function XF(n,e){function t(){let A=!1,ie=new At,W=null,ge=new At(0,0,0,0);return{setMask:function(oe){W!==oe&&!A&&(n.colorMask(oe,oe,oe,oe),W=oe)},setLocked:function(oe){A=oe},setClear:function(oe,Z,Se,Be,Bt){Bt===!0&&(oe*=Be,Z*=Be,Se*=Be),ie.set(oe,Z,Se,Be),ge.equals(ie)===!1&&(n.clearColor(oe,Z,Se,Be),ge.copy(ie))},reset:function(){A=!1,W=null,ge.set(-1,0,0,0)}}}function i(){let A=!1,ie=!1,W=null,ge=null,oe=null;return{setReversed:function(Z){if(ie!==Z){let Se=e.get("EXT_clip_control");Z?Se.clipControlEXT(Se.LOWER_LEFT_EXT,Se.ZERO_TO_ONE_EXT):Se.clipControlEXT(Se.LOWER_LEFT_EXT,Se.NEGATIVE_ONE_TO_ONE_EXT),ie=Z;let Be=oe;oe=null,this.setClear(Be)}},getReversed:function(){return ie},setTest:function(Z){Z?re(n.DEPTH_TEST):Ae(n.DEPTH_TEST)},setMask:function(Z){W!==Z&&!A&&(n.depthMask(Z),W=Z)},setFunc:function(Z){if(ie&&(Z=rE[Z]),ge!==Z){switch(Z){case _f:n.depthFunc(n.NEVER);break;case xf:n.depthFunc(n.ALWAYS);break;case bf:n.depthFunc(n.LESS);break;case Ys:n.depthFunc(n.LEQUAL);break;case Sf:n.depthFunc(n.EQUAL);break;case Mf:n.depthFunc(n.GEQUAL);break;case Ef:n.depthFunc(n.GREATER);break;case wf:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}ge=Z}},setLocked:function(Z){A=Z},setClear:function(Z){oe!==Z&&(oe=Z,ie&&(Z=1-Z),n.clearDepth(Z))},reset:function(){A=!1,W=null,ge=null,oe=null,ie=!1}}}function r(){let A=!1,ie=null,W=null,ge=null,oe=null,Z=null,Se=null,Be=null,Bt=null;return{setTest:function(vt){A||(vt?re(n.STENCIL_TEST):Ae(n.STENCIL_TEST))},setMask:function(vt){ie!==vt&&!A&&(n.stencilMask(vt),ie=vt)},setFunc:function(vt,$i,Ei){(W!==vt||ge!==$i||oe!==Ei)&&(n.stencilFunc(vt,$i,Ei),W=vt,ge=$i,oe=Ei)},setOp:function(vt,$i,Ei){(Z!==vt||Se!==$i||Be!==Ei)&&(n.stencilOp(vt,$i,Ei),Z=vt,Se=$i,Be=Ei)},setLocked:function(vt){A=vt},setClear:function(vt){Bt!==vt&&(n.clearStencil(vt),Bt=vt)},reset:function(){A=!1,ie=null,W=null,ge=null,oe=null,Z=null,Se=null,Be=null,Bt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f={},h=new WeakMap,g=[],y=null,m=!1,p=null,b=null,M=null,E=null,D=null,w=null,I=null,_=new qe(0,0,0),T=0,L=!1,C=null,U=null,j=null,q=null,P=null,z=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),k=!1,Q=0,ee=n.getParameter(n.VERSION);ee.indexOf("WebGL")!==-1?(Q=parseFloat(/^WebGL (\d)/.exec(ee)[1]),k=Q>=1):ee.indexOf("OpenGL ES")!==-1&&(Q=parseFloat(/^OpenGL ES (\d)/.exec(ee)[1]),k=Q>=2);let ue=null,xe={},Te=n.getParameter(n.SCISSOR_BOX),nt=n.getParameter(n.VIEWPORT),mt=new At().fromArray(Te),Ge=new At().fromArray(nt);function Y(A,ie,W,ge){let oe=new Uint8Array(4),Z=n.createTexture();n.bindTexture(A,Z),n.texParameteri(A,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(A,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Se=0;Se<W;Se++)A===n.TEXTURE_3D||A===n.TEXTURE_2D_ARRAY?n.texImage3D(ie,0,n.RGBA,1,1,ge,0,n.RGBA,n.UNSIGNED_BYTE,oe):n.texImage2D(ie+Se,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,oe);return Z}let pe={};pe[n.TEXTURE_2D]=Y(n.TEXTURE_2D,n.TEXTURE_2D,1),pe[n.TEXTURE_CUBE_MAP]=Y(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),pe[n.TEXTURE_2D_ARRAY]=Y(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),pe[n.TEXTURE_3D]=Y(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),re(n.DEPTH_TEST),o.setFunc(Ys),jt(!1),Dt(sy),re(n.CULL_FACE),Tt(zi);function re(A){u[A]!==!0&&(n.enable(A),u[A]=!0)}function Ae(A){u[A]!==!1&&(n.disable(A),u[A]=!1)}function ke(A,ie){return f[A]!==ie?(n.bindFramebuffer(A,ie),f[A]=ie,A===n.DRAW_FRAMEBUFFER&&(f[n.FRAMEBUFFER]=ie),A===n.FRAMEBUFFER&&(f[n.DRAW_FRAMEBUFFER]=ie),!0):!1}function Ne(A,ie){let W=g,ge=!1;if(A){W=h.get(ie),W===void 0&&(W=[],h.set(ie,W));let oe=A.textures;if(W.length!==oe.length||W[0]!==n.COLOR_ATTACHMENT0){for(let Z=0,Se=oe.length;Z<Se;Z++)W[Z]=n.COLOR_ATTACHMENT0+Z;W.length=oe.length,ge=!0}}else W[0]!==n.BACK&&(W[0]=n.BACK,ge=!0);ge&&n.drawBuffers(W)}function Ct(A){return y!==A?(n.useProgram(A),y=A,!0):!1}let Ze={[Jr]:n.FUNC_ADD,[CM]:n.FUNC_SUBTRACT,[DM]:n.FUNC_REVERSE_SUBTRACT};Ze[IM]=n.MIN,Ze[AM]=n.MAX;let gt={[RM]:n.ZERO,[NM]:n.ONE,[PM]:n.SRC_COLOR,[vf]:n.SRC_ALPHA,[BM]:n.SRC_ALPHA_SATURATE,[kM]:n.DST_COLOR,[LM]:n.DST_ALPHA,[OM]:n.ONE_MINUS_SRC_COLOR,[yf]:n.ONE_MINUS_SRC_ALPHA,[UM]:n.ONE_MINUS_DST_COLOR,[FM]:n.ONE_MINUS_DST_ALPHA,[VM]:n.CONSTANT_COLOR,[HM]:n.ONE_MINUS_CONSTANT_COLOR,[zM]:n.CONSTANT_ALPHA,[GM]:n.ONE_MINUS_CONSTANT_ALPHA};function Tt(A,ie,W,ge,oe,Z,Se,Be,Bt,vt){if(A===zi){m===!0&&(Ae(n.BLEND),m=!1);return}if(m===!1&&(re(n.BLEND),m=!0),A!==TM){if(A!==p||vt!==L){if((b!==Jr||D!==Jr)&&(n.blendEquation(n.FUNC_ADD),b=Jr,D=Jr),vt)switch(A){case qs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case oy:n.blendFunc(n.ONE,n.ONE);break;case ay:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case cy:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Re("WebGLState: Invalid blending: ",A);break}else switch(A){case qs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case oy:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case ay:Re("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case cy:Re("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Re("WebGLState: Invalid blending: ",A);break}M=null,E=null,w=null,I=null,_.set(0,0,0),T=0,p=A,L=vt}return}oe=oe||ie,Z=Z||W,Se=Se||ge,(ie!==b||oe!==D)&&(n.blendEquationSeparate(Ze[ie],Ze[oe]),b=ie,D=oe),(W!==M||ge!==E||Z!==w||Se!==I)&&(n.blendFuncSeparate(gt[W],gt[ge],gt[Z],gt[Se]),M=W,E=ge,w=Z,I=Se),(Be.equals(_)===!1||Bt!==T)&&(n.blendColor(Be.r,Be.g,Be.b,Bt),_.copy(Be),T=Bt),p=A,L=!1}function Ye(A,ie){A.side===Hi?Ae(n.CULL_FACE):re(n.CULL_FACE);let W=A.side===Dn;ie&&(W=!W),jt(W),A.blending===qs&&A.transparent===!1?Tt(zi):Tt(A.blending,A.blendEquation,A.blendSrc,A.blendDst,A.blendEquationAlpha,A.blendSrcAlpha,A.blendDstAlpha,A.blendColor,A.blendAlpha,A.premultipliedAlpha),o.setFunc(A.depthFunc),o.setTest(A.depthTest),o.setMask(A.depthWrite),s.setMask(A.colorWrite);let ge=A.stencilWrite;a.setTest(ge),ge&&(a.setMask(A.stencilWriteMask),a.setFunc(A.stencilFunc,A.stencilRef,A.stencilFuncMask),a.setOp(A.stencilFail,A.stencilZFail,A.stencilZPass)),R(A.polygonOffset,A.polygonOffsetFactor,A.polygonOffsetUnits),A.alphaToCoverage===!0?re(n.SAMPLE_ALPHA_TO_COVERAGE):Ae(n.SAMPLE_ALPHA_TO_COVERAGE)}function jt(A){C!==A&&(A?n.frontFace(n.CW):n.frontFace(n.CCW),C=A)}function Dt(A){A!==EM?(re(n.CULL_FACE),A!==U&&(A===sy?n.cullFace(n.BACK):A===wM?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Ae(n.CULL_FACE),U=A}function Pn(A){A!==j&&(k&&n.lineWidth(A),j=A)}function R(A,ie,W){A?(re(n.POLYGON_OFFSET_FILL),(q!==ie||P!==W)&&(q=ie,P=W,o.getReversed()&&(ie=-ie),n.polygonOffset(ie,W))):Ae(n.POLYGON_OFFSET_FILL)}function Wt(A){A?re(n.SCISSOR_TEST):Ae(n.SCISSOR_TEST)}function Ke(A){A===void 0&&(A=n.TEXTURE0+z-1),ue!==A&&(n.activeTexture(A),ue=A)}function Et(A,ie,W){W===void 0&&(ue===null?W=n.TEXTURE0+z-1:W=ue);let ge=xe[W];ge===void 0&&(ge={type:void 0,texture:void 0},xe[W]=ge),(ge.type!==A||ge.texture!==ie)&&(ue!==W&&(n.activeTexture(W),ue=W),n.bindTexture(A,ie||pe[A]),ge.type=A,ge.texture=ie)}function ce(){let A=xe[ue];A!==void 0&&A.type!==void 0&&(n.bindTexture(A.type,null),A.type=void 0,A.texture=void 0)}function Rt(){try{n.compressedTexImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function S(){try{n.compressedTexImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function v(){try{n.texSubImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function F(){try{n.texSubImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function $(){try{n.compressedTexSubImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function K(){try{n.compressedTexSubImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function te(){try{n.texStorage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function ae(){try{n.texStorage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function G(){try{n.texImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function X(){try{n.texImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function me(A){return d[A]!==void 0?d[A]:n.getParameter(A)}function ye(A,ie){d[A]!==ie&&(n.pixelStorei(A,ie),d[A]=ie)}function se(A){mt.equals(A)===!1&&(n.scissor(A.x,A.y,A.z,A.w),mt.copy(A))}function ne(A){Ge.equals(A)===!1&&(n.viewport(A.x,A.y,A.z,A.w),Ge.copy(A))}function Oe(A,ie){let W=l.get(ie);W===void 0&&(W=new WeakMap,l.set(ie,W));let ge=W.get(A);ge===void 0&&(ge=n.getUniformBlockIndex(ie,A.name),W.set(A,ge))}function je(A,ie){let ge=l.get(ie).get(A);c.get(ie)!==ge&&(n.uniformBlockBinding(ie,ge,A.__bindingPointIndex),c.set(ie,ge))}function rt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},d={},ue=null,xe={},f={},h=new WeakMap,g=[],y=null,m=!1,p=null,b=null,M=null,E=null,D=null,w=null,I=null,_=new qe(0,0,0),T=0,L=!1,C=null,U=null,j=null,q=null,P=null,mt.set(0,0,n.canvas.width,n.canvas.height),Ge.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:re,disable:Ae,bindFramebuffer:ke,drawBuffers:Ne,useProgram:Ct,setBlending:Tt,setMaterial:Ye,setFlipSided:jt,setCullFace:Dt,setLineWidth:Pn,setPolygonOffset:R,setScissorTest:Wt,activeTexture:Ke,bindTexture:Et,unbindTexture:ce,compressedTexImage2D:Rt,compressedTexImage3D:S,texImage2D:G,texImage3D:X,pixelStorei:ye,getParameter:me,updateUBOMapping:Oe,uniformBlockBinding:je,texStorage2D:te,texStorage3D:ae,texSubImage2D:v,texSubImage3D:F,compressedTexSubImage2D:$,compressedTexSubImage3D:K,scissor:se,viewport:ne,reset:rt}}function qF(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new ot,u=new WeakMap,d=new Set,f,h=new WeakMap,g=!1;try{g=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch(S){}function y(S,v){return g?new OffscreenCanvas(S,v):Gc("canvas")}function m(S,v,F){let $=1,K=Rt(S);if((K.width>F||K.height>F)&&($=F/Math.max(K.width,K.height)),$<1)if(typeof HTMLImageElement<"u"&&S instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&S instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&S instanceof ImageBitmap||typeof VideoFrame<"u"&&S instanceof VideoFrame){let te=Math.floor($*K.width),ae=Math.floor($*K.height);f===void 0&&(f=y(te,ae));let G=v?y(te,ae):f;return G.width=te,G.height=ae,G.getContext("2d").drawImage(S,0,0,te,ae),De("WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+te+"x"+ae+")."),G}else return"data"in S&&De("WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),S;return S}function p(S){return S.generateMipmaps}function b(S){n.generateMipmap(S)}function M(S){return S.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:S.isWebGL3DRenderTarget?n.TEXTURE_3D:S.isWebGLArrayRenderTarget||S.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function E(S,v,F,$,K,te=!1){if(S!==null){if(n[S]!==void 0)return n[S];De("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+S+"'")}let ae;$&&(ae=e.get("EXT_texture_norm16"),ae||De("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let G=v;if(v===n.RED&&(F===n.FLOAT&&(G=n.R32F),F===n.HALF_FLOAT&&(G=n.R16F),F===n.UNSIGNED_BYTE&&(G=n.R8),F===n.UNSIGNED_SHORT&&ae&&(G=ae.R16_EXT),F===n.SHORT&&ae&&(G=ae.R16_SNORM_EXT)),v===n.RED_INTEGER&&(F===n.UNSIGNED_BYTE&&(G=n.R8UI),F===n.UNSIGNED_SHORT&&(G=n.R16UI),F===n.UNSIGNED_INT&&(G=n.R32UI),F===n.BYTE&&(G=n.R8I),F===n.SHORT&&(G=n.R16I),F===n.INT&&(G=n.R32I)),v===n.RG&&(F===n.FLOAT&&(G=n.RG32F),F===n.HALF_FLOAT&&(G=n.RG16F),F===n.UNSIGNED_BYTE&&(G=n.RG8),F===n.UNSIGNED_SHORT&&ae&&(G=ae.RG16_EXT),F===n.SHORT&&ae&&(G=ae.RG16_SNORM_EXT)),v===n.RG_INTEGER&&(F===n.UNSIGNED_BYTE&&(G=n.RG8UI),F===n.UNSIGNED_SHORT&&(G=n.RG16UI),F===n.UNSIGNED_INT&&(G=n.RG32UI),F===n.BYTE&&(G=n.RG8I),F===n.SHORT&&(G=n.RG16I),F===n.INT&&(G=n.RG32I)),v===n.RGB_INTEGER&&(F===n.UNSIGNED_BYTE&&(G=n.RGB8UI),F===n.UNSIGNED_SHORT&&(G=n.RGB16UI),F===n.UNSIGNED_INT&&(G=n.RGB32UI),F===n.BYTE&&(G=n.RGB8I),F===n.SHORT&&(G=n.RGB16I),F===n.INT&&(G=n.RGB32I)),v===n.RGBA_INTEGER&&(F===n.UNSIGNED_BYTE&&(G=n.RGBA8UI),F===n.UNSIGNED_SHORT&&(G=n.RGBA16UI),F===n.UNSIGNED_INT&&(G=n.RGBA32UI),F===n.BYTE&&(G=n.RGBA8I),F===n.SHORT&&(G=n.RGBA16I),F===n.INT&&(G=n.RGBA32I)),v===n.RGB&&(F===n.UNSIGNED_SHORT&&ae&&(G=ae.RGB16_EXT),F===n.SHORT&&ae&&(G=ae.RGB16_SNORM_EXT),F===n.UNSIGNED_INT_5_9_9_9_REV&&(G=n.RGB9_E5),F===n.UNSIGNED_INT_10F_11F_11F_REV&&(G=n.R11F_G11F_B10F)),v===n.RGBA){let X=te?zc:Qe.getTransfer(K);F===n.FLOAT&&(G=n.RGBA32F),F===n.HALF_FLOAT&&(G=n.RGBA16F),F===n.UNSIGNED_BYTE&&(G=X===ft?n.SRGB8_ALPHA8:n.RGBA8),F===n.UNSIGNED_SHORT&&ae&&(G=ae.RGBA16_EXT),F===n.SHORT&&ae&&(G=ae.RGBA16_SNORM_EXT),F===n.UNSIGNED_SHORT_4_4_4_4&&(G=n.RGBA4),F===n.UNSIGNED_SHORT_5_5_5_1&&(G=n.RGB5_A1)}return(G===n.R16F||G===n.R32F||G===n.RG16F||G===n.RG32F||G===n.RGBA16F||G===n.RGBA32F)&&e.get("EXT_color_buffer_float"),G}function D(S,v){let F;return S?v===null||v===Si||v===_a?F=n.DEPTH24_STENCIL8:v===Mi?F=n.DEPTH32F_STENCIL8:v===ya&&(F=n.DEPTH24_STENCIL8,De("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===Si||v===_a?F=n.DEPTH_COMPONENT24:v===Mi?F=n.DEPTH_COMPONENT32F:v===ya&&(F=n.DEPTH_COMPONENT16),F}function w(S,v){return p(S)===!0||S.isFramebufferTexture&&S.minFilter!==sn&&S.minFilter!==hn?Math.log2(Math.max(v.width,v.height))+1:S.mipmaps!==void 0&&S.mipmaps.length>0?S.mipmaps.length:S.isCompressedTexture&&Array.isArray(S.image)?v.mipmaps.length:1}function I(S){let v=S.target;v.removeEventListener("dispose",I),T(v),v.isVideoTexture&&u.delete(v),v.isHTMLTexture&&d.delete(v)}function _(S){let v=S.target;v.removeEventListener("dispose",_),C(v)}function T(S){let v=i.get(S);if(v.__webglInit===void 0)return;let F=S.source,$=h.get(F);if($){let K=$[v.__cacheKey];K.usedTimes--,K.usedTimes===0&&L(S),Object.keys($).length===0&&h.delete(F)}i.remove(S)}function L(S){let v=i.get(S);n.deleteTexture(v.__webglTexture);let F=S.source,$=h.get(F);delete $[v.__cacheKey],o.memory.textures--}function C(S){let v=i.get(S);if(S.depthTexture&&(S.depthTexture.dispose(),i.remove(S.depthTexture)),S.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(v.__webglFramebuffer[$]))for(let K=0;K<v.__webglFramebuffer[$].length;K++)n.deleteFramebuffer(v.__webglFramebuffer[$][K]);else n.deleteFramebuffer(v.__webglFramebuffer[$]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[$])}else{if(Array.isArray(v.__webglFramebuffer))for(let $=0;$<v.__webglFramebuffer.length;$++)n.deleteFramebuffer(v.__webglFramebuffer[$]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let $=0;$<v.__webglColorRenderbuffer.length;$++)v.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[$]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let F=S.textures;for(let $=0,K=F.length;$<K;$++){let te=i.get(F[$]);te.__webglTexture&&(n.deleteTexture(te.__webglTexture),o.memory.textures--),i.remove(F[$])}i.remove(S)}let U=0;function j(){U=0}function q(){return U}function P(S){U=S}function z(){let S=U;return S>=r.maxTextures&&De("WebGLTextures: Trying to use "+S+" texture units while this GPU supports only "+r.maxTextures),U+=1,S}function k(S){let v=[];return v.push(S.wrapS),v.push(S.wrapT),v.push(S.wrapR||0),v.push(S.magFilter),v.push(S.minFilter),v.push(S.anisotropy),v.push(S.internalFormat),v.push(S.format),v.push(S.type),v.push(S.generateMipmaps),v.push(S.premultiplyAlpha),v.push(S.flipY),v.push(S.unpackAlignment),v.push(S.colorSpace),v.join()}function Q(S,v){let F=i.get(S);if(S.isVideoTexture&&Et(S),S.isRenderTargetTexture===!1&&S.isExternalTexture!==!0&&S.version>0&&F.__version!==S.version){let $=S.image;if($===null)De("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)De("WebGLRenderer: Texture marked for update but image is incomplete");else{Ae(F,S,v);return}}else S.isExternalTexture&&(F.__webglTexture=S.sourceTexture?S.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,F.__webglTexture,n.TEXTURE0+v)}function ee(S,v){let F=i.get(S);if(S.isRenderTargetTexture===!1&&S.version>0&&F.__version!==S.version){Ae(F,S,v);return}else S.isExternalTexture&&(F.__webglTexture=S.sourceTexture?S.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,F.__webglTexture,n.TEXTURE0+v)}function ue(S,v){let F=i.get(S);if(S.isRenderTargetTexture===!1&&S.version>0&&F.__version!==S.version){Ae(F,S,v);return}t.bindTexture(n.TEXTURE_3D,F.__webglTexture,n.TEXTURE0+v)}function xe(S,v){let F=i.get(S);if(S.isCubeDepthTexture!==!0&&S.version>0&&F.__version!==S.version){ke(F,S,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,F.__webglTexture,n.TEXTURE0+v)}let Te={[Tf]:n.REPEAT,[Li]:n.CLAMP_TO_EDGE,[Cf]:n.MIRRORED_REPEAT},nt={[sn]:n.NEAREST,[$M]:n.NEAREST_MIPMAP_NEAREST,[ll]:n.NEAREST_MIPMAP_LINEAR,[hn]:n.LINEAR,[th]:n.LINEAR_MIPMAP_NEAREST,[is]:n.LINEAR_MIPMAP_LINEAR},mt={[YM]:n.NEVER,[eE]:n.ALWAYS,[ZM]:n.LESS,[Bh]:n.LEQUAL,[KM]:n.EQUAL,[Vh]:n.GEQUAL,[JM]:n.GREATER,[QM]:n.NOTEQUAL};function Ge(S,v){if(v.type===Mi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===hn||v.magFilter===th||v.magFilter===ll||v.magFilter===is||v.minFilter===hn||v.minFilter===th||v.minFilter===ll||v.minFilter===is)&&De("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(S,n.TEXTURE_WRAP_S,Te[v.wrapS]),n.texParameteri(S,n.TEXTURE_WRAP_T,Te[v.wrapT]),(S===n.TEXTURE_3D||S===n.TEXTURE_2D_ARRAY)&&n.texParameteri(S,n.TEXTURE_WRAP_R,Te[v.wrapR]),n.texParameteri(S,n.TEXTURE_MAG_FILTER,nt[v.magFilter]),n.texParameteri(S,n.TEXTURE_MIN_FILTER,nt[v.minFilter]),v.compareFunction&&(n.texParameteri(S,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(S,n.TEXTURE_COMPARE_FUNC,mt[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===sn||v.minFilter!==ll&&v.minFilter!==is||v.type===Mi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let F=e.get("EXT_texture_filter_anisotropic");n.texParameterf(S,F.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function Y(S,v){let F=!1;S.__webglInit===void 0&&(S.__webglInit=!0,v.addEventListener("dispose",I));let $=v.source,K=h.get($);K===void 0&&(K={},h.set($,K));let te=k(v);if(te!==S.__cacheKey){K[te]===void 0&&(K[te]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,F=!0),K[te].usedTimes++;let ae=K[S.__cacheKey];ae!==void 0&&(K[S.__cacheKey].usedTimes--,ae.usedTimes===0&&L(v)),S.__cacheKey=te,S.__webglTexture=K[te].texture}return F}function pe(S,v,F){return Math.floor(Math.floor(S/F)/v)}function re(S,v,F,$){let te=S.updateRanges;if(te.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,F,$,v.data);else{te.sort((ye,se)=>ye.start-se.start);let ae=0;for(let ye=1;ye<te.length;ye++){let se=te[ae],ne=te[ye],Oe=se.start+se.count,je=pe(ne.start,v.width,4),rt=pe(se.start,v.width,4);ne.start<=Oe+1&&je===rt&&pe(ne.start+ne.count-1,v.width,4)===je?se.count=Math.max(se.count,ne.start+ne.count-se.start):(++ae,te[ae]=ne)}te.length=ae+1;let G=t.getParameter(n.UNPACK_ROW_LENGTH),X=t.getParameter(n.UNPACK_SKIP_PIXELS),me=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let ye=0,se=te.length;ye<se;ye++){let ne=te[ye],Oe=Math.floor(ne.start/4),je=Math.ceil(ne.count/4),rt=Oe%v.width,A=Math.floor(Oe/v.width),ie=je,W=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,rt),t.pixelStorei(n.UNPACK_SKIP_ROWS,A),t.texSubImage2D(n.TEXTURE_2D,0,rt,A,ie,W,F,$,v.data)}S.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,G),t.pixelStorei(n.UNPACK_SKIP_PIXELS,X),t.pixelStorei(n.UNPACK_SKIP_ROWS,me)}}function Ae(S,v,F){let $=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&($=n.TEXTURE_3D);let K=Y(S,v),te=v.source;t.bindTexture($,S.__webglTexture,n.TEXTURE0+F);let ae=i.get(te);if(te.version!==ae.__version||K===!0){if(t.activeTexture(n.TEXTURE0+F),(typeof ImageBitmap<"u"&&v.image instanceof ImageBitmap)===!1){let W=Qe.getPrimaries(Qe.workingColorSpace),ge=v.colorSpace===vr?null:Qe.getPrimaries(v.colorSpace),oe=v.colorSpace===vr||W===ge?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,oe)}t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment);let X=m(v.image,!1,r.maxTextureSize);X=ce(v,X);let me=s.convert(v.format,v.colorSpace),ye=s.convert(v.type),se=E(v.internalFormat,me,ye,v.normalized,v.colorSpace,v.isVideoTexture);Ge($,v);let ne,Oe=v.mipmaps,je=v.isVideoTexture!==!0,rt=ae.__version===void 0||K===!0,A=te.dataReady,ie=w(v,X);if(v.isDepthTexture)se=D(v.format===rs,v.type),rt&&(je?t.texStorage2D(n.TEXTURE_2D,1,se,X.width,X.height):t.texImage2D(n.TEXTURE_2D,0,se,X.width,X.height,0,me,ye,null));else if(v.isDataTexture)if(Oe.length>0){je&&rt&&t.texStorage2D(n.TEXTURE_2D,ie,se,Oe[0].width,Oe[0].height);for(let W=0,ge=Oe.length;W<ge;W++)ne=Oe[W],je?A&&t.texSubImage2D(n.TEXTURE_2D,W,0,0,ne.width,ne.height,me,ye,ne.data):t.texImage2D(n.TEXTURE_2D,W,se,ne.width,ne.height,0,me,ye,ne.data);v.generateMipmaps=!1}else je?(rt&&t.texStorage2D(n.TEXTURE_2D,ie,se,X.width,X.height),A&&re(v,X,me,ye)):t.texImage2D(n.TEXTURE_2D,0,se,X.width,X.height,0,me,ye,X.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){je&&rt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ie,se,Oe[0].width,Oe[0].height,X.depth);for(let W=0,ge=Oe.length;W<ge;W++)if(ne=Oe[W],v.format!==ai)if(me!==null)if(je){if(A)if(v.layerUpdates.size>0){let oe=Iy(ne.width,ne.height,v.format,v.type);for(let Z of v.layerUpdates){let Se=ne.data.subarray(Z*oe/ne.data.BYTES_PER_ELEMENT,(Z+1)*oe/ne.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,W,0,0,Z,ne.width,ne.height,1,me,Se)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,W,0,0,0,ne.width,ne.height,X.depth,me,ne.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,W,se,ne.width,ne.height,X.depth,0,ne.data,0,0);else De("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else je?A&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,W,0,0,0,ne.width,ne.height,X.depth,me,ye,ne.data):t.texImage3D(n.TEXTURE_2D_ARRAY,W,se,ne.width,ne.height,X.depth,0,me,ye,ne.data)}else{je&&rt&&t.texStorage2D(n.TEXTURE_2D,ie,se,Oe[0].width,Oe[0].height);for(let W=0,ge=Oe.length;W<ge;W++)ne=Oe[W],v.format!==ai?me!==null?je?A&&t.compressedTexSubImage2D(n.TEXTURE_2D,W,0,0,ne.width,ne.height,me,ne.data):t.compressedTexImage2D(n.TEXTURE_2D,W,se,ne.width,ne.height,0,ne.data):De("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):je?A&&t.texSubImage2D(n.TEXTURE_2D,W,0,0,ne.width,ne.height,me,ye,ne.data):t.texImage2D(n.TEXTURE_2D,W,se,ne.width,ne.height,0,me,ye,ne.data)}else if(v.isDataArrayTexture)if(je){if(rt&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ie,se,X.width,X.height,X.depth),A)if(v.layerUpdates.size>0){let W=Iy(X.width,X.height,v.format,v.type);for(let ge of v.layerUpdates){let oe=X.data.subarray(ge*W/X.data.BYTES_PER_ELEMENT,(ge+1)*W/X.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,ge,X.width,X.height,1,me,ye,oe)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,X.width,X.height,X.depth,me,ye,X.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,se,X.width,X.height,X.depth,0,me,ye,X.data);else if(v.isData3DTexture)je?(rt&&t.texStorage3D(n.TEXTURE_3D,ie,se,X.width,X.height,X.depth),A&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,X.width,X.height,X.depth,me,ye,X.data)):t.texImage3D(n.TEXTURE_3D,0,se,X.width,X.height,X.depth,0,me,ye,X.data);else if(v.isFramebufferTexture){if(rt)if(je)t.texStorage2D(n.TEXTURE_2D,ie,se,X.width,X.height);else{let W=X.width,ge=X.height;for(let oe=0;oe<ie;oe++)t.texImage2D(n.TEXTURE_2D,oe,se,W,ge,0,me,ye,null),W>>=1,ge>>=1}}else if(v.isHTMLTexture){if("texElementImage2D"in n){let W=n.canvas;if(W.hasAttribute("layoutsubtree")||W.setAttribute("layoutsubtree","true"),X.parentNode!==W){W.appendChild(X),d.add(v),W.onpaint=Be=>{let Bt=Be.changedElements;for(let vt of d)Bt.includes(vt.image)&&(vt.needsUpdate=!0)},W.requestPaint();return}let ge=0,oe=n.RGBA,Z=n.RGBA,Se=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,ge,oe,Z,Se,X),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(Oe.length>0){if(je&&rt){let W=Rt(Oe[0]);t.texStorage2D(n.TEXTURE_2D,ie,se,W.width,W.height)}for(let W=0,ge=Oe.length;W<ge;W++)ne=Oe[W],je?A&&t.texSubImage2D(n.TEXTURE_2D,W,0,0,me,ye,ne):t.texImage2D(n.TEXTURE_2D,W,se,me,ye,ne);v.generateMipmaps=!1}else if(je){if(rt){let W=Rt(X);t.texStorage2D(n.TEXTURE_2D,ie,se,W.width,W.height)}A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,me,ye,X)}else t.texImage2D(n.TEXTURE_2D,0,se,me,ye,X);p(v)&&b($),ae.__version=te.version,v.onUpdate&&v.onUpdate(v)}S.__version=v.version}function ke(S,v,F){if(v.image.length!==6)return;let $=Y(S,v),K=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,S.__webglTexture,n.TEXTURE0+F);let te=i.get(K);if(K.version!==te.__version||$===!0){t.activeTexture(n.TEXTURE0+F);let ae=Qe.getPrimaries(Qe.workingColorSpace),G=v.colorSpace===vr?null:Qe.getPrimaries(v.colorSpace),X=v.colorSpace===vr||ae===G?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,X);let me=v.isCompressedTexture||v.image[0].isCompressedTexture,ye=v.image[0]&&v.image[0].isDataTexture,se=[];for(let Z=0;Z<6;Z++)!me&&!ye?se[Z]=m(v.image[Z],!0,r.maxCubemapSize):se[Z]=ye?v.image[Z].image:v.image[Z],se[Z]=ce(v,se[Z]);let ne=se[0],Oe=s.convert(v.format,v.colorSpace),je=s.convert(v.type),rt=E(v.internalFormat,Oe,je,v.normalized,v.colorSpace),A=v.isVideoTexture!==!0,ie=te.__version===void 0||$===!0,W=K.dataReady,ge=w(v,ne);Ge(n.TEXTURE_CUBE_MAP,v);let oe;if(me){A&&ie&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ge,rt,ne.width,ne.height);for(let Z=0;Z<6;Z++){oe=se[Z].mipmaps;for(let Se=0;Se<oe.length;Se++){let Be=oe[Se];v.format!==ai?Oe!==null?A?W&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,0,0,Be.width,Be.height,Oe,Be.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,rt,Be.width,Be.height,0,Be.data):De("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,0,0,Be.width,Be.height,Oe,je,Be.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,rt,Be.width,Be.height,0,Oe,je,Be.data)}}}else{if(oe=v.mipmaps,A&&ie){oe.length>0&&ge++;let Z=Rt(se[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ge,rt,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ye){A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,se[Z].width,se[Z].height,Oe,je,se[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,rt,se[Z].width,se[Z].height,0,Oe,je,se[Z].data);for(let Se=0;Se<oe.length;Se++){let Bt=oe[Se].image[Z].image;A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,0,0,Bt.width,Bt.height,Oe,je,Bt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,rt,Bt.width,Bt.height,0,Oe,je,Bt.data)}}else{A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Oe,je,se[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,rt,Oe,je,se[Z]);for(let Se=0;Se<oe.length;Se++){let Be=oe[Se];A?W&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,0,0,Oe,je,Be.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,rt,Oe,je,Be.image[Z])}}}p(v)&&b(n.TEXTURE_CUBE_MAP),te.__version=K.version,v.onUpdate&&v.onUpdate(v)}S.__version=v.version}function Ne(S,v,F,$,K,te){let ae=s.convert(F.format,F.colorSpace),G=s.convert(F.type),X=E(F.internalFormat,ae,G,F.normalized,F.colorSpace),me=i.get(v),ye=i.get(F);if(ye.__renderTarget=v,!me.__hasExternalTextures){let se=Math.max(1,v.width>>te),ne=Math.max(1,v.height>>te);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,te,X,se,ne,v.depth,0,ae,G,null):t.texImage2D(K,te,X,se,ne,0,ae,G,null)}t.bindFramebuffer(n.FRAMEBUFFER,S),Ke(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,K,ye.__webglTexture,0,Wt(v)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,K,ye.__webglTexture,te),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Ct(S,v,F){if(n.bindRenderbuffer(n.RENDERBUFFER,S),v.depthBuffer){let $=v.depthTexture,K=$&&$.isDepthTexture?$.type:null,te=D(v.stencilBuffer,K),ae=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Ke(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Wt(v),te,v.width,v.height):F?n.renderbufferStorageMultisample(n.RENDERBUFFER,Wt(v),te,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,te,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ae,n.RENDERBUFFER,S)}else{let $=v.textures;for(let K=0;K<$.length;K++){let te=$[K],ae=s.convert(te.format,te.colorSpace),G=s.convert(te.type),X=E(te.internalFormat,ae,G,te.normalized,te.colorSpace);Ke(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,Wt(v),X,v.width,v.height):F?n.renderbufferStorageMultisample(n.RENDERBUFFER,Wt(v),X,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,X,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ze(S,v,F){let $=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,S),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let K=i.get(v.depthTexture);if(K.__renderTarget=v,(!K.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),$){if(K.__webglInit===void 0&&(K.__webglInit=!0,v.depthTexture.addEventListener("dispose",I)),K.__webglTexture===void 0){K.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,K.__webglTexture),Ge(n.TEXTURE_CUBE_MAP,v.depthTexture);let me=s.convert(v.depthTexture.format),ye=s.convert(v.depthTexture.type),se;v.depthTexture.format===Fi?se=n.DEPTH_COMPONENT24:v.depthTexture.format===rs&&(se=n.DEPTH24_STENCIL8);for(let ne=0;ne<6;ne++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ne,0,se,v.width,v.height,0,me,ye,null)}}else Q(v.depthTexture,0);let te=K.__webglTexture,ae=Wt(v),G=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+F:n.TEXTURE_2D,X=v.depthTexture.format===rs?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Fi)Ke(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,G,te,0,ae):n.framebufferTexture2D(n.FRAMEBUFFER,X,G,te,0);else if(v.depthTexture.format===rs)Ke(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,G,te,0,ae):n.framebufferTexture2D(n.FRAMEBUFFER,X,G,te,0);else throw new Error("Unknown depthTexture format")}function gt(S){let v=i.get(S),F=S.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==S.depthTexture){let $=S.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),$){let K=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,$.removeEventListener("dispose",K)};$.addEventListener("dispose",K),v.__depthDisposeCallback=K}v.__boundDepthTexture=$}if(S.depthTexture&&!v.__autoAllocateDepthBuffer)if(F)for(let $=0;$<6;$++)Ze(v.__webglFramebuffer[$],S,$);else{let $=S.texture.mipmaps;$&&$.length>0?Ze(v.__webglFramebuffer[0],S,0):Ze(v.__webglFramebuffer,S,0)}else if(F){v.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[$]),v.__webglDepthbuffer[$]===void 0)v.__webglDepthbuffer[$]=n.createRenderbuffer(),Ct(v.__webglDepthbuffer[$],S,!1);else{let K=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,te=v.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,te),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,te)}}else{let $=S.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Ct(v.__webglDepthbuffer,S,!1);else{let K=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,te=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,te),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,te)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function Tt(S,v,F){let $=i.get(S);v!==void 0&&Ne($.__webglFramebuffer,S,S.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),F!==void 0&&gt(S)}function Ye(S){let v=S.texture,F=i.get(S),$=i.get(v);S.addEventListener("dispose",_);let K=S.textures,te=S.isWebGLCubeRenderTarget===!0,ae=K.length>1;if(ae||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=v.version,o.memory.textures++),te){F.__webglFramebuffer=[];for(let G=0;G<6;G++)if(v.mipmaps&&v.mipmaps.length>0){F.__webglFramebuffer[G]=[];for(let X=0;X<v.mipmaps.length;X++)F.__webglFramebuffer[G][X]=n.createFramebuffer()}else F.__webglFramebuffer[G]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){F.__webglFramebuffer=[];for(let G=0;G<v.mipmaps.length;G++)F.__webglFramebuffer[G]=n.createFramebuffer()}else F.__webglFramebuffer=n.createFramebuffer();if(ae)for(let G=0,X=K.length;G<X;G++){let me=i.get(K[G]);me.__webglTexture===void 0&&(me.__webglTexture=n.createTexture(),o.memory.textures++)}if(S.samples>0&&Ke(S)===!1){F.__webglMultisampledFramebuffer=n.createFramebuffer(),F.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,F.__webglMultisampledFramebuffer);for(let G=0;G<K.length;G++){let X=K[G];F.__webglColorRenderbuffer[G]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,F.__webglColorRenderbuffer[G]);let me=s.convert(X.format,X.colorSpace),ye=s.convert(X.type),se=E(X.internalFormat,me,ye,X.normalized,X.colorSpace,S.isXRRenderTarget===!0),ne=Wt(S);n.renderbufferStorageMultisample(n.RENDERBUFFER,ne,se,S.width,S.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+G,n.RENDERBUFFER,F.__webglColorRenderbuffer[G])}n.bindRenderbuffer(n.RENDERBUFFER,null),S.depthBuffer&&(F.__webglDepthRenderbuffer=n.createRenderbuffer(),Ct(F.__webglDepthRenderbuffer,S,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(te){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),Ge(n.TEXTURE_CUBE_MAP,v);for(let G=0;G<6;G++)if(v.mipmaps&&v.mipmaps.length>0)for(let X=0;X<v.mipmaps.length;X++)Ne(F.__webglFramebuffer[G][X],S,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+G,X);else Ne(F.__webglFramebuffer[G],S,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0);p(v)&&b(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ae){for(let G=0,X=K.length;G<X;G++){let me=K[G],ye=i.get(me),se=n.TEXTURE_2D;(S.isWebGL3DRenderTarget||S.isWebGLArrayRenderTarget)&&(se=S.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(se,ye.__webglTexture),Ge(se,me),Ne(F.__webglFramebuffer,S,me,n.COLOR_ATTACHMENT0+G,se,0),p(me)&&b(se)}t.unbindTexture()}else{let G=n.TEXTURE_2D;if((S.isWebGL3DRenderTarget||S.isWebGLArrayRenderTarget)&&(G=S.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(G,$.__webglTexture),Ge(G,v),v.mipmaps&&v.mipmaps.length>0)for(let X=0;X<v.mipmaps.length;X++)Ne(F.__webglFramebuffer[X],S,v,n.COLOR_ATTACHMENT0,G,X);else Ne(F.__webglFramebuffer,S,v,n.COLOR_ATTACHMENT0,G,0);p(v)&&b(G),t.unbindTexture()}S.depthBuffer&&gt(S)}function jt(S){let v=S.textures;for(let F=0,$=v.length;F<$;F++){let K=v[F];if(p(K)){let te=M(S),ae=i.get(K).__webglTexture;t.bindTexture(te,ae),b(te),t.unbindTexture()}}}let Dt=[],Pn=[];function R(S){if(S.samples>0){if(Ke(S)===!1){let v=S.textures,F=S.width,$=S.height,K=n.COLOR_BUFFER_BIT,te=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ae=i.get(S),G=v.length>1;if(G)for(let me=0;me<v.length;me++)t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ae.__webglMultisampledFramebuffer);let X=S.texture.mipmaps;X&&X.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ae.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ae.__webglFramebuffer);for(let me=0;me<v.length;me++){if(S.resolveDepthBuffer&&(S.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),S.stencilBuffer&&S.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),G){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ae.__webglColorRenderbuffer[me]);let ye=i.get(v[me]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,ye,0)}n.blitFramebuffer(0,0,F,$,0,0,F,$,K,n.NEAREST),c===!0&&(Dt.length=0,Pn.length=0,Dt.push(n.COLOR_ATTACHMENT0+me),S.depthBuffer&&S.resolveDepthBuffer===!1&&(Dt.push(te),Pn.push(te),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,Pn)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Dt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),G)for(let me=0;me<v.length;me++){t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.RENDERBUFFER,ae.__webglColorRenderbuffer[me]);let ye=i.get(v[me]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.TEXTURE_2D,ye,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ae.__webglMultisampledFramebuffer)}else if(S.depthBuffer&&S.resolveDepthBuffer===!1&&c){let v=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function Wt(S){return Math.min(r.maxSamples,S.samples)}function Ke(S){let v=i.get(S);return S.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function Et(S){let v=o.render.frame;u.get(S)!==v&&(u.set(S,v),S.update())}function ce(S,v){let F=S.colorSpace,$=S.format,K=S.type;return S.isCompressedTexture===!0||S.isVideoTexture===!0||F!==Hc&&F!==vr&&(Qe.getTransfer(F)===ft?($!==ai||K!==Nn)&&De("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Re("WebGLTextures: Unsupported texture color space:",F)),v}function Rt(S){return typeof HTMLImageElement<"u"&&S instanceof HTMLImageElement?(l.width=S.naturalWidth||S.width,l.height=S.naturalHeight||S.height):typeof VideoFrame<"u"&&S instanceof VideoFrame?(l.width=S.displayWidth,l.height=S.displayHeight):(l.width=S.width,l.height=S.height),l}this.allocateTextureUnit=z,this.resetTextureUnits=j,this.getTextureUnits=q,this.setTextureUnits=P,this.setTexture2D=Q,this.setTexture2DArray=ee,this.setTexture3D=ue,this.setTextureCube=xe,this.rebindTextures=Tt,this.setupRenderTarget=Ye,this.updateRenderTargetMipmap=jt,this.updateMultisampleRenderTarget=R,this.setupDepthRenderbuffer=gt,this.setupFrameBufferTexture=Ne,this.useMultisampledRTT=Ke,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function YF(n,e){function t(i,r=vr){let s,o=Qe.getTransfer(r);if(i===Nn)return n.UNSIGNED_BYTE;if(i===ih)return n.UNSIGNED_SHORT_4_4_4_4;if(i===rh)return n.UNSIGNED_SHORT_5_5_5_1;if(i===yy)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===_y)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===gy)return n.BYTE;if(i===vy)return n.SHORT;if(i===ya)return n.UNSIGNED_SHORT;if(i===nh)return n.INT;if(i===Si)return n.UNSIGNED_INT;if(i===Mi)return n.FLOAT;if(i===Gi)return n.HALF_FLOAT;if(i===xy)return n.ALPHA;if(i===by)return n.RGB;if(i===ai)return n.RGBA;if(i===Fi)return n.DEPTH_COMPONENT;if(i===rs)return n.DEPTH_STENCIL;if(i===Sy)return n.RED;if(i===sh)return n.RED_INTEGER;if(i===ss)return n.RG;if(i===oh)return n.RG_INTEGER;if(i===ah)return n.RGBA_INTEGER;if(i===ul||i===dl||i===fl||i===hl)if(o===ft)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===ul)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===dl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===fl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===hl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===ul)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===dl)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===fl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===hl)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===ch||i===lh||i===uh||i===dh)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===ch)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===lh)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===uh)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===dh)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===fh||i===hh||i===ph||i===mh||i===gh||i===pl||i===vh)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===fh||i===hh)return o===ft?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===ph)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===mh)return s.COMPRESSED_R11_EAC;if(i===gh)return s.COMPRESSED_SIGNED_R11_EAC;if(i===pl)return s.COMPRESSED_RG11_EAC;if(i===vh)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===yh||i===_h||i===xh||i===bh||i===Sh||i===Mh||i===Eh||i===wh||i===Th||i===Ch||i===Dh||i===Ih||i===Ah||i===Rh)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===yh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===_h)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===xh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===bh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Sh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Mh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Eh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===wh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Th)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Ch)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Dh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Ih)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Ah)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Rh)return o===ft?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Nh||i===Ph||i===Oh)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Nh)return o===ft?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Ph)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Oh)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Lh||i===Fh||i===ml||i===kh)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===Lh)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Fh)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===ml)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===kh)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===_a?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var ZF=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,KF=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,Gy=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Jc(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new Gn({vertexShader:ZF,fragmentShader:KF,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new pt(new Qc(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},jy=class extends ki{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,g=null,y=typeof XRWebGLBinding<"u",m=new Gy,p={},b=t.getContextAttributes(),M=null,E=null,D=[],w=[],I=new ot,_=null,T=new fn;T.viewport=new At;let L=new fn;L.viewport=new At;let C=[T,L],U=new Kf,j=null,q=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(Y){let pe=D[Y];return pe===void 0&&(pe=new fa,D[Y]=pe),pe.getTargetRaySpace()},this.getControllerGrip=function(Y){let pe=D[Y];return pe===void 0&&(pe=new fa,D[Y]=pe),pe.getGripSpace()},this.getHand=function(Y){let pe=D[Y];return pe===void 0&&(pe=new fa,D[Y]=pe),pe.getHandSpace()};function P(Y){let pe=w.indexOf(Y.inputSource);if(pe===-1)return;let re=D[pe];re!==void 0&&(re.update(Y.inputSource,Y.frame,l||o),re.dispatchEvent({type:Y.type,data:Y.inputSource}))}function z(){r.removeEventListener("select",P),r.removeEventListener("selectstart",P),r.removeEventListener("selectend",P),r.removeEventListener("squeeze",P),r.removeEventListener("squeezestart",P),r.removeEventListener("squeezeend",P),r.removeEventListener("end",z),r.removeEventListener("inputsourceschange",k);for(let Y=0;Y<D.length;Y++){let pe=w[Y];pe!==null&&(w[Y]=null,D[Y].disconnect(pe))}j=null,q=null,m.reset();for(let Y in p)delete p[Y];e.setRenderTarget(M),h=null,f=null,d=null,r=null,E=null,Ge.stop(),i.isPresenting=!1,e.setPixelRatio(_),e.setSize(I.width,I.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(Y){s=Y,i.isPresenting===!0&&De("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(Y){a=Y,i.isPresenting===!0&&De("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(Y){l=Y},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&y&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function(Y){return ht(this,null,function*(){if(r=Y,r!==null){if(M=e.getRenderTarget(),r.addEventListener("select",P),r.addEventListener("selectstart",P),r.addEventListener("selectend",P),r.addEventListener("squeeze",P),r.addEventListener("squeezestart",P),r.addEventListener("squeezeend",P),r.addEventListener("end",z),r.addEventListener("inputsourceschange",k),b.xrCompatible!==!0&&(yield t.makeXRCompatible()),_=e.getPixelRatio(),e.getSize(I),y&&"createProjectionLayer"in XRWebGLBinding.prototype){let re=null,Ae=null,ke=null;b.depth&&(ke=b.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,re=b.stencil?rs:Fi,Ae=b.stencil?_a:Si);let Ne={colorFormat:t.RGBA8,depthFormat:ke,scaleFactor:s};d=this.getBinding(),f=d.createProjectionLayer(Ne),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),E=new zn(f.textureWidth,f.textureHeight,{format:ai,type:Nn,depthTexture:new gr(f.textureWidth,f.textureHeight,Ae,void 0,void 0,void 0,void 0,void 0,void 0,re),stencilBuffer:b.stencil,colorSpace:e.outputColorSpace,samples:b.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1})}else{let re={antialias:b.antialias,alpha:!0,depth:b.depth,stencil:b.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,re),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),E=new zn(h.framebufferWidth,h.framebufferHeight,{format:ai,type:Nn,colorSpace:e.outputColorSpace,stencilBuffer:b.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}E.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=yield r.requestReferenceSpace(a),Ge.setContext(r),Ge.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function k(Y){for(let pe=0;pe<Y.removed.length;pe++){let re=Y.removed[pe],Ae=w.indexOf(re);Ae>=0&&(w[Ae]=null,D[Ae].disconnect(re))}for(let pe=0;pe<Y.added.length;pe++){let re=Y.added[pe],Ae=w.indexOf(re);if(Ae===-1){for(let Ne=0;Ne<D.length;Ne++)if(Ne>=w.length){w.push(re),Ae=Ne;break}else if(w[Ne]===null){w[Ne]=re,Ae=Ne;break}if(Ae===-1)break}let ke=D[Ae];ke&&ke.connect(re)}}let Q=new O,ee=new O;function ue(Y,pe,re){Q.setFromMatrixPosition(pe.matrixWorld),ee.setFromMatrixPosition(re.matrixWorld);let Ae=Q.distanceTo(ee),ke=pe.projectionMatrix.elements,Ne=re.projectionMatrix.elements,Ct=ke[14]/(ke[10]-1),Ze=ke[14]/(ke[10]+1),gt=(ke[9]+1)/ke[5],Tt=(ke[9]-1)/ke[5],Ye=(ke[8]-1)/ke[0],jt=(Ne[8]+1)/Ne[0],Dt=Ct*Ye,Pn=Ct*jt,R=Ae/(-Ye+jt),Wt=R*-Ye;if(pe.matrixWorld.decompose(Y.position,Y.quaternion,Y.scale),Y.translateX(Wt),Y.translateZ(R),Y.matrixWorld.compose(Y.position,Y.quaternion,Y.scale),Y.matrixWorldInverse.copy(Y.matrixWorld).invert(),ke[10]===-1)Y.projectionMatrix.copy(pe.projectionMatrix),Y.projectionMatrixInverse.copy(pe.projectionMatrixInverse);else{let Ke=Ct+R,Et=Ze+R,ce=Dt-Wt,Rt=Pn+(Ae-Wt),S=gt*Ze/Et*Ke,v=Tt*Ze/Et*Ke;Y.projectionMatrix.makePerspective(ce,Rt,S,v,Ke,Et),Y.projectionMatrixInverse.copy(Y.projectionMatrix).invert()}}function xe(Y,pe){pe===null?Y.matrixWorld.copy(Y.matrix):Y.matrixWorld.multiplyMatrices(pe.matrixWorld,Y.matrix),Y.matrixWorldInverse.copy(Y.matrixWorld).invert()}this.updateCamera=function(Y){if(r===null)return;let pe=Y.near,re=Y.far;m.texture!==null&&(m.depthNear>0&&(pe=m.depthNear),m.depthFar>0&&(re=m.depthFar)),U.near=L.near=T.near=pe,U.far=L.far=T.far=re,(j!==U.near||q!==U.far)&&(r.updateRenderState({depthNear:U.near,depthFar:U.far}),j=U.near,q=U.far),U.layers.mask=Y.layers.mask|6,T.layers.mask=U.layers.mask&-5,L.layers.mask=U.layers.mask&-3;let Ae=Y.parent,ke=U.cameras;xe(U,Ae);for(let Ne=0;Ne<ke.length;Ne++)xe(ke[Ne],Ae);ke.length===2?ue(U,T,L):U.projectionMatrix.copy(T.projectionMatrix),Te(Y,U,Ae)};function Te(Y,pe,re){re===null?Y.matrix.copy(pe.matrixWorld):(Y.matrix.copy(re.matrixWorld),Y.matrix.invert(),Y.matrix.multiply(pe.matrixWorld)),Y.matrix.decompose(Y.position,Y.quaternion,Y.scale),Y.updateMatrixWorld(!0),Y.projectionMatrix.copy(pe.projectionMatrix),Y.projectionMatrixInverse.copy(pe.projectionMatrixInverse),Y.isPerspectiveCamera&&(Y.fov=la*2*Math.atan(1/Y.projectionMatrix.elements[5]),Y.zoom=1)}this.getCamera=function(){return U},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(Y){c=Y,f!==null&&(f.fixedFoveation=Y),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=Y)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(U)},this.getCameraTexture=function(Y){return p[Y]};let nt=null;function mt(Y,pe){if(u=pe.getViewerPose(l||o),g=pe,u!==null){let re=u.views;h!==null&&(e.setRenderTargetFramebuffer(E,h.framebuffer),e.setRenderTarget(E));let Ae=!1;re.length!==U.cameras.length&&(U.cameras.length=0,Ae=!0);for(let Ze=0;Ze<re.length;Ze++){let gt=re[Ze],Tt=null;if(h!==null)Tt=h.getViewport(gt);else{let jt=d.getViewSubImage(f,gt);Tt=jt.viewport,Ze===0&&(e.setRenderTargetTextures(E,jt.colorTexture,jt.depthStencilTexture),e.setRenderTarget(E))}let Ye=C[Ze];Ye===void 0&&(Ye=new fn,Ye.layers.enable(Ze),Ye.viewport=new At,C[Ze]=Ye),Ye.matrix.fromArray(gt.transform.matrix),Ye.matrix.decompose(Ye.position,Ye.quaternion,Ye.scale),Ye.projectionMatrix.fromArray(gt.projectionMatrix),Ye.projectionMatrixInverse.copy(Ye.projectionMatrix).invert(),Ye.viewport.set(Tt.x,Tt.y,Tt.width,Tt.height),Ze===0&&(U.matrix.copy(Ye.matrix),U.matrix.decompose(U.position,U.quaternion,U.scale)),Ae===!0&&U.cameras.push(Ye)}let ke=r.enabledFeatures;if(ke&&ke.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&y){d=i.getBinding();let Ze=d.getDepthInformation(re[0]);Ze&&Ze.isValid&&Ze.texture&&m.init(Ze,r.renderState)}if(ke&&ke.includes("camera-access")&&y){e.state.unbindTexture(),d=i.getBinding();for(let Ze=0;Ze<re.length;Ze++){let gt=re[Ze].camera;if(gt){let Tt=p[gt];Tt||(Tt=new Jc,p[gt]=Tt);let Ye=d.getCameraImage(gt);Tt.sourceTexture=Ye}}}}for(let re=0;re<D.length;re++){let Ae=w[re],ke=D[re];Ae!==null&&ke!==void 0&&ke.update(Ae,pe,l||o)}nt&&nt(Y,pe),pe.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:pe}),g=null}let Ge=new IE;Ge.setAnimationLoop(mt),this.setAnimationLoop=function(Y){nt=Y},this.dispose=function(){}}},JF=new It,LE=new Fe;LE.set(-1,0,0,0,1,0,0,0,1);function QF(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,Ty(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,b,M,E){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),f(m,p),p.isMeshPhysicalMaterial&&h(m,p,E)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),y(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,b,M):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===Dn&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===Dn&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let b=e.get(p),M=b.envMap,E=b.envMapRotation;M&&(m.envMap.value=M,m.envMapRotation.value.setFromMatrix4(JF.makeRotationFromEuler(E)).transpose(),M.isCubeTexture&&M.isRenderTargetTexture===!1&&m.envMapRotation.value.premultiply(LE),m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,b,M){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*b,m.scale.value=M*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function f(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,b){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===Dn&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=b.texture,m.transmissionSamplerSize.value.set(b.width,b.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function y(m,p){let b=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(b.matrixWorld),m.nearDistance.value=b.shadow.camera.near,m.farDistance.value=b.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function e2(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(b,M){let E=M.program;i.uniformBlockBinding(b,E)}function l(b,M){let E=r[b.id];E===void 0&&(g(b),E=u(b),r[b.id]=E,b.addEventListener("dispose",m));let D=M.program;i.updateUBOMapping(b,D);let w=e.render.frame;s[b.id]!==w&&(f(b),s[b.id]=w)}function u(b){let M=d();b.__bindingPointIndex=M;let E=n.createBuffer(),D=b.__size,w=b.usage;return n.bindBuffer(n.UNIFORM_BUFFER,E),n.bufferData(n.UNIFORM_BUFFER,D,w),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,M,E),E}function d(){for(let b=0;b<a;b++)if(o.indexOf(b)===-1)return o.push(b),b;return Re("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(b){let M=r[b.id],E=b.uniforms,D=b.__cache;n.bindBuffer(n.UNIFORM_BUFFER,M);for(let w=0,I=E.length;w<I;w++){let _=Array.isArray(E[w])?E[w]:[E[w]];for(let T=0,L=_.length;T<L;T++){let C=_[T];if(h(C,w,T,D)===!0){let U=C.__offset,j=Array.isArray(C.value)?C.value:[C.value],q=0;for(let P=0;P<j.length;P++){let z=j[P],k=y(z);typeof z=="number"||typeof z=="boolean"?(C.__data[0]=z,n.bufferSubData(n.UNIFORM_BUFFER,U+q,C.__data)):z.isMatrix3?(C.__data[0]=z.elements[0],C.__data[1]=z.elements[1],C.__data[2]=z.elements[2],C.__data[3]=0,C.__data[4]=z.elements[3],C.__data[5]=z.elements[4],C.__data[6]=z.elements[5],C.__data[7]=0,C.__data[8]=z.elements[6],C.__data[9]=z.elements[7],C.__data[10]=z.elements[8],C.__data[11]=0):ArrayBuffer.isView(z)?C.__data.set(new z.constructor(z.buffer,z.byteOffset,C.__data.length)):(z.toArray(C.__data,q),q+=k.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,U,C.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(b,M,E,D){let w=b.value,I=M+"_"+E;if(D[I]===void 0)return typeof w=="number"||typeof w=="boolean"?D[I]=w:ArrayBuffer.isView(w)?D[I]=w.slice():D[I]=w.clone(),!0;{let _=D[I];if(typeof w=="number"||typeof w=="boolean"){if(_!==w)return D[I]=w,!0}else{if(ArrayBuffer.isView(w))return!0;if(_.equals(w)===!1)return _.copy(w),!0}}return!1}function g(b){let M=b.uniforms,E=0,D=16;for(let I=0,_=M.length;I<_;I++){let T=Array.isArray(M[I])?M[I]:[M[I]];for(let L=0,C=T.length;L<C;L++){let U=T[L],j=Array.isArray(U.value)?U.value:[U.value];for(let q=0,P=j.length;q<P;q++){let z=j[q],k=y(z),Q=E%D,ee=Q%k.boundary,ue=Q+ee;E+=ee,ue!==0&&D-ue<k.storage&&(E+=D-ue),U.__data=new Float32Array(k.storage/Float32Array.BYTES_PER_ELEMENT),U.__offset=E,E+=k.storage}}}let w=E%D;return w>0&&(E+=D-w),b.__size=E,b.__cache={},this}function y(b){let M={boundary:0,storage:0};return typeof b=="number"||typeof b=="boolean"?(M.boundary=4,M.storage=4):b.isVector2?(M.boundary=8,M.storage=8):b.isVector3||b.isColor?(M.boundary=16,M.storage=12):b.isVector4?(M.boundary=16,M.storage=16):b.isMatrix3?(M.boundary=48,M.storage=48):b.isMatrix4?(M.boundary=64,M.storage=64):b.isTexture?De("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(b)?(M.boundary=16,M.storage=b.byteLength):De("WebGLRenderer: Unsupported uniform value type.",b),M}function m(b){let M=b.target;M.removeEventListener("dispose",m);let E=o.indexOf(M.__bindingPointIndex);o.splice(E,1),n.deleteBuffer(r[M.id]),delete r[M.id],delete s[M.id]}function p(){for(let b in r)n.deleteBuffer(r[b]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var t2=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),ji=null;function n2(){return ji===null&&(ji=new Pf(t2,16,16,ss,Gi),ji.name="DFG_LUT",ji.minFilter=hn,ji.magFilter=hn,ji.wrapS=Li,ji.wrapT=Li,ji.generateMipmaps=!1,ji.needsUpdate=!0),ji}var Wh=class{constructor(e={}){let{canvas:t=tE(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=Nn}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let y=h,m=new Set([ah,oh,sh]),p=new Set([Nn,Si,ya,_a,ih,rh]),b=new Uint32Array(4),M=new Int32Array(4),E=new O,D=null,w=null,I=[],_=[],T=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=bi,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let L=this,C=!1,U=null;this._outputColorSpace=Cn;let j=0,q=0,P=null,z=-1,k=null,Q=new At,ee=new At,ue=null,xe=new qe(0),Te=0,nt=t.width,mt=t.height,Ge=1,Y=null,pe=null,re=new At(0,0,nt,mt),Ae=new At(0,0,nt,mt),ke=!1,Ne=new ha,Ct=!1,Ze=!1,gt=new It,Tt=new O,Ye=new At,jt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Dt=!1;function Pn(){return P===null?Ge:1}let R=i;function Wt(x,N){return t.getContext(x,N)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"184"}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",Se,!1),t.addEventListener("webglcontextcreationerror",Be,!1),R===null){let N="webgl2";if(R=Wt(N,x),R===null)throw Wt(N)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw Re("WebGLRenderer: "+x.message),x}let Ke,Et,ce,Rt,S,v,F,$,K,te,ae,G,X,me,ye,se,ne,Oe,je,rt,A,ie,W;function ge(){Ke=new lL(R),Ke.init(),A=new YF(R,Ke),Et=new tL(R,Ke,e,A),ce=new XF(R,Ke),Et.reversedDepthBuffer&&f&&ce.buffers.depth.setReversed(!0),Rt=new fL(R),S=new PF,v=new qF(R,Ke,ce,S,Et,A,Rt),F=new cL(L),$=new gN(R),ie=new QO(R,$),K=new uL(R,$,Rt,ie),te=new pL(R,K,$,ie,Rt),Oe=new hL(R,Et,v),ye=new nL(S),ae=new NF(L,F,Ke,Et,ie,ye),G=new QF(L,S),X=new LF,me=new HF(Ke),ne=new JO(L,F,ce,te,g,c),se=new $F(L,te,Et),W=new e2(R,Rt,Et,ce),je=new eL(R,Ke,Rt),rt=new dL(R,Ke,Rt),Rt.programs=ae.programs,L.capabilities=Et,L.extensions=Ke,L.properties=S,L.renderLists=X,L.shadowMap=se,L.state=ce,L.info=Rt}ge(),y!==Nn&&(T=new gL(y,t.width,t.height,r,s));let oe=new jy(L,R);this.xr=oe,this.getContext=function(){return R},this.getContextAttributes=function(){return R.getContextAttributes()},this.forceContextLoss=function(){let x=Ke.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=Ke.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return Ge},this.setPixelRatio=function(x){x!==void 0&&(Ge=x,this.setSize(nt,mt,!1))},this.getSize=function(x){return x.set(nt,mt)},this.setSize=function(x,N,H=!0){if(oe.isPresenting){De("WebGLRenderer: Can't change size while VR device is presenting.");return}nt=x,mt=N,t.width=Math.floor(x*Ge),t.height=Math.floor(N*Ge),H===!0&&(t.style.width=x+"px",t.style.height=N+"px"),T!==null&&T.setSize(t.width,t.height),this.setViewport(0,0,x,N)},this.getDrawingBufferSize=function(x){return x.set(nt*Ge,mt*Ge).floor()},this.setDrawingBufferSize=function(x,N,H){nt=x,mt=N,Ge=H,t.width=Math.floor(x*H),t.height=Math.floor(N*H),this.setViewport(0,0,x,N)},this.setEffects=function(x){if(y===Nn){Re("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let N=0;N<x.length;N++)if(x[N].isOutputPass===!0){De("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}T.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(Q)},this.getViewport=function(x){return x.copy(re)},this.setViewport=function(x,N,H,B){x.isVector4?re.set(x.x,x.y,x.z,x.w):re.set(x,N,H,B),ce.viewport(Q.copy(re).multiplyScalar(Ge).round())},this.getScissor=function(x){return x.copy(Ae)},this.setScissor=function(x,N,H,B){x.isVector4?Ae.set(x.x,x.y,x.z,x.w):Ae.set(x,N,H,B),ce.scissor(ee.copy(Ae).multiplyScalar(Ge).round())},this.getScissorTest=function(){return ke},this.setScissorTest=function(x){ce.setScissorTest(ke=x)},this.setOpaqueSort=function(x){Y=x},this.setTransparentSort=function(x){pe=x},this.getClearColor=function(x){return x.copy(ne.getClearColor())},this.setClearColor=function(){ne.setClearColor(...arguments)},this.getClearAlpha=function(){return ne.getClearAlpha()},this.setClearAlpha=function(){ne.setClearAlpha(...arguments)},this.clear=function(x=!0,N=!0,H=!0){let B=0;if(x){let V=!1;if(P!==null){let fe=P.texture.format;V=m.has(fe)}if(V){let fe=P.texture.type,_e=p.has(fe),de=ne.getClearColor(),be=ne.getClearAlpha(),we=de.r,Ve=de.g,Xe=de.b;_e?(b[0]=we,b[1]=Ve,b[2]=Xe,b[3]=be,R.clearBufferuiv(R.COLOR,0,b)):(M[0]=we,M[1]=Ve,M[2]=Xe,M[3]=be,R.clearBufferiv(R.COLOR,0,M))}else B|=R.COLOR_BUFFER_BIT}N&&(B|=R.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),H&&(B|=R.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),B!==0&&R.clear(B)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(x){x.setRenderer(this),U=x},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",Se,!1),t.removeEventListener("webglcontextcreationerror",Be,!1),ne.dispose(),X.dispose(),me.dispose(),S.dispose(),F.dispose(),te.dispose(),ie.dispose(),W.dispose(),ae.dispose(),oe.dispose(),oe.removeEventListener("sessionstart",$y),oe.removeEventListener("sessionend",Xy),as.stop()};function Z(x){x.preventDefault(),Ey("WebGLRenderer: Context Lost."),C=!0}function Se(){Ey("WebGLRenderer: Context Restored."),C=!1;let x=Rt.autoReset,N=se.enabled,H=se.autoUpdate,B=se.needsUpdate,V=se.type;ge(),Rt.autoReset=x,se.enabled=N,se.autoUpdate=H,se.needsUpdate=B,se.type=V}function Be(x){Re("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function Bt(x){let N=x.target;N.removeEventListener("dispose",Bt),vt(N)}function vt(x){$i(x),S.remove(x)}function $i(x){let N=S.get(x).programs;N!==void 0&&(N.forEach(function(H){ae.releaseProgram(H)}),x.isShaderMaterial&&ae.releaseShaderCache(x))}this.renderBufferDirect=function(x,N,H,B,V,fe){N===null&&(N=jt);let _e=V.isMesh&&V.matrixWorld.determinant()<0,de=HE(x,N,H,B,V);ce.setMaterial(B,_e);let be=H.index,we=1;if(B.wireframe===!0){if(be=K.getWireframeAttribute(H),be===void 0)return;we=2}let Ve=H.drawRange,Xe=H.attributes.position,Ce=Ve.start*we,yt=(Ve.start+Ve.count)*we;fe!==null&&(Ce=Math.max(Ce,fe.start*we),yt=Math.min(yt,(fe.start+fe.count)*we)),be!==null?(Ce=Math.max(Ce,0),yt=Math.min(yt,be.count)):Xe!=null&&(Ce=Math.max(Ce,0),yt=Math.min(yt,Xe.count));let Vt=yt-Ce;if(Vt<0||Vt===1/0)return;ie.setup(V,B,de,H,be);let Nt,bt=je;if(be!==null&&(Nt=$.get(be),bt=rt,bt.setIndex(Nt)),V.isMesh)B.wireframe===!0?(ce.setLineWidth(B.wireframeLinewidth*Pn()),bt.setMode(R.LINES)):bt.setMode(R.TRIANGLES);else if(V.isLine){let pn=B.linewidth;pn===void 0&&(pn=1),ce.setLineWidth(pn*Pn()),V.isLineSegments?bt.setMode(R.LINES):V.isLineLoop?bt.setMode(R.LINE_LOOP):bt.setMode(R.LINE_STRIP)}else V.isPoints?bt.setMode(R.POINTS):V.isSprite&&bt.setMode(R.TRIANGLES);if(V.isBatchedMesh)if(Ke.get("WEBGL_multi_draw"))bt.renderMultiDraw(V._multiDrawStarts,V._multiDrawCounts,V._multiDrawCount);else{let pn=V._multiDrawStarts,ve=V._multiDrawCounts,On=V._multiDrawCount,it=be?$.get(be).bytesPerElement:1,Wn=S.get(B).currentProgram.getUniforms();for(let wi=0;wi<On;wi++)Wn.setValue(R,"_gl_DrawID",wi),bt.render(pn[wi]/it,ve[wi])}else if(V.isInstancedMesh)bt.renderInstances(Ce,Vt,V.count);else if(H.isInstancedBufferGeometry){let pn=H._maxInstanceCount!==void 0?H._maxInstanceCount:1/0,ve=Math.min(H.instanceCount,pn);bt.renderInstances(Ce,Vt,ve)}else bt.render(Ce,Vt)};function Ei(x,N,H){x.transparent===!0&&x.side===Hi&&x.forceSinglePass===!1?(x.side=Dn,x.needsUpdate=!0,wl(x,N,H),x.side=hr,x.needsUpdate=!0,wl(x,N,H),x.side=Hi):wl(x,N,H)}this.compile=function(x,N,H=null){H===null&&(H=x),w=me.get(H),w.init(N),_.push(w),H.traverseVisible(function(V){V.isLight&&V.layers.test(N.layers)&&(w.pushLight(V),V.castShadow&&w.pushShadow(V))}),x!==H&&x.traverseVisible(function(V){V.isLight&&V.layers.test(N.layers)&&(w.pushLight(V),V.castShadow&&w.pushShadow(V))}),w.setupLights();let B=new Set;return x.traverse(function(V){if(!(V.isMesh||V.isPoints||V.isLine||V.isSprite))return;let fe=V.material;if(fe)if(Array.isArray(fe))for(let _e=0;_e<fe.length;_e++){let de=fe[_e];Ei(de,H,V),B.add(de)}else Ei(fe,H,V),B.add(fe)}),w=_.pop(),B},this.compileAsync=function(x,N,H=null){let B=this.compile(x,N,H);return new Promise(V=>{function fe(){if(B.forEach(function(_e){S.get(_e).currentProgram.isReady()&&B.delete(_e)}),B.size===0){V(x);return}setTimeout(fe,10)}Ke.get("KHR_parallel_shader_compile")!==null?fe():setTimeout(fe,10)})};let Zh=null;function BE(x){Zh&&Zh(x)}function $y(){as.stop()}function Xy(){as.start()}let as=new IE;as.setAnimationLoop(BE),typeof self<"u"&&as.setContext(self),this.setAnimationLoop=function(x){Zh=x,oe.setAnimationLoop(x),x===null?as.stop():as.start()},oe.addEventListener("sessionstart",$y),oe.addEventListener("sessionend",Xy),this.render=function(x,N){if(N!==void 0&&N.isCamera!==!0){Re("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(C===!0)return;U!==null&&U.renderStart(x,N);let H=oe.enabled===!0&&oe.isPresenting===!0,B=T!==null&&(P===null||H)&&T.begin(L,P);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),N.parent===null&&N.matrixWorldAutoUpdate===!0&&N.updateMatrixWorld(),oe.enabled===!0&&oe.isPresenting===!0&&(T===null||T.isCompositing()===!1)&&(oe.cameraAutoUpdate===!0&&oe.updateCamera(N),N=oe.getCamera()),x.isScene===!0&&x.onBeforeRender(L,x,N,P),w=me.get(x,_.length),w.init(N),w.state.textureUnits=v.getTextureUnits(),_.push(w),gt.multiplyMatrices(N.projectionMatrix,N.matrixWorldInverse),Ne.setFromProjectionMatrix(gt,xi,N.reversedDepth),Ze=this.localClippingEnabled,Ct=ye.init(this.clippingPlanes,Ze),D=X.get(x,I.length),D.init(),I.push(D),oe.enabled===!0&&oe.isPresenting===!0){let _e=L.xr.getDepthSensingMesh();_e!==null&&Kh(_e,N,-1/0,L.sortObjects)}Kh(x,N,0,L.sortObjects),D.finish(),L.sortObjects===!0&&D.sort(Y,pe),Dt=oe.enabled===!1||oe.isPresenting===!1||oe.hasDepthSensing()===!1,Dt&&ne.addToRenderList(D,x),this.info.render.frame++,Ct===!0&&ye.beginShadows();let V=w.state.shadowsArray;if(se.render(V,x,N),Ct===!0&&ye.endShadows(),this.info.autoReset===!0&&this.info.reset(),(B&&T.hasRenderPass())===!1){let _e=D.opaque,de=D.transmissive;if(w.setupLights(),N.isArrayCamera){let be=N.cameras;if(de.length>0)for(let we=0,Ve=be.length;we<Ve;we++){let Xe=be[we];Yy(_e,de,x,Xe)}Dt&&ne.render(x);for(let we=0,Ve=be.length;we<Ve;we++){let Xe=be[we];qy(D,x,Xe,Xe.viewport)}}else de.length>0&&Yy(_e,de,x,N),Dt&&ne.render(x),qy(D,x,N)}P!==null&&q===0&&(v.updateMultisampleRenderTarget(P),v.updateRenderTargetMipmap(P)),B&&T.end(L),x.isScene===!0&&x.onAfterRender(L,x,N),ie.resetDefaultState(),z=-1,k=null,_.pop(),_.length>0?(w=_[_.length-1],v.setTextureUnits(w.state.textureUnits),Ct===!0&&ye.setGlobalState(L.clippingPlanes,w.state.camera)):w=null,I.pop(),I.length>0?D=I[I.length-1]:D=null,U!==null&&U.renderEnd()};function Kh(x,N,H,B){if(x.visible===!1)return;if(x.layers.test(N.layers)){if(x.isGroup)H=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(N);else if(x.isLightProbeGrid)w.pushLightProbeGrid(x);else if(x.isLight)w.pushLight(x),x.castShadow&&w.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||Ne.intersectsSprite(x)){B&&Ye.setFromMatrixPosition(x.matrixWorld).applyMatrix4(gt);let _e=te.update(x),de=x.material;de.visible&&D.push(x,_e,de,H,Ye.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||Ne.intersectsObject(x))){let _e=te.update(x),de=x.material;if(B&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),Ye.copy(x.boundingSphere.center)):(_e.boundingSphere===null&&_e.computeBoundingSphere(),Ye.copy(_e.boundingSphere.center)),Ye.applyMatrix4(x.matrixWorld).applyMatrix4(gt)),Array.isArray(de)){let be=_e.groups;for(let we=0,Ve=be.length;we<Ve;we++){let Xe=be[we],Ce=de[Xe.materialIndex];Ce&&Ce.visible&&D.push(x,_e,Ce,H,Ye.z,Xe)}}else de.visible&&D.push(x,_e,de,H,Ye.z,null)}}let fe=x.children;for(let _e=0,de=fe.length;_e<de;_e++)Kh(fe[_e],N,H,B)}function qy(x,N,H,B){let{opaque:V,transmissive:fe,transparent:_e}=x;w.setupLightsView(H),Ct===!0&&ye.setGlobalState(L.clippingPlanes,H),B&&ce.viewport(Q.copy(B)),V.length>0&&El(V,N,H),fe.length>0&&El(fe,N,H),_e.length>0&&El(_e,N,H),ce.buffers.depth.setTest(!0),ce.buffers.depth.setMask(!0),ce.buffers.color.setMask(!0),ce.setPolygonOffset(!1)}function Yy(x,N,H,B){if((H.isScene===!0?H.overrideMaterial:null)!==null)return;if(w.state.transmissionRenderTarget[B.id]===void 0){let Ce=Ke.has("EXT_color_buffer_half_float")||Ke.has("EXT_color_buffer_float");w.state.transmissionRenderTarget[B.id]=new zn(1,1,{generateMipmaps:!0,type:Ce?Gi:Nn,minFilter:is,samples:Math.max(4,Et.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:Qe.workingColorSpace})}let fe=w.state.transmissionRenderTarget[B.id],_e=B.viewport||Q;fe.setSize(_e.z*L.transmissionResolutionScale,_e.w*L.transmissionResolutionScale);let de=L.getRenderTarget(),be=L.getActiveCubeFace(),we=L.getActiveMipmapLevel();L.setRenderTarget(fe),L.getClearColor(xe),Te=L.getClearAlpha(),Te<1&&L.setClearColor(16777215,.5),L.clear(),Dt&&ne.render(H);let Ve=L.toneMapping;L.toneMapping=bi;let Xe=B.viewport;if(B.viewport!==void 0&&(B.viewport=void 0),w.setupLightsView(B),Ct===!0&&ye.setGlobalState(L.clippingPlanes,B),El(x,H,B),v.updateMultisampleRenderTarget(fe),v.updateRenderTargetMipmap(fe),Ke.has("WEBGL_multisampled_render_to_texture")===!1){let Ce=!1;for(let yt=0,Vt=N.length;yt<Vt;yt++){let Nt=N[yt],{object:bt,geometry:pn,material:ve,group:On}=Nt;if(ve.side===Hi&&bt.layers.test(B.layers)){let it=ve.side;ve.side=Dn,ve.needsUpdate=!0,Zy(bt,H,B,pn,ve,On),ve.side=it,ve.needsUpdate=!0,Ce=!0}}Ce===!0&&(v.updateMultisampleRenderTarget(fe),v.updateRenderTargetMipmap(fe))}L.setRenderTarget(de,be,we),L.setClearColor(xe,Te),Xe!==void 0&&(B.viewport=Xe),L.toneMapping=Ve}function El(x,N,H){let B=N.isScene===!0?N.overrideMaterial:null;for(let V=0,fe=x.length;V<fe;V++){let _e=x[V],{object:de,geometry:be,group:we}=_e,Ve=_e.material;Ve.allowOverride===!0&&B!==null&&(Ve=B),de.layers.test(H.layers)&&Zy(de,N,H,be,Ve,we)}}function Zy(x,N,H,B,V,fe){x.onBeforeRender(L,N,H,B,V,fe),x.modelViewMatrix.multiplyMatrices(H.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),V.onBeforeRender(L,N,H,B,x,fe),V.transparent===!0&&V.side===Hi&&V.forceSinglePass===!1?(V.side=Dn,V.needsUpdate=!0,L.renderBufferDirect(H,N,B,V,x,fe),V.side=hr,V.needsUpdate=!0,L.renderBufferDirect(H,N,B,V,x,fe),V.side=Hi):L.renderBufferDirect(H,N,B,V,x,fe),x.onAfterRender(L,N,H,B,V,fe)}function wl(x,N,H){N.isScene!==!0&&(N=jt);let B=S.get(x),V=w.state.lights,fe=w.state.shadowsArray,_e=V.state.version,de=ae.getParameters(x,V.state,fe,N,H,w.state.lightProbeGridArray),be=ae.getProgramCacheKey(de),we=B.programs;B.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?N.environment:null,B.fog=N.fog;let Ve=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;B.envMap=F.get(x.envMap||B.environment,Ve),B.envMapRotation=B.environment!==null&&x.envMap===null?N.environmentRotation:x.envMapRotation,we===void 0&&(x.addEventListener("dispose",Bt),we=new Map,B.programs=we);let Xe=we.get(be);if(Xe!==void 0){if(B.currentProgram===Xe&&B.lightsStateVersion===_e)return Jy(x,de),Xe}else de.uniforms=ae.getUniforms(x),U!==null&&x.isNodeMaterial&&U.build(x,H,de),x.onBeforeCompile(de,L),Xe=ae.acquireProgram(de,be),we.set(be,Xe),B.uniforms=de.uniforms;let Ce=B.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(Ce.clippingPlanes=ye.uniform),Jy(x,de),B.needsLights=GE(x),B.lightsStateVersion=_e,B.needsLights&&(Ce.ambientLightColor.value=V.state.ambient,Ce.lightProbe.value=V.state.probe,Ce.directionalLights.value=V.state.directional,Ce.directionalLightShadows.value=V.state.directionalShadow,Ce.spotLights.value=V.state.spot,Ce.spotLightShadows.value=V.state.spotShadow,Ce.rectAreaLights.value=V.state.rectArea,Ce.ltc_1.value=V.state.rectAreaLTC1,Ce.ltc_2.value=V.state.rectAreaLTC2,Ce.pointLights.value=V.state.point,Ce.pointLightShadows.value=V.state.pointShadow,Ce.hemisphereLights.value=V.state.hemi,Ce.directionalShadowMatrix.value=V.state.directionalShadowMatrix,Ce.spotLightMatrix.value=V.state.spotLightMatrix,Ce.spotLightMap.value=V.state.spotLightMap,Ce.pointShadowMatrix.value=V.state.pointShadowMatrix),B.lightProbeGrid=w.state.lightProbeGridArray.length>0,B.currentProgram=Xe,B.uniformsList=null,Xe}function Ky(x){if(x.uniformsList===null){let N=x.currentProgram.getUniforms();x.uniformsList=Sa.seqWithValue(N.seq,x.uniforms)}return x.uniformsList}function Jy(x,N){let H=S.get(x);H.outputColorSpace=N.outputColorSpace,H.batching=N.batching,H.batchingColor=N.batchingColor,H.instancing=N.instancing,H.instancingColor=N.instancingColor,H.instancingMorph=N.instancingMorph,H.skinning=N.skinning,H.morphTargets=N.morphTargets,H.morphNormals=N.morphNormals,H.morphColors=N.morphColors,H.morphTargetsCount=N.morphTargetsCount,H.numClippingPlanes=N.numClippingPlanes,H.numIntersection=N.numClipIntersection,H.vertexAlphas=N.vertexAlphas,H.vertexTangents=N.vertexTangents,H.toneMapping=N.toneMapping}function VE(x,N){if(x.length===0)return null;if(x.length===1)return x[0].texture!==null?x[0]:null;E.setFromMatrixPosition(N.matrixWorld);for(let H=0,B=x.length;H<B;H++){let V=x[H];if(V.texture!==null&&V.boundingBox.containsPoint(E))return V}return null}function HE(x,N,H,B,V){N.isScene!==!0&&(N=jt),v.resetTextureUnits();let fe=N.fog,_e=B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial?N.environment:null,de=P===null?L.outputColorSpace:P.isXRRenderTarget===!0?P.texture.colorSpace:Qe.workingColorSpace,be=B.isMeshStandardMaterial||B.isMeshLambertMaterial&&!B.envMap||B.isMeshPhongMaterial&&!B.envMap,we=F.get(B.envMap||_e,be),Ve=B.vertexColors===!0&&!!H.attributes.color&&H.attributes.color.itemSize===4,Xe=!!H.attributes.tangent&&(!!B.normalMap||B.anisotropy>0),Ce=!!H.morphAttributes.position,yt=!!H.morphAttributes.normal,Vt=!!H.morphAttributes.color,Nt=bi;B.toneMapped&&(P===null||P.isXRRenderTarget===!0)&&(Nt=L.toneMapping);let bt=H.morphAttributes.position||H.morphAttributes.normal||H.morphAttributes.color,pn=bt!==void 0?bt.length:0,ve=S.get(B),On=w.state.lights;if(Ct===!0&&(Ze===!0||x!==k)){let wt=x===k&&B.id===z;ye.setState(B,x,wt)}let it=!1;B.version===ve.__version?(ve.needsLights&&ve.lightsStateVersion!==On.state.version||ve.outputColorSpace!==de||V.isBatchedMesh&&ve.batching===!1||!V.isBatchedMesh&&ve.batching===!0||V.isBatchedMesh&&ve.batchingColor===!0&&V.colorTexture===null||V.isBatchedMesh&&ve.batchingColor===!1&&V.colorTexture!==null||V.isInstancedMesh&&ve.instancing===!1||!V.isInstancedMesh&&ve.instancing===!0||V.isSkinnedMesh&&ve.skinning===!1||!V.isSkinnedMesh&&ve.skinning===!0||V.isInstancedMesh&&ve.instancingColor===!0&&V.instanceColor===null||V.isInstancedMesh&&ve.instancingColor===!1&&V.instanceColor!==null||V.isInstancedMesh&&ve.instancingMorph===!0&&V.morphTexture===null||V.isInstancedMesh&&ve.instancingMorph===!1&&V.morphTexture!==null||ve.envMap!==we||B.fog===!0&&ve.fog!==fe||ve.numClippingPlanes!==void 0&&(ve.numClippingPlanes!==ye.numPlanes||ve.numIntersection!==ye.numIntersection)||ve.vertexAlphas!==Ve||ve.vertexTangents!==Xe||ve.morphTargets!==Ce||ve.morphNormals!==yt||ve.morphColors!==Vt||ve.toneMapping!==Nt||ve.morphTargetsCount!==pn||!!ve.lightProbeGrid!=w.state.lightProbeGridArray.length>0)&&(it=!0):(it=!0,ve.__version=B.version);let Wn=ve.currentProgram;it===!0&&(Wn=wl(B,N,V),U&&B.isNodeMaterial&&U.onUpdateProgram(B,Wn,ve));let wi=!1,_r=!1,no=!1,St=Wn.getUniforms(),Ht=ve.uniforms;if(ce.useProgram(Wn.program)&&(wi=!0,_r=!0,no=!0),B.id!==z&&(z=B.id,_r=!0),ve.needsLights){let wt=VE(w.state.lightProbeGridArray,V);ve.lightProbeGrid!==wt&&(ve.lightProbeGrid=wt,_r=!0)}if(wi||k!==x){ce.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),St.setValue(R,"projectionMatrix",x.projectionMatrix),St.setValue(R,"viewMatrix",x.matrixWorldInverse);let br=St.map.cameraPosition;br!==void 0&&br.setValue(R,Tt.setFromMatrixPosition(x.matrixWorld)),Et.logarithmicDepthBuffer&&St.setValue(R,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(B.isMeshPhongMaterial||B.isMeshToonMaterial||B.isMeshLambertMaterial||B.isMeshBasicMaterial||B.isMeshStandardMaterial||B.isShaderMaterial)&&St.setValue(R,"isOrthographic",x.isOrthographicCamera===!0),k!==x&&(k=x,_r=!0,no=!0)}if(ve.needsLights&&(On.state.directionalShadowMap.length>0&&St.setValue(R,"directionalShadowMap",On.state.directionalShadowMap,v),On.state.spotShadowMap.length>0&&St.setValue(R,"spotShadowMap",On.state.spotShadowMap,v),On.state.pointShadowMap.length>0&&St.setValue(R,"pointShadowMap",On.state.pointShadowMap,v)),V.isSkinnedMesh){St.setOptional(R,V,"bindMatrix"),St.setOptional(R,V,"bindMatrixInverse");let wt=V.skeleton;wt&&(wt.boneTexture===null&&wt.computeBoneTexture(),St.setValue(R,"boneTexture",wt.boneTexture,v))}V.isBatchedMesh&&(St.setOptional(R,V,"batchingTexture"),St.setValue(R,"batchingTexture",V._matricesTexture,v),St.setOptional(R,V,"batchingIdTexture"),St.setValue(R,"batchingIdTexture",V._indirectTexture,v),St.setOptional(R,V,"batchingColorTexture"),V._colorsTexture!==null&&St.setValue(R,"batchingColorTexture",V._colorsTexture,v));let xr=H.morphAttributes;if((xr.position!==void 0||xr.normal!==void 0||xr.color!==void 0)&&Oe.update(V,H,Wn),(_r||ve.receiveShadow!==V.receiveShadow)&&(ve.receiveShadow=V.receiveShadow,St.setValue(R,"receiveShadow",V.receiveShadow)),(B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial)&&B.envMap===null&&N.environment!==null&&(Ht.envMapIntensity.value=N.environmentIntensity),Ht.dfgLUT!==void 0&&(Ht.dfgLUT.value=n2()),_r){if(St.setValue(R,"toneMappingExposure",L.toneMappingExposure),ve.needsLights&&zE(Ht,no),fe&&B.fog===!0&&G.refreshFogUniforms(Ht,fe),G.refreshMaterialUniforms(Ht,B,Ge,mt,w.state.transmissionRenderTarget[x.id]),ve.needsLights&&ve.lightProbeGrid){let wt=ve.lightProbeGrid;Ht.probesSH.value=wt.texture,Ht.probesMin.value.copy(wt.boundingBox.min),Ht.probesMax.value.copy(wt.boundingBox.max),Ht.probesResolution.value.copy(wt.resolution)}Sa.upload(R,Ky(ve),Ht,v)}if(B.isShaderMaterial&&B.uniformsNeedUpdate===!0&&(Sa.upload(R,Ky(ve),Ht,v),B.uniformsNeedUpdate=!1),B.isSpriteMaterial&&St.setValue(R,"center",V.center),St.setValue(R,"modelViewMatrix",V.modelViewMatrix),St.setValue(R,"normalMatrix",V.normalMatrix),St.setValue(R,"modelMatrix",V.matrixWorld),B.uniformsGroups!==void 0){let wt=B.uniformsGroups;for(let br=0,io=wt.length;br<io;br++){let Qy=wt[br];W.update(Qy,Wn),W.bind(Qy,Wn)}}return Wn}function zE(x,N){x.ambientLightColor.needsUpdate=N,x.lightProbe.needsUpdate=N,x.directionalLights.needsUpdate=N,x.directionalLightShadows.needsUpdate=N,x.pointLights.needsUpdate=N,x.pointLightShadows.needsUpdate=N,x.spotLights.needsUpdate=N,x.spotLightShadows.needsUpdate=N,x.rectAreaLights.needsUpdate=N,x.hemisphereLights.needsUpdate=N}function GE(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return j},this.getActiveMipmapLevel=function(){return q},this.getRenderTarget=function(){return P},this.setRenderTargetTextures=function(x,N,H){let B=S.get(x);B.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,B.__autoAllocateDepthBuffer===!1&&(B.__useRenderToTexture=!1),S.get(x.texture).__webglTexture=N,S.get(x.depthTexture).__webglTexture=B.__autoAllocateDepthBuffer?void 0:H,B.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,N){let H=S.get(x);H.__webglFramebuffer=N,H.__useDefaultFramebuffer=N===void 0};let jE=R.createFramebuffer();this.setRenderTarget=function(x,N=0,H=0){P=x,j=N,q=H;let B=null,V=!1,fe=!1;if(x){let de=S.get(x);if(de.__useDefaultFramebuffer!==void 0){ce.bindFramebuffer(R.FRAMEBUFFER,de.__webglFramebuffer),Q.copy(x.viewport),ee.copy(x.scissor),ue=x.scissorTest,ce.viewport(Q),ce.scissor(ee),ce.setScissorTest(ue),z=-1;return}else if(de.__webglFramebuffer===void 0)v.setupRenderTarget(x);else if(de.__hasExternalTextures)v.rebindTextures(x,S.get(x.texture).__webglTexture,S.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let Ve=x.depthTexture;if(de.__boundDepthTexture!==Ve){if(Ve!==null&&S.has(Ve)&&(x.width!==Ve.image.width||x.height!==Ve.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");v.setupDepthRenderbuffer(x)}}let be=x.texture;(be.isData3DTexture||be.isDataArrayTexture||be.isCompressedArrayTexture)&&(fe=!0);let we=S.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(we[N])?B=we[N][H]:B=we[N],V=!0):x.samples>0&&v.useMultisampledRTT(x)===!1?B=S.get(x).__webglMultisampledFramebuffer:Array.isArray(we)?B=we[H]:B=we,Q.copy(x.viewport),ee.copy(x.scissor),ue=x.scissorTest}else Q.copy(re).multiplyScalar(Ge).floor(),ee.copy(Ae).multiplyScalar(Ge).floor(),ue=ke;if(H!==0&&(B=jE),ce.bindFramebuffer(R.FRAMEBUFFER,B)&&ce.drawBuffers(x,B),ce.viewport(Q),ce.scissor(ee),ce.setScissorTest(ue),V){let de=S.get(x.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_CUBE_MAP_POSITIVE_X+N,de.__webglTexture,H)}else if(fe){let de=N;for(let be=0;be<x.textures.length;be++){let we=S.get(x.textures[be]);R.framebufferTextureLayer(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0+be,we.__webglTexture,H,de)}}else if(x!==null&&H!==0){let de=S.get(x.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,de.__webglTexture,H)}z=-1},this.readRenderTargetPixels=function(x,N,H,B,V,fe,_e,de=0){if(!(x&&x.isWebGLRenderTarget)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let be=S.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(be=be[_e]),be){ce.bindFramebuffer(R.FRAMEBUFFER,be);try{let we=x.textures[de],Ve=we.format,Xe=we.type;if(x.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+de),!Et.textureFormatReadable(Ve)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Et.textureTypeReadable(Xe)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}N>=0&&N<=x.width-B&&H>=0&&H<=x.height-V&&R.readPixels(N,H,B,V,A.convert(Ve),A.convert(Xe),fe)}finally{let we=P!==null?S.get(P).__webglFramebuffer:null;ce.bindFramebuffer(R.FRAMEBUFFER,we)}}},this.readRenderTargetPixelsAsync=function(x,N,H,B,V,fe,_e,de=0){return ht(this,null,function*(){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let be=S.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(be=be[_e]),be)if(N>=0&&N<=x.width-B&&H>=0&&H<=x.height-V){ce.bindFramebuffer(R.FRAMEBUFFER,be);let we=x.textures[de],Ve=we.format,Xe=we.type;if(x.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+de),!Et.textureFormatReadable(Ve))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Et.textureTypeReadable(Xe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Ce=R.createBuffer();R.bindBuffer(R.PIXEL_PACK_BUFFER,Ce),R.bufferData(R.PIXEL_PACK_BUFFER,fe.byteLength,R.STREAM_READ),R.readPixels(N,H,B,V,A.convert(Ve),A.convert(Xe),0);let yt=P!==null?S.get(P).__webglFramebuffer:null;ce.bindFramebuffer(R.FRAMEBUFFER,yt);let Vt=R.fenceSync(R.SYNC_GPU_COMMANDS_COMPLETE,0);return R.flush(),yield iE(R,Vt,4),R.bindBuffer(R.PIXEL_PACK_BUFFER,Ce),R.getBufferSubData(R.PIXEL_PACK_BUFFER,0,fe),R.deleteBuffer(Ce),R.deleteSync(Vt),fe}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")})},this.copyFramebufferToTexture=function(x,N=null,H=0){let B=Math.pow(2,-H),V=Math.floor(x.image.width*B),fe=Math.floor(x.image.height*B),_e=N!==null?N.x:0,de=N!==null?N.y:0;v.setTexture2D(x,0),R.copyTexSubImage2D(R.TEXTURE_2D,H,0,0,_e,de,V,fe),ce.unbindTexture()};let WE=R.createFramebuffer(),$E=R.createFramebuffer();this.copyTextureToTexture=function(x,N,H=null,B=null,V=0,fe=0){let _e,de,be,we,Ve,Xe,Ce,yt,Vt,Nt=x.isCompressedTexture?x.mipmaps[fe]:x.image;if(H!==null)_e=H.max.x-H.min.x,de=H.max.y-H.min.y,be=H.isBox3?H.max.z-H.min.z:1,we=H.min.x,Ve=H.min.y,Xe=H.isBox3?H.min.z:0;else{let Ht=Math.pow(2,-V);_e=Math.floor(Nt.width*Ht),de=Math.floor(Nt.height*Ht),x.isDataArrayTexture?be=Nt.depth:x.isData3DTexture?be=Math.floor(Nt.depth*Ht):be=1,we=0,Ve=0,Xe=0}B!==null?(Ce=B.x,yt=B.y,Vt=B.z):(Ce=0,yt=0,Vt=0);let bt=A.convert(N.format),pn=A.convert(N.type),ve;N.isData3DTexture?(v.setTexture3D(N,0),ve=R.TEXTURE_3D):N.isDataArrayTexture||N.isCompressedArrayTexture?(v.setTexture2DArray(N,0),ve=R.TEXTURE_2D_ARRAY):(v.setTexture2D(N,0),ve=R.TEXTURE_2D),ce.activeTexture(R.TEXTURE0),ce.pixelStorei(R.UNPACK_FLIP_Y_WEBGL,N.flipY),ce.pixelStorei(R.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),ce.pixelStorei(R.UNPACK_ALIGNMENT,N.unpackAlignment);let On=ce.getParameter(R.UNPACK_ROW_LENGTH),it=ce.getParameter(R.UNPACK_IMAGE_HEIGHT),Wn=ce.getParameter(R.UNPACK_SKIP_PIXELS),wi=ce.getParameter(R.UNPACK_SKIP_ROWS),_r=ce.getParameter(R.UNPACK_SKIP_IMAGES);ce.pixelStorei(R.UNPACK_ROW_LENGTH,Nt.width),ce.pixelStorei(R.UNPACK_IMAGE_HEIGHT,Nt.height),ce.pixelStorei(R.UNPACK_SKIP_PIXELS,we),ce.pixelStorei(R.UNPACK_SKIP_ROWS,Ve),ce.pixelStorei(R.UNPACK_SKIP_IMAGES,Xe);let no=x.isDataArrayTexture||x.isData3DTexture,St=N.isDataArrayTexture||N.isData3DTexture;if(x.isDepthTexture){let Ht=S.get(x),xr=S.get(N),wt=S.get(Ht.__renderTarget),br=S.get(xr.__renderTarget);ce.bindFramebuffer(R.READ_FRAMEBUFFER,wt.__webglFramebuffer),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,br.__webglFramebuffer);for(let io=0;io<be;io++)no&&(R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,S.get(x).__webglTexture,V,Xe+io),R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,S.get(N).__webglTexture,fe,Vt+io)),R.blitFramebuffer(we,Ve,_e,de,Ce,yt,_e,de,R.DEPTH_BUFFER_BIT,R.NEAREST);ce.bindFramebuffer(R.READ_FRAMEBUFFER,null),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else if(V!==0||x.isRenderTargetTexture||S.has(x)){let Ht=S.get(x),xr=S.get(N);ce.bindFramebuffer(R.READ_FRAMEBUFFER,WE),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,$E);for(let wt=0;wt<be;wt++)no?R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,Ht.__webglTexture,V,Xe+wt):R.framebufferTexture2D(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,Ht.__webglTexture,V),St?R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,xr.__webglTexture,fe,Vt+wt):R.framebufferTexture2D(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,xr.__webglTexture,fe),V!==0?R.blitFramebuffer(we,Ve,_e,de,Ce,yt,_e,de,R.COLOR_BUFFER_BIT,R.NEAREST):St?R.copyTexSubImage3D(ve,fe,Ce,yt,Vt+wt,we,Ve,_e,de):R.copyTexSubImage2D(ve,fe,Ce,yt,we,Ve,_e,de);ce.bindFramebuffer(R.READ_FRAMEBUFFER,null),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else St?x.isDataTexture||x.isData3DTexture?R.texSubImage3D(ve,fe,Ce,yt,Vt,_e,de,be,bt,pn,Nt.data):N.isCompressedArrayTexture?R.compressedTexSubImage3D(ve,fe,Ce,yt,Vt,_e,de,be,bt,Nt.data):R.texSubImage3D(ve,fe,Ce,yt,Vt,_e,de,be,bt,pn,Nt):x.isDataTexture?R.texSubImage2D(R.TEXTURE_2D,fe,Ce,yt,_e,de,bt,pn,Nt.data):x.isCompressedTexture?R.compressedTexSubImage2D(R.TEXTURE_2D,fe,Ce,yt,Nt.width,Nt.height,bt,Nt.data):R.texSubImage2D(R.TEXTURE_2D,fe,Ce,yt,_e,de,bt,pn,Nt);ce.pixelStorei(R.UNPACK_ROW_LENGTH,On),ce.pixelStorei(R.UNPACK_IMAGE_HEIGHT,it),ce.pixelStorei(R.UNPACK_SKIP_PIXELS,Wn),ce.pixelStorei(R.UNPACK_SKIP_ROWS,wi),ce.pixelStorei(R.UNPACK_SKIP_IMAGES,_r),fe===0&&N.generateMipmaps&&R.generateMipmap(ve),ce.unbindTexture()},this.initRenderTarget=function(x){S.get(x).__webglFramebuffer===void 0&&v.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?v.setTextureCube(x,0):x.isData3DTexture?v.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?v.setTexture2DArray(x,0):v.setTexture2D(x,0),ce.unbindTexture()},this.resetState=function(){j=0,q=0,P=null,ce.reset(),ie.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return xi}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=Qe._getDrawingBufferColorSpace(e),t.unpackColorSpace=Qe._getUnpackColorSpace()}};var _l=class n{obstacleBox=new Bi;closestPoint=new O;findTrackContact(e,t,i){let r=null,s=Number.POSITIVE_INFINITY;for(let o of i){if(e.z>o.nearZ+t*.9||e.z<o.farZ-t*.9)continue;let a=o.width*.5-t*.12,c=e.x-o.centerX;if(Math.abs(c)<=a){let l=Ut.clamp((o.nearZ-e.z)/o.length,0,1),u=Ut.lerp(o.nearY,o.farY,l),d=u+t,f=e.y-d;if(f<-t*1.65)continue;let h=Math.abs(f)+Math.max(0,-f)*.35;if(h>=s)continue;s=h,r={segment:o,surfaceY:u,edgeDistance:a-Math.abs(c)}}}return r}hitObstacle(e,t,i){let r=t*t;for(let s of i)if(this.obstacleBox.setFromObject(s.mesh),this.closestPoint.copy(e).clamp(this.obstacleBox.min,this.obstacleBox.max),this.closestPoint.distanceToSquared(e)<=r)return s;return null}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var xl=class n{frameId=0;lastTime=0;elapsed=0;running=!1;start(e){this.stop(),this.running=!0,this.elapsed=0,this.lastTime=performance.now();let t=i=>{if(!this.running)return;let r=Math.min((i-this.lastTime)/1e3,.05);this.lastTime=i,this.elapsed+=r,e(r,this.elapsed),this.frameId=requestAnimationFrame(t)};this.frameId=requestAnimationFrame(t)}stop(){this.frameId!==0&&(cancelAnimationFrame(this.frameId),this.frameId=0),this.running=!1}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var bl=class n{leftKeys=new Set(["KeyA","ArrowLeft"]);rightKeys=new Set(["KeyD","ArrowRight"]);pressedKeys=new Set;touchDirection=0;restartQueued=!1;keyboardTarget=null;keyDownHandler=e=>{if(this.leftKeys.has(e.code)||this.rightKeys.has(e.code)){this.pressedKeys.add(e.code),e.preventDefault();return}(e.code==="Space"||e.code==="Enter"||e.code==="KeyR")&&(this.restartQueued=!0,e.preventDefault())};keyUpHandler=e=>{(this.leftKeys.has(e.code)||this.rightKeys.has(e.code))&&(this.pressedKeys.delete(e.code),e.preventDefault())};attach(e=window){this.detach(),this.keyboardTarget=e,e.addEventListener("keydown",this.keyDownHandler,{passive:!1}),e.addEventListener("keyup",this.keyUpHandler,{passive:!1})}detach(){this.keyboardTarget&&(this.keyboardTarget.removeEventListener("keydown",this.keyDownHandler),this.keyboardTarget.removeEventListener("keyup",this.keyUpHandler),this.keyboardTarget=null,this.pressedKeys.clear(),this.touchDirection=0)}get steering(){let e=Number(this.isPressed(this.rightKeys))-Number(this.isPressed(this.leftKeys));return Math.max(-1,Math.min(1,e+this.touchDirection))}setTouchDirection(e){this.touchDirection=Math.max(-1,Math.min(1,e))}releaseTouchDirection(e){this.touchDirection===e&&(this.touchDirection=0)}consumeRestart(){let e=this.restartQueued;return this.restartQueued=!1,e}isPressed(e){for(let t of e)if(this.pressedKeys.has(t))return!0;return!1}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var FE="neon-slope-best-stage-score",Sl=class n{score=ei(0);speed=ei(24);bestScore=ei(this.readBestScore());elapsed=0;slowdownPenalty=0;baseSpeed=24;maxSpeed=82;reset(){this.elapsed=0,this.slowdownPenalty=0,this.score.set(0),this.speed.set(this.baseSpeed),this.bestScore.set(this.readBestScore())}update(e){this.elapsed+=e,this.slowdownPenalty=Math.max(0,this.slowdownPenalty-e*6.2)}applySlowdown(e){this.slowdownPenalty=Math.min(22,this.slowdownPenalty+e)}completeStages(e){e<=0||this.score.update(t=>t+e)}syncSpeed(e){this.speed.set(e)}slowdown(){return this.slowdownPenalty}commitBestScore(){let e=this.score();if(!(e<=this.bestScore())){this.bestScore.set(e);try{localStorage.setItem(FE,String(e))}catch(t){}}}difficulty(){return Math.min(1,(this.score()+this.elapsed*.075)/20)}minSpeed(){return this.baseSpeed*.75}maxSpeedLimit(){return this.maxSpeed}readBestScore(){try{let e=localStorage.getItem(FE);return e&&Number(e)||0}catch(e){return 0}}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var Ml=class n{leadDistance=390;cleanupDistance=300;trackThickness=95;downhillRate=.135;minRoadCenterGap=12.4;maxRoadCenterGap=26.5;scene=null;trackMaterial=null;rampMaterial=null;edgeMaterial=null;laneMaterial=null;obstacleMaterial=null;barMaterial=null;buildingMaterial=null;wireMaterial=null;dimWireMaterial=null;gateMaterial=null;nextNearZ=28;nextNearY=0;lastCenterX=0;segmentIndex=0;generatedStageIndex=0;lastStagePattern=-1;activeRoadCount=1;activeRoadCenters=[0];activeRoadStates=[];activeNearbyIslands=[];nextIslandId=0;roadClampOffset=0;roadClampHalfRange=null;roadClampAbsoluteMinX=null;roadClampAbsoluteMaxX=null;stagePatternBag=[];mutableSegments=[];mutableObstacles=[];mutableDecorations=[];stageGates=[];get segments(){return this.mutableSegments}get obstacles(){return this.mutableObstacles}init(e){this.scene=e,this.createMaterials(),this.reset()}reset(){for(this.clearDynamicObjects(),this.nextNearZ=28,this.nextNearY=0,this.lastCenterX=0,this.segmentIndex=0,this.generatedStageIndex=0,this.lastStagePattern=-1,this.activeRoadCount=1,this.activeRoadCenters=[0],this.activeRoadStates=[],this.activeNearbyIslands=[],this.nextIslandId=0,this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null,this.stagePatternBag=[],this.stageGates.length=0,this.createStarterRunway();this.nextNearZ>-this.leadDistance;)this.addStage()}ensureAhead(e){for(;this.nextNearZ>e-this.leadDistance;)this.addStage();this.cleanupBehind(e)}consumePassedStages(e){let t=0;for(;this.stageGates.length>0&&e<=this.stageGates[0].z;)this.stageGates.shift(),t+=1;return t}update(e,t){this.edgeMaterial&&(this.edgeMaterial.emissiveIntensity=2.8+Math.sin(t*6.4)*.5),this.rampMaterial&&(this.rampMaterial.emissiveIntensity=.65+Math.sin(t*3)*.16);for(let i of this.mutableObstacles)i.moveAmplitude!==0&&(i.mesh.position.x=i.baseX+Math.sin(t*i.moveSpeed+i.movePhase)*i.moveAmplitude)}surfaceYAt(e,t){let i=Ut.clamp((e.nearZ-t)/e.length,0,1);return Ut.lerp(e.nearY,e.farY,i)}fallLimitForZ(e){let t=null,i=Number.POSITIVE_INFINITY,r=Number.POSITIVE_INFINITY;for(let o of this.mutableSegments){let a=e>o.nearZ?e-o.nearZ:e<o.farZ?o.farZ-e:0,c=Ut.clamp(e,o.farZ,o.nearZ),l=this.surfaceYAt(o,c);a<90&&(r=Math.min(r,l)),a<i&&(i=a,t=o)}if(Number.isFinite(r))return r-75;if(!t)return this.lowestActiveRoadY()-75;let s=Ut.clamp(e,t.farZ,t.nearZ);return this.surfaceYAt(t,s)-75}destroy(){this.clearDynamicObjects(),this.trackMaterial?.dispose(),this.rampMaterial?.dispose(),this.edgeMaterial?.dispose(),this.laneMaterial?.dispose(),this.obstacleMaterial?.dispose(),this.barMaterial?.dispose(),this.buildingMaterial?.dispose(),this.wireMaterial?.dispose(),this.dimWireMaterial?.dispose(),this.gateMaterial?.dispose(),this.trackMaterial=null,this.rampMaterial=null,this.edgeMaterial=null,this.laneMaterial=null,this.obstacleMaterial=null,this.barMaterial=null,this.buildingMaterial=null,this.wireMaterial=null,this.dimWireMaterial=null,this.gateMaterial=null,this.scene=null}createStarterRunway(){this.addTrackSegment({centerX:0,farYOffset:-6.2,kind:"starter",length:54,stageIndex:-1,width:12})}addStage(){let e=this.generatedStageIndex,t=Math.min(1,e/18);e%5===0&&this.activateRoadSequence();let i=this.nextNearZ,r=this.lowestActiveRoadY(),s=this.random(2.8,5.6+t*1.2),o=this.random(3.4,6.4+t*1.8);if(this.activeRoadCount===2||this.activeRoadCount===3)this.addParallelGeneratedRoads(e,t,this.activeRoadCount,s,o);else{let c=this.nextStagePattern();this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null,this.runStagePattern(c,e,t),this.advanceGap(s,o),this.activeRoadCenters=[this.lastCenterX],this.activeRoadStates=[{centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY}]}let a=this.captureRoadCursor();this.maybeSpawnNearbyIsland(e,i,r),this.generateNearbyIslands(e,t,s,o),this.restoreRoadCursor(a),this.stageGates.push({stageIndex:e,z:this.nextNearZ}),this.addEnvironmentBand(i,this.nextNearZ,r,this.lowestActiveRoadY(),e),this.generatedStageIndex+=1}rollLaneCount(){let e=Math.random();return e<.5?1:e<.8?2:3}activateRoadSequence(){let e=this.activeRoadStates.length>0?this.activeRoadStates:[{centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY}],t=this.average(e.map(l=>l.centerX)),i=Math.max(...e.map(l=>l.nextNearZ)),r=this.average(e.map(l=>l.nextNearY)),s=Math.min(...e.map(l=>l.nextNearY));this.activeRoadCount=this.rollLaneCount(),this.activeRoadCenters=this.centersForRoadCount(this.activeRoadCount,t);let o=this.roadHeightOffsets(this.activeRoadCount),a=Math.max(...o),c=s-this.random(2.4,4.1);this.activeRoadStates=this.activeRoadCenters.map((l,u)=>({centerX:l,nextNearZ:i,nextNearY:this.activeRoadCount===1?r:c+o[u]-a})),this.lastCenterX=this.activeRoadCount===1?this.activeRoadCenters[0]:this.average(this.activeRoadCenters),this.syncCursorFromRoadStates()}centersForRoadCount(e,t){if(e===1)return[this.clampValue(t+this.randomSigned(1),-7,7)];if(e===2){let o=this.random(17.6,26.8),a=this.clampValue(t+this.randomSigned(1.2),-23+o*.5,23-o*.5);return[a-o*.5,a+o*.5]}let i=this.random(15.4,25.8),r=this.random(15.4,25.8),s=this.clampValue(t+this.randomSigned(1.4),-24+i,24-r);return[s-i,s,s+r]}roadHeightOffsets(e){if(e===1)return[0];let t=e===2?[-1.9,2.2]:[-3.2,.4,3.1];return this.shuffleNumbers(t).map(i=>i+this.randomSigned(.55))}nextStagePattern(){if(this.stagePatternBag.length===0&&(this.stagePatternBag=this.shuffleNumbers([0,1,2,3,4,5,6,7,8])),this.stagePatternBag[0]===this.lastStagePattern&&this.stagePatternBag.length>1){let t=Math.floor(this.random(1,this.stagePatternBag.length));[this.stagePatternBag[0],this.stagePatternBag[t]]=[this.stagePatternBag[t],this.stagePatternBag[0]]}let e=this.stagePatternBag.shift()??0;return this.lastStagePattern=e,e}runStagePattern(e,t,i){switch(e){case 0:this.addLaunchRampStage(t,i);break;case 1:this.addBrakeLaunchStage(t,i);break;case 2:this.addSkyDropStage(t,i);break;case 3:this.addMovingLaneStage(t,i);break;case 4:this.addRampChainStage(t,i);break;case 5:this.addSlalomStage(t,i);break;case 6:this.addNarrowLandingStage(t,i);break;case 8:this.addBoostTunnelStage(t,i);break;default:this.addLanePressureStage(t,i);break}}addParallelGeneratedRoads(e,t,i,r,s){if(this.activeRoadStates.length!==i){let l=this.centersForRoadCount(i,this.lastCenterX),u=this.roadHeightOffsets(i);this.activeRoadStates=l.map((d,f)=>({centerX:d,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY+u[f]}))}let o=[],a=i===2?2.55:1.45,c=[...this.activeRoadStates].sort((l,u)=>l.centerX-u.centerX);for(let l of this.activeRoadStates){let u=c.indexOf(l),d=c[u-1],f=c[u+1];this.nextNearZ=l.nextNearZ,this.nextNearY=l.nextNearY,this.lastCenterX=l.centerX,this.roadClampOffset=l.centerX,this.roadClampHalfRange=a,this.roadClampAbsoluteMinX=d?d.centerX+this.minRoadCenterGap:-24,this.roadClampAbsoluteMaxX=f?f.centerX-this.minRoadCenterGap:24,this.runStagePattern(this.nextStagePattern(),e,t),this.advanceGap(Math.max(1.8,r+this.randomSigned(1.3)),Math.max(2.4,s+this.randomSigned(1.2))),o.push({centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY})}this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null,o=this.settleRoadSpacing(o,i),this.activeRoadStates=o,this.activeRoadCenters=o.map(l=>l.centerX),this.lastCenterX=this.average(this.activeRoadCenters),this.syncCursorFromRoadStates()}maybeSpawnNearbyIsland(e,t,i){if(e<1||this.activeNearbyIslands.length>=2||Math.random()>.2)return;let r=this.chooseIslandPlacement();r&&(this.activeNearbyIslands.push({id:this.nextIslandId,state:{centerX:r.centerX,nextNearZ:t-this.random(2.5,6.5),nextNearY:i-this.random(1.2,4.4)},remainingStages:Math.floor(this.random(2,5)),farZ:t,minX:r.minX,maxX:r.maxX}),this.nextIslandId+=1)}generateNearbyIslands(e,t,i,r){for(let s of this.activeNearbyIslands)if(!(s.remainingStages<=0)){if(!this.refreshIslandBounds(s)){s.remainingStages=0;continue}this.nextNearZ=s.state.nextNearZ,this.nextNearY=s.state.nextNearY,this.lastCenterX=s.state.centerX,this.roadClampOffset=s.state.centerX,this.roadClampHalfRange=2.1,this.roadClampAbsoluteMinX=s.minX,this.roadClampAbsoluteMaxX=s.maxX,this.runStagePattern(this.randomIslandStagePattern(),e,t),this.advanceGap(Math.max(1.8,i+this.randomSigned(1.8)),Math.max(2.2,r+this.randomSigned(1.4))),s.state={centerX:this.clampValue(this.lastCenterX,s.minX,s.maxX),nextNearZ:this.nextNearZ,nextNearY:this.nextNearY},s.farZ=this.nextNearZ,s.remainingStages-=1}this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null}chooseIslandPlacement(){let t=[...this.activeRoadStates.length>0?this.activeRoadStates.map(d=>d.centerX):[this.lastCenterX]].sort((d,f)=>d-f),i=[...t,...this.activeNearbyIslands.map(d=>d.state.centerX)],r=[];for(let d=0;d<t.length-1;d+=1){let f=t[d],h=t[d+1],g=f+this.minRoadCenterGap,y=h-this.minRoadCenterGap;y-g>1.4&&r.push({centerX:this.random(g,y),minX:g,maxX:y})}let s=t[0],o=t[t.length-1],a=Math.max(-27,s-this.maxRoadCenterGap),c=Math.min(27,s-this.minRoadCenterGap),l=Math.max(-27,o+this.minRoadCenterGap),u=Math.min(27,o+this.maxRoadCenterGap);c-a>2.4&&r.push({centerX:this.random(a,c),minX:a,maxX:c}),u-l>2.4&&r.push({centerX:this.random(l,u),minX:l,maxX:u});for(let d of this.shuffleObjects(r)){let f=this.clampValue(d.centerX,d.minX,d.maxX);if(this.hasRoadClearance(f,i))return st(he({},d),{centerX:f})}return null}refreshIslandBounds(e){let t=this.activeNearbyIslands.filter(c=>c.id!==e.id).map(c=>c.state.centerX),i=this.activeRoadStates.length>0?[...this.activeRoadStates.map(c=>c.centerX)].sort((c,l)=>c-l):[this.lastCenterX],r=[...i].reverse().find(c=>c<e.state.centerX),s=i.find(c=>c>e.state.centerX),o=-27,a=27;if(r!==void 0&&(o=Math.max(o,r+this.minRoadCenterGap)),s!==void 0&&(a=Math.min(a,s-this.minRoadCenterGap)),r===void 0){let c=i[0];o=Math.max(o,c-this.maxRoadCenterGap),a=Math.min(a,c-this.minRoadCenterGap)}if(s===void 0){let c=i[i.length-1];o=Math.max(o,c+this.minRoadCenterGap),a=Math.min(a,c+this.maxRoadCenterGap)}for(let c of t)c<e.state.centerX?o=Math.max(o,c+this.minRoadCenterGap):a=Math.min(a,c-this.minRoadCenterGap);return a-o<1.1?!1:(e.minX=o,e.maxX=a,e.state.centerX=this.clampValue(e.state.centerX+this.randomSigned(1.15),o,a),!0)}hasRoadClearance(e,t){return t.every(i=>Math.abs(e-i)>=this.minRoadCenterGap)}settleRoadSpacing(e,t){let i=[...e].sort((d,f)=>d.centerX-f.centerX),r=t===2?this.maxRoadCenterGap:this.maxRoadCenterGap-1.2;for(let d=0;d<2;d+=1)for(let f=1;f<i.length;f+=1){let h=i[f-1],g=i[f],y=g.centerX-h.centerX;if(y<this.minRoadCenterGap){let m=(this.minRoadCenterGap-y)*.5;h.centerX-=m,g.centerX+=m}else if(y>r){let m=(y-r)*.5;h.centerX+=m,g.centerX-=m}}let s=this.average(i.map(d=>d.centerX)),o=-24,a=24,c=Math.min(...i.map(d=>d.centerX))-o,l=a-Math.max(...i.map(d=>d.centerX)),u=c<0?-c:l<0?l:-s*.08;return i.map(d=>st(he({},d),{centerX:this.clampValue(d.centerX+u,o,a)}))}addParallelRoadStage(e,t,i){let r=this.nextNearZ,s=this.nextNearY,o=this.nextStagePattern(),a=o===0||o===4||o===6,c=o===1,l=o===3||o===7,u=a?32+t*5:c?38+t*5:68+t*14,d=r-u,f=s+this.parallelStageYOffset(u,o,t),h=i===2?4.25-t*.25:3.45-t*.15,g=a?"launch":c?"brake":o===2?"ramp":"straight",y=this.centersForRoadCount(i,this.average(this.activeRoadCenters)),m=Math.floor(this.random(0,i));for(let p=0;p<i;p+=1){let b=this.createTrackSegment({centerX:y[p],farY:f,farZ:d,kind:g,length:u,nearY:s,nearZ:r,stageIndex:e,width:h});if(p!==m){let M=l||p%2===1?"slider":"cube";this.spawnObstacle(b,0,M,this.random(.38,.72))}else t>.6&&l&&this.spawnObstacle(b,Math.random()>.5?-1:1,"cube",.68)}this.nextNearZ=d,this.nextNearY=f,this.activeRoadCenters=y,this.lastCenterX=this.average(y),a&&this.advanceGap(24+t*8,10+t*4)}parallelStageYOffset(e,t,i){switch(t){case 0:case 4:case 6:return 6.5+i*3.5;case 1:return 4.5+i*2.2;case 2:return-e*(.18+i*.035);default:return-e*(.12+i*.025)}}addSplitLaneStage(e,t,i){let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(.9),-5.5,5.5),farYOffset:-5.8-t*2.2,kind:"straight",length:26,stageIndex:e,width:11.4-t});this.spawnObstacle(r,Math.random()>.5?-1:1,"cube",.54);let s=this.nextNearZ,o=this.nextNearY,a=i===2?78+t*12:88+t*14,c=s-a,l=o-a*(.12+t*.025),u=this.clamp(this.lastCenterX+this.randomSigned(1),i===3?-2.2:-4.8,i===3?2.2:4.8),d=i===2?[-3.25,3.25]:[-5.1,0,5.1],f=i===2?3.85-t*.2:3.25-t*.12,h=Math.floor(this.random(0,i)),g=[];for(let m=0;m<i;m+=1)g.push(this.createTrackSegment({centerX:u+d[m],farY:l,farZ:c,kind:"split",length:a,nearY:o,nearZ:s,stageIndex:e,width:f}));for(let m=0;m<g.length;m+=1){let p=g[m];m!==h&&this.spawnObstacle(p,0,m%2===0?"wall":"slider",.38+m*.18)}this.nextNearZ=c,this.nextNearY=l,this.lastCenterX=u,this.advanceGap(2.2+t*.8,2.5+t);let y=this.addTrackSegment({centerX:this.clamp(u+this.randomSigned(1.2),-6.5,6.5),farYOffset:-8-t*3.5,kind:"landing",length:38+t*8,stageIndex:e,width:10.2-t});t>.55&&this.spawnObstacle(y,Math.random()>.5?-1:1,"slider",.68)}addLaunchRampStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.2),-6.5,6.5),farYOffset:-8.5-t*3,kind:"straight",length:38,stageIndex:e,width:10.8-t});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.48),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.6+t),-7.2,7.2),farYOffset:8.5+t*4.5,kind:"launch",length:30+t*5,stageIndex:e,width:9.8-t*.9}),this.advanceGap(36+t*14,13+t*6);let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8+t*1.1),-7.4,7.4),farYOffset:-13-t*6,kind:"landing",length:58+t*10,stageIndex:e,width:10-t*1.25});this.spawnObstacle(r,0,"slider",.58)}addBrakeLaunchStage(e,t){this.addTrackSegment({centerX:this.lastCenterX,farYOffset:-7-t*2,kind:"straight",length:34,stageIndex:e,width:10.4-t});let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.2),-6.6,6.6),farYOffset:5.8+t*2.6,kind:"brake",length:28+t*4,stageIndex:e,width:9.6-t});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.42),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4+t),-7.2,7.2),farYOffset:7.2+t*3,kind:"launch",length:25+t*5,stageIndex:e,width:9.2-t}),this.advanceGap(30+t*12,11+t*5),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8),-7.4,7.4),farYOffset:-12-t*4,kind:"landing",length:48+t*12,stageIndex:e,width:8.8-t})}addSkyDropStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.4),-7.4,7.4),farYOffset:-22-t*12,kind:"ramp",length:82+t*12,stageIndex:e,width:10.5-t*1.4});this.spawnObstacle(i,-1,"wall",.36),this.spawnObstacle(i,1,"slider",.68),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.7),-7.5,7.5),farYOffset:5.8+t*2.8,kind:"launch",length:24+t*5,stageIndex:e,width:8.6-t}),this.advanceGap(28+t*10,12+t*5),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2),-7.8,7.8),farYOffset:-10-t*5,kind:"landing",length:42+t*12,stageIndex:e,width:8.2-t*.7})}addMovingLaneStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8),-7,7),farYOffset:-(86+t*10)*(.12+t*.025),kind:"straight",length:86+t*10,stageIndex:e,width:11-t*1.2});this.spawnObstacle(i,-1,"slider",.22),this.spawnObstacle(i,1,"slider",.48),this.spawnObstacle(i,0,"slider",.75)}addRampChainStage(e,t){for(let r=0;r<3;r+=1)this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.6+t),-7.2,7.2),farYOffset:r===1?4.8+t*2:-8.2-t*2,kind:r===1?"launch":"ramp",length:28+r*5,stageIndex:e,width:9.6-t});this.advanceGap(26+t*8,10+t*5);let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.7+t*.8),-7.5,7.5),farYOffset:-10-t*4,kind:"landing",length:44+t*10,stageIndex:e,width:9-t});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.5)}addNarrowLandingStage(e,t){this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4),-6.4,6.4),farYOffset:8+t*3.8,kind:"launch",length:32+t*4,stageIndex:e,width:9.4-t*.8}),this.advanceGap(34+t*14,14+t*6);let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.2+t),-8,8),farYOffset:-12-t*6,kind:"landing",length:64+t*12,stageIndex:e,width:8-t*.8});this.spawnObstacle(i,Math.random()>.5?-1:1,"wall",.62)}addFastDropStage(e,t){let i=68+t*18,r=10.8-t*1.6,s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.6+t*2),-6.8,6.8),farYOffset:-i*(.18+t*.04),kind:"ramp",length:i,stageIndex:e,width:r});this.spawnObstacle(s,-1,"cube",.32),t>.25&&this.spawnObstacle(s,1,"slider",.68)}addSweepingTurnStage(e,t){let i=Math.random()>.5?1:-1,r=9.8-t*1.4;for(let s=0;s<4;s+=1){let o=(s+1)/4,a=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+i*(1.4+o*(1.35+t)),-8,8),farYOffset:-(19+t*4)*(.14+o*.035),kind:"chicane",length:19+t*4,stageIndex:e,width:r-s*.25});s===1&&this.spawnObstacle(a,-i,"wall",.48),s===3&&t>.35&&this.spawnObstacle(a,0,"slider",.58)}}addNarrowSlopeStage(e,t){let i=84+t*12,r=7.2-t*1.1,s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.2+t*1.4),-7.6,7.6),farYOffset:-i*(.15+t*.035),kind:"straight",length:i,stageIndex:e,width:r});this.spawnObstacle(s,-1,"wall",.28),this.spawnObstacle(s,1,"wall",.56),t>.55&&this.spawnObstacle(s,0,"slider",.78)}addRollingSlopeStage(e,t){for(let i=0;i<4;i+=1){let r=i%2===0?.18+t*.035:.09+t*.02,s=18+i*2+t*3,o=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.2+t*1.8),-7.2,7.2),farYOffset:-s*r,kind:i%2===0?"ramp":"straight",length:s,stageIndex:e,width:9.4-t*1.1});i===2&&this.spawnObstacle(o,Math.random()>.5?-1:1,"cube",.48)}}addLanePressureStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8+t*1.6),-7,7),farYOffset:-(72+t*14)*(.145+t*.04),kind:"straight",length:72+t*14,stageIndex:e,width:10-t*1.25}),r=Math.random()>.5?-1:1;this.spawnObstacle(i,r,"wall",.25),this.spawnObstacle(i,-r,"cube",.48),this.spawnObstacle(i,0,t>.45?"slider":"cube",.72)}addBrakeRampStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.3),-6.5,6.5),farYOffset:-5.2-t*1.5,kind:"straight",length:28,stageIndex:e,width:10.5-t*1.2});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.52);let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.1+t),-7,7),farYOffset:3.4+t*2.1,kind:"brake",length:30+t*7,stageIndex:e,width:9.4-t});this.spawnObstacle(r,0,"slider",.56);let s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.2+t*1.4),-7.8,7.8),farYOffset:-8.8-t*4.6,kind:"ramp",length:36+t*8,stageIndex:e,width:8.8-t*1.1});this.spawnObstacle(s,Math.random()>.5?-1:1,t>.4?"slider":"cube",.64)}addSlalomStage(e,t){let i=Math.random()>.5?1:-1;for(let r=0;r<5;r+=1){let s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+i*(r%2===0?2+t*.65:-2.65-t),-7.7,7.7),farYOffset:-(20+t*3)*(.115+r*.012),kind:"chicane",length:20+t*3,stageIndex:e,width:9.35-t*.85});r%2===1?this.spawnObstacle(s,0,"slider",.58):r>0&&this.spawnObstacle(s,-i,"wall",.48)}}addBoostTunnelStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.1),-6.4,6.4),farYOffset:-5.8-t*1.6,kind:"straight",length:28,stageIndex:e,width:10.6-t*.7});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.52);let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(.7),-6.5,6.5),farYOffset:-(66+t*10)*(.165+t*.02),kind:"boost",length:66+t*10,stageIndex:e,width:10.2-t*.65});this.addTunnelVisual(r);let s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4+t),-7.1,7.1),farYOffset:-9.2-t*3.5,kind:"landing",length:38+t*6,stageIndex:e,width:9.6-t*.9});t>.45&&this.spawnObstacle(s,Math.random()>.5?-1:1,"slider",.62)}addPinballStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4),-6.4,6.4),farYOffset:-(82+t*8)*(.125+t*.03),kind:"straight",length:82+t*8,stageIndex:e,width:11.4-t*1.35});this.spawnObstacle(i,-1,"slider",.26),this.spawnObstacle(i,1,"slider",.52),this.spawnObstacle(i,0,t>.35?"slider":"cube",.76)}addRampJumpStage(e,t){let i=this.randomSigned(1.2+t*2.4),r=this.addTrackSegment({centerX:this.lastCenterX,kind:"straight",length:18,stageIndex:e,width:10.5-t*1.2});this.spawnObstacle(r,t>.35?-1:1,"cube",.45),this.addTrackSegment({centerX:this.lastCenterX+i*.4,farYOffset:1.75+t*.75,kind:"ramp",length:17,stageIndex:e,width:9.2-t}),this.advanceGap(5.2+t*2.8,1.35+t*.7);let s=this.addTrackSegment({centerX:this.lastCenterX+i,farYOffset:-1.4,kind:"landing",length:25,stageIndex:e,width:8.8-t*1.2});this.spawnObstacle(s,0,t>.45?"slider":"cube",.6)}addSplitBridgeStage(e,t){this.addTrackSegment({centerX:this.lastCenterX,kind:"straight",length:16,stageIndex:e,width:10.4-t}),this.advanceGap(3.5+t,.25);let i=this.nextNearZ,r=this.nextNearY,s=36,o=r-s*this.downhillRate,a=2.55+t*.55,c=3.15-t*.28,l=Math.random()>.5?-1:1,u=this.createTrackSegment({centerX:this.lastCenterX-a,farY:o,farZ:i-s,kind:"split",length:s,nearY:r,nearZ:i,stageIndex:e,width:c}),d=this.createTrackSegment({centerX:this.lastCenterX+a,farY:o,farZ:i-s,kind:"split",length:s,nearY:r,nearZ:i,stageIndex:e,width:c});this.spawnObstacle(l<0?d:u,0,t>.5?"slider":"cube",.5),this.nextNearZ=i-s,this.nextNearY=o,this.advanceGap(3.8+t*1.2,.35),this.addTrackSegment({centerX:this.lastCenterX,kind:"landing",length:20,stageIndex:e,width:9.4-t})}addStepDropStage(e,t){let i=3+(t>.45?1:0);for(let r=0;r<i;r+=1){let s=this.randomSigned(1.4+t*2.2),o=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+s,-7.2,7.2),farYOffset:-.75-t*.25,kind:"steps",length:15+r*1.5,stageIndex:e,width:7.5-t*1.5});(r>0||t>.35)&&this.spawnObstacle(o,r%2===0?-1:1,"wall",.48),this.advanceGap(2.5+t*2.4,.85+t*.45)}this.addTrackSegment({centerX:this.lastCenterX,farYOffset:-1.2,kind:"landing",length:22,stageIndex:e,width:9})}addChicaneStage(e,t){let i=Math.random()>.5?1:-1,r=7.6-t*1.2,s=[this.lastCenterX,this.clamp(this.lastCenterX+i*(3.2+t),-8,8),this.clamp(this.lastCenterX-i*(3.6+t*1.2),-8,8),this.clamp(this.lastCenterX+i*(2.2+t*1.4),-8,8)];for(let o=0;o<s.length;o+=1){let a=this.addTrackSegment({centerX:s[o],kind:"chicane",length:18,stageIndex:e,width:r});o>0&&this.spawnObstacle(a,-i,o%2===0?"cube":"wall",.52),o<s.length-1&&this.advanceGap(1.2+t*.9,.12)}}addWaveStage(e,t){for(let i=0;i<4;i+=1){let r=i%2===0,s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8+t),-7,7),farYOffset:r?1.2+t*.4:-2-t*.35,kind:"ramp",length:17,stageIndex:e,width:8.6-t*.9});r||this.spawnObstacle(s,0,"slider",.58)}}addThreadTheNeedleStage(e,t){let i=this.addTrackSegment({centerX:this.lastCenterX+this.randomSigned(1.8),kind:"straight",length:48,stageIndex:e,width:10-t*1.2});this.spawnObstacle(i,-1,"wall",.3),this.spawnObstacle(i,1,"wall",.58),t>.45&&this.spawnObstacle(i,0,"slider",.78)}addTrackSegment(e){let t=this.nextNearZ,i=t-e.length,r=this.nextNearY,s=r+(e.farYOffset??-e.length*this.downhillRate),o=this.createTrackSegment({centerX:e.centerX,farY:s,farZ:i,kind:e.kind,length:e.length,nearY:r,nearZ:t,stageIndex:e.stageIndex,width:e.width});return this.nextNearZ=i,this.nextNearY=s,this.lastCenterX=e.centerX,o}createTrackSegment(e){if(!this.scene||!this.trackMaterial||!this.rampMaterial||!this.edgeMaterial||!this.laneMaterial)throw new Error("WorldService must be initialized before creating track segments.");let t=(e.nearZ+e.farZ)*.5,i=(e.nearY+e.farY)*.5,r=Math.asin((e.farY-e.nearY)/e.length),s=new ri;s.position.set(e.centerX,i,t),s.rotation.x=r;let o=new kt(e.width,this.trackThickness,e.length),a=e.kind==="ramp"||e.kind==="launch"||e.kind==="brake"||e.kind==="boost"?this.rampMaterial:this.trackMaterial,c=new pt(o,a);c.position.y=-this.trackThickness*.5,c.receiveShadow=!0,s.add(c);let l=new kt(.12,.12,e.length),u=new pt(l,this.edgeMaterial);u.position.set(-e.width*.5,.08,0),s.add(u);let d=new pt(l.clone(),this.edgeMaterial);d.position.set(e.width*.5,.08,0),s.add(d);let f=e.width>5.6?2:1;for(let y=0;y<f;y+=1){let m=f===1?0:(y===0?-1:1)*e.width*.17,p=new pt(new kt(.045,.045,e.length*.86),this.laneMaterial);p.position.set(m,.105,0),s.add(p)}let h=Math.max(3,Math.floor(e.length/5));for(let y=0;y<h;y+=1){let m=-e.length*.38+y/Math.max(1,h-1)*e.length*.76,p=new pt(new kt(e.width*.78,.035,.035),this.laneMaterial);p.position.set(0,.11,m),s.add(p)}if(e.kind==="boost"){let y=new pt(new kt(e.width*.46,.055,e.length*.9),this.laneMaterial);y.position.set(0,.14,0),s.add(y)}this.scene.add(s);let g={group:s,centerX:e.centerX,centerZ:t,nearZ:e.nearZ,farZ:e.farZ,nearY:e.nearY,farY:e.farY,width:e.width,length:e.length,index:this.segmentIndex,stageIndex:e.stageIndex,kind:e.kind};return this.segmentIndex+=1,this.mutableSegments.push(g),g}addTunnelVisual(e){if(!this.edgeMaterial||!this.barMaterial||!this.laneMaterial)return;let t=9,i=e.width*.5,r=.28,s=4.2,o=s;for(let u=0;u<t;u+=1){let d=u/Math.max(1,t-1),f=-e.length*.42+d*e.length*.84,h=new pt(new kt(.18,s,r),this.edgeMaterial),g=new pt(new kt(.18,s,r),this.edgeMaterial),y=new pt(new kt(e.width*.84,.18,r),this.edgeMaterial);h.position.set(-i*.88,s*.5,f),g.position.set(i*.88,s*.5,f),y.position.set(0,o,f),e.group.add(h),e.group.add(g),e.group.add(y)}let a=new pt(new kt(.12,.12,e.length*.84),this.barMaterial),c=new pt(new kt(.12,.12,e.length*.84),this.barMaterial),l=new pt(new kt(e.width*.64,.08,e.length*.84),this.laneMaterial);a.position.set(-i*.88,2.35,0),c.position.set(i*.88,2.35,0),l.position.set(0,o+.1,0),e.group.add(a),e.group.add(c),e.group.add(l)}spawnObstacle(e,t,i,r){if(!this.scene||!this.obstacleMaterial||!this.barMaterial)return;let s=this.sizeForKind(i,e.width),o=e.width*.28,a=Ut.lerp(e.nearZ,e.farZ,r),c=e.centerX+t*o,l=this.surfaceYAt(e,a)+s.y*.5,u=new kt(s.x,s.y,s.z),d=this.obstacleMaterial,f=new pt(u,d);f.position.set(c,l,a),f.castShadow=!0,f.receiveShadow=!0,this.scene.add(f),this.mutableObstacles.push({mesh:f,kind:i,baseX:c,moveAmplitude:i==="slider"?Math.max(0,e.width*.38-s.x*.35):0,moveSpeed:i==="slider"?this.random(1.35,2.25):this.random(.8,1.25),movePhase:Math.random()*Math.PI*2})}addStageGate(e,t,i){if(!this.scene||!this.gateMaterial)return;let r=new ri;r.userData.nearZ=e+1;let s=new kt(24+i%3*2,14,.35),o=new Js(s);s.dispose();let a=new mr(o,this.gateMaterial);a.position.set(0,t+6.7,e),r.add(a),this.scene.add(r),this.mutableDecorations.push(r)}addEnvironmentBand(e,t,i,r,s){if(!this.scene||!this.buildingMaterial||!this.wireMaterial||!this.dimWireMaterial||!this.gateMaterial)return;let o=new ri;o.userData.nearZ=e;let a=Math.abs(e-t),c=Math.max(4,Math.floor(a/15));for(let l=0;l<=c;l+=1){let u=l/c,d=Ut.lerp(e,t,u),f=Ut.lerp(i,r,u);for(let h of[-1,1]){let g=f+this.random(14,42+s*.5),y=f-this.random(125,190),m=g-y,p=this.random(2.2,5.8),b=this.random(7,16),M=h*this.random(28,56),E=this.createSolidBlock(p,m,b,this.buildingMaterial,l%3===0?this.gateMaterial:this.wireMaterial);if(E.position.set(M,y+m*.5,d+this.random(-4,4)),o.add(E),l%2===1){let D=f+this.random(6,18),w=f-this.random(105,165),I=D-w,_=this.createSolidBlock(this.random(4,8),I,this.random(4,8),this.buildingMaterial,this.dimWireMaterial);_.position.set(h*this.random(36,62),w+I*.5,d+this.random(-5,5)),o.add(_)}}}this.scene.add(o),this.mutableDecorations.push(o)}cleanupBehind(e){for(let t=this.activeNearbyIslands.length-1;t>=0;t-=1){let i=this.activeNearbyIslands[t];i.remainingStages<=0&&i.farZ>e+this.cleanupDistance&&this.activeNearbyIslands.splice(t,1)}for(;this.mutableSegments.length>0&&this.mutableSegments[0].nearZ>e+this.cleanupDistance;){let t=this.mutableSegments.shift();t&&this.disposeObject(t.group,!1)}for(let t=this.mutableObstacles.length-1;t>=0;t-=1){let i=this.mutableObstacles[t];i.mesh.position.z>e+this.cleanupDistance&&(this.mutableObstacles.splice(t,1),this.disposeObject(i.mesh,!1))}for(let t=this.mutableDecorations.length-1;t>=0;t-=1){let i=this.mutableDecorations[t];Number(i.userData.nearZ??i.position.z)>e+this.cleanupDistance&&(this.mutableDecorations.splice(t,1),this.disposeObject(i,!1))}}clearDynamicObjects(){for(let e of this.mutableSegments)this.disposeObject(e.group,!1);for(let e of this.mutableObstacles)this.disposeObject(e.mesh,!1);for(let e of this.mutableDecorations)this.disposeObject(e,!1);this.mutableSegments.length=0,this.mutableObstacles.length=0,this.mutableDecorations.length=0}disposeObject(e,t){e.parent?e.parent.remove(e):this.scene&&this.scene.remove(e),e.traverse(i=>{let r=i;if(r.geometry?.dispose(),!(!t||!r.material))if(Array.isArray(r.material))for(let s of r.material)s.dispose();else r.material.dispose()})}createSolidBlock(e,t,i,r,s){let o=new ri,a=new kt(e,t,i),c=new pt(a,r);c.receiveShadow=!0,o.add(c);let l=new Js(a),u=new mr(l,s);return o.add(u),o}createWireBox(e,t,i,r){let s=new kt(e,t,i),o=new Js(s);return s.dispose(),new mr(o,r)}createMaterials(){this.trackMaterial=new oi({color:462859,roughness:.42,metalness:.34,emissive:267532,emissiveIntensity:.72}),this.rampMaterial=new oi({color:661006,roughness:.34,metalness:.42,emissive:797975,emissiveIntensity:.72}),this.edgeMaterial=new oi({color:3800852,roughness:.12,metalness:.1,emissive:3800852,emissiveIntensity:3}),this.laneMaterial=new Ks({color:7536443,transparent:!0,opacity:.66}),this.obstacleMaterial=new oi({color:15739989,roughness:.22,metalness:.18,emissive:11014190,emissiveIntensity:1.6}),this.barMaterial=new oi({color:9109294,roughness:.18,metalness:.25,emissive:4652818,emissiveIntensity:2.1}),this.buildingMaterial=new oi({color:463882,roughness:.72,metalness:.18,emissive:204553,emissiveIntensity:.42,transparent:!0,opacity:.92}),this.wireMaterial=new Vi({color:3800852,transparent:!0,opacity:.9}),this.dimWireMaterial=new Vi({color:2078782,transparent:!0,opacity:.5}),this.gateMaterial=new Vi({color:10223440,transparent:!0,opacity:.96})}sizeForKind(e,t){switch(e){case"slider":return new O(1.75,1.75,1.75);case"wall":return new O(Math.min(2.35,t*.25),2.35,.8);case"cube":return new O(1.5,1.5,1.5)}}advanceGap(e,t){this.nextNearZ-=e,this.nextNearY-=t}random(e,t){return e+Math.random()*(t-e)}randomSigned(e){return this.random(-e,e)}randomIslandStagePattern(){return Math.floor(this.random(0,9))}shuffleNumbers(e){let t=[...e];for(let i=t.length-1;i>0;i-=1){let r=Math.floor(this.random(0,i+1));[t[i],t[r]]=[t[r],t[i]]}return t}shuffleObjects(e){let t=[...e];for(let i=t.length-1;i>0;i-=1){let r=Math.floor(this.random(0,i+1));[t[i],t[r]]=[t[r],t[i]]}return t}average(e){return e.length===0?this.lastCenterX:e.reduce((t,i)=>t+i,0)/e.length}captureRoadCursor(){return{centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY}}restoreRoadCursor(e){this.lastCenterX=e.centerX,this.nextNearZ=e.nextNearZ,this.nextNearY=e.nextNearY,this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null}syncCursorFromRoadStates(){this.activeRoadStates.length!==0&&(this.nextNearZ=Math.max(...this.activeRoadStates.map(e=>e.nextNearZ)),this.nextNearY=this.lowestActiveRoadY())}lowestActiveRoadY(){return this.activeRoadStates.length===0?this.nextNearY:Math.min(...this.activeRoadStates.map(e=>e.nextNearY))}clampValue(e,t,i){return Math.max(t,Math.min(i,e))}clamp(e,t,i){let r=this.roadClampOffset;if(this.roadClampHalfRange!==null){let s=Math.max(t,-this.roadClampHalfRange),o=Math.min(i,this.roadClampHalfRange);return this.clampWithAbsoluteBounds(e,r+s,r+o)}return this.clampWithAbsoluteBounds(e,t+r,i+r)}clampWithAbsoluteBounds(e,t,i){let r=this.roadClampAbsoluteMinX===null?t:Math.max(t,this.roadClampAbsoluteMinX),s=this.roadClampAbsoluteMaxX===null?i:Math.min(i,this.roadClampAbsoluteMaxX);return r>s?(r+s)*.5:this.clampValue(e,r,s)}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var i2=["gameCanvas"];function r2(n,e){if(n&1){let t=Ig();Lt(0,"div",5)(1,"div",9)(2,"span",10),dn(3,"Run ended"),Yt(),Lt(4,"h1"),dn(5,"Game Over"),Yt(),Lt(6,"div",11)(7,"div")(8,"span"),dn(9,"Stages cleared"),Yt(),Lt(10,"strong"),dn(11),Fs(12,"number"),Yt()(),Lt(13,"div")(14,"span"),dn(15,"Best"),Yt(),Lt(16,"strong"),dn(17),Fs(18,"number"),Yt()()(),Lt(19,"button",12),No("click",function(){pu(t);let r=od();return mu(r.restartGame())}),dn(20,"Restart"),Yt()()()}if(n&2){let t=od();Br(11),Hr(ks(12,2,t.score.score(),"1.0-0")),Br(6),Hr(ks(18,5,t.score.bestScore(),"1.0-0"))}}var qh=class n{canvasRef;input=J(bl);score=J(Sl);gameOver=ei(!1);collision=J(_l);cdr=J(ac);gameLoop=J(xl);hostRef=J(kr);ngZone=J(Ot);world=J(Ml);ballRadius=.62;gravity=-18.5;desiredCameraPosition=new O;cameraFocus=new O;resizeHandler=()=>this.resizeRenderer();renderer=null;scene=null;camera=null;ball=null;followLight=null;forwardVelocity=24;sideVelocity=0;verticalVelocity=0;wobblePhase=0;wasGrounded=!1;lastSurfaceY=0;lastGroundedSegment=null;launchedFromSegmentIndex=-1;hudAccumulator=0;destroyed=!1;ngAfterViewInit(){this.ngZone.runOutsideAngular(()=>{this.setupScene(),this.input.attach(window),window.addEventListener("resize",this.resizeHandler),this.gameLoop.start((e,t)=>this.tick(e,t))})}ngOnDestroy(){this.destroyed=!0,this.gameLoop.stop(),this.input.detach(),window.removeEventListener("resize",this.resizeHandler),this.ball&&(this.disposeObject(this.ball),this.ball=null),this.world.destroy(),this.renderer?.dispose(),this.renderer?.forceContextLoss(),this.renderer=null,this.scene=null,this.camera=null,this.followLight=null}restartGame(){if(!this.ball)return;this.world.reset(),this.score.reset(),this.forwardVelocity=24,this.sideVelocity=0,this.verticalVelocity=0,this.wobblePhase=0,this.wasGrounded=!0,this.lastGroundedSegment=null,this.launchedFromSegmentIndex=-1;let e=12,i=(this.collision.findTrackContact(new O(0,0,e),this.ballRadius,this.world.segments)?.surfaceY??0)+this.ballRadius;this.lastSurfaceY=i,this.ball.position.set(0,i,e),this.ball.rotation.set(0,0,0),this.gameOver.set(!1),this.cdr.detectChanges()}setupScene(){let e=this.canvasRef.nativeElement,t=new Wh({canvas:e,antialias:!0,powerPreference:"high-performance"});t.setClearColor(197642,1),t.setPixelRatio(Math.min(window.devicePixelRatio,2)),t.outputColorSpace=Cn,t.toneMapping=al,t.toneMappingExposure=1.12,t.shadowMap.enabled=!0,t.shadowMap.type=Jf,this.renderer=t;let i=new Xc;i.background=new qe(197642),i.fog=new $c(133382,.013),this.scene=i,this.camera=new fn(68,1,.1,720),this.camera.position.set(0,6,24);let r=new nl(8781673,329482,1.15);i.add(r);let s=new sl(14155744,2.2);s.position.set(-7,12,9),s.castShadow=!0,s.shadow.mapSize.set(1024,1024),i.add(s),this.followLight=new rl(3800852,7.4,34),i.add(this.followLight),this.world.init(i),this.createBall(i),this.restartGame(),this.resizeRenderer()}createBall(e){let t=new pa(this.ballRadius,42,28),i=new oi({color:14680040,roughness:.18,metalness:.78,emissive:1193493,emissiveIntensity:.65});this.ball=new pt(t,i),this.ball.castShadow=!0;let r=new el(new pa(this.ballRadius*1.012,18,12)),s=new Vi({color:6160175,transparent:!0,opacity:.78}),o=new mr(r,s);this.ball.add(o),e.add(this.ball)}tick(e,t){if(!(!this.renderer||!this.scene||!this.camera||!this.ball)){if(this.input.consumeRestart()){this.ngZone.run(()=>this.restartGame());return}this.gameOver()?this.world.update(e*.35,t):this.updatePlayer(e),this.updateCamera(e),this.renderer.render(this.scene,this.camera),this.syncHud(e)}}updatePlayer(e){if(!this.ball)return;let t=this.input.steering,i=this.ball.position.x,r=this.ball.position.z,s=this.wasGrounded,o=this.lastGroundedSegment,a=o,c=a?(a.nearY-a.farY)/a.length:0;if(s&&a){let m=c*95,p=1.15+Math.abs(this.sideVelocity)*.025,b=a.kind==="brake"?24+this.score.slowdown()*1.6:0,M=a.kind==="boost"?34+this.score.difficulty()*12:0;this.forwardVelocity+=(m+4.2+M-p-b)*e,a.kind==="brake"&&this.score.applySlowdown(e*20),a.kind==="boost"&&(this.sideVelocity*=Math.max(.9,1-e*2.4))}else this.forwardVelocity+=(1.2-this.forwardVelocity*.012)*e;this.forwardVelocity=Ut.clamp(this.forwardVelocity,this.score.minSpeed(),this.score.maxSpeedLimit());let l=s?1:.28,u=(24+this.forwardVelocity*.46)*l,d=s?t===0?1.7:.66:.08,f=11.4+this.forwardVelocity*.1;this.wobblePhase+=e*(8.5+this.forwardVelocity*.15+Math.abs(t)*7);let h=Math.sin(this.wobblePhase)*Math.abs(t)*(1.6+this.score.difficulty()*2.2);this.sideVelocity+=(t*u+h)*e,this.sideVelocity-=this.sideVelocity*Math.min(.58,d*e),this.sideVelocity=Ut.clamp(this.sideVelocity,-f,f),this.ball.position.x+=this.sideVelocity*e,this.ball.position.z-=this.forwardVelocity*e,this.world.ensureAhead(this.ball.position.z),s||(this.verticalVelocity+=this.gravity*e),this.ball.position.y+=this.verticalVelocity*e;let g=this.collision.findTrackContact(this.ball.position,this.ballRadius,this.world.segments),y=!1;if(g){let m=g.surfaceY+this.ballRadius,p=Math.max(0,-this.verticalVelocity*e),b=s?1.35:Ut.clamp(.9+p,.9,2.45),M=s?1.42:Ut.clamp(1.05+p*.5,1.05,1.75),E=this.ball.position.y>=m-M,D=g.segment.kind==="ramp"||g.segment.kind==="launch"||g.segment.kind==="brake"||g.segment.kind==="boost",w=this.verticalVelocity<=4.5||s||D&&this.ball.position.y<=m+.55+p;if(E&&this.ball.position.y<=m+b&&w){let I=this.wasGrounded?(m-this.lastSurfaceY)/Math.max(e,.001):0;if(this.ball.position.y=m,this.verticalVelocity=Ut.clamp(I*.35,-5,5),this.lastSurfaceY=m,y=!0,this.lastGroundedSegment=g.segment,g.segment.kind==="brake"&&(this.forwardVelocity=Math.max(this.score.minSpeed(),this.forwardVelocity-e*20),this.sideVelocity*=Math.max(.74,1-e*6)),g.segment.kind==="boost"&&(this.forwardVelocity=Math.min(this.score.maxSpeedLimit(),this.forwardVelocity+e*(26+this.forwardVelocity*.12))),g.edgeDistance<.78){let _=Math.sign(this.ball.position.x-g.segment.centerX);this.sideVelocity+=_*(.78-g.edgeDistance)*4.5*e}}else if(!E&&this.ball.position.y<m-this.ballRadius*1.1){this.endGame();return}}if(!y&&s&&o?.kind==="launch"&&this.launchedFromSegmentIndex!==o.index){let m=Ut.clamp(9.2+this.forwardVelocity*.075+this.score.difficulty()*1.7,10.5,16.5);this.verticalVelocity=Math.max(this.verticalVelocity,m),this.forwardVelocity=Math.min(this.score.maxSpeedLimit(),this.forwardVelocity+3.5),this.launchedFromSegmentIndex=o.index}this.wasGrounded=y,this.rollBall(this.ball.position.x-i,this.ball.position.z-r),this.world.update(e,performance.now()*.001),this.score.completeStages(this.world.consumePassedStages(this.ball.position.z)),this.score.update(e),this.score.syncSpeed(this.forwardVelocity),(this.collision.hitObstacle(this.ball.position,this.ballRadius,this.world.obstacles)||this.ball.position.y<this.world.fallLimitForZ(this.ball.position.z))&&this.endGame()}rollBall(e,t){this.ball&&(this.ball.rotation.x-=t/this.ballRadius,this.ball.rotation.z-=e/this.ballRadius,this.ball.rotation.y+=this.sideVelocity*.004)}updateCamera(e){if(!this.camera||!this.ball)return;let t=Ut.clamp((this.score.speed()-this.score.minSpeed())/(this.score.maxSpeedLimit()-this.score.minSpeed()),0,1),i=Ut.clamp(1-Math.exp(-e*4.2),0,1),r=Ut.clamp(1-Math.exp(-e*9.5),0,1);this.desiredCameraPosition.set(this.ball.position.x*.9-this.sideVelocity*.04,this.ball.position.y+5.4+t*2.4,this.ball.position.z+13.2+t*4.8),this.camera.position.y+=(this.desiredCameraPosition.y-this.camera.position.y)*i,this.camera.position.z+=(this.desiredCameraPosition.z-this.camera.position.z)*i,this.camera.position.x+=(this.desiredCameraPosition.x-this.camera.position.x)*r,this.cameraFocus.set(this.ball.position.x*.92+this.sideVelocity*.015,this.ball.position.y+.8,this.ball.position.z-18-t*5),this.camera.lookAt(this.cameraFocus),this.camera.rotation.z-=this.sideVelocity*.0028+Math.sin(this.wobblePhase)*.007;let s=67+t*18;this.camera.fov+=(s-this.camera.fov)*Math.min(1,e*2.5),this.camera.updateProjectionMatrix(),this.followLight&&this.followLight.position.set(this.ball.position.x,this.ball.position.y+4.5,this.ball.position.z+6)}endGame(){this.gameOver()||(this.score.commitBestScore(),this.ngZone.run(()=>{this.gameOver.set(!0),this.cdr.detectChanges()}))}resizeRenderer(){if(!this.renderer||!this.camera)return;let e=this.renderer.domElement,t=e.clientWidth||this.hostRef.nativeElement.clientWidth||window.innerWidth,i=e.clientHeight||this.hostRef.nativeElement.clientHeight||window.innerHeight;this.renderer.setSize(t,i,!1),this.camera.aspect=t/i,this.camera.updateProjectionMatrix()}syncHud(e){this.hudAccumulator+=e,!(this.destroyed||this.hudAccumulator<.1)&&(this.hudAccumulator=0,this.ngZone.run(()=>this.cdr.detectChanges()))}disposeObject(e){e.parent&&e.parent.remove(e),e.traverse(t=>{let i=t;if(i.geometry?.dispose(),Array.isArray(i.material))for(let r of i.material)r.dispose();else i.material?.dispose()})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Ps({type:n,selectors:[["app-game"]],viewQuery:function(t,i){if(t&1&&ad(i2,7),t&2){let r;Ag(r=Rg())&&(i.canvasRef=r.first)}},features:[Pg([_l,xl,bl,Sl,Ml])],decls:30,vars:13,consts:[["gameCanvas",""],[1,"game-shell"],["aria-label","Neon rolling ball endless runner",1,"game-canvas"],["aria-live","polite",1,"hud"],[1,"stat"],[1,"overlay"],["aria-hidden","true",1,"touch-controls"],["type","button","aria-label","Steer left",1,"touch-button",3,"pointerdown","pointerup","pointercancel","pointerleave"],["type","button","aria-label","Steer right",1,"touch-button",3,"pointerdown","pointerup","pointercancel","pointerleave"],[1,"game-over-panel"],[1,"panel-kicker"],[1,"final-stats"],["type","button",1,"restart-button",3,"click"]],template:function(t,i){t&1&&(Lt(0,"section",1),rd(1,"canvas",2,0),Lt(3,"div",3)(4,"div",4)(5,"span"),dn(6,"Stage"),Yt(),Lt(7,"strong"),dn(8),Fs(9,"number"),Yt()(),Lt(10,"div",4)(11,"span"),dn(12,"Speed"),Yt(),Lt(13,"strong"),dn(14),Fs(15,"number"),Yt()(),Lt(16,"div",4)(17,"span"),dn(18,"Best"),Yt(),Lt(19,"strong"),dn(20),Fs(21,"number"),Yt()()(),wg(22,r2,21,8,"div",5),Lt(23,"div",6)(24,"button",7),No("pointerdown",function(){return i.input.setTouchDirection(-1)})("pointerup",function(){return i.input.releaseTouchDirection(-1)})("pointercancel",function(){return i.input.releaseTouchDirection(-1)})("pointerleave",function(){return i.input.releaseTouchDirection(-1)}),Lt(25,"span"),dn(26,"<"),Yt()(),Lt(27,"button",8),No("pointerdown",function(){return i.input.setTouchDirection(1)})("pointerup",function(){return i.input.releaseTouchDirection(1)})("pointercancel",function(){return i.input.releaseTouchDirection(1)})("pointerleave",function(){return i.input.releaseTouchDirection(1)}),Lt(28,"span"),dn(29,">"),Yt()()()()),t&2&&(Br(8),Hr(ks(9,4,i.score.score(),"1.0-0")),Br(6),Hr(ks(15,7,i.score.speed(),"1.0-0")),Br(6),Hr(ks(21,10,i.score.bestScore(),"1.0-0")),Br(2),Cg(i.gameOver()?22:-1))},dependencies:[Hg],styles:["[_nghost-%COMP%]{display:block;width:100vw;height:100vh;overflow:hidden;background:#03040a}.game-shell[_ngcontent-%COMP%]{position:relative;width:100%;height:100%;isolation:isolate}.game-canvas[_ngcontent-%COMP%]{display:block;width:100%;height:100%;background:radial-gradient(circle at 50% 18%,#0b1c10,#041008 42%,#010403);outline:none}.hud[_ngcontent-%COMP%]{position:absolute;top:18px;left:18px;z-index:2;display:flex;gap:10px;max-width:calc(100% - 36px)}.stat[_ngcontent-%COMP%], .game-over-panel[_ngcontent-%COMP%]{border:1px solid rgba(88,255,55,.36);border-radius:8px;background:#010805ad;box-shadow:0 0 24px #39ff1424,inset 0 0 18px #61ff4712;-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px)}.stat[_ngcontent-%COMP%]{min-width:104px;padding:10px 12px}.stat[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .final-stats[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .panel-kicker[_ngcontent-%COMP%]{display:block;color:#92ff82;font-size:.7rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase}.stat[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{display:block;margin-top:2px;color:#f6fff3;font-size:1.32rem;font-variant-numeric:tabular-nums;line-height:1;text-shadow:0 0 14px rgba(57,255,20,.36)}.overlay[_ngcontent-%COMP%]{position:absolute;inset:0;z-index:4;display:grid;place-items:center;padding:18px;background:linear-gradient(#02030842,#020308a3),radial-gradient(circle at center,rgba(57,255,20,.12),transparent 44%)}.game-over-panel[_ngcontent-%COMP%]{width:min(420px,100%);padding:26px;text-align:center}.game-over-panel[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:8px 0 18px;color:#fff;font-size:clamp(2rem,7vw,4rem);font-weight:900;line-height:.95;text-shadow:0 0 14px rgba(255,49,95,.72),0 0 28px rgba(57,255,20,.24)}.final-stats[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-bottom:20px}.final-stats[_ngcontent-%COMP%]   div[_ngcontent-%COMP%]{border:1px solid rgba(146,255,130,.2);border-radius:8px;padding:12px;background:#ffffff0b}.final-stats[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{display:block;margin-top:4px;color:#39ff14;font-size:1.42rem;font-variant-numeric:tabular-nums}.restart-button[_ngcontent-%COMP%], .touch-button[_ngcontent-%COMP%]{border:1px solid rgba(57,255,20,.5);border-radius:8px;color:#041006;background:linear-gradient(135deg,#39ff14,#b7ff58);box-shadow:0 0 20px #39ff143d;cursor:pointer}.restart-button[_ngcontent-%COMP%]{min-width:150px;min-height:44px;padding:0 18px;font:inherit;font-weight:900;text-transform:uppercase}.touch-controls[_ngcontent-%COMP%]{position:absolute;right:18px;bottom:18px;z-index:3;display:none;gap:12px}.touch-button[_ngcontent-%COMP%]{width:68px;height:58px;touch-action:none;-webkit-user-select:none;user-select:none}.touch-button[_ngcontent-%COMP%]   span[_ngcontent-%COMP%]{display:block;font-size:2rem;font-weight:900;line-height:1}.restart-button[_ngcontent-%COMP%]:hover, .touch-button[_ngcontent-%COMP%]:hover{filter:brightness(1.08)}.restart-button[_ngcontent-%COMP%]:active, .touch-button[_ngcontent-%COMP%]:active{transform:translateY(1px)}@media(pointer:coarse){.touch-controls[_ngcontent-%COMP%]{display:flex}}@media(max-width:620px){.hud[_ngcontent-%COMP%]{top:10px;left:10px;gap:6px;max-width:calc(100% - 20px)}.stat[_ngcontent-%COMP%]{min-width:0;padding:8px 9px}.stat[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{font-size:1rem}.touch-controls[_ngcontent-%COMP%]{right:10px;bottom:10px}}"]})};var kE=[{path:"",component:qh},{path:"**",redirectTo:""}];var UE={providers:[wm(),Fb({eventCoalescing:!0}),Mv(kE)]};var Yh=class n{static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Ps({type:n,selectors:[["app-root"]],decls:1,vars:0,template:function(t,i){t&1&&Ro(0,"router-outlet")},dependencies:[Ac],styles:["[_nghost-%COMP%]{display:block;min-height:100vh}"]})};Zg(Yh,UE).catch(n=>console.error(n));

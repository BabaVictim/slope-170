var KE=Object.defineProperty,JE=Object.defineProperties;var QE=Object.getOwnPropertyDescriptors;var s0=Object.getOwnPropertySymbols;var ew=Object.prototype.hasOwnProperty,tw=Object.prototype.propertyIsEnumerable;var o0=(n,e,t)=>e in n?KE(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,fe=(n,e)=>{for(var t in e||={})ew.call(e,t)&&o0(n,t,e[t]);if(s0)for(var t of s0(e))tw.call(e,t)&&o0(n,t,e[t]);return n},ot=(n,e)=>JE(n,QE(e));var mt=(n,e,t)=>new Promise((i,r)=>{var s=c=>{try{a(t.next(c))}catch(l){r(l)}},o=c=>{try{a(t.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(s,o);a((t=t.apply(n,e)).next())});var mn=null,Ol=!1,sp=1,nw=null,Bn=Symbol("SIGNAL");function He(n){let e=mn;return mn=n,e}function Fl(){return mn}var Aa={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ra(n){if(Ol)throw new Error("");if(mn===null)return;mn.consumerOnSignalRead(n);let e=mn.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=mn.recomputing;if(i&&(t=e!==void 0?e.nextProducer:mn.producers,t!==void 0&&t.producer===n)){mn.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===mn&&(!i||rw(r,mn)))return;let s=lo(mn),o={producer:n,consumer:mn,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};mn.producersTail=o,e!==void 0?e.nextProducer=o:mn.producers=o,s&&u0(n,o)}function a0(){sp++}function op(n){if(!(lo(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===sp)){if(!n.producerMustRecompute(n)&&!Ul(n)){Ll(n);return}n.producerRecomputeValue(n),Ll(n)}}function ap(n){if(n.consumers===void 0)return;let e=Ol;Ol=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||iw(i)}}finally{Ol=e}}function cp(){return mn?.consumerAllowSignalWrites!==!1}function iw(n){n.dirty=!0,ap(n),n.consumerMarkedDirty?.(n)}function Ll(n){n.dirty=!1,n.lastCleanEpoch=sp}function Na(n){return n&&c0(n),He(n)}function c0(n){n.producersTail=void 0,n.recomputing=!0}function kl(n,e){He(e),n&&l0(n)}function l0(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(lo(n))do t=lp(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function Ul(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(op(t),i!==t.version))return!0}return!1}function Pa(n){if(lo(n)){let e=n.producers;for(;e!==void 0;)e=lp(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function u0(n,e){let t=n.consumersTail,i=lo(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)u0(r.producer,r)}function lp(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!lo(e)){let s=e.producers;for(;s!==void 0;)s=lp(s)}return t}function lo(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function up(n){nw?.(n)}function rw(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function dp(n,e){return Object.is(n,e)}function sw(){throw new Error}var d0=sw;function h0(n){d0(n)}function hp(n){d0=n}var ow=null;function fp(n,e){let t=Object.create(Bl);t.value=n,e!==void 0&&(t.equal=e);let i=()=>f0(t);return i[Bn]=t,up(t),[i,o=>uo(t,o),o=>pp(t,o)]}function f0(n){return Ra(n),n.value}function uo(n,e){cp()||h0(n),n.equal(n.value,e)||(n.value=e,aw(n))}function pp(n,e){cp()||h0(n),uo(n,e(n.value))}var Bl=ot(fe({},Aa),{equal:dp,value:void 0,kind:"signal"});function aw(n){n.version++,a0(),ap(n),ow?.(n)}function Ue(n){return typeof n=="function"}function ho(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Vl=ho(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Oa(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Yt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Ue(i))try{i()}catch(s){e=s instanceof Vl?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{p0(s)}catch(o){e=e??[],o instanceof Vl?e=[...e,...o.errors]:e.push(o)}}if(e)throw new Vl(e)}}add(e){var t;if(e&&e!==this)if(this.closed)p0(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Oa(t,e)}remove(e){let{_finalizers:t}=this;t&&Oa(t,e),e instanceof n&&e._removeParent(this)}};Yt.EMPTY=(()=>{let n=new Yt;return n.closed=!0,n})();var mp=Yt.EMPTY;function Hl(n){return n instanceof Yt||n&&"closed"in n&&Ue(n.remove)&&Ue(n.add)&&Ue(n.unsubscribe)}function p0(n){Ue(n)?n():n.unsubscribe()}var di={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var fo={setTimeout(n,e,...t){let{delegate:i}=fo;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=fo;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function zl(n){fo.setTimeout(()=>{let{onUnhandledError:e}=di;if(e)e(n);else throw n})}function La(){}var m0=gp("C",void 0,void 0);function g0(n){return gp("E",void 0,n)}function v0(n){return gp("N",n,void 0)}function gp(n,e,t){return{kind:n,value:e,error:t}}var fs=null;function po(n){if(di.useDeprecatedSynchronousErrorHandling){let e=!fs;if(e&&(fs={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=fs;if(fs=null,t)throw i}}else n()}function y0(n){di.useDeprecatedSynchronousErrorHandling&&fs&&(fs.errorThrown=!0,fs.error=n)}var ps=class extends Yt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Hl(e)&&e.add(this)):this.destination=uw}static create(e,t,i){return new mo(e,t,i)}next(e){this.isStopped?yp(v0(e),this):this._next(e)}error(e){this.isStopped?yp(g0(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?yp(m0,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},cw=Function.prototype.bind;function vp(n,e){return cw.call(n,e)}var _p=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){Gl(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){Gl(i)}else Gl(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){Gl(t)}}},mo=class extends ps{constructor(e,t,i){super();let r;if(Ue(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&di.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&vp(e.next,s),error:e.error&&vp(e.error,s),complete:e.complete&&vp(e.complete,s)}):r=e}this.destination=new _p(r)}};function Gl(n){di.useDeprecatedSynchronousErrorHandling?y0(n):zl(n)}function lw(n){throw n}function yp(n,e){let{onStoppedNotification:t}=di;t&&fo.setTimeout(()=>t(n,e))}var uw={closed:!0,next:La,error:lw,complete:La};var go=typeof Symbol=="function"&&Symbol.observable||"@@observable";function hi(n){return n}function xp(...n){return bp(n)}function bp(n){return n.length===0?hi:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var ct=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=hw(t)?t:new mo(t,i,r);return po(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=_0(i),new i((r,s)=>{let o=new mo({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[go](){return this}pipe(...t){return bp(t)(this)}toPromise(t){return t=_0(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function _0(n){var e;return(e=n??di.Promise)!==null&&e!==void 0?e:Promise}function dw(n){return n&&Ue(n.next)&&Ue(n.error)&&Ue(n.complete)}function hw(n){return n&&n instanceof ps||dw(n)&&Hl(n)}function fw(n){return Ue(n?.lift)}function ht(n){return e=>{if(fw(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function xt(n,e,t,i,r){return new Sp(n,e,t,i,r)}var Sp=class extends ps{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var x0=ho(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var an=(()=>{class n extends ct{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new jl(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new x0}next(t){po(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){po(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){po(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?mp:(this.currentObservers=null,s.push(t),new Yt(()=>{this.currentObservers=null,Oa(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new ct;return t.source=this,t}}return n.create=(e,t)=>new jl(e,t),n})(),jl=class extends an{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:mp}};var cn=class extends an{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var ln=new ct(n=>n.complete());function b0(n){return n&&Ue(n.schedule)}function S0(n){return n[n.length-1]}function M0(n){return Ue(S0(n))?n.pop():void 0}function Cr(n){return b0(S0(n))?n.pop():void 0}function w0(n,e,t,i){function r(s){return s instanceof t?s:new t(function(o){o(s)})}return new(t||(t=Promise))(function(s,o){function a(u){try{l(i.next(u))}catch(d){o(d)}}function c(u){try{l(i.throw(u))}catch(d){o(d)}}function l(u){u.done?s(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function E0(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function ms(n){return this instanceof ms?(this.v=n,this):new ms(n)}function T0(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,s=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",o),r[Symbol.asyncIterator]=function(){return this},r;function o(f){return function(g){return Promise.resolve(g).then(f,d)}}function a(f,g){i[f]&&(r[f]=function(y){return new Promise(function(m,p){s.push([f,y,m,p])>1||c(f,y)})},g&&(r[f]=g(r[f])))}function c(f,g){try{l(i[f](g))}catch(y){h(s[0][3],y)}}function l(f){f.value instanceof ms?Promise.resolve(f.value.v).then(u,d):h(s[0][2],f)}function u(f){c("next",f)}function d(f){c("throw",f)}function h(f,g){f(g),s.shift(),s.length&&c(s[0][0],s[0][1])}}function C0(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof E0=="function"?E0(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(s){t[s]=n[s]&&function(o){return new Promise(function(a,c){o=n[s](o),r(a,c,o.done,o.value)})}}function r(s,o,a,c){Promise.resolve(c).then(function(l){s({value:l,done:a})},o)}}var Wl=n=>n&&typeof n.length=="number"&&typeof n!="function";function $l(n){return Ue(n?.then)}function Xl(n){return Ue(n[go])}function ql(n){return Symbol.asyncIterator&&Ue(n?.[Symbol.asyncIterator])}function Yl(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function pw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Zl=pw();function Kl(n){return Ue(n?.[Zl])}function Jl(n){return T0(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield ms(t.read());if(r)return yield ms(void 0);yield yield ms(i)}}finally{t.releaseLock()}})}function Ql(n){return Ue(n?.getReader)}function en(n){if(n instanceof ct)return n;if(n!=null){if(Xl(n))return mw(n);if(Wl(n))return gw(n);if($l(n))return vw(n);if(ql(n))return D0(n);if(Kl(n))return yw(n);if(Ql(n))return _w(n)}throw Yl(n)}function mw(n){return new ct(e=>{let t=n[go]();if(Ue(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function gw(n){return new ct(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function vw(n){return new ct(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,zl)})}function yw(n){return new ct(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function D0(n){return new ct(e=>{xw(n,e).catch(t=>e.error(t))})}function _w(n){return D0(Jl(n))}function xw(n,e){var t,i,r,s;return w0(this,void 0,void 0,function*(){try{for(t=C0(n);i=yield t.next(),!i.done;){let o=i.value;if(e.next(o),e.closed)return}}catch(o){r={error:o}}finally{try{i&&!i.done&&(s=t.return)&&(yield s.call(t))}finally{if(r)throw r.error}}e.complete()})}function Rn(n,e,t,i=0,r=!1){let s=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(s),!r)return s}function eu(n,e=0){return ht((t,i)=>{t.subscribe(xt(i,r=>Rn(i,n,()=>i.next(r),e),()=>Rn(i,n,()=>i.complete(),e),r=>Rn(i,n,()=>i.error(r),e)))})}function tu(n,e=0){return ht((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function I0(n,e){return en(n).pipe(tu(e),eu(e))}function A0(n,e){return en(n).pipe(tu(e),eu(e))}function R0(n,e){return new ct(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function N0(n,e){return new ct(t=>{let i;return Rn(t,e,()=>{i=n[Zl](),Rn(t,e,()=>{let r,s;try{({value:r,done:s}=i.next())}catch(o){t.error(o);return}s?t.complete():t.next(r)},0,!0)}),()=>Ue(i?.return)&&i.return()})}function nu(n,e){if(!n)throw new Error("Iterable cannot be null");return new ct(t=>{Rn(t,e,()=>{let i=n[Symbol.asyncIterator]();Rn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function P0(n,e){return nu(Jl(n),e)}function O0(n,e){if(n!=null){if(Xl(n))return I0(n,e);if(Wl(n))return R0(n,e);if($l(n))return A0(n,e);if(ql(n))return nu(n,e);if(Kl(n))return N0(n,e);if(Ql(n))return P0(n,e)}throw Yl(n)}function Gt(n,e){return e?O0(n,e):en(n)}function Qe(...n){let e=Cr(n);return Gt(n,e)}function Mp(n,e){let t=Ue(n)?n:()=>n,i=r=>r.error(t());return new ct(e?r=>e.schedule(i,0,r):i)}function iu(n){return!!n&&(n instanceof ct||Ue(n.lift)&&Ue(n.subscribe))}var gs=ho(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function kt(n,e){return ht((t,i)=>{let r=0;t.subscribe(xt(i,s=>{i.next(n.call(e,s,r++))}))})}var{isArray:bw}=Array;function Sw(n,e){return bw(e)?n(...e):n(e)}function L0(n){return kt(e=>Sw(n,e))}var{isArray:Mw}=Array,{getPrototypeOf:Ew,prototype:ww,keys:Tw}=Object;function F0(n){if(n.length===1){let e=n[0];if(Mw(e))return{args:e,keys:null};if(Cw(e)){let t=Tw(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Cw(n){return n&&typeof n=="object"&&Ew(n)===ww}function k0(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Ep(...n){let e=Cr(n),t=M0(n),{args:i,keys:r}=F0(n);if(i.length===0)return Gt([],e);let s=new ct(Dw(i,e,r?o=>k0(r,o):hi));return t?s.pipe(L0(t)):s}function Dw(n,e,t=hi){return i=>{U0(e,()=>{let{length:r}=n,s=new Array(r),o=r,a=r;for(let c=0;c<r;c++)U0(e,()=>{let l=Gt(n[c],e),u=!1;l.subscribe(xt(i,d=>{s[c]=d,u||(u=!0,a--),a||i.next(t(s.slice()))},()=>{--o||i.complete()}))},i)},i)}}function U0(n,e,t){n?Rn(t,n,e):e()}function B0(n,e,t,i,r,s,o,a){let c=[],l=0,u=0,d=!1,h=()=>{d&&!c.length&&!l&&e.complete()},f=y=>l<i?g(y):c.push(y),g=y=>{s&&e.next(y),l++;let m=!1;en(t(y,u++)).subscribe(xt(e,p=>{r?.(p),s?f(p):e.next(p)},()=>{m=!0},void 0,()=>{if(m)try{for(l--;c.length&&l<i;){let p=c.shift();o?Rn(e,o,()=>g(p)):g(p)}h()}catch(p){e.error(p)}}))};return n.subscribe(xt(e,f,()=>{d=!0,h()})),()=>{a?.()}}function En(n,e,t=1/0){return Ue(e)?En((i,r)=>kt((s,o)=>e(i,s,r,o))(en(n(i,r))),t):(typeof e=="number"&&(t=e),ht((i,r)=>B0(i,r,n,t)))}function V0(n=1/0){return En(hi,n)}function H0(){return V0(1)}function vo(...n){return H0()(Gt(n,Cr(n)))}function Fa(n){return new ct(e=>{en(n()).subscribe(e)})}function Ki(n,e){return ht((t,i)=>{let r=0;t.subscribe(xt(i,s=>n.call(e,s,r++)&&i.next(s)))})}function ka(n){return ht((e,t)=>{let i=null,r=!1,s;i=e.subscribe(xt(t,void 0,void 0,o=>{s=en(n(o,ka(n)(e))),i?(i.unsubscribe(),i=null,s.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,s.subscribe(t))})}function ru(n,e){return Ue(e)?En(n,e,1):En(n,1)}function z0(n){return ht((e,t)=>{let i=!1;e.subscribe(xt(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function Ji(n){return n<=0?()=>ln:ht((e,t)=>{let i=0;e.subscribe(xt(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function G0(n=Iw){return ht((e,t)=>{let i=!1;e.subscribe(xt(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function Iw(){return new gs}function wp(n){return ht((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function Qi(n,e){let t=arguments.length>=2;return i=>i.pipe(n?Ki((r,s)=>n(r,s,i)):hi,Ji(1),t?z0(e):G0(()=>new gs))}function su(n){return n<=0?()=>ln:ht((e,t)=>{let i=[];e.subscribe(xt(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Tp(...n){let e=Cr(n);return ht((t,i)=>{(e?vo(n,t,e):vo(n,t)).subscribe(i)})}function fi(n,e){return ht((t,i)=>{let r=null,s=0,o=!1,a=()=>o&&!r&&i.complete();t.subscribe(xt(i,c=>{r?.unsubscribe();let l=0,u=s++;en(n(c,u)).subscribe(r=xt(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{o=!0,a()}))})}function Ua(n){return ht((e,t)=>{en(n).subscribe(xt(t,()=>t.complete(),La)),!t.closed&&e.subscribe(t)})}function Kn(n,e,t){let i=Ue(n)||e||t?{next:n,error:e,complete:t}:n;return i?ht((r,s)=>{var o;(o=i.subscribe)===null||o===void 0||o.call(i);let a=!0;r.subscribe(xt(s,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),s.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),s.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),s.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):hi}var Cp;function ou(){return Cp}function Ai(n){let e=Cp;return Cp=n,e}var j0=Symbol("NotFound");function yo(n){return n===j0||n?.name==="\u0275NotFound"}function W0(n){let e=He(null);try{return n()}finally{He(e)}}var Me=class extends Error{code;constructor(e,t){super(xo(e,t)),this.code=e}};function Pw(n){return`NG0${Math.abs(n)}`}function xo(n,e){return`${Pw(n)}${e?": "+e:""}`}var Ms=globalThis;function ft(n){for(let e in n)if(n[e]===ft)return e;throw Error("")}function du(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(du).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function zp(n,e){return n?e?`${n} ${e}`:n:e||""}var Ow=ft({__forward_ref__:ft});function hu(n){return n.__forward_ref__=hu,n}function un(n){return Gp(n)?n():n}function Gp(n){return typeof n=="function"&&n.hasOwnProperty(Ow)&&n.__forward_ref__===hu}function Ee(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function ja(n){return Lw(n,fu)}function jp(n){return ja(n)!==null}function Lw(n,e){return n.hasOwnProperty(e)&&n[e]||null}function Fw(n){let e=n?.[fu]??null;return e||null}function Ip(n){return n&&n.hasOwnProperty(cu)?n[cu]:null}var fu=ft({\u0275prov:ft}),cu=ft({\u0275inj:ft}),Ie=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Ee({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Wp(n){return n&&!!n.\u0275providers}var $p=ft({\u0275cmp:ft}),Xp=ft({\u0275dir:ft}),qp=ft({\u0275pipe:ft}),Yp=ft({\u0275mod:ft}),Va=ft({\u0275fac:ft}),Es=ft({__NG_ELEMENT_ID__:ft}),$0=ft({__NG_ENV_ID__:ft});function Zp(n){return pu(n,"@NgModule"),n[Yp]||null}function Ar(n){return pu(n,"@Component"),n[$p]||null}function Kp(n){return pu(n,"@Directive"),n[Xp]||null}function Z0(n){return pu(n,"@Pipe"),n[qp]||null}function pu(n,e){if(n==null)throw new Me(-919,!1)}function K0(n){return typeof n=="string"?n:n==null?"":String(n)}var J0=ft({ngErrorCode:ft}),kw=ft({ngErrorMessage:ft}),Uw=ft({ngTokenPath:ft});function Jp(n,e){return Q0("",-200,e)}function mu(n,e){throw new Me(-201,!1)}function Q0(n,e,t){let i=new Me(e,n);return i[J0]=e,i[kw]=n,t&&(i[Uw]=t),i}function Bw(n){return n[J0]}var Ap;function e_(){return Ap}function wn(n){let e=Ap;return Ap=n,e}function Qp(n,e,t){let i=ja(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;mu(n,"")}var Vw={},vs=Vw,Hw="__NG_DI_FLAG__",Rp=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=ys(t)||0;try{return this.injector.get(e,i&8?null:vs,i)}catch(r){if(yo(r))return r;throw r}}};function zw(n,e=0){let t=ou();if(t===void 0)throw new Me(-203,!1);if(t===null)return Qp(n,void 0,e);{let i=Gw(e),r=t.retrieve(n,i);if(yo(r)){if(i.optional)return null;throw r}return r}}function We(n,e=0){return(e_()||zw)(un(n),e)}function J(n,e){return We(n,ys(e))}function ys(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Gw(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Np(n){let e=[];for(let t=0;t<n.length;t++){let i=un(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Me(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=jw(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(We(r,s))}else e.push(We(i))}return e}function jw(n){return n[Hw]}function Dr(n,e){let t=n.hasOwnProperty(Va);return t?n[Va]:null}function t_(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function n_(n){return n.flat(Number.POSITIVE_INFINITY)}function gu(n,e){n.forEach(t=>Array.isArray(t)?gu(t,e):e(t))}function em(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Wa(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}var ws={},_s=[],nr=new Ie(""),tm=new Ie("",-1),nm=new Ie(""),Ha=class{get(e,t=vs){if(t===vs){let r=Q0("",-201);throw r.name="\u0275NotFound",r}return t}};function Ts(n){return{\u0275providers:n}}function i_(n){return Ts([{provide:nr,multi:!0,useValue:n}])}function r_(...n){return{\u0275providers:im(!0,n),\u0275fromNgModule:!0}}function im(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return gu(e,o=>{let a=o;lu(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&s_(r,s),t}function s_(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];rm(r,s=>{e(s,i)})}}function lu(n,e,t,i){if(n=un(n),!n)return!1;let r=null,s=Ip(n),o=!s&&Ar(n);if(!s&&!o){let c=n.ngModule;if(s=Ip(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)lu(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;gu(s.imports,u=>{lu(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&s_(l,e)}if(!a){let l=Dr(r)||(()=>new r);e({provide:r,useFactory:l,deps:_s},r),e({provide:nm,useValue:r,multi:!0},r),e({provide:nr,useValue:()=>We(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;rm(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function rm(n,e){for(let t of n)Wp(t)&&(t=t.\u0275providers),Array.isArray(t)?rm(t,e):e(t)}var Ww=ft({provide:String,useValue:ft});function o_(n){return n!==null&&typeof n=="object"&&Ww in n}function $w(n){return!!(n&&n.useExisting)}function Xw(n){return!!(n&&n.useFactory)}function xs(n){return typeof n=="function"}function a_(n){return!!n.useClass}var $a=new Ie(""),au={},X0={},Dp;function Xa(){return Dp===void 0&&(Dp=new Ha),Dp}var Zt=class{},bs=class extends Zt{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Op(e,o=>this.processProvider(o)),this.records.set(tm,_o(void 0,this)),r.has("environment")&&this.records.set(Zt,_o(void 0,this));let s=this.records.get($a);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(nm,_s,{self:!0}))}retrieve(e,t){let i=ys(t)||0;try{return this.get(e,vs,i)}catch(r){if(yo(r))return r;throw r}}destroy(){Ba(this),this._destroyed=!0;let e=He(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),He(e)}}onDestroy(e){return Ba(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Ba(this);let t=Ai(this),i=wn(void 0),r;try{return e()}finally{Ai(t),wn(i)}}get(e,t=vs,i){if(Ba(this),e.hasOwnProperty($0))return e[$0](this);let r=ys(i),s,o=Ai(this),a=wn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=Jw(e)&&ja(e);u&&this.injectableDefInScope(u)?l=_o(Pp(e),au):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Xa():this.parent;return t=r&8&&t===vs?null:t,c.get(e,t)}catch(c){let l=Bw(c);throw l===-200||l===-201?new Me(l,null):c}finally{wn(a),Ai(o)}}resolveInjectorInitializers(){let e=He(null),t=Ai(this),i=wn(void 0),r;try{let s=this.get(nr,_s,{self:!0});for(let o of s)o()}finally{Ai(t),wn(i),He(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=un(e);let t=xs(e)?e:un(e&&e.provide),i=Yw(e);if(!xs(e)&&e.multi===!0){let r=this.records.get(t);r||(r=_o(void 0,au,!0),r.factory=()=>Np(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=He(null);try{if(t.value===X0)throw Jp("");return t.value===au&&(t.value=X0,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Kw(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{He(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=un(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Pp(n){let e=ja(n),t=e!==null?e.factory:Dr(n);if(t!==null)return t;if(n instanceof Ie)throw new Me(-204,!1);if(n instanceof Function)return qw(n);throw new Me(-204,!1)}function qw(n){if(n.length>0)throw new Me(-204,!1);let t=Fw(n);return t!==null?()=>t.factory(n):()=>new n}function Yw(n){if(o_(n))return _o(void 0,n.useValue);{let e=sm(n);return _o(e,au)}}function sm(n,e,t){let i;if(xs(n)){let r=un(n);return Dr(r)||Pp(r)}else if(o_(n))i=()=>un(n.useValue);else if(Xw(n))i=()=>n.useFactory(...Np(n.deps||[]));else if($w(n))i=(r,s)=>We(un(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=un(n&&(n.useClass||n.provide));if(Zw(n))i=()=>new r(...Np(n.deps));else return Dr(r)||Pp(r)}return i}function Ba(n){if(n.destroyed)throw new Me(-205,!1)}function _o(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Zw(n){return!!n.deps}function Kw(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Jw(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Op(n,e){for(let t of n)Array.isArray(t)?Op(t,e):t&&Wp(t)?Op(t.\u0275providers,e):e(t)}function gn(n,e){let t;n instanceof bs?(Ba(n),t=n):t=new Rp(n);let i,r=Ai(t),s=wn(void 0);try{return e()}finally{Ai(r),wn(s)}}function c_(){return e_()!==void 0||ou()!=null}var mi=0,Pe=1,ze=2,tn=3,Jn=4,Qn=5,qa=6,bo=7,dn=8,Rr=9,Ri=10,vn=11,So=12,om=13,Cs=14,ei=15,Nr=16,Ds=17,Ni=18,Pr=19,am=20,er=21,vu=22,Ya=23,Vn=24,yu=25,Mo=26,nn=27,l_=1;var Or=7,Za=8,Is=9,yn=10;function Lr(n){return Array.isArray(n)&&typeof n[l_]=="object"}function gi(n){return Array.isArray(n)&&n[l_]===!0}function cm(n){return(n.flags&4)!==0}function Fr(n){return n.componentOffset>-1}function _u(n){return(n.flags&1)===1}function kr(n){return!!n.template}function Eo(n){return(n[ze]&512)!==0}function As(n){return(n[ze]&256)===256}var lm="svg",u_="math";function ti(n){for(;Array.isArray(n);)n=n[mi];return n}function d_(n,e){return ti(e[n])}function ir(n,e){return ti(e[n.index])}function um(n,e){return n.data[e]}function h_(n,e){return n[e]}function dm(n,e,t,i){t>=n.data.length&&(n.data[t]=null,n.blueprint[t]=null),e[t]=i}function Pi(n,e){let t=e[n];return Lr(t)?t:t[mi]}function f_(n){return(n[ze]&4)===4}function xu(n){return(n[ze]&128)===128}function p_(n){return gi(n[tn])}function Ur(n,e){return e==null?null:n[e]}function hm(n){n[Ds]=0}function fm(n){n[ze]&1024||(n[ze]|=1024,xu(n)&&Ja(n))}function m_(n,e){for(;n>0;)e=e[Cs],n--;return e}function Ka(n){return!!(n[ze]&9216||n[Vn]?.dirty)}function bu(n){n[Ri].changeDetectionScheduler?.notify(8),n[ze]&64&&(n[ze]|=1024),Ka(n)&&Ja(n)}function Ja(n){n[Ri].changeDetectionScheduler?.notify(0);let e=Ir(n);for(;e!==null&&!(e[ze]&8192||(e[ze]|=8192,!xu(e)));)e=Ir(e)}function pm(n,e){if(As(n))throw new Me(911,!1);n[er]===null&&(n[er]=[]),n[er].push(e)}function g_(n,e){if(n[er]===null)return;let t=n[er].indexOf(e);t!==-1&&n[er].splice(t,1)}function Ir(n){let e=n[tn];return gi(e)?e[tn]:e}function mm(n){return n[bo]??=[]}function gm(n){return n.cleanup??=[]}function v_(n,e,t,i){let r=mm(e);r.push(t),n.firstCreatePass&&gm(n).push(i,r.length-1)}var Ze={lFrame:A_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Lp=!1;function y_(){return Ze.lFrame.elementDepthCount}function __(){Ze.lFrame.elementDepthCount++}function vm(){Ze.lFrame.elementDepthCount--}function x_(){return Ze.bindingsEnabled}function b_(){return Ze.skipHydrationRootTNode!==null}function ym(n){return Ze.skipHydrationRootTNode===n}function _m(){Ze.skipHydrationRootTNode=null}function wt(){return Ze.lFrame.lView}function ni(){return Ze.lFrame.tView}function wo(n){return Ze.lFrame.contextLView=n,n[dn]}function To(n){return Ze.lFrame.contextLView=null,n}function ii(){let n=xm();for(;n!==null&&n.type===64;)n=n.parent;return n}function xm(){return Ze.lFrame.currentTNode}function S_(){let n=Ze.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Co(n,e){let t=Ze.lFrame;t.currentTNode=n,t.isParent=e}function bm(){return Ze.lFrame.isParent}function M_(){Ze.lFrame.isParent=!1}function Sm(){return Lp}function Mm(n){let e=Lp;return Lp=n,e}function E_(){let n=Ze.lFrame,e=n.bindingRootIndex;return e===-1&&(e=n.bindingRootIndex=n.tView.bindingStartIndex),e}function w_(n){return Ze.lFrame.bindingIndex=n}function Em(){return Ze.lFrame.bindingIndex++}function T_(){return Ze.lFrame.inI18n}function C_(n,e){let t=Ze.lFrame;t.bindingIndex=t.bindingRootIndex=n,Su(e)}function D_(){return Ze.lFrame.currentDirectiveIndex}function Su(n){Ze.lFrame.currentDirectiveIndex=n}function wm(){return Ze.lFrame.currentQueryIndex}function Mu(n){Ze.lFrame.currentQueryIndex=n}function Qw(n){let e=n[Pe];return e.type===2?e.declTNode:e.type===1?n[Qn]:null}function Tm(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=Qw(s),r===null||(s=s[Cs],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ze.lFrame=I_();return i.currentTNode=e,i.lView=n,!0}function Eu(n){let e=I_(),t=n[Pe];Ze.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function I_(){let n=Ze.lFrame,e=n===null?null:n.child;return e===null?A_(n):e}function A_(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function R_(){let n=Ze.lFrame;return Ze.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Cm=R_;function wu(){let n=R_();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function N_(n){return(Ze.lFrame.contextLView=m_(n,Ze.lFrame.contextLView))[dn]}function Tu(){return Ze.lFrame.selectedIndex}function Br(n){Ze.lFrame.selectedIndex=n}function Cu(){Ze.lFrame.currentNamespace=lm}function Du(){eT()}function eT(){Ze.lFrame.currentNamespace=null}function P_(){return Ze.lFrame.currentNamespace}var O_=!0;function Iu(){return O_}function Au(n){O_=n}function Fp(n,e=null,t=null,i){let r=Dm(n,e,t,i);return r.resolveInjectorInitializers(),r}function Dm(n,e=null,t=null,i,r=new Set){let s=[t||_s,r_(n)],o;return new bs(s,e||Xa(),o||null,r)}var pi=class n{static THROW_IF_NOT_FOUND=vs;static NULL=new Ha;static create(e,t){if(Array.isArray(e))return Fp({name:""},t,e,"");{let i=e.name??"";return Fp({name:i},e.parent,e.providers,i)}}static \u0275prov=Ee({token:n,providedIn:"any",factory:()=>We(tm)});static __NG_ELEMENT_ID__=-1},Kt=new Ie(""),Vr=(()=>{class n{static __NG_ELEMENT_ID__=tT;static __NG_ENV_ID__=t=>t}return n})(),kp=class extends Vr{_lView;constructor(e){super(),this._lView=e}get destroyed(){return As(this._lView)}onDestroy(e){let t=this._lView;return pm(t,e),()=>g_(t,e)}};function tT(){return new kp(wt())}var Im=!1,L_=new Ie(""),rr=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new cn(!1);debugTaskTracker=J(L_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ct(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>new n})}return n})(),Up=class extends an{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,c_()&&(this.destroyRef=J(Vr,{optional:!0})??void 0,this.pendingTasks=J(rr,{optional:!0})??void 0)}emit(e){let t=He(null);try{super.next(e)}finally{He(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Yt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Tn=Up;function uu(...n){}function Am(n){let e,t;function i(){n=uu;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch(r){}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function F_(n){return queueMicrotask(()=>n()),()=>{n=uu}}var Rm="isAngularZone",za=Rm+"_ID",nT=0,Ut=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Tn(!1);onMicrotaskEmpty=new Tn(!1);onStable=new Tn(!1);onError=new Tn(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Im}=e;if(typeof Zone>"u")throw new Me(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,sT(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Rm)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Me(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Me(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,iT,uu,uu);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},iT={};function Nm(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function rT(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Am(()=>{n.callbackScheduled=!1,Bp(n),n.isCheckStableRunning=!0,Nm(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Bp(n)}function sT(n){let e=()=>{rT(n)},t=nT++;n._inner=n._inner.fork({name:"angular",properties:{[Rm]:!0,[za]:t,[za+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(oT(c))return i.invokeTask(s,o,a,c);try{return q0(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),Y0(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return q0(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!aT(c)&&e(),Y0(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Bp(n),Nm(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Bp(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function q0(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function Y0(n){n._nesting--,Nm(n)}var Ga=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Tn;onMicrotaskEmpty=new Tn;onStable=new Tn;onError=new Tn;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function oT(n){return k_(n,"__ignore_ng_zone__")}function aT(n){return k_(n,"__scheduler_tick__")}function k_(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var tr=class{_console=console;handleError(e){this._console.error("ERROR",e)}},ri=new Ie("",{factory:()=>{let n=J(Ut),e=J(Zt),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(tr),t.handleError(i))})}}}),U_={provide:nr,useValue:()=>{let n=J(tr,{optional:!0})},multi:!0},cT=new Ie("",{factory:()=>{let n=J(Kt).defaultView;if(!n)return;let e=J(ri),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),J(Vr).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function Pm(){return Ts([i_(()=>{J(cT)})])}function Nn(n,e){let[t,i,r]=fp(n,e?.equal),s=t,o=s[Bn];return s.set=i,s.update=r,s.asReadonly=B_.bind(s),s}function B_(){let n=this[Bn];if(n.readonlyFn===void 0){let e=()=>this();e[Bn]=n,n.readonlyFn=e}return n.readonlyFn}var Ss=class{},Do=new Ie("",{factory:()=>!0});var Ru=new Ie("");var Om=(()=>{class n{static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>new Vp})}return n})(),Vp=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},Hp=class{[Bn];constructor(e){this[Bn]=e}destroy(){this[Bn].destroy()}};function Qu(n){return{toString:n}.toString()}function yT(n){return typeof n=="function"}function fx(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var Fu=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},ed=(()=>{let n=()=>px;return n.ngInherit=!0,n})();function px(n){return n.type.prototype.ngOnChanges&&(n.setInput=xT),_T}function _T(){let n=gx(this),e=n?.current;if(e){let t=n.previous;if(t===ws)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function xT(n,e,t,i,r){let s=this.declaredInputs[i],o=gx(n)||bT(n,{previous:ws,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Fu(l&&l.currentValue,t,c===ws),fx(n,e,r,t)}var mx="__ngSimpleChanges__";function gx(n){return n[mx]||null}function bT(n,e){return n[mx]=e}var V_=[];var bt=function(n,e=null,t){for(let i=0;i<V_.length;i++){let r=V_[i];r(n,e,t)}},lt=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(lt||{});function ST(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=px(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function MT(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function Nu(n,e,t){vx(n,e,3,t)}function Pu(n,e,t,i){(n[ze]&3)===t&&vx(n,e,t,i)}function Lm(n,e){let t=n[ze];(t&3)===e&&(t&=16383,t+=1,n[ze]=t)}function vx(n,e,t,i){let r=i!==void 0?n[Ds]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Ds]+=65536),(a<s||s==-1)&&(ET(n,t,e,c),n[Ds]=(n[Ds]&4294901760)+c+2),c++}function H_(n,e){bt(lt.LifecycleHookStart,n,e);let t=He(null);try{e.call(n)}finally{He(t),bt(lt.LifecycleHookEnd,n,e)}}function ET(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[ze]>>14<n[Ds]>>16&&(n[ze]&3)===e&&(n[ze]+=16384,H_(a,s)):H_(a,s)}var Ao=-1,Ns=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function wT(n){return(n.flags&8)!==0}function TT(n){return(n.flags&16)!==0}function CT(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];IT(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function DT(n){return n===3||n===4||n===6}function IT(n){return n.charCodeAt(0)===64}function td(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?z_(n,t,r,null,e[++i]):z_(n,t,r,null,null))}}return n}function z_(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function yx(n){return n!==Ao}function ku(n){return n&32767}function AT(n){return n>>16}function Uu(n,e){let t=AT(n),i=e;for(;t>0;)i=i[Cs],t--;return i}var zm=!0;function Bu(n){let e=zm;return zm=n,e}var RT=256,_x=RT-1,xx=5,NT=0,Oi={};function PT(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Es)&&(i=t[Es]),i==null&&(i=t[Es]=NT++);let r=i&_x,s=1<<r;e.data[n+(r>>xx)]|=s}function Vu(n,e){let t=bx(n,e);if(t!==-1)return t;let i=e[Pe];i.firstCreatePass&&(n.injectorIndex=e.length,Fm(i.data,n),Fm(e,null),Fm(i.blueprint,null));let r=lg(n,e),s=n.injectorIndex;if(yx(r)){let o=ku(r),a=Uu(r,e),c=a[Pe].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function Fm(n,e){n.push(0,0,0,0,0,0,0,0,e)}function bx(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function lg(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Tx(r),i===null)return Ao;if(t++,r=r[Cs],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Ao}function Gm(n,e,t){PT(n,e,t)}function Sx(n,e,t){if(t&8||n!==void 0)return n;mu(e,"NodeInjector")}function Mx(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[Rr],s=wn(void 0);try{return r?r.get(e,i,t&8):Qp(e,i,t&8)}finally{wn(s)}}return Sx(i,e,t)}function Ex(n,e,t,i=0,r){if(n!==null){if(e[ze]&2048&&!(i&2)){let o=kT(n,e,t,i,Oi);if(o!==Oi)return o}let s=wx(n,e,t,i,Oi);if(s!==Oi)return s}return Mx(e,t,i,r)}function wx(n,e,t,i,r){let s=LT(t);if(typeof s=="function"){if(!Tm(e,n,i))return i&1?Sx(r,t,i):Mx(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))mu(t);else return o}finally{Cm()}}else if(typeof s=="number"){let o=null,a=bx(n,e),c=Ao,l=i&1?e[ei][Qn]:null;for((a===-1||i&4)&&(c=a===-1?lg(n,e):e[a+8],c===Ao||!j_(i,!1)?a=-1:(o=e[Pe],a=ku(c),e=Uu(c,e)));a!==-1;){let u=e[Pe];if(G_(s,a,u.data)){let d=OT(a,e,t,o,i,l);if(d!==Oi)return d}c=e[a+8],c!==Ao&&j_(i,e[Pe].data[a+8]===l)&&G_(s,a,e)?(o=u,a=ku(c),e=Uu(c,e)):a=-1}}return r}function OT(n,e,t,i,r,s){let o=e[Pe],a=o.data[n+8],c=i==null?Fr(a)&&zm:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=Ou(a,o,t,c,l);return u!==null?ic(e,o,u,a,r):Oi}function Ou(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,d=i?a:a+u,h=r?a+u:l;for(let f=d;f<h;f++){let g=o[f];if(f<c&&t===g||f>=c&&g.type===t)return f}if(r){let f=o[c];if(f&&kr(f)&&f.type===t)return c}return null}function ic(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof Ns){let a=s;if(a.resolving)throw Jp("");let c=Bu(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,d=a.injectImpl?wn(a.injectImpl):null,h=Tm(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&ST(t,o[t],e)}finally{d!==null&&wn(d),Bu(c),a.resolving=!1,Cm()}}return s}function LT(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Es)?n[Es]:void 0;return typeof e=="number"?e>=0?e&_x:FT:e}function G_(n,e,t){let i=1<<n;return!!(t[e+(n>>xx)]&i)}function j_(n,e){return!(n&2)&&!(n&1&&e)}var Rs=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Ex(this._tNode,this._lView,e,ys(i),t)}};function FT(){return new Rs(ii(),wt())}function oc(n){return Qu(()=>{let e=n.prototype.constructor,t=e[Va]||jm(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let s=r[Va]||jm(r);if(s&&s!==t)return s;r=Object.getPrototypeOf(r)}return s=>new s})}function jm(n){return Gp(n)?()=>{let e=jm(un(n));return e&&e()}:Dr(n)}function kT(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[ze]&2048&&!Eo(o);){let a=wx(s,o,t,i|2,Oi);if(a!==Oi)return a;let c=s.parent;if(!c){let l=o[am];if(l){let u=l.get(t,Oi,i&-5);if(u!==Oi)return u}c=Tx(o),o=o[Cs]}s=c}return r}function Tx(n){let e=n[Pe],t=e.type;return t===2?e.declTNode:t===1?n[Qn]:null}function UT(){return Lo(ii(),wt())}function Lo(n,e){return new zr(ir(n,e))}var zr=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=UT}return n})();function BT(n){return n instanceof zr?n.nativeElement:n}function VT(){return this._results[Symbol.iterator]()}var Hu=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new an}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=n_(e);(this._changesDetected=!t_(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=VT};function Cx(n){return(n.flags&128)===128}var ug=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(ug||{}),Dx=new Map,HT=0;function zT(){return HT++}function GT(n){Dx.set(n[Pr],n)}function Wm(n){Dx.delete(n[Pr])}var W_="__ngContext__";function Ro(n,e){Lr(e)?(n[W_]=e[Pr],GT(e)):n[W_]=e}function Ix(n){return Rx(n[So])}function Ax(n){return Rx(n[Jn])}function Rx(n){for(;n!==null&&!gi(n);)n=n[Jn];return n}var jT;function dg(n){jT=n}var nd=new Ie("",{factory:()=>WT}),WT="ng";var id=new Ie(""),ac=new Ie("",{providedIn:"platform",factory:()=>"unknown"});var rd=new Ie("",{factory:()=>J(Kt).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Nx=!1,Px=new Ie("",{factory:()=>Nx});var $_=new WeakMap;function $T(n,e){if(n==null||typeof n!="object")return;let t=$_.get(n);t||(t=new WeakSet,$_.set(n,t)),t.add(e)}var XT=(n,e,t,i)=>{};function qT(n,e,t,i){XT(n,e,t,i)}function hg(n){return(n.flags&32)===32}var YT=()=>null;function Ox(n,e,t=!1){return YT(n,e,t)}function Lx(n,e){let t=n.contentQueries;if(t!==null){let i=He(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Mu(s),a.contentQueries(2,e[o],o)}}}finally{He(i)}}}function $m(n,e,t){Mu(0);let i=He(null);try{e(n,t)}finally{He(i)}}function Fx(n,e,t){if(cm(e)){let i=He(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{He(i)}}}var yi=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(yi||{});function ZT(n,e){return n.createText(e)}function KT(n,e,t){n.setValue(e,t)}function kx(n,e,t){return n.createElement(e,t)}function zu(n,e,t,i,r){n.insertBefore(e,t,i,r)}function Ux(n,e,t){n.appendChild(e,t)}function X_(n,e,t,i,r){i!==null?zu(n,e,t,i,r):Ux(n,e,t)}function JT(n,e,t,i){n.removeChild(null,e,t,i)}function QT(n,e,t){n.setAttribute(e,"style",t)}function eC(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Bx(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&CT(n,e,i),r!==null&&eC(n,e,r),s!==null&&QT(n,e,s)}function Vx(n){return n instanceof Function?n():n}function tC(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var Hx="ng-template";function nC(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&tC(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(fg(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function fg(n){return n.type===4&&n.value!==Hx}function iC(n,e,t){let i=n.type===4&&!t?Hx:n.value;return e===i}function rC(n,e,t){let i=4,r=n.attrs,s=r!==null?aC(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!vi(i)&&!vi(c))return!1;if(o&&vi(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!iC(n,c,t)||c===""&&e.length===1){if(vi(i))return!1;o=!0}}else if(i&8){if(r===null||!nC(n,r,c,t)){if(vi(i))return!1;o=!0}}else{let l=e[++a],u=sC(c,r,fg(n),t);if(u===-1){if(vi(i))return!1;o=!0;continue}if(l!==""){let d;if(u>s?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(vi(i))return!1;o=!0}}}}return vi(i)||o}function vi(n){return(n&1)===0}function sC(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return cC(e,n)}function oC(n,e,t=!1){for(let i=0;i<e.length;i++)if(rC(n,e[i],t))return!0;return!1}function aC(n){for(let e=0;e<n.length;e++){let t=n[e];if(DT(t))return e}return n.length}function cC(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function q_(n,e){return n?":not("+e.trim()+")":e}function lC(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!vi(o)&&(e+=q_(s,r),r=""),i=o,s=s||!vi(i);t++}return r!==""&&(e+=q_(s,r)),e}function uC(n){return n.map(lC).join(",")}function dC(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!vi(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var Gr={};function pg(n,e,t,i,r,s,o,a,c,l,u){let d=nn+i,h=d+r,f=hC(d,h),g=typeof l=="function"?l():l;return f[Pe]={type:n,blueprint:f,template:t,queries:null,viewQuery:a,declTNode:e,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function hC(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Gr);return t}function fC(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=pg(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function mg(n,e,t,i,r,s,o,a,c,l,u){let d=e.blueprint.slice();return d[mi]=r,d[ze]=i|4|128|8|64|1024,(l!==null||n&&n[ze]&2048)&&(d[ze]|=2048),hm(d),d[tn]=d[Cs]=n,d[dn]=t,d[Ri]=o||n&&n[Ri],d[vn]=a||n&&n[vn],d[Rr]=c||n&&n[Rr]||null,d[Qn]=s,d[Pr]=zT(),d[qa]=u,d[am]=l,d[ei]=e.type==2?n[ei]:d,d}function pC(n,e,t){let i=ir(e,n),r=fC(t),s=n[Ri].rendererFactory,o=gg(n,mg(n,r,null,zx(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function zx(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function Gx(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function gg(n,e){return n[So]?n[om][Jn]=e:n[So]=e,n[om]=e,e}function _i(n=1){jx(ni(),wt(),Tu()+n,!1)}function jx(n,e,t,i){if(!i)if((e[ze]&3)===3){let s=n.preOrderCheckHooks;s!==null&&Nu(e,s,t)}else{let s=n.preOrderHooks;s!==null&&Pu(e,s,0,t)}Br(t)}var sd=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(sd||{});function Xm(n,e,t,i){let r=He(null);try{let[s,o,a]=n.inputs[t],c=null;(o&sd.SignalBased)!==0&&(c=e[s][Bn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):fx(e,c,s,i)}finally{He(r)}}var jr=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(jr||{}),mC;function vg(n,e){return mC(n,e)}var cz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var qm=new WeakMap,ec=new WeakSet;function gC(n,e){let t=qm.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),ec.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function vC(n,e){let t=qm.get(n);t?t.includes(e)||t.push(e):qm.set(n,[e])}var No=new Set,yg=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(yg||{}),Fo=new Ie(""),Y_=new Set;function Ls(n){Y_.has(n)||(Y_.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var Wx=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>new n})}return n})();var yC=new Ie("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:J(Zt)})});function $x(n,e,t){let i=n.get(yC);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function _C(n,e){for(let[t,i]of e)$x(n,i.animateFns)}function Z_(n,e,t,i){let r=n?.[Mo]?.enter;e!==null&&r&&r.has(t.index)&&_C(i,r)}function Io(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;gi(r)?c=r:Lr(r)&&(l=!0,r=r[mi]);let u=ti(r);n===0&&i!==null?(Z_(a,i,s,t),o==null?Ux(e,i,u):zu(e,i,u,o||null,!0)):n===1&&i!==null?(Z_(a,i,s,t),zu(e,i,u,o||null,!0),gC(s,u)):n===2?(a?.[Mo]?.leave?.has(s.index)&&vC(s,u),ec.delete(u),K_(a,s,t,d=>{if(ec.has(u)){ec.delete(u);return}JT(e,u,l,d)})):n===3&&(ec.delete(u),K_(a,s,t,()=>{e.destroyNode(u)})),c!=null&&NC(e,n,t,c,s,i,o)}}function xC(n,e){Xx(n,e),e[mi]=null,e[Qn]=null}function bC(n,e,t,i,r,s){i[mi]=r,i[Qn]=e,od(n,i,t,1,r,s)}function Xx(n,e){e[Ri].changeDetectionScheduler?.notify(9),od(n,e,e[vn],2,null,null)}function SC(n){let e=n[So];if(!e)return km(n[Pe],n);for(;e;){let t=null;if(Lr(e))t=e[So];else{let i=e[yn];i&&(t=i)}if(!t){for(;e&&!e[Jn]&&e!==n;)Lr(e)&&km(e[Pe],e),e=e[tn];e===null&&(e=n),Lr(e)&&km(e[Pe],e),t=e&&e[Jn]}e=t}}function _g(n,e){let t=n[Is],i=t.indexOf(e);t.splice(i,1)}function xg(n,e){if(As(e))return;let t=e[vn];t.destroyNode&&od(n,e,t,3,null,null),SC(e)}function km(n,e){if(As(e))return;let t=He(null);try{e[ze]&=-129,e[ze]|=256,e[Vn]&&Pa(e[Vn]),wC(n,e),EC(n,e),e[Pe].type===1&&e[vn].destroy();let i=e[Nr];if(i!==null&&gi(e[tn])){i!==e[tn]&&_g(i,e);let r=e[Ni];r!==null&&r.detachView(n)}Wm(e)}finally{He(t)}}function K_(n,e,t,i){let r=n?.[Mo];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&No.add(n[Pr]),$x(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),MC(n,i)}else n&&No.delete(n[Pr]),i(!1)},r)}function MC(n,e){let t=n[Mo]?.running;if(t){t.then(()=>{n[Mo].running=void 0,No.delete(n[Pr]),e(!0)});return}e(!1)}function EC(n,e){let t=n.cleanup,i=e[bo];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[bo]=null);let r=e[er];if(r!==null){e[er]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Ya];if(s!==null){e[Ya]=null;for(let o of s)o.destroy()}}function wC(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Ns)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];bt(lt.LifecycleHookStart,a,c);try{c.call(a)}finally{bt(lt.LifecycleHookEnd,a,c)}}else{bt(lt.LifecycleHookStart,r,s);try{s.call(r)}finally{bt(lt.LifecycleHookEnd,r,s)}}}}}function TC(n,e,t){return CC(n,e.parent,t)}function CC(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[mi];if(Fr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===yi.None||r===yi.Emulated)return null}return ir(i,t)}function DC(n,e,t){return AC(n,e,t)}function IC(n,e,t){return n.type&40?ir(n,t):null}var AC=IC,J_;function bg(n,e,t,i){let r=TC(n,i,e),s=e[vn],o=i.parent||e[Qn],a=DC(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)X_(s,r,t[c],a,!1);else X_(s,r,t,a,!1);J_!==void 0&&J_(s,i,e,t,r)}function tc(n,e){if(e!==null){let t=e.type;if(t&3)return ir(e,n);if(t&4)return Ym(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return tc(n,i);{let r=n[e.index];return gi(r)?Ym(-1,r):ti(r)}}else{if(t&128)return tc(n,e.next);if(t&32)return vg(e,n)()||ti(n[e.index]);{let i=qx(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Ir(n[ei]);return tc(r,i)}else return tc(n,e.next)}}}return null}function qx(n,e){if(e!==null){let i=n[ei][Qn],r=e.projection;return i.projection[r]}return null}function Ym(n,e){let t=yn+n+1;if(t<e.length){let i=e[t],r=i[Pe].firstChild;if(r!==null)return tc(i,r)}return e[Or]}function Sg(n,e,t,i,r,s,o){for(;t!=null;){let a=i[Rr];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&Ro(ti(c),i),t.flags|=2),!hg(t))if(l&8)Sg(n,e,t.child,i,r,s,!1),Io(e,n,a,r,c,t,s,i);else if(l&32){let u=vg(t,i),d;for(;d=u();)Io(e,n,a,r,d,t,s,i);Io(e,n,a,r,c,t,s,i)}else l&16?RC(n,e,i,t,r,s):Io(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function od(n,e,t,i,r,s){Sg(t,i,n.firstChild,e,r,s,!1)}function RC(n,e,t,i,r,s){let o=t[ei],c=o[Qn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Io(e,n,t[Rr],r,u,i,s,t)}else{let l=c,u=o[tn];Cx(i)&&(l.flags|=128),Sg(n,e,l,u,r,s,!0)}}function NC(n,e,t,i,r,s,o){let a=i[Or],c=ti(i);a!==c&&Io(e,n,t,s,a,r,o);for(let l=yn;l<i.length;l++){let u=i[l];od(u[Pe],u,n,e,s,a)}}function Yx(n,e,t,i,r){let s=Tu(),o=i&2;try{Br(-1),o&&e.length>nn&&jx(n,e,nn,!1);let a=o?lt.TemplateUpdateStart:lt.TemplateCreateStart;bt(a,r,t),t(i,r)}finally{Br(s);let a=o?lt.TemplateUpdateEnd:lt.TemplateCreateEnd;bt(a,r,t)}}function Zx(n,e,t){FC(n,e,t),(t.flags&64)===64&&kC(n,e,t)}function Mg(n,e,t=ir){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function PC(n,e,t,i){let s=i.get(Px,Nx)||t===yi.ShadowDom||t===yi.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);return OC(o),o}function OC(n){LC(n)}var LC=()=>null;function FC(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Fr(t)&&pC(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Vu(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=ic(e,n,o,t);if(Ro(c,e),s!==null&&VC(e,o-i,c,a,t,s),kr(a)){let l=Pi(t.index,e);l[dn]=ic(e,n,o,t)}}}function kC(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=D_();try{Br(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Su(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&UC(c,l)}}finally{Br(-1),Su(o)}}function UC(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function BC(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];oC(e,s.selectors,!1)&&(i??=[],kr(s)?i.unshift(s):i.push(s))}return i}function VC(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];Xm(i,t,c,l)}}function Kx(n,e,t,i,r){let s=nn+t,o=e[Pe],a=r(o,e,n,i,t);e[s]=a,Co(n,!0);let c=n.type===2;return c?(Bx(e[vn],a,n),(y_()===0||_u(n))&&Ro(a,e),__()):Ro(a,e),Iu()&&(!c||!hg(n))&&bg(o,e,a,n),n}function Jx(n){let e=n;return bm()?M_():(e=e.parent,Co(e,!1)),e}function HC(n,e){let t=n[Rr];if(!t)return;let i;try{i=t.get(ri,null)}catch(r){i=null}i?.(e)}function Qx(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],d=e.data[l];Xm(d,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];Xm(u,l,i,r),a=!0}return a}function zC(n,e){let t=Pi(e,n),i=t[Pe];GC(i,t);let r=t[mi];r!==null&&t[qa]===null&&(t[qa]=Ox(r,t[Rr])),bt(lt.ComponentStart);try{Eg(i,t,t[dn])}finally{bt(lt.ComponentEnd,t[dn])}}function GC(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Eg(n,e,t){Eu(e);try{let i=n.viewQuery;i!==null&&$m(1,i,t);let r=n.template;r!==null&&Yx(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Ni]?.finishViewCreation(n),n.staticContentQueries&&Lx(n,e),n.staticViewQueries&&$m(2,n.viewQuery,t);let s=n.components;s!==null&&jC(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[ze]&=-5,wu()}}function jC(n,e){for(let t=0;t<e.length;t++)zC(n,e[t])}function eb(n,e,t,i){let r=He(null);try{let s=e.tView,a=n[ze]&4096?4096:16,c=mg(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[Nr]=l;let u=n[Ni];return u!==null&&(c[Ni]=u.createEmbeddedView(s)),Eg(s,c,t),c}finally{He(r)}}function Zm(n,e){return!e||e.firstChild===null||Cx(n)}function rc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(ti(s)),gi(s)&&tb(s,i);let o=t.type;if(o&8)rc(n,e,t.child,i);else if(o&32){let a=vg(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=qx(e,t);if(Array.isArray(a))i.push(...a);else{let c=Ir(e[ei]);rc(c[Pe],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function tb(n,e){for(let t=yn;t<n.length;t++){let i=n[t],r=i[Pe].firstChild;r!==null&&rc(i[Pe],i,r,e)}n[Or]!==n[mi]&&e.push(n[Or])}function nb(n){if(n[yu]!==null){for(let e of n[yu])e.impl.addSequence(e);n[yu].length=0}}var ib=[];function WC(n){return n[Vn]??$C(n)}function $C(n){let e=ib.pop()??Object.create(qC);return e.lView=n,e}function XC(n){n.lView[Vn]!==n&&(n.lView=null,ib.push(n))}var qC=ot(fe({},Aa),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Ja(n.lView)},consumerOnSignalRead(){this.lView[Vn]=this}});function YC(n){let e=n[Vn]??Object.create(ZC);return e.lView=n,e}var ZC=ot(fe({},Aa),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Ir(n.lView);for(;e&&!rb(e[Pe]);)e=Ir(e);e&&fm(e)},consumerOnSignalRead(){this.lView[Vn]=this}});function rb(n){return n.type!==2}function sb(n){if(n[Ya]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Ya])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[ze]&8192)}}var KC=100;function ob(n,e=0){let i=n[Ri].rendererFactory,r=!1;r||i.begin?.();try{JC(n,e)}finally{r||i.end?.()}}function JC(n,e){let t=Sm();try{Mm(!0),Km(n,e);let i=0;for(;Ka(n);){if(i===KC)throw new Me(103,!1);i++,Km(n,1)}}finally{Mm(t)}}function QC(n,e,t,i){if(As(e))return;let r=e[ze],s=!1,o=!1;Eu(e);let a=!0,c=null,l=null;s||(rb(n)?(l=WC(e),c=Na(l)):Fl()===null?(a=!1,l=YC(e),c=Na(l)):e[Vn]&&(Pa(e[Vn]),e[Vn]=null));try{hm(e),w_(n.bindingStartIndex),t!==null&&Yx(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let f=n.preOrderCheckHooks;f!==null&&Nu(e,f,null)}else{let f=n.preOrderHooks;f!==null&&Pu(e,f,0,null),Lm(e,0)}if(o||eD(e),sb(e),ab(e,0),n.contentQueries!==null&&Lx(n,e),!s)if(u){let f=n.contentCheckHooks;f!==null&&Nu(e,f)}else{let f=n.contentHooks;f!==null&&Pu(e,f,1),Lm(e,1)}nD(n,e);let d=n.components;d!==null&&lb(e,d,0);let h=n.viewQuery;if(h!==null&&$m(2,h,i),!s)if(u){let f=n.viewCheckHooks;f!==null&&Nu(e,f)}else{let f=n.viewHooks;f!==null&&Pu(e,f,2),Lm(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[vu]){for(let f of e[vu])f();e[vu]=null}s||(nb(e),e[ze]&=-73)}catch(u){throw s||Ja(e),u}finally{l!==null&&(kl(l,c),a&&XC(l)),wu()}}function ab(n,e){for(let t=Ix(n);t!==null;t=Ax(t))for(let i=yn;i<t.length;i++){let r=t[i];cb(r,e)}}function eD(n){for(let e=Ix(n);e!==null;e=Ax(e)){if(!(e[ze]&2))continue;let t=e[Is];for(let i=0;i<t.length;i++){let r=t[i];fm(r)}}}function tD(n,e,t){bt(lt.ComponentStart);let i=Pi(e,n);try{cb(i,t)}finally{bt(lt.ComponentEnd,i[dn])}}function cb(n,e){xu(n)&&Km(n,e)}function Km(n,e){let i=n[Pe],r=n[ze],s=n[Vn],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&Ul(s)),o||=!1,s&&(s.dirty=!1),n[ze]&=-9217,o)QC(i,n,i.template,n[dn]);else if(r&8192){let a=He(null);try{sb(n),ab(n,1);let c=i.components;c!==null&&lb(n,c,1),nb(n)}finally{He(a)}}}function lb(n,e,t){for(let i=0;i<e.length;i++)tD(n,e[i],t)}function nD(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Br(~r);else{let s=r,o=t[++i],a=t[++i];C_(o,s);let c=e[s];bt(lt.HostBindingsUpdateStart,c);try{a(2,c)}finally{bt(lt.HostBindingsUpdateEnd,c)}}}}finally{Br(-1)}}function wg(n,e){let t=Sm()?64:1088;for(n[Ri].changeDetectionScheduler?.notify(e);n;){n[ze]|=t;let i=Ir(n);if(Eo(n)&&!i)return n;n=i}return null}function ub(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function iD(n,e){let t=yn+e;if(t<n.length)return n[t]}function db(n,e,t,i=!0){let r=e[Pe];if(sD(r,e,n,t),i){let o=Ym(t,n),a=e[vn],c=a.parentNode(n[Or]);c!==null&&bC(r,n[Qn],a,e,c,o)}let s=e[qa];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function rD(n,e){let t=Gu(n,e);return t!==void 0&&xg(t[Pe],t),t}function Gu(n,e){if(n.length<=yn)return;let t=yn+e,i=n[t];if(i){let r=i[Nr];r!==null&&r!==n&&_g(r,i),e>0&&(n[t-1][Jn]=i[Jn]);let s=Wa(n,yn+e);xC(i[Pe],i);let o=s[Ni];o!==null&&o.detachView(s[Pe]),i[tn]=null,i[Jn]=null,i[ze]&=-129}return i}function sD(n,e,t,i){let r=yn+i,s=t.length;i>0&&(t[r-1][Jn]=e),i<s-yn?(e[Jn]=t[r],em(t,yn+i,e)):(t.push(e),e[Jn]=null),e[tn]=t;let o=e[Nr];o!==null&&t!==o&&hb(o,e);let a=e[Ni];a!==null&&a.insertView(n),bu(e),e[ze]|=128}function hb(n,e){let t=n[Is],i=e[tn];if(Lr(i))n[ze]|=2;else{let r=i[tn][ei];e[ei]!==r&&(n[ze]|=2)}t===null?n[Is]=[e]:t.push(e)}var Hr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Pe];return rc(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[dn]}set context(e){this._lView[dn]=e}get destroyed(){return As(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[tn];if(gi(e)){let t=e[Za],i=t?t.indexOf(this):-1;i>-1&&(Gu(e,i),Wa(t,i))}this._attachedToViewContainer=!1}xg(this._lView[Pe],this._lView)}onDestroy(e){pm(this._lView,e)}markForCheck(){wg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[ze]&=-129}reattach(){bu(this._lView),this._lView[ze]|=128}detectChanges(){this._lView[ze]|=1024,ob(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Me(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Eo(this._lView),t=this._lView[Nr];t!==null&&!e&&_g(t,this._lView),Xx(this._lView[Pe],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Me(902,!1);this._appRef=e;let t=Eo(this._lView),i=this._lView[Nr];i!==null&&!t&&hb(i,this._lView),bu(this._lView)}};var Po=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=oD;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=eb(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new Hr(s)}}return n})();function oD(){return Tg(ii(),wt())}function Tg(n,e){return n.type&4?new Po(e,n,Lo(n,e)):null}function ad(n,e,t,i,r){let s=n.data[e];if(s===null)s=aD(n,e,t,i,r),T_()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=S_();s.injectorIndex=o===null?-1:o.injectorIndex}return Co(s,!0),s}function aD(n,e,t,i,r){let s=xm(),o=bm(),a=o?s:s&&s.parent,c=n.data[e]=lD(n,a,t,e,i,r);return cD(n,c,s,o),c}function cD(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function lD(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return b_()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var uD=()=>null,dD=()=>null;function Q_(n,e){return uD(n,e)}function hD(n,e,t){return dD(n,e,t)}var fb=class{},cd=class{},Jm=class{resolveComponentFactory(e){throw new Me(917,!1)}},cc=class{static NULL=new Jm},Ps=class{};var pb=(()=>{class n{static \u0275prov=Ee({token:n,providedIn:"root",factory:()=>null})}return n})();var Lu={},Qm=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Lu,i);return r!==Lu||t===Lu?r:this.parentInjector.get(e,t,i)}};function ju(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=zp(r,a);else if(s==2){let c=a,l=e[++o];i=zp(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Fs(n,e=0){let t=wt();if(t===null)return We(n,e);let i=ii();return Ex(i,t,un(n),e)}function fD(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}gD(n,e,t,a,s,c,l)}s!==null&&i!==null&&pD(t,i,s)}function pD(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Me(-301,!1);i.push(e[r],s)}}function mD(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function gD(n,e,t,i,r,s,o){let a=i.length,c=null;for(let h=0;h<a;h++){let f=i[h];c===null&&kr(f)&&(c=f,mD(n,t,h)),Gm(Vu(t,e),n,f.type)}SD(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let f=i[h];f.providersResolver&&f.providersResolver(f)}let l=!1,u=!1,d=Gx(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let h=0;h<a;h++){let f=i[h];if(t.mergedAttrs=td(t.mergedAttrs,f.hostAttrs),yD(n,t,e,d,f),bD(d,f,r),o!==null&&o.has(f)){let[y,m]=o.get(f);t.directiveToIndex.set(f.type,[d,y+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(f))&&t.directiveToIndex.set(f.type,d);f.contentQueries!==null&&(t.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(t.flags|=64);let g=f.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}vD(n,t,s)}function vD(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))ex(0,e,r,i),ex(1,e,r,i),nx(e,i,!1);else{let s=t.get(r);tx(0,e,s,i),tx(1,e,s,i),nx(e,i,!0)}}}function ex(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),mb(e,s)}}function tx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),mb(e,o)}}function mb(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function nx(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||fg(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function yD(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Dr(r.type,!0)),o=new Ns(s,kr(r),Fs,null);n.blueprint[i]=o,t[i]=o,_D(n,e,i,Gx(n,t,r.hostVars,Gr),r)}function _D(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;xD(o)!=a&&o.push(a),o.push(t,i,s)}}function xD(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function bD(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;kr(e)&&(t[""]=n)}}function SD(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function gb(n,e,t,i,r,s,o,a){let c=e[Pe],l=c.consts,u=Ur(l,o),d=ad(c,n,t,i,u);return s&&fD(c,e,d,Ur(l,a),r),d.mergedAttrs=td(d.mergedAttrs,d.attrs),d.attrs!==null&&ju(d,d.attrs,!1),d.mergedAttrs!==null&&ju(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function vb(n,e){MT(n,e),cm(e)&&n.queries.elementEnd(e)}function MD(n,e,t,i,r,s){let o=e.consts,a=Ur(o,r),c=ad(e,n,t,i,a);if(c.mergedAttrs=td(c.mergedAttrs,c.attrs),s!=null){let l=Ur(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&ju(c,c.attrs,!1),c.mergedAttrs!==null&&ju(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function ED(n,e,t){return n[e]=t}function Wu(n,e,t){if(t===Gr)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function wD(n,e,t,i){let r=Wu(n,e,t);return Wu(n,e+1,i)||r}function TD(n,e,t){return function i(r){let s=i.__ngNativeEl__;s!==void 0&&$T(r,s);let o=Fr(n)?Pi(n.index,e):e;wg(o,5);let a=e[dn],c=ix(e,a,t,r),l=i.__ngNextListenerFn__;for(;l;)c=ix(e,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function ix(n,e,t,i){let r=He(null);try{return bt(lt.OutputStart,e,t),t(i)!==!1}catch(s){return HC(n,s),!1}finally{bt(lt.OutputEnd,e,t),He(r)}}function CD(n,e,t,i,r,s,o,a){let c=_u(n),l=!1,u=null;if(!i&&c&&(u=ID(e,t,s,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=o,u.__ngLastListenerFn__=o,l=!0}else{let d=ir(n,t),h=i?i(d):d;qT(t,h,s,a),i||(a.__ngNativeEl__=d);let f=r.listen(h,s,a);if(!DD(s)){let g=i?y=>i(ti(y[n.index])):n.index;AD(g,e,t,s,a,f,!1)}}return l}function DD(n){return n.startsWith("animation")||n.startsWith("transition")}function ID(n,e,t,i){let r=n.cleanup;if(r!=null)for(let s=0;s<r.length-1;s+=2){let o=r[s];if(o===t&&r[s+1]===i){let a=e[bo],c=r[s+2];return a&&a.length>c?a[c]:null}typeof o=="string"&&(s+=2)}return null}function AD(n,e,t,i,r,s,o){let a=e.firstCreatePass?gm(e):null,c=mm(t),l=c.length;c.push(r,s),a&&a.push(i,n,l,(l+1)*(o?-1:1))}var eg=Symbol("BINDING");function yb(n){return n.debugInfo?.className||n.type.name||null}var $u=class extends cc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=Ar(e);return new Oo(t,this.ngModule)}};function RD(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&sd.SignalBased)!==0};return r&&(s.transform=r),s})}function ND(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function PD(n,e,t){let i=e instanceof Zt?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Qm(t,i):t}function OD(n){let e=n.get(Ps,null);if(e===null)throw new Me(407,!1);let t=n.get(pb,null),i=n.get(Ss,null),r=n.get(Fo,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function LD(n,e){let t=_b(n);return kx(e,t,t==="svg"?lm:t==="math"?u_:null)}function _b(n){return(n.selectors[0][0]||"div").toLowerCase()}var Oo=class extends cd{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=RD(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=ND(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=uC(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){bt(lt.DynamicComponentStart);let a=He(null);try{let c=this.componentDef,l=PD(c,r||this.ngModule,e),u=OD(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(yb(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{He(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=FD(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?PC(l,r,a.encapsulation,t):LD(a,l),d=o?.some(rx)||s?.some(g=>typeof g!="function"&&g.bindings.some(rx)),h=mg(null,c,null,512|zx(a),null,null,e,l,t,null,Ox(u,t,!0));h[nn]=u,Eu(h);let f=null;try{let g=gb(nn,h,2,"#host",()=>c.directiveRegistry,!0,0);Bx(l,u,g),Ro(u,h),Zx(c,h,g),Fx(c,g,h),vb(c,g),i!==void 0&&UD(g,this.ngContentSelectors,i),f=Pi(g.index,h),h[dn]=f[dn],Eg(c,h,null)}catch(g){throw f!==null&&Wm(f),Wm(h),g}finally{bt(lt.DynamicComponentEnd),wu()}return new Xu(this.componentType,h,!!d)}};function FD(n,e,t,i){let r=n?["ng-version","21.2.12"]:dC(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[eg].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let h of d.bindings){a+=h[eg].requiredVars;let f=u+1;h.create&&(h.targetIdx=f,(s??=[]).push(h)),h.update&&(h.targetIdx=f,(o??=[]).push(h))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,h=Kp(d);c.push(h)}return pg(0,null,kD(s,o),1,a,c,null,null,null,[r],null)}function kD(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function rx(n){let e=n[eg].kind;return e==="input"||e==="twoWay"}var Xu=class extends fb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=um(t[Pe],nn),this.location=Lo(this._tNode,t),this.instance=Pi(this._tNode.index,t)[dn],this.hostView=this.changeDetectorRef=new Hr(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=Qx(i,r[Pe],r,e,t);this.previousInputValues.set(e,t);let o=Pi(i.index,r);wg(o,1)}get injector(){return new Rs(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function UD(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var ks=(()=>{class n{static __NG_ELEMENT_ID__=BD}return n})();function BD(){let n=ii();return xb(n,wt())}var tg=class n extends ks{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return Lo(this._hostTNode,this._hostLView)}get injector(){return new Rs(this._hostTNode,this._hostLView)}get parentInjector(){let e=lg(this._hostTNode,this._hostLView);if(yx(e)){let t=Uu(e,this._hostLView),i=ku(e),r=t[Pe].data[i+8];return new Rs(r,t)}else return new Rs(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=sx(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-yn}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=Q_(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,Zm(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!yT(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new Oo(Ar(e)),d=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?d:this.parentInjector).get(Zt,null);p&&(s=p)}let h=Ar(u.componentType??{}),f=Q_(this._lContainer,h?.id??null),g=f?.firstChild??null,y=u.create(d,r,g,s,o,a);return this.insertImpl(y.hostView,l,Zm(this._hostTNode,f)),y}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(p_(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[tn],l=new n(c,c[Qn],c[tn]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return db(o,r,s,i),e.attachToViewContainerRef(),em(Um(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=sx(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=Gu(this._lContainer,t);i&&(Wa(Um(this._lContainer),t),xg(i[Pe],i))}detach(e){let t=this._adjustIndex(e,-1),i=Gu(this._lContainer,t);return i&&Wa(Um(this._lContainer),t)!=null?new Hr(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function sx(n){return n[Za]}function Um(n){return n[Za]||(n[Za]=[])}function xb(n,e){let t,i=e[n.index];return gi(i)?t=i:(t=ub(i,e,null,n),e[n.index]=t,gg(e,t)),HD(t,e,n,i),new tg(t,n,e)}function VD(n,e){let t=n[vn],i=t.createComment(""),r=ir(e,n),s=t.parentNode(r);return zu(t,s,i,t.nextSibling(r),!1),i}var HD=jD,zD=()=>!1;function GD(n,e,t){return zD(n,e,t)}function jD(n,e,t,i){if(n[Or])return;let r;t.type&8?r=ti(i):r=VD(e,t),n[Or]=r}var ng=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},ig=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)Cg(e,t).matches!==null&&this.queries[t].setDirty()}},rg=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=JD(e):this.predicate=e}},sg=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},og=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,WD(t,s)),this.matchTNodeWithReadOption(e,t,Ou(t,e,s,!1,!1))}else i===Po?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Ou(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===zr||r===ks||r===Po&&t.type&4)this.addMatch(t.index,-2);else{let s=Ou(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function WD(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function $D(n,e){return n.type&11?Lo(n,e):n.type&4?Tg(n,e):null}function XD(n,e,t,i){return t===-1?$D(e,n):t===-2?qD(n,e,i):ic(n,n[Pe],t,e)}function qD(n,e,t){if(t===zr)return Lo(e,n);if(t===Po)return Tg(e,n);if(t===ks)return xb(e,n)}function bb(n,e,t,i){let r=e[Ni].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(XD(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function ag(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=bb(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let d=yn;d<u.length;d++){let h=u[d];h[Nr]===h[tn]&&ag(h[Pe],h,l,i)}if(u[Is]!==null){let d=u[Is];for(let h=0;h<d.length;h++){let f=d[h];ag(f[Pe],f,l,i)}}}}}return i}function YD(n,e){return n[Ni].queries[e].queryList}function ZD(n,e,t){let i=new Hu((t&4)===4);return v_(n,e,i,i.destroy),(e[Ni]??=new ig).queries.push(new ng(i))-1}function KD(n,e,t){let i=ni();return i.firstCreatePass&&(QD(i,new rg(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),ZD(i,wt(),e)}function JD(n){return n.split(",").map(e=>e.trim())}function QD(n,e,t){n.queries===null&&(n.queries=new sg),n.queries.track(new og(e,t))}function Cg(n,e){return n.queries.getByIndex(e)}function eI(n,e){let t=n[Pe],i=Cg(t,e);return i.crossesNgTemplate?ag(t,n,e,[]):bb(t,n,i,e)}var Os=class{},ld=class{};var qu=class extends Os{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new $u(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let s=Zp(e);this._bootstrapComponents=Vx(s.bootstrap),this._r3Injector=Dm(e,t,[{provide:Os,useValue:this},{provide:cc,useValue:this.componentFactoryResolver},...i],du(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},Yu=class extends ld{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new qu(this.moduleType,e,[])}};var sc=class extends Os{injector;componentFactoryResolver=new $u(this);instance=null;constructor(e){super();let t=new bs([...e.providers,{provide:Os,useValue:this},{provide:cc,useValue:this.componentFactoryResolver}],e.parent||Xa(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function lc(n,e,t=null){return new sc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var tI=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=im(!1,t.type),r=i.length>0?lc([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Ee({token:n,providedIn:"environment",factory:()=>new n(We(Zt))})}return n})();function Us(n){return Qu(()=>{let e=Sb(n),t=ot(fe({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===ug.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(tI).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||yi.Emulated,styles:n.styles||_s,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&Ls("NgStandalone"),Mb(t);let i=n.dependencies;return t.directiveDefs=ox(i,nI),t.pipeDefs=ox(i,Z0),t.id=sI(t),t})}function nI(n){return Ar(n)||Kp(n)}function iI(n,e){if(n==null)return ws;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=sd.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function rI(n){if(n==null)return ws;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function ud(n){return Qu(()=>{let e=Sb(n);return Mb(e),e})}function Dg(n){return{type:n.type,name:n.name,factory:null,pure:n.pure!==!1,standalone:n.standalone??!0,onDestroy:n.type.prototype.ngOnDestroy||null}}function Sb(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||ws,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||_s,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:iI(n.inputs,e),outputs:rI(n.outputs),debugInfo:null}}function Mb(n){n.features?.forEach(e=>e(n))}function ox(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function sI(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function oI(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=td(n.mergedAttrs,n.attrs);let u=n.tView=pg(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Co(n,!1);let c=aI(t,e,n,i);Iu()&&bg(t,e,c,n),Ro(c,e);let l=ub(c,e,c,n);e[i+nn]=l,gg(e,l),GD(l,n,e)}function Eb(n,e,t,i,r,s,o,a,c,l,u){let d=t+nn,h;if(e.firstCreatePass){if(h=ad(e,d,4,o||null,a||null),l!=null){let f=Ur(e.consts,l);h.localNames=[];for(let g=0;g<f.length;g+=2)h.localNames.push(f[g],-1)}}else h=e.data[d];return oI(h,n,e,t,i,r,s,c),l!=null&&Mg(n,h,u),h}var aI=cI;function cI(n,e,t,i){return Au(!0),e[vn].createComment("")}var Ig=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();var Ag=new Ie("");function ko(n){return!!n&&typeof n.then=="function"}function Rg(n){return!!n&&typeof n.subscribe=="function"}var wb=new Ie("");var Ng=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=J(wb,{optional:!0})??[];injector=J(pi);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=gn(this.injector,r);if(ko(s))t.push(s);else if(Rg(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),dd=new Ie("");function Tb(){hp(()=>{let n="";throw new Me(600,n)})}function Cb(n){return n.isBoundToModule}var lI=10;var Bs=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=J(ri);afterRenderManager=J(Wx);zonelessEnabled=J(Do);rootEffectScheduler=J(Om);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new an;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=J(rr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(kt(t=>!t))}constructor(){J(Fo,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=J(Zt);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=pi.NULL){return this._injector.get(Ut).run(()=>{bt(lt.BootstrapComponentStart);let o=t instanceof cd;if(!this._injector.get(Ng).done){let g="";throw new Me(405,g)}let c;o?c=t:c=this._injector.get(cc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=Cb(c)?void 0:this._injector.get(Os),u=i||c.selector,d=c.create(r,[],u,l),h=d.location.nativeElement,f=d.injector.get(Ag,null);return f?.registerApplication(h),d.onDestroy(()=>{this.detachView(d.hostView),nc(this.components,d),f?.unregisterApplication(h)}),this._loadComponent(d),bt(lt.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){bt(lt.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(yg.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw bt(lt.ChangeDetectionEnd),new Me(101,!1);let t=He(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,He(t),this.afterTick.next(),bt(lt.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ps,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<lI;){bt(lt.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{bt(lt.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ka(r))continue;let s=i&&!this.zonelessEnabled?0:1;ob(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Ka(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;nc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(dd,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>nc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Me(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function nc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function uc(n,e,t,i,r,s,o,a){Ls("NgControlFlow");let c=wt(),l=ni(),u=Ur(l.consts,s);return Eb(c,l,n,e,t,i,r,u,256,o,a),Pg}function Pg(n,e,t,i,r,s,o,a){Ls("NgControlFlow");let c=wt(),l=ni(),u=Ur(l.consts,s);return Eb(c,l,n,e,t,i,r,u,512,o,a),Pg}function dc(n,e){Ls("NgControlFlow");let t=wt(),i=Em(),r=t[i]!==Gr?t[i]:-1,s=r!==-1?ax(t,nn+r):void 0,o=0;if(Wu(t,i,n)){let a=He(null);try{if(s!==void 0&&rD(s,o),n!==-1){let c=nn+n,l=ax(t,c),u=uI(t[Pe],c),d=hD(l,u,t),h=eb(t,u,e,{dehydratedView:d});db(l,h,o,Zm(u,d))}}finally{He(a)}}else if(s!==void 0){let a=iD(s,o);a!==void 0&&(a[dn]=e)}}function ax(n,e){return n[e]}function uI(n,e){return um(n,e)}function cx(n,e,t,i,r){Qx(e,n,t,r?"class":"style",i)}function Zu(n,e,t,i){let r=wt(),s=r[Pe],o=n+nn,a=s.firstCreatePass?gb(o,r,2,e,BC,x_(),t,i):s.data[o];if(Fr(a)){let c=r[Ri].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(yb(l),()=>(lx(n,e,r,a,i),Zu))}}return lx(n,e,r,a,i),Zu}function lx(n,e,t,i,r){if(Kx(i,t,n,e,Db),_u(i)){let s=t[Pe];Zx(s,t,i),Fx(s,i,t)}r!=null&&Mg(t,i)}function Og(){let n=ni(),e=ii(),t=Jx(e);return n.firstCreatePass&&vb(n,t),ym(t)&&_m(),vm(),t.classesWithoutHost!=null&&wT(t)&&cx(n,t,wt(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&TT(t)&&cx(n,t,wt(),t.stylesWithoutHost,!1),Og}function Uo(n,e,t,i){return Zu(n,e,t,i),Og(),Uo}function St(n,e,t,i){let r=wt(),s=r[Pe],o=n+nn,a=s.firstCreatePass?MD(o,s,2,e,t,i):s.data[o];return Kx(a,r,n,e,Db),i!=null&&Mg(r,a),St}function Dt(){let n=ii(),e=Jx(n);return ym(e)&&_m(),vm(),Dt}function hc(n,e,t,i){return St(n,e,t,i),Dt(),hc}var Db=(n,e,t,i,r)=>(Au(!0),kx(e[vn],i,P_()));function hd(){return wt()}var Qa=void 0;function dI(n){let e=Math.floor(Math.abs(n)),t=n.toString().replace(/^[^.]*\.?/,"").length;return e===1&&t===0?1:5}var hI=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Qa,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Qa,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Qa,Qa,Qa],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",dI],Bm={};function fd(n){let e=fI(n),t=ux(e);if(t)return t;let i=e.split("-")[0];if(t=ux(i),t)return t;if(i==="en")return hI;throw new Me(701,!1)}function ux(n){return n in Bm||(Bm[n]=Ms.ng&&Ms.ng.common&&Ms.ng.common.locales&&Ms.ng.common.locales[n]),Bm[n]}var Vs=(function(n){return n[n.LocaleId=0]="LocaleId",n[n.DayPeriodsFormat=1]="DayPeriodsFormat",n[n.DayPeriodsStandalone=2]="DayPeriodsStandalone",n[n.DaysFormat=3]="DaysFormat",n[n.DaysStandalone=4]="DaysStandalone",n[n.MonthsFormat=5]="MonthsFormat",n[n.MonthsStandalone=6]="MonthsStandalone",n[n.Eras=7]="Eras",n[n.FirstDayOfWeek=8]="FirstDayOfWeek",n[n.WeekendRange=9]="WeekendRange",n[n.DateFormat=10]="DateFormat",n[n.TimeFormat=11]="TimeFormat",n[n.DateTimeFormat=12]="DateTimeFormat",n[n.NumberSymbols=13]="NumberSymbols",n[n.NumberFormats=14]="NumberFormats",n[n.CurrencyCode=15]="CurrencyCode",n[n.CurrencySymbol=16]="CurrencySymbol",n[n.CurrencyName=17]="CurrencyName",n[n.Currencies=18]="Currencies",n[n.Directionality=19]="Directionality",n[n.PluralCase=20]="PluralCase",n[n.ExtraData=21]="ExtraData",n})(Vs||{});function fI(n){return n.toLowerCase().replace(/_/g,"-")}var fc="en-US";var pI=fc;function Ib(n){typeof n=="string"&&(pI=n.toLowerCase().replace(/_/g,"-"))}function sr(n,e,t){let i=wt(),r=ni(),s=ii();return(s.type&3||t)&&CD(s,r,i,t,i[vn],n,e,TD(s,i,e)),sr}function Wr(n=1){return N_(n)}function pd(n,e,t){return KD(n,e,t),pd}function Lg(n){let e=wt(),t=ni(),i=wm();Mu(i+1);let r=Cg(t,i);if(n.dirty&&f_(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=eI(e,i);n.reset(s,BT),n.notifyOnChanges()}return!0}return!1}function Fg(){return YD(wt(),wm())}function jt(n,e=""){let t=wt(),i=ni(),r=n+nn,s=i.firstCreatePass?ad(i,r,1,e,null):i.data[r],o=mI(i,t,s,e);t[r]=o,Iu()&&bg(i,t,o,s),Co(s,!1)}var mI=(n,e,t,i)=>(Au(!0),ZT(e[vn],i));function gI(n,e,t,i=""){return Wu(n,Em(),t)?e+K0(t)+i:Gr}function or(n){return kg("",n),or}function kg(n,e,t){let i=wt(),r=gI(i,n,e,t);return r!==Gr&&vI(i,Tu(),r),kg}function vI(n,e,t){let i=d_(e,n);KT(n[vn],i,t)}function dx(n,e,t){let i=ni();i.firstCreatePass&&Ab(e,i.data,i.blueprint,kr(n),t)}function Ab(n,e,t,i,r){if(n=un(n),Array.isArray(n))for(let s=0;s<n.length;s++)Ab(n[s],e,t,i,r);else{let s=ni(),o=wt(),a=ii(),c=xs(n)?n:un(n.provide),l=sm(n),u=a.providerIndexes&1048575,d=a.directiveStart,h=a.providerIndexes>>20;if(xs(n)||!n.multi){let f=new Ns(l,r,Fs,null),g=Hm(c,e,r?u:u+h,d);g===-1?(Gm(Vu(a,o),s,c),Vm(s,n,e.length),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(f),o.push(f)):(t[g]=f,o[g]=f)}else{let f=Hm(c,e,u+h,d),g=Hm(c,e,u,u+h),y=f>=0&&t[f],m=g>=0&&t[g];if(r&&!m||!r&&!y){Gm(Vu(a,o),s,c);let p=xI(r?_I:yI,t.length,r,i,l,n);!r&&m&&(t[g].providerFactory=p),Vm(s,n,e.length,0),e.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),t.push(p),o.push(p)}else{let p=Rb(t[r?g:f],l,!r&&i);Vm(s,n,f>-1?f:g,p)}!r&&i&&m&&t[g].componentProviders++}}}function Vm(n,e,t,i){let r=xs(e),s=a_(e);if(r||s){let c=(s?un(e.useClass):e).prototype.ngOnDestroy;if(c){let l=n.destroyHooks||(n.destroyHooks=[]);if(!r&&e.multi){let u=l.indexOf(t);u===-1?l.push(t,[i,c]):l[u+1].push(i,c)}else l.push(t,c)}}}function Rb(n,e,t){return t&&n.componentProviders++,n.multi.push(e)-1}function Hm(n,e,t,i){for(let r=t;r<i;r++)if(e[r]===n)return r;return-1}function yI(n,e,t,i,r){return cg(this.multi,[])}function _I(n,e,t,i,r){let s=this.multi,o;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=ic(i,i[Pe],this.providerFactory.index,r);o=c.slice(0,a),cg(s,o);for(let l=a;l<c.length;l++)o.push(c[l])}else o=[],cg(s,o);return o}function cg(n,e){for(let t=0;t<n.length;t++){let i=n[t];e.push(i())}return e}function xI(n,e,t,i,r,s){let o=new Ns(n,t,Fs,null);return o.multi=[],o.index=e,o.componentProviders=0,Rb(o,r,i&&!t),o}function Ug(n,e){return t=>{t.providersResolver=(i,r)=>dx(i,r?r(n):n,!1),e&&(t.viewProvidersResolver=(i,r)=>dx(i,r?r(e):e,!0))}}function bI(n,e){let t=n[e];return t===Gr?void 0:t}function SI(n,e,t,i,r,s,o){let a=e+t;return wD(n,a,r,s)?ED(n,a+2,o?i.call(o,r,s):i(r,s)):bI(n,a+2)}function Hs(n,e){let t=ni(),i,r=n+nn;t.firstCreatePass?(i=MI(e,t.pipeRegistry),t.data[r]=i,i.onDestroy&&(t.destroyHooks??=[]).push(r,i.onDestroy)):i=t.data[r];let s=i.factory||(i.factory=Dr(i.type,!0)),o,a=wn(Fs);try{let c=Bu(!1),l=s();return Bu(c),dm(t,wt(),r,l),l}finally{wn(a)}}function MI(n,e){if(e)for(let t=e.length-1;t>=0;t--){let i=e[t];if(n===i.name)return i}}function zs(n,e,t,i){let r=n+nn,s=wt(),o=h_(s,r);return EI(s,r)?SI(s,E_(),e,o.transform,t,i,o):o.transform(t,i)}function EI(n,e){return n[Pe].data[e].pure}var Ku=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},Bg=(()=>{class n{compileModuleSync(t){return new Yu(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=Zp(t),s=Vx(r.declarations).reduce((o,a)=>{let c=Ar(a);return c&&o.push(new Oo(c)),o},[]);return new Ku(i,s)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Nb=(()=>{class n{applicationErrorHandler=J(ri);appRef=J(Bs);taskService=J(rr);ngZone=J(Ut);zonelessEnabled=J(Do);tracing=J(Fo,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Yt;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(za):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(J(Ru,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?F_:Am;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(za+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Pb(){return[{provide:Ss,useExisting:Nb},{provide:Ut,useClass:Ga},{provide:Do,useValue:!0}]}function wI(){return typeof $localize<"u"&&$localize.locale||fc}var pc=new Ie("",{factory:()=>J(pc,{optional:!0,skipSelf:!0})||wI()});function ar(n){return W0(n)}var kb=Symbol("InputSignalNode#UNSET"),zI=ot(fe({},Bl),{transformFn:void 0,applyValueToInputSignal(n,e){uo(n,e)}});function Ub(n,e){let t=Object.create(zI);t.value=n,t.transformFn=e?.transform;function i(){if(Ra(t),t.value===kb){let r=null;throw new Me(-950,r)}return t.value}return i[Bn]=t,i}function Ob(n,e){return Ub(n,e)}function GI(n){return Ub(kb,n)}var Bb=(Ob.required=GI,Ob);var jI=(()=>{class n{zone=J(Ut);changeDetectionScheduler=J(Ss);applicationRef=J(Bs);applicationErrorHandler=J(ri);_onMicrotaskEmptySubscription;initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{try{this.applicationRef.dirtyFlags|=1,this.applicationRef._tick()}catch(t){this.applicationErrorHandler(t)}})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),WI=new Ie("",{factory:()=>!1});function $I({ngZoneFactory:n,scheduleInRootZone:e}){return n??=()=>new Ut(ot(fe({},Hb()),{scheduleInRootZone:e})),[{provide:Do,useValue:!1},{provide:Ut,useFactory:n},{provide:nr,multi:!0,useFactory:()=>{let t=J(jI,{optional:!0});return()=>t.initialize()}},{provide:nr,multi:!0,useFactory:()=>{let t=J(XI);return()=>{t.initialize()}}},{provide:Ru,useValue:e??Im}]}function Vb(n){let e=n?.scheduleInRootZone,t=$I({ngZoneFactory:()=>{let i=Hb(n);return i.scheduleInRootZone=e,i.shouldCoalesceEventChangeDetection&&Ls("NgZone_CoalesceEvent"),new Ut(i)},scheduleInRootZone:e});return Ts([{provide:WI,useValue:!0},t])}function Hb(n){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:n?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:n?.runCoalescing??!1}}var XI=(()=>{class n{subscription=new Yt;initialized=!1;zone=J(Ut);pendingTasks=J(rr);initialize(){if(this.initialized)return;this.initialized=!0;let t=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(t=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{Ut.assertNotInAngularZone(),queueMicrotask(()=>{t!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(t),t=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{Ut.assertInAngularZone(),t??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Vg=new Ie(""),qI=new Ie("");function mc(n){return!n.moduleRef}function YI(n){let e=mc(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Ut);return t.run(()=>{mc(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ri),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),mc(n)){let s=()=>e.destroy(),o=n.platformInjector.get(Vg);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(Vg);o.add(s),n.moduleRef.onDestroy(()=>{nc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return KI(i,t,()=>{let s=e.get(rr),o=s.add(),a=e.get(Ng);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(pc,fc);if(Ib(c||fc),!e.get(qI,!0))return mc(n)?e.get(Bs):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(mc(n)){let u=e.get(Bs);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return ZI?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var ZI;function KI(n,e,t){try{let i=t();return ko(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var md=null;function JI(n=[],e){return pi.create({name:e,providers:[{provide:$a,useValue:"platform"},{provide:Vg,useValue:new Set([()=>md=null])},...n]})}function QI(n=[]){if(md)return md;let e=JI(n);return md=e,Tb(),eA(e),e}function eA(n){let e=n.get(id,null);gn(n,()=>{e?.forEach(t=>t())})}var tA=1e4;var o6=tA-1e3;var gc=(()=>{class n{static __NG_ELEMENT_ID__=nA}return n})();function nA(n){return iA(ii(),wt(),(n&16)===16)}function iA(n,e,t){if(Fr(n)&&!t){let i=Pi(n.index,e);return new Hr(i,i)}else if(n.type&175){let i=e[ei];return new Hr(i,e)}return null}function zb(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;bt(lt.BootstrapApplicationStart);try{let s=r?.injector??QI(i),o=[Pb(),U_,...t||[]],a=new sc({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return YI({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{bt(lt.BootstrapApplicationEnd)}}var Gb=null;function cr(){return Gb}function zg(n){Gb??=n}var vc=class{},gd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(jb),providedIn:"platform"})}return n})();var jb=(()=>{class n extends gd{_location;_history;_doc=J(Kt);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return cr().getBaseHref(this._doc)}onPopState(t){let i=cr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=cr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function Xb(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function Wb(n){let e=n.search(/#|\?|$/);return n[e-1]==="/"?n.slice(0,e-1)+n.slice(e):n}function $r(n){return n&&n[0]!=="?"?`?${n}`:n}var vd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(sA),providedIn:"root"})}return n})(),rA=new Ie(""),sA=(()=>{class n extends vd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??J(Kt).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Xb(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+$r(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,s){let o=this.prepareExternalUrl(r+$r(s));this._platformLocation.pushState(t,i,o)}replaceState(t,i,r,s){let o=this.prepareExternalUrl(r+$r(s));this._platformLocation.replaceState(t,i,o)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(We(gd),We(rA,8))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Bo=(()=>{class n{_subject=new an;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=cA(Wb($b(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+$r(i))}normalize(t){return n.stripTrailingSlash(aA(this._basePath,$b(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+$r(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+$r(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=$r;static joinWithSlash=Xb;static stripTrailingSlash=Wb;static \u0275fac=function(i){return new(i||n)(We(vd))};static \u0275prov=Ee({token:n,factory:()=>oA(),providedIn:"root"})}return n})();function oA(){return new Bo(We(vd))}function aA(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function $b(n){return n.replace(/\/index.html$/,"")}function cA(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}var Wg=(function(n){return n[n.Decimal=0]="Decimal",n[n.Percent=1]="Percent",n[n.Currency=2]="Currency",n[n.Scientific=3]="Scientific",n})(Wg||{});var Li={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Vo(n,e){let t=fd(n),i=t[Vs.NumberSymbols][e];if(typeof i>"u"){if(e===Li.CurrencyDecimal)return t[Vs.NumberSymbols][Li.Decimal];if(e===Li.CurrencyGroup)return t[Vs.NumberSymbols][Li.Group]}return i}function Yb(n,e){return fd(n)[Vs.NumberFormats][e]}var lA=/^(\d+)?\.((\d+)(-(\d+))?)?$/,qb=22,yd=".",yc="0",uA=";",dA=",",Gg="#";function hA(n,e,t,i,r,s,o=!1){let a="",c=!1;if(!isFinite(n))a=Vo(t,Li.Infinity);else{let l=mA(n);o&&(l=pA(l));let u=e.minInt,d=e.minFrac,h=e.maxFrac;if(s){let b=s.match(lA);if(b===null)throw new Me(2306,!1);let w=b[1],M=b[3],I=b[5];w!=null&&(u=jg(w)),M!=null&&(d=jg(M)),I!=null?h=jg(I):M!=null&&d>h&&(h=d)}gA(l,d,h);let f=l.digits,g=l.integerLen,y=l.exponent,m=[];for(c=f.every(b=>!b);g<u;g++)f.unshift(0);for(;g<0;g++)f.unshift(0);g>0?m=f.splice(g,f.length):(m=f,f=[0]);let p=[];for(f.length>=e.lgSize&&p.unshift(f.splice(-e.lgSize,f.length).join(""));f.length>e.gSize;)p.unshift(f.splice(-e.gSize,f.length).join(""));f.length&&p.unshift(f.join("")),a=p.join(Vo(t,i)),m.length&&(a+=Vo(t,r)+m.join("")),y&&(a+=Vo(t,Li.Exponential)+"+"+y)}return n<0&&!c?a=e.negPre+a+e.negSuf:a=e.posPre+a+e.posSuf,a}function Zb(n,e,t){let i=Yb(e,Wg.Decimal),r=fA(i,Vo(e,Li.MinusSign));return hA(n,r,e,Li.Group,Li.Decimal,t)}function fA(n,e="-"){let t={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=n.split(uA),r=i[0],s=i[1],o=r.indexOf(yd)!==-1?r.split(yd):[r.substring(0,r.lastIndexOf(yc)+1),r.substring(r.lastIndexOf(yc)+1)],a=o[0],c=o[1]||"";t.posPre=a.substring(0,a.indexOf(Gg));for(let u=0;u<c.length;u++){let d=c.charAt(u);d===yc?t.minFrac=t.maxFrac=u+1:d===Gg?t.maxFrac=u+1:t.posSuf+=d}let l=a.split(dA);if(t.gSize=l[1]?l[1].length:0,t.lgSize=l[2]||l[1]?(l[2]||l[1]).length:0,s){let u=r.length-t.posPre.length-t.posSuf.length,d=s.indexOf(Gg);t.negPre=s.substring(0,d).replace(/'/g,""),t.negSuf=s.slice(d+u).replace(/'/g,"")}else t.negPre=e+t.posPre,t.negSuf=t.posSuf;return t}function pA(n){if(n.digits[0]===0)return n;let e=n.digits.length-n.integerLen;return n.exponent?n.exponent+=2:(e===0?n.digits.push(0,0):e===1&&n.digits.push(0),n.integerLen+=2),n}function mA(n){let e=Math.abs(n)+"",t=0,i,r,s,o,a;for((r=e.indexOf(yd))>-1&&(e=e.replace(yd,"")),(s=e.search(/e/i))>0?(r<0&&(r=s),r+=+e.slice(s+1),e=e.substring(0,s)):r<0&&(r=e.length),s=0;e.charAt(s)===yc;s++);if(s===(a=e.length))i=[0],r=1;else{for(a--;e.charAt(a)===yc;)a--;for(r-=s,i=[],o=0;s<=a;s++,o++)i[o]=Number(e.charAt(s))}return r>qb&&(i=i.splice(0,qb-1),t=r-1,r=1),{digits:i,exponent:t,integerLen:r}}function gA(n,e,t){if(e>t)throw new Me(2307,!1);let i=n.digits,r=i.length-n.integerLen,s=Math.min(Math.max(e,r),t),o=s+n.integerLen,a=i[o];if(o>0){i.splice(Math.max(n.integerLen,o));for(let d=o;d<i.length;d++)i[d]=0}else{r=Math.max(0,r),n.integerLen=1,i.length=Math.max(1,o=s+1),i[0]=0;for(let d=1;d<o;d++)i[d]=0}if(a>=5)if(o-1<0){for(let d=0;d>o;d--)i.unshift(0),n.integerLen++;i.unshift(1),n.integerLen++}else i[o-1]++;for(;r<Math.max(0,s);r++)i.push(0);let c=s!==0,l=e+n.integerLen,u=i.reduceRight(function(d,h,f,g){return h=h+d,g[f]=h<10?h:h-10,c&&(g[f]===0&&f>=l?g.pop():c=!1),h>=10?1:0},0);u&&(i.unshift(u),n.integerLen++)}function jg(n){let e=parseInt(n);if(isNaN(e))throw new Me(2305,!1);return e}function vA(n,e){return new Me(2100,!1)}var $g=(()=>{class n{_locale;constructor(t){this._locale=t}transform(t,i,r){if(!yA(t))return null;r||=this._locale;try{let s=_A(t);return Zb(s,r,i)}catch(s){throw vA(n,s.message)}}static \u0275fac=function(i){return new(i||n)(Fs(pc,16))};static \u0275pipe=Dg({name:"number",type:n,pure:!0})}return n})();function yA(n){return!(n==null||n===""||n!==n)}function _A(n){if(typeof n=="string"&&!isNaN(Number(n)-parseFloat(n)))return Number(n);if(typeof n!="number")throw new Me(2309,!1);return n}function Xg(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var _c=class{};var Kb="browser";var xc=class{_doc;constructor(e){this._doc=e}manager},_d=(()=>{class n extends xc{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(We(Kt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),Sd=new Ie(""),Kg=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof _d));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof _d);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Me(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(We(Sd),We(Ut))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),qg="ng-app-id";function Jb(n){for(let e of n)e.remove()}function Qb(n,e){let t=e.createElement("style");return t.textContent=n,t}function SA(n,e,t,i){let r=n.head?.querySelectorAll(`style[${qg}="${e}"],link[${qg}="${e}"]`);if(r)for(let s of r)s.removeAttribute(qg),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function Zg(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var Jg=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,SA(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,Qb);i?.forEach(r=>this.addUsage(r,this.external,Zg))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(Jb(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Jb(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,Qb(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,Zg(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(We(Kt),We(nd),We(rd,8),We(ac))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),Yg={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Qg=/%COMP%/g;var tS="%COMP%",MA=`_nghost-${tS}`,EA=`_ngcontent-${tS}`,wA=!0,TA=new Ie("",{factory:()=>wA});function CA(n){return EA.replace(Qg,n)}function DA(n){return MA.replace(Qg,n)}function nS(n,e){return e.map(t=>t.replace(Qg,n))}var ev=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new bc(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof bd?r.applyToHost(t):r instanceof Sc&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case yi.Emulated:s=new bd(c,l,i,this.appId,u,o,a,d);break;case yi.ShadowDom:return new xd(c,t,i,o,a,this.nonce,d,l);case yi.ExperimentalIsolatedShadowDom:return new xd(c,t,i,o,a,this.nonce,d);default:s=new Sc(c,l,i,u,o,a,d);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(We(Kg),We(Jg),We(nd),We(TA),We(Kt),We(Ut),We(rd),We(Fo,8))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),bc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(Yg[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(eS(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(eS(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Me(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=Yg[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=Yg[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(jr.DashCase|jr.Important)?e.style.setProperty(t,i,r&jr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&jr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=cr().getGlobalEventTarget(this.doc,e),!e))throw new Me(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function eS(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var xd=class extends bc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=nS(i.id,l);for(let d of l){let h=document.createElement("style");o&&h.setAttribute("nonce",o),h.textContent=d,this.shadowRoot.appendChild(h)}let u=i.getExternalStyles?.();if(u)for(let d of u){let h=Zg(d,r);o&&h.setAttribute("nonce",o),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Sc=class extends bc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?nS(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&No.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},bd=class extends Sc{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=CA(l),this.hostAttr=DA(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Md=class n extends vc{supportsDOMEvents=!0;static makeCurrent(){zg(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=IA();return t==null?null:AA(t)}resetBaseElement(){Mc=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Xg(document.cookie,e)}},Mc=null;function IA(){return Mc=Mc||document.head.querySelector("base"),Mc?Mc.getAttribute("href"):null}function AA(n){return new URL(n,document.baseURI).pathname}var RA=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})(),iS=["alt","control","meta","shift"],NA={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},PA={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},rS=(()=>{class n extends xc{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>cr().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),iS.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=NA[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),iS.forEach(o=>{if(o!==r){let a=PA[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(We(Kt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac})}return n})();function tv(n,e,t){return mt(this,null,function*(){let i=fe({rootComponent:n},OA(e,t));return zb(i)})}function OA(n,e){return{platformRef:e?.platformRef,appProviders:[...BA,...n?.providers??[]],platformProviders:UA}}function LA(){Md.makeCurrent()}function FA(){return new tr}function kA(){return dg(document),document}var UA=[{provide:ac,useValue:Kb},{provide:id,useValue:LA,multi:!0},{provide:Kt,useFactory:kA}];var BA=[{provide:$a,useValue:"root"},{provide:tr,useFactory:FA},{provide:Sd,useClass:_d,multi:!0},{provide:Sd,useClass:rS,multi:!0},ev,Jg,Kg,{provide:Ps,useExisting:ev},{provide:_c,useClass:RA},[]];var sS=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(We(Kt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Le="primary",kc=Symbol("RouteTitle"),ov=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function js(n){return new ov(n)}function nv(n,e,t){for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(r[0]===":")t[r.substring(1)]=s;else if(r!==s.path)return!1}return!0}function fS(n,e,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let c={},l=n.slice(0,i.length);return nv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let s=i.slice(0,r),o=i.slice(r+1);if(s.length+o.length>n.length||t.pathMatch==="full"&&e.hasChildren()&&t.path!=="**")return null;let a={};return!nv(s,n.slice(0,s.length),a)||!nv(o,n.slice(n.length-o.length),a)?null:{consumed:n,posParams:a}}function Id(n){return new Promise((e,t)=>{n.pipe(Qi()).subscribe({next:i=>e(i),error:i=>t(i)})})}function zA(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!Fi(n[t],e[t]))return!1;return!0}function Fi(n,e){let t=n?av(n):void 0,i=e?av(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let s=0;s<t.length;s++)if(r=t[s],!pS(n[r],e[r]))return!1;return!0}function av(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function pS(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,s)=>i[s]===r)}else return n===e}function GA(n){return n.length>0?n[n.length-1]:null}function Xs(n){return iu(n)?n:ko(n)?Gt(Promise.resolve(n)):Qe(n)}function mS(n){return iu(n)?Id(n):Promise.resolve(n)}var jA={exact:yS,subset:_S},gS={exact:WA,subset:$A,ignored:()=>!0},vS={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},cv={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function oS(n,e,t){return jA[t.paths](n.root,e.root,t.matrixParams)&&gS[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function WA(n,e){return Fi(n,e)}function yS(n,e,t){if(!Gs(n.segments,e.segments)||!Td(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!yS(n.children[i],e.children[i],t))return!1;return!0}function $A(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>pS(n[t],e[t]))}function _S(n,e,t){return xS(n,e,e.segments,t)}function xS(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!Gs(r,t)||e.hasChildren()||!Td(r,t,i))}else if(n.segments.length===t.length){if(!Gs(n.segments,t)||!Td(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!_S(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),s=t.slice(n.segments.length);return!Gs(n.segments,r)||!Td(n.segments,r,i)||!n.children[Le]?!1:xS(n.children[Le],e,s,i)}}function Td(n,e,t){return e.every((i,r)=>gS[t](n[r].parameters,i.parameters))}var oi=class{root;queryParams;fragment;_queryParamMap;constructor(e=new ut([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=js(this.queryParams),this._queryParamMap}toString(){return YA.serialize(this)}},ut=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Cd(this)}},Xr=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=js(this.parameters),this._parameterMap}toString(){return SS(this)}};function XA(n,e){return Gs(n,e)&&n.every((t,i)=>Fi(t.parameters,e[i].parameters))}function Gs(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function qA(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===Le&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==Le&&(t=t.concat(e(r,i)))}),t}var Uc=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>new qr,providedIn:"root"})}return n})(),qr=class{parse(e){let t=new uv(e);return new oi(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${Ec(e.root,!0)}`,i=JA(e.queryParams),r=typeof e.fragment=="string"?`#${ZA(e.fragment)}`:"";return`${t}${i}${r}`}},YA=new qr;function Cd(n){return n.segments.map(e=>SS(e)).join("/")}function Ec(n,e){if(!n.hasChildren())return Cd(n);if(e){let t=n.children[Le]?Ec(n.children[Le],!1):"",i=[];return Object.entries(n.children).forEach(([r,s])=>{r!==Le&&i.push(`${r}:${Ec(s,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=qA(n,(i,r)=>r===Le?[Ec(n.children[Le],!1)]:[`${r}:${Ec(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[Le]!=null?`${Cd(n)}/${t[0]}`:`${Cd(n)}/(${t.join("//")})`}}function bS(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Ed(n){return bS(n).replace(/%3B/gi,";")}function ZA(n){return encodeURI(n)}function lv(n){return bS(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Dd(n){return decodeURIComponent(n)}function aS(n){return Dd(n.replace(/\+/g,"%20"))}function SS(n){return`${lv(n.path)}${KA(n.parameters)}`}function KA(n){return Object.entries(n).map(([e,t])=>`;${lv(e)}=${lv(t)}`).join("")}function JA(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${Ed(t)}=${Ed(r)}`).join("&"):`${Ed(t)}=${Ed(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var QA=/^[^\/()?;#]+/;function iv(n){let e=n.match(QA);return e?e[0]:""}var eR=/^[^\/()?;=#]+/;function tR(n){let e=n.match(eR);return e?e[0]:""}var nR=/^[^=?&#]+/;function iR(n){let e=n.match(nR);return e?e[0]:""}var rR=/^[^&#]+/;function sR(n){let e=n.match(rR);return e?e[0]:""}var uv=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ut([],{}):new ut([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new Me(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,e));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,e)),(t.length>0||Object.keys(i).length>0)&&(r[Le]=new ut(t,i)),r}parseSegment(){let e=iv(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Me(4009,!1);return this.capture(e),new Xr(Dd(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=tR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=iv(this.remaining);r&&(i=r,this.capture(i))}e[Dd(t)]=Dd(i)}parseQueryParam(e){let t=iR(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let o=sR(this.remaining);o&&(i=o,this.capture(i))}let r=aS(t),s=aS(i);if(e.hasOwnProperty(r)){let o=e[r];Array.isArray(o)||(o=[o],e[r]=o),o.push(s)}else e[r]=s}parseParens(e,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=iv(this.remaining),s=this.remaining[r.length];if(s!=="/"&&s!==")"&&s!==";")throw new Me(4010,!1);let o;r.indexOf(":")>-1?(o=r.slice(0,r.indexOf(":")),this.capture(o),this.capture(":")):e&&(o=Le);let a=this.parseChildren(t+1);i[o??Le]=Object.keys(a).length===1&&a[Le]?a[Le]:new ut([],a),this.consumeOptional("//")}return i}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Me(4011,!1)}};function MS(n){return n.segments.length>0?new ut([],{[Le]:n}):n}function ES(n){let e={};for(let[i,r]of Object.entries(n.children)){let s=ES(r);if(i===Le&&s.segments.length===0&&s.hasChildren())for(let[o,a]of Object.entries(s.children))e[o]=a;else(s.segments.length>0||s.hasChildren())&&(e[i]=s)}let t=new ut(n.segments,e);return oR(t)}function oR(n){if(n.numberOfChildren===1&&n.children[Le]){let e=n.children[Le];return new ut(n.segments.concat(e.segments),e.children)}return n}function jo(n){return n instanceof oi}function wS(n,e,t=null,i=null,r=new qr){let s=TS(n);return CS(s,e,t,i,r)}function TS(n){let e;function t(s){let o={};for(let c of s.children){let l=t(c);o[c.outlet]=l}let a=new ut(s.url,o);return s===n&&(e=a),a}let i=t(n.root),r=MS(i);return e??r}function CS(n,e,t,i,r){let s=n;for(;s.parent;)s=s.parent;if(e.length===0)return rv(s,s,s,t,i,r);let o=aR(e);if(o.toRoot())return rv(s,s,new ut([],{}),t,i,r);let a=cR(o,s,n),c=a.processChildren?Tc(a.segmentGroup,a.index,o.commands):IS(a.segmentGroup,a.index,o.commands);return rv(s,a.segmentGroup,c,t,i,r)}function Ad(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function Ic(n){return typeof n=="object"&&n!=null&&n.outlets}function cS(n,e,t){n||="\u0275";let i=new oi;return i.queryParams={[n]:e},t.parse(t.serialize(i)).queryParams[n]}function rv(n,e,t,i,r,s){let o={};for(let[l,u]of Object.entries(i??{}))o[l]=Array.isArray(u)?u.map(d=>cS(l,d,s)):cS(l,u,s);let a;n===e?a=t:a=DS(n,e,t);let c=MS(ES(a));return new oi(c,o,r)}function DS(n,e,t){let i={};return Object.entries(n.children).forEach(([r,s])=>{s===e?i[r]=t:i[r]=DS(s,e,t)}),new ut(n.segments,i)}var Rd=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&Ad(i[0]))throw new Me(4003,!1);let r=i.find(Ic);if(r&&r!==GA(i))throw new Me(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function aR(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new Rd(!0,0,n);let e=0,t=!1,i=n.reduce((r,s,o)=>{if(typeof s=="object"&&s!=null){if(s.outlets){let a={};return Object.entries(s.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(s.segmentPath)return[...r,s.segmentPath]}return typeof s!="string"?[...r,s]:o===0?(s.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,s]},[]);return new Rd(t,e,i)}var zo=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function cR(n,e,t){if(n.isAbsolute)return new zo(e,!0,0);if(!t)return new zo(e,!1,NaN);if(t.parent===null)return new zo(t,!0,0);let i=Ad(n.commands[0])?0:1,r=t.segments.length-1+i;return lR(t,r,n.numberOfDoubleDots)}function lR(n,e,t){let i=n,r=e,s=t;for(;s>r;){if(s-=r,i=i.parent,!i)throw new Me(4005,!1);r=i.segments.length}return new zo(i,!1,r-s)}function uR(n){return Ic(n[0])?n[0].outlets:{[Le]:n}}function IS(n,e,t){if(n??=new ut([],{}),n.segments.length===0&&n.hasChildren())return Tc(n,e,t);let i=dR(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let s=new ut(n.segments.slice(0,i.pathIndex),{});return s.children[Le]=new ut(n.segments.slice(i.pathIndex),n.children),Tc(s,0,r)}else return i.match&&r.length===0?new ut(n.segments,{}):i.match&&!n.hasChildren()?dv(n,e,t):i.match?Tc(n,0,r):dv(n,e,t)}function Tc(n,e,t){if(t.length===0)return new ut(n.segments,{});{let i=uR(t),r={};if(Object.keys(i).some(s=>s!==Le)&&n.children[Le]&&n.numberOfChildren===1&&n.children[Le].segments.length===0){let s=Tc(n.children[Le],e,t);return new ut(n.segments,s.children)}return Object.entries(i).forEach(([s,o])=>{typeof o=="string"&&(o=[o]),o!==null&&(r[s]=IS(n.children[s],e,o))}),Object.entries(n.children).forEach(([s,o])=>{i[s]===void 0&&(r[s]=o)}),new ut(n.segments,r)}}function dR(n,e,t){let i=0,r=e,s={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return s;let o=n.segments[r],a=t[i];if(Ic(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!uS(c,l,o))return s;i+=2}else{if(!uS(c,{},o))return s;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function dv(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let s=t[r];if(Ic(s)){let c=hR(s.outlets);return new ut(i,c)}if(r===0&&Ad(t[0])){let c=n.segments[e];i.push(new Xr(c.path,lS(t[0]))),r++;continue}let o=Ic(s)?s.outlets[Le]:`${s}`,a=r<t.length-1?t[r+1]:null;o&&a&&Ad(a)?(i.push(new Xr(o,lS(a))),r+=2):(i.push(new Xr(o,{})),r++)}return new ut(i,{})}function hR(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=dv(new ut([],{}),0,i))}),e}function lS(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function uS(n,e,t){return n==t.path&&Fi(e,t.parameters)}var Cc="imperative",rn=(function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n})(rn||{}),zn=class{id;url;constructor(e,t){this.id=e,this.url=t}},Ws=class extends zn{type=rn.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ur=class extends zn{urlAfterRedirects;type=rn.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},_n=(function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n[n.Aborted=4]="Aborted",n})(_n||{}),Ac=(function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n})(Ac||{}),si=class extends zn{reason;code;type=rn.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function AS(n){return n instanceof si&&(n.code===_n.Redirect||n.code===_n.SupersededByNewNavigation)}var dr=class extends zn{reason;code;type=rn.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},$s=class extends zn{error;target;type=rn.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Rc=class extends zn{urlAfterRedirects;state;type=rn.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Nd=class extends zn{urlAfterRedirects;state;type=rn.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Pd=class extends zn{urlAfterRedirects;state;shouldActivate;type=rn.GuardsCheckEnd;constructor(e,t,i,r,s){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=s}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Od=class extends zn{urlAfterRedirects;state;type=rn.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Ld=class extends zn{urlAfterRedirects;state;type=rn.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Fd=class{route;type=rn.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},kd=class{route;type=rn.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Ud=class{snapshot;type=rn.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Bd=class{snapshot;type=rn.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Vd=class{snapshot;type=rn.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Hd=class{snapshot;type=rn.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Wo=class{},Nc=class{},$o=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function fR(n){return!(n instanceof Wo)&&!(n instanceof $o)&&!(n instanceof Nc)}var zd=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new Zo(this.rootInjector)}},Zo=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new zd(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(We(Zt))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Gd=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=hv(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=hv(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=fv(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return fv(e,this._root).map(t=>t.value)}};function hv(n,e){if(n===e.value)return e;for(let t of e.children){let i=hv(n,t);if(i)return i}return null}function fv(n,e){if(n===e.value)return[e];for(let t of e.children){let i=fv(n,t);if(i.length)return i.unshift(e),i}return[]}var Hn=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function Ho(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var Pc=class extends Gd{snapshot;constructor(e,t){super(e),this.snapshot=t,Sv(this,e)}toString(){return this.snapshot.toString()}};function RS(n,e){let t=pR(n,e),i=new cn([new Xr("",{})]),r=new cn({}),s=new cn({}),o=new cn({}),a=new cn(""),c=new Yr(i,r,o,a,s,Le,n,t.root);return c.snapshot=t.root,new Pc(new Hn(c,[]),t)}function pR(n,e){let t={},i={},r={},o=new Xo([],t,r,"",i,Le,n,null,{},e);return new Oc("",new Hn(o,[]))}var Yr=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,s,o,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=s,this.outlet=o,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(kt(l=>l[kc]))??Qe(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(kt(e=>js(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(kt(e=>js(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function bv(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:fe(fe({},e.params),n.params),data:fe(fe({},e.data),n.data),resolve:fe(fe(fe(fe({},n.data),e.data),r?.data),n._resolvedData)}:i={params:fe({},n.params),data:fe({},n.data),resolve:fe(fe({},n.data),n._resolvedData??{})},r&&PS(r)&&(i.resolve[kc]=r.title),i}var Xo=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[kc]}constructor(e,t,i,r,s,o,a,c,l,u){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=s,this.outlet=o,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=js(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=js(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},Oc=class extends Gd{url;constructor(e,t){super(t),this.url=e,Sv(this,t)}toString(){return NS(this._root)}};function Sv(n,e){e.value._routerState=n,e.children.forEach(t=>Sv(n,t))}function NS(n){let e=n.children.length>0?` { ${n.children.map(NS).join(", ")} } `:"";return`${n.value}${e}`}function sv(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,Fi(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),Fi(e.params,t.params)||n.paramsSubject.next(t.params),zA(e.url,t.url)||n.urlSubject.next(t.url),Fi(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function pv(n,e){let t=Fi(n.params,e.params)&&XA(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||pv(n.parent,e.parent))}function PS(n){return typeof n.title=="string"||n.title===null}var OS=new Ie(""),Bc=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Le;activateEvents=new Tn;deactivateEvents=new Tn;attachEvents=new Tn;detachEvents=new Tn;routerOutletData=Bb();parentContexts=J(Zo);location=J(ks);changeDetector=J(gc);inputBinder=J(Xd,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Me(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Me(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Me(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Me(4013,!1);this._activatedRoute=t;let r=this.location,o=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new mv(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(o,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=ud({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[ed]})}return n})(),mv=class{route;childContexts;parent;outletData;constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===Yr?this.route:e===Zo?this.childContexts:e===OS?this.outletData:this.parent.get(e,t)}},Xd=new Ie("");var Mv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=Us({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&Uo(0,"router-outlet")},dependencies:[Bc],encapsulation:2})}return n})();function Ev(n){let e=n.children&&n.children.map(Ev),t=e?ot(fe({},n),{children:e}):fe({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==Le&&(t.component=Mv),t}function mR(n,e,t){let i=Lc(n,e._root,t?t._root:void 0);return new Pc(i,e)}function Lc(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=gR(n,e,t);return new Hn(i,r)}else{if(n.shouldAttach(e.value)){let s=n.retrieve(e.value);if(s!==null){let o=s.route;return o.value._futureSnapshot=e.value,o.children=e.children.map(a=>Lc(n,a)),o}}let i=vR(e.value),r=e.children.map(s=>Lc(n,s));return new Hn(i,r)}}function gR(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return Lc(n,i,r);return Lc(n,i)})}function vR(n){return new Yr(new cn(n.url),new cn(n.params),new cn(n.queryParams),new cn(n.fragment),new cn(n.data),n.outlet,n.component,n)}var qo=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},LS="ngNavigationCancelingError";function jd(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=jo(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=FS(!1,_n.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function FS(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[LS]=!0,t.cancellationCode=e,t}function yR(n){return kS(n)&&jo(n.url)}function kS(n){return!!n&&n[LS]}var gv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,s){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=s}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),sv(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=Ho(t);e.children.forEach(s=>{let o=s.value.outlet;this.deactivateRoutes(s,r[o],i),delete r[o]}),Object.values(r).forEach(s=>{this.deactivateRouteAndItsChildren(s,i)})}deactivateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(r===s)if(r.component){let o=i.getContext(r.outlet);o&&this.deactivateChildRoutes(e,t,o.children)}else this.deactivateChildRoutes(e,t,i);else s&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=Ho(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);if(i&&i.outlet){let o=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:o,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,s=Ho(e);for(let o of Object.values(s))this.deactivateRouteAndItsChildren(o,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=Ho(t);e.children.forEach(s=>{this.activateRoutes(s,r[s.value.outlet],i),this.forwardEvent(new Hd(s.value.snapshot))}),e.children.length&&this.forwardEvent(new Bd(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,s=t?t.value:null;if(sv(r),r===s)if(r.component){let o=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,o.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let o=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),o.children.onOutletReAttached(a.contexts),o.attachRef=a.componentRef,o.route=a.route.value,o.outlet&&o.outlet.attach(a.componentRef,a.route.value),sv(a.route.value),this.activateChildRoutes(e,null,o.children)}else o.attachRef=null,o.route=r,o.outlet&&o.outlet.activateWith(r,o.injector),this.activateChildRoutes(e,null,o.children)}else this.activateChildRoutes(e,null,i)}},Wd=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},Go=class{component;route;constructor(e,t){this.component=e,this.route=t}};function _R(n,e,t){let i=n._root,r=e?e._root:null;return wc(i,r,t,[i.value])}function xR(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function Ko(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!jp(n)?n:e.get(n):i}function wc(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=Ho(e);return n.children.forEach(o=>{bR(o,s[o.value.outlet],t,i.concat([o.value]),r),delete s[o.value.outlet]}),Object.entries(s).forEach(([o,a])=>Dc(a,t.getContext(o),r)),r}function bR(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let s=n.value,o=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(o&&s.routeConfig===o.routeConfig){let c=SR(o,s,s.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Wd(i)):(s.data=o.data,s._resolvedData=o._resolvedData),s.component?wc(n,e,a?a.children:null,i,r):wc(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new Go(a.outlet.component,o))}else o&&Dc(e,a,r),r.canActivateChecks.push(new Wd(i)),s.component?wc(n,null,a?a.children:null,i,r):wc(n,null,t,i,r);return r}function SR(n,e,t){if(typeof t=="function")return gn(e._environmentInjector,()=>t(n,e));switch(t){case"pathParamsChange":return!Gs(n.url,e.url);case"pathParamsOrQueryParamsChange":return!Gs(n.url,e.url)||!Fi(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!pv(n,e)||!Fi(n.queryParams,e.queryParams);default:return!pv(n,e)}}function Dc(n,e,t){let i=Ho(n),r=n.value;Object.entries(i).forEach(([s,o])=>{r.component?e?Dc(o,e.children.getContext(s),t):Dc(o,null,t):Dc(o,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new Go(e.outlet.component,r)):t.canDeactivateChecks.push(new Go(null,r)):t.canDeactivateChecks.push(new Go(null,r))}function Vc(n){return typeof n=="function"}function MR(n){return typeof n=="boolean"}function ER(n){return n&&Vc(n.canLoad)}function wR(n){return n&&Vc(n.canActivate)}function TR(n){return n&&Vc(n.canActivateChild)}function CR(n){return n&&Vc(n.canDeactivate)}function DR(n){return n&&Vc(n.canMatch)}function US(n){return n instanceof gs||n?.name==="EmptyError"}var wd=Symbol("INITIAL_VALUE");function Yo(){return fi(n=>Ep(n.map(e=>e.pipe(Ji(1),Tp(wd)))).pipe(kt(e=>{for(let t of e)if(t!==!0){if(t===wd)return wd;if(t===!1||IR(t))return t}return!0}),Ki(e=>e!==wd),Ji(1)))}function IR(n){return jo(n)||n instanceof qo}function BS(n){return n.aborted?Qe(void 0).pipe(Ji(1)):new ct(e=>{let t=()=>{e.next(),e.complete()};return n.addEventListener("abort",t),()=>n.removeEventListener("abort",t)})}function VS(n){return Ua(BS(n))}function AR(n){return En(e=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:s}}=e;return s.length===0&&r.length===0?Qe(ot(fe({},e),{guardsResult:!0})):RR(s,t,i).pipe(En(o=>o&&MR(o)?NR(t,r,n):Qe(o)),kt(o=>ot(fe({},e),{guardsResult:o})))})}function RR(n,e,t){return Gt(n).pipe(En(i=>kR(i.component,i.route,t,e)),Qi(i=>i!==!0,!0))}function NR(n,e,t){return Gt(e).pipe(ru(i=>vo(OR(i.route.parent,t),PR(i.route,t),FR(n,i.path),LR(n,i.route))),Qi(i=>i!==!0,!0))}function PR(n,e){return n!==null&&e&&e(new Vd(n)),Qe(!0)}function OR(n,e){return n!==null&&e&&e(new Ud(n)),Qe(!0)}function LR(n,e){let t=e.routeConfig?e.routeConfig.canActivate:null;if(!t||t.length===0)return Qe(!0);let i=t.map(r=>Fa(()=>{let s=e._environmentInjector,o=Ko(r,s),a=wR(o)?o.canActivate(e,n):gn(s,()=>o(e,n));return Xs(a).pipe(Qi())}));return Qe(i).pipe(Yo())}function FR(n,e){let t=e[e.length-1],r=e.slice(0,e.length-1).reverse().map(s=>xR(s)).filter(s=>s!==null).map(s=>Fa(()=>{let o=s.guards.map(a=>{let c=s.node._environmentInjector,l=Ko(a,c),u=TR(l)?l.canActivateChild(t,n):gn(c,()=>l(t,n));return Xs(u).pipe(Qi())});return Qe(o).pipe(Yo())}));return Qe(r).pipe(Yo())}function kR(n,e,t,i){let r=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!r||r.length===0)return Qe(!0);let s=r.map(o=>{let a=e._environmentInjector,c=Ko(o,a),l=CR(c)?c.canDeactivate(n,e,t,i):gn(a,()=>c(n,e,t,i));return Xs(l).pipe(Qi())});return Qe(s).pipe(Yo())}function UR(n,e,t,i,r){let s=e.canLoad;if(s===void 0||s.length===0)return Qe(!0);let o=s.map(a=>{let c=Ko(a,n),l=ER(c)?c.canLoad(e,t):gn(n,()=>c(e,t)),u=Xs(l);return r?u.pipe(VS(r)):u});return Qe(o).pipe(Yo(),HS(i))}function HS(n){return xp(Kn(e=>{if(typeof e!="boolean")throw jd(n,e)}),kt(e=>e===!0))}function BR(n,e,t,i,r,s){let o=e.canMatch;if(!o||o.length===0)return Qe(!0);let a=o.map(c=>{let l=Ko(c,n),u=DR(l)?l.canMatch(e,t,r):gn(n,()=>l(e,t,r));return Xs(u).pipe(VS(s))});return Qe(a).pipe(Yo(),HS(i))}var lr=class n extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,n.prototype)}},Fc=class n extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,n.prototype)}};function VR(n){throw new Me(4e3,!1)}function HR(n){throw FS(!1,_n.GuardRejected)}var vv=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}lineralizeSegments(e,t){return mt(this,null,function*(){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Le])throw VR(`${e.redirectTo}`);r=r.children[Le]}})}applyRedirectCommands(e,t,i,r,s){return mt(this,null,function*(){let o=yield zR(t,r,s);if(o instanceof oi)throw new Fc(o);let a=this.applyRedirectCreateUrlTree(o,this.urlSerializer.parse(o),e,i);if(o[0]==="/")throw new Fc(a);return a})}applyRedirectCreateUrlTree(e,t,i,r){let s=this.createSegmentGroup(e,t.root,i,r);return new oi(s,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,s])=>{if(typeof s=="string"&&s[0]===":"){let a=s.substring(1);i[r]=t[a]}else i[r]=s}),i}createSegmentGroup(e,t,i,r){let s=this.createSegments(e,t.segments,i,r),o={};return Object.entries(t.children).forEach(([a,c])=>{o[a]=this.createSegmentGroup(e,c,i,r)}),new ut(s,o)}createSegments(e,t,i,r){return t.map(s=>s.path[0]===":"?this.findPosParam(e,s,r):this.findOrReturn(s,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Me(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}};function zR(n,e,t){if(typeof n=="string")return Promise.resolve(n);let i=n;return Id(Xs(gn(t,()=>i(e))))}function GR(n,e){return n.providers&&!n._injector&&(n._injector=lc(n.providers,e,`Route: ${n.path}`)),n._injector??e}function xi(n){return n.outlet||Le}function jR(n,e){let t=n.filter(i=>xi(i)===e);return t.push(...n.filter(i=>xi(i)!==e)),t}var yv={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function zS(n){return{routeConfig:n.routeConfig,url:n.url,params:n.params,queryParams:n.queryParams,fragment:n.fragment,data:n.data,outlet:n.outlet,title:n.title,paramMap:n.paramMap,queryParamMap:n.queryParamMap}}function WR(n,e,t,i,r,s,o){let a=GS(n,e,t);if(!a.matched)return Qe(a);let c=zS(s(a));return i=GR(e,i),BR(i,e,t,r,c,o).pipe(kt(l=>l===!0?a:fe({},yv)))}function GS(n,e,t){if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?fe({},yv):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||fS)(t,n,e);if(!r)return fe({},yv);let s={};Object.entries(r.posParams??{}).forEach(([a,c])=>{s[a]=c.path});let o=r.consumed.length>0?fe(fe({},s),r.consumed[r.consumed.length-1].parameters):s;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:o,positionalParamSegments:r.posParams??{}}}function dS(n,e,t,i,r){return t.length>0&&qR(n,t,i,r)?{segmentGroup:new ut(e,XR(i,new ut(t,n.children))),slicedSegments:[]}:t.length===0&&YR(n,t,i)?{segmentGroup:new ut(n.segments,$R(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new ut(n.segments,n.children),slicedSegments:t}}function $R(n,e,t,i){let r={};for(let s of t)if(qd(n,e,s)&&!i[xi(s)]){let o=new ut([],{});r[xi(s)]=o}return fe(fe({},i),r)}function XR(n,e){let t={};t[Le]=e;for(let i of n)if(i.path===""&&xi(i)!==Le){let r=new ut([],{});t[xi(i)]=r}return t}function qR(n,e,t,i){return t.some(r=>!qd(n,e,r)||!(xi(r)!==Le)?!1:!(i!==void 0&&xi(r)===i))}function YR(n,e,t){return t.some(i=>qd(n,e,i))}function qd(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function ZR(n,e,t){return e.length===0&&!n.children[t]}var _v=class{};function KR(n,e,t,i,r,s,o="emptyOnly",a){return mt(this,null,function*(){return new xv(n,e,t,i,r,o,s,a).recognize()})}var JR=31,xv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,t,i,r,s,o,a,c){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=s,this.paramsInheritanceStrategy=o,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new vv(this.urlSerializer,this.urlTree)}noMatchError(e){return new Me(4002,`'${e.segmentGroup}'`)}recognize(){return mt(this,null,function*(){let e=dS(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=yield this.match(e),r=new Hn(i,t),s=new Oc("",r),o=wS(i,[],this.urlTree.queryParams,this.urlTree.fragment);return o.queryParams=this.urlTree.queryParams,s.url=this.urlSerializer.serialize(o),{state:s,tree:o}})}match(e){return mt(this,null,function*(){let t=new Xo([],Object.freeze({}),Object.freeze(fe({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Le,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,e,Le,t),rootSnapshot:t}}catch(i){if(i instanceof Fc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof lr?this.noMatchError(i):i}})}processSegmentGroup(e,t,i,r,s){return mt(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(e,t,i,s);let o=yield this.processSegment(e,t,i,i.segments,r,!0,s);return o instanceof Hn?[o]:[]})}processChildren(e,t,i,r){return mt(this,null,function*(){let s=[];for(let c of Object.keys(i.children))c==="primary"?s.unshift(c):s.push(c);let o=[];for(let c of s){let l=i.children[c],u=jR(t,c),d=yield this.processSegmentGroup(e,u,l,c,r);o.push(...d)}let a=jS(o);return QR(a),a})}processSegment(e,t,i,r,s,o,a){return mt(this,null,function*(){for(let c of t)try{return yield this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,s,o,a)}catch(l){if(l instanceof lr||US(l))continue;throw l}if(ZR(i,r,s))return new _v;throw new lr(i)})}processSegmentAgainstRoute(e,t,i,r,s,o,a,c){return mt(this,null,function*(){if(xi(i)!==o&&(o===Le||!qd(r,s,i)))throw new lr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,r,i,s,o,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,s,o,c);throw new lr(r)})}expandSegmentAgainstRouteUsingRedirect(e,t,i,r,s,o,a){return mt(this,null,function*(){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:h}=GS(t,r,s);if(!c)throw new lr(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>JR&&(this.allowRedirects=!1));let f=this.createSnapshot(e,r,s,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let g=yield this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,zS(f),e),y=yield this.applyRedirects.lineralizeSegments(r,g);return this.processSegment(e,i,t,y.concat(h),o,!1,a)})}createSnapshot(e,t,i,r,s){let o=new Xo(i,r,Object.freeze(fe({},this.urlTree.queryParams)),this.urlTree.fragment,t1(t),xi(t),t.component??t._loadedComponent??null,t,n1(t),e),a=bv(o,s,this.paramsInheritanceStrategy);return o.params=Object.freeze(a.params),o.data=Object.freeze(a.data),o}matchSegmentAgainstRoute(e,t,i,r,s,o){return mt(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=w=>this.createSnapshot(e,i,w.consumedSegments,w.parameters,o),c=yield Id(WR(t,i,r,e,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new lr(t);e=i._injector??e;let{routes:l}=yield this.getChildConfig(e,i,r),u=i._loadedInjector??e,{parameters:d,consumedSegments:h,remainingSegments:f}=c,g=this.createSnapshot(e,i,h,d,o),{segmentGroup:y,slicedSegments:m}=dS(t,h,f,l,s);if(m.length===0&&y.hasChildren()){let w=yield this.processChildren(u,l,y,g);return new Hn(g,w)}if(l.length===0&&m.length===0)return new Hn(g,[]);let p=xi(i)===s,b=yield this.processSegment(u,l,y,m,p?Le:s,!0,g);return new Hn(g,b instanceof Hn?[b]:[])})}getChildConfig(e,t,i){return mt(this,null,function*(){if(t.children)return{routes:t.children,injector:e};if(t.loadChildren){if(t._loadedRoutes!==void 0){let s=t._loadedNgModuleFactory;return s&&!t._loadedInjector&&(t._loadedInjector=s.create(e).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield Id(UR(e,t,i,this.urlSerializer,this.abortSignal))){let s=yield this.configLoader.loadChildren(e,t);return t._loadedRoutes=s.routes,t._loadedInjector=s.injector,t._loadedNgModuleFactory=s.factory,s}throw HR(t)}return{routes:[],injector:e}})}};function QR(n){n.sort((e,t)=>e.value.outlet===Le?-1:t.value.outlet===Le?1:e.value.outlet.localeCompare(t.value.outlet))}function e1(n){let e=n.value.routeConfig;return e&&e.path===""}function jS(n){let e=[],t=new Set;for(let i of n){if(!e1(i)){e.push(i);continue}let r=e.find(s=>i.value.routeConfig===s.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):e.push(i)}for(let i of t){let r=jS(i.children);e.push(new Hn(i.value,r))}return e.filter(i=>!t.has(i))}function t1(n){return n.data||{}}function n1(n){return n.resolve||{}}function i1(n,e,t,i,r,s,o){return En(a=>mt(null,null,function*(){let{state:c,tree:l}=yield KR(n,e,t,i,a.extractedUrl,r,s,o);return ot(fe({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function r1(n){return En(e=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=e;if(!i.length)return Qe(e);let r=new Set(i.map(a=>a.route)),s=new Set;for(let a of r)if(!s.has(a))for(let c of WS(a))s.add(c);let o=0;return Gt(s).pipe(ru(a=>r.has(a)?s1(a,t,n):(a.data=bv(a,a.parent,n).resolve,Qe(void 0))),Kn(()=>o++),su(1),En(a=>o===s.size?Qe(e):ln))})}function WS(n){let e=n.children.map(t=>WS(t)).flat();return[n,...e]}function s1(n,e,t){let i=n.routeConfig,r=n._resolve;return i?.title!==void 0&&!PS(i)&&(r[kc]=i.title),Fa(()=>(n.data=bv(n,n.parent,t).resolve,o1(r,n,e).pipe(kt(s=>(n._resolvedData=s,n.data=fe(fe({},n.data),s),null)))))}function o1(n,e,t){let i=av(n);if(i.length===0)return Qe({});let r={};return Gt(i).pipe(En(s=>a1(n[s],e,t).pipe(Qi(),Kn(o=>{if(o instanceof qo)throw jd(new qr,o);r[s]=o}))),su(1),kt(()=>r),ka(s=>US(s)?ln:Mp(s)))}function a1(n,e,t){let i=e._environmentInjector,r=Ko(n,i),s=r.resolve?r.resolve(e,t):gn(i,()=>r(e,t));return Xs(s)}function hS(n){return fi(e=>{let t=n(e);return t?Gt(t).pipe(kt(()=>e)):Qe(e)})}var wv=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(s=>s.outlet===Le);return i}getResolvedTitleForRoute(t){return t.data[kc]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J($S),providedIn:"root"})}return n})(),$S=(()=>{class n extends wv{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(We(sS))};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Hc=new Ie("",{factory:()=>({})}),zc=new Ie(""),XS=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=J(Bg);loadComponent(t,i){return mt(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=mt(this,null,function*(){try{let s=yield mS(gn(t,()=>i.loadComponent())),o=yield ZS(YS(s));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=o,o}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=mt(this,null,function*(){try{let s=yield qS(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=s.routes,i._loadedInjector=s.injector,i._loadedNgModuleFactory=s.factory,s}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function qS(n,e,t,i){return mt(this,null,function*(){let r=yield mS(gn(t,()=>n.loadChildren())),s=yield ZS(YS(r)),o;s instanceof ld||Array.isArray(s)?o=s:o=yield e.compileModuleAsync(s),i&&i(n);let a,c,l=!1,u;return Array.isArray(o)?(c=o,l=!0):(a=o.create(t).injector,u=o,c=a.get(zc,[],{optional:!0,self:!0}).flat()),{routes:c.map(Ev),injector:a,factory:u}})}function c1(n){return n&&typeof n=="object"&&"default"in n}function YS(n){return c1(n)?n.default:n}function ZS(n){return mt(this,null,function*(){return n})}var Yd=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(l1),providedIn:"root"})}return n})(),l1=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),KS=new Ie("");var u1=()=>{},JS=new Ie(""),QS=(()=>{class n{currentNavigation=Nn(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Nn(null);events=new an;transitionAbortWithErrorSubject=new an;configLoader=J(XS);environmentInjector=J(Zt);destroyRef=J(Vr);urlSerializer=J(Uc);rootContexts=J(Zo);location=J(Bo);inputBindingEnabled=J(Xd,{optional:!0})!==null;titleStrategy=J(wv);options=J(Hc,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=J(Yd);createViewTransition=J(KS,{optional:!0});navigationErrorHandler=J(JS,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Qe(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Fd(r)),i=r=>this.events.next(new kd(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;ar(()=>{this.transitions?.next(ot(fe({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new cn(null),this.transitions.pipe(Ki(i=>i!==null),fi(i=>{let r=!1,s=new AbortController,o=()=>!r&&this.currentTransition?.id===i.id;return Qe(i).pipe(fi(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",_n.SupersededByNewNavigation),ln;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?ot(fe({},c),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new dr(a.id,this.urlSerializer.serialize(a.rawUrl),"",Ac.IgnoredSameUrlNavigation)),a.resolve(!1),ln;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return Qe(a).pipe(fi(d=>(this.events.next(new Ws(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?ln:Promise.resolve(d))),i1(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),Kn(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=d.urlAfterRedirects,h)),this.events.next(new Nc)}),fi(d=>Gt(i.routesRecognizeHandler.deferredHandle??Qe(void 0)).pipe(kt(()=>d))),Kn(()=>{let d=new Rc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:h,source:f,restoredState:g,extras:y}=a,m=new Ws(d,this.urlSerializer.serialize(h),f,g);this.events.next(m);let p=RS(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=ot(fe({},a),{targetSnapshot:p,urlAfterRedirects:h,extras:ot(fe({},y),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(b=>(b.finalUrl=h,b)),Qe(i)}else return this.events.next(new dr(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Ac.IgnoredByUrlHandlingStrategy)),a.resolve(!1),ln}),kt(a=>{let c=new Nd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=ot(fe({},a),{guards:_R(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),AR(a=>this.events.next(a)),fi(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw jd(this.urlSerializer,a.guardsResult);let c=new Pd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!o())return ln;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",_n.GuardRejected),ln;if(a.guards.canActivateChecks.length===0)return Qe(a);let l=new Od(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!o())return ln;let u=!1;return Qe(a).pipe(r1(this.paramsInheritanceStrategy),Kn({next:()=>{u=!0;let d=new Ld(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",_n.NoDataFromResolver)}}))}),hS(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let h=u._environmentInjector;d.push(this.configLoader.loadComponent(h,u.routeConfig).then(f=>{u.component=f}))}for(let h of u.children)d.push(...c(h));return d},l=c(a.targetSnapshot.root);return l.length===0?Qe(a):Gt(Promise.all(l).then(()=>a))}),hS(()=>this.afterPreactivation()),fi(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?Gt(l).pipe(kt(()=>i)):Qe(i)}),Ji(1),fi(a=>{let c=mR(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=ot(fe({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new Wo);let l=i.beforeActivateHandler.deferredHandle;return l?Gt(l.then(()=>a)):Qe(a)}),Kn(a=>{new gv(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),o()&&(r=!0,this.currentNavigation.update(c=>(c.abort=u1,c)),this.lastSuccessfulNavigation.set(ar(this.currentNavigation)),this.events.next(new ur(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Ua(BS(s.signal).pipe(Ki(()=>!r&&!i.targetRouterState),Kn(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",_n.Aborted)}))),Kn({complete:()=>{r=!0}}),Ua(this.transitionAbortWithErrorSubject.pipe(Kn(a=>{throw a}))),wp(()=>{s.abort(),r||this.cancelNavigationTransition(i,"",_n.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),ka(a=>{if(r=!0,this.destroyed)return i.resolve(!1),ln;if(kS(a))this.events.next(new si(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),yR(a)?this.events.next(new $o(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new $s(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=gn(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof qo){let{message:u,cancellationCode:d}=jd(this.urlSerializer,l);this.events.next(new si(i.id,this.urlSerializer.serialize(i.extractedUrl),u,d)),this.events.next(new $o(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return ln}))}))}cancelNavigationTransition(t,i,r){let s=new si(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(s),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=ar(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function d1(n){return n!==Cc}var eM=new Ie("");var tM=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(h1),providedIn:"root"})}return n})(),$d=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}shouldDestroyInjector(e){return!0}},h1=(()=>{class n extends $d{static \u0275fac=(()=>{let t;return function(r){return(t||(t=oc(n)))(r||n)}})();static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Tv=(()=>{class n{urlSerializer=J(Uc);options=J(Hc,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=J(Bo);urlHandlingStrategy=J(Yd);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new oi;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let s=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,o=r??s;return o instanceof oi?this.urlSerializer.serialize(o):o}routerUrlState(t){return t?.targetBrowserUrl===void 0||t?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(t.finalUrl)}}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=RS(null,J(Zt));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:()=>J(f1),providedIn:"root"})}return n})(),f1=(()=>{class n extends Tv{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof Ws?this.updateStateMemento():t instanceof dr?this.commitTransition(i):t instanceof Rc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Wo?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof si&&!AS(t)?this.restoreHistory(i):t instanceof $s?this.restoreHistory(i,!0):t instanceof ur&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,i){let{extras:r,id:s}=i,{replaceUrl:o,state:a}=r;if(this.location.isCurrentPathEqualTo(t)||o){let c=this.browserPageId,l=fe(fe({},a),this.generateNgRouterState(s,c,i));this.location.replaceState(t,"",l)}else{let c=fe(fe({},a),this.generateNgRouterState(s,this.browserPageId+1,i));this.location.go(t,"",c)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,s=this.currentPageId-r;s!==0?this.location.historyGo(s):this.getCurrentUrlTree()===t.finalUrl&&s===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i,r){return this.canceledNavigationResolution==="computed"?fe({navigationId:t,\u0275routerPageId:i},this.routerUrlState(r)):fe({navigationId:t},this.routerUrlState(r))}static \u0275fac=(()=>{let t;return function(r){return(t||(t=oc(n)))(r||n)}})();static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Cv(n,e){n.events.pipe(Ki(t=>t instanceof ur||t instanceof si||t instanceof $s||t instanceof dr),kt(t=>t instanceof ur||t instanceof dr?0:(t instanceof si?t.code===_n.Redirect||t.code===_n.SupersededByNewNavigation:!1)?2:1),Ki(t=>t!==2),Ji(1)).subscribe(()=>{e()})}var Zd=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=J(Ig);stateManager=J(Tv);options=J(Hc,{optional:!0})||{};pendingTasks=J(rr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=J(QS);urlSerializer=J(Uc);location=J(Bo);urlHandlingStrategy=J(Yd);injector=J(Zt);_events=new an;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=J(tM);injectorCleanup=J(eM,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=J(zc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!J(Xd,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new Yt;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,s=ar(this.navigationTransitions.currentNavigation);if(r!==null&&s!==null){if(this.stateManager.handleRouterEvent(i,s),i instanceof si&&i.code!==_n.Redirect&&i.code!==_n.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ur)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof $o){let o=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=fe({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||d1(r.source)},o);this.scheduleNavigation(a,Cc,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}fR(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Cc,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,s)=>{this.navigateToSyncWithBrowser(t,r,i,s)})}navigateToSyncWithBrowser(t,i,r,s){let o=r?.navigationId?r:null,a=r?.\u0275routerUrl??t;if(r?.\u0275routerUrl&&(s=ot(fe({},s),{browserUrl:t})),r){let l=fe({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(s.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,o,s).catch(l=>{this.disposed||this.injector.get(ri)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return ar(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(Ev),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:s,fragment:o,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:o,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=fe(fe({},this.currentUrlTree.queryParams),s);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=s||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let h=r?r.snapshot:this.routerState.snapshot.root;d=TS(h)}catch(h){(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return CS(d,t,u,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=jo(t)?t:this.parseUrl(t),s=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(s,Cc,null,i)}navigate(t,i={skipLocationChange:!1}){return p1(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch(i){return this.console.warn(xo(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=fe({},vS):i===!1?r=fe({},cv):r=fe(fe({},cv),i),jo(t))return oS(this.currentUrlTree,t,r);let s=this.parseUrl(t);return oS(this.currentUrlTree,s,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,s])=>(s!=null&&(i[r]=s),i),{})}scheduleNavigation(t,i,r,s,o){if(this.disposed)return Promise.resolve(!1);let a,c,l;o?(a=o.resolve,c=o.reject,l=o.promise):l=new Promise((d,h)=>{a=d,c=h});let u=this.pendingTasks.add();return Cv(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:s,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function p1(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Me(4008,!1)}var v1=new Ie("");function Dv(n,...e){return Ts([{provide:zc,multi:!0,useValue:n},[],{provide:Yr,useFactory:y1},{provide:dd,multi:!0,useFactory:_1},e.map(t=>t.\u0275providers)])}function y1(){return J(Zd).routerState.root}function _1(){let n=J(pi);return e=>{let t=n.get(Bs);if(e!==t.components[0])return;let i=n.get(Zd),r=n.get(x1);n.get(b1)===1&&i.initialNavigation(),n.get(S1,null,{optional:!0})?.setUpPreloading(),n.get(v1,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var x1=new Ie("",{factory:()=>new an}),b1=new Ie("",{factory:()=>1});var S1=new Ie("");var DM=0,uy=1,IM=2;var pl=1,sf=2,Ma=3,_r=0,An=1,Wi=2,$i=0,Qs=1,dy=2,hy=3,fy=4,AM=5;var ns=100,RM=101,NM=102,PM=103,OM=104,LM=200,FM=201,kM=202,UM=203,Mh=204,Eh=205,BM=206,VM=207,HM=208,zM=209,GM=210,jM=211,WM=212,$M=213,XM=214,wh=0,Th=1,Ch=2,eo=3,Dh=4,Ih=5,Ah=6,Rh=7,py=0,qM=1,YM=2,wi=0,my=1,gy=2,vy=3,ml=4,yy=5,_y=6,xy=7;var Qv=300,as=301,ro=302,of=303,af=304,gl=306,Nh=1e3,Bi=1001,Ph=1002,on=1003,ZM=1004;var vl=1005;var fn=1006,cf=1007;var cs=1008;var Fn=1009,by=1010,Sy=1011,Ea=1012,lf=1013,Ti=1014,Ci=1015,Xi=1016,uf=1017,df=1018,wa=1020,My=35902,Ey=35899,wy=1021,Ty=1022,ui=1023,Vi=1026,ls=1027,Cy=1028,hf=1029,us=1030,ff=1031;var pf=1033,yl=33776,_l=33777,xl=33778,bl=33779,mf=35840,gf=35841,vf=35842,yf=35843,_f=36196,xf=37492,bf=37496,Sf=37488,Mf=37489,Sl=37490,Ef=37491,wf=37808,Tf=37809,Cf=37810,Df=37811,If=37812,Af=37813,Rf=37814,Nf=37815,Pf=37816,Of=37817,Lf=37818,Ff=37819,kf=37820,Uf=37821,Bf=36492,Vf=36494,Hf=36495,zf=36283,Gf=36284,Ml=36285,jf=36286;var Yc=2300,Oh=2301,Sh=2302,ey=2303,ty=2400,ny=2401,iy=2402;var KM=3200;var Wf=0,JM=1,Sr="",Dn="srgb",Zc="srgb-linear",Kc="linear",pt="srgb";var Js=7680;var ry=519,QM=512,eE=513,tE=514,$f=515,nE=516,iE=517,Xf=518,rE=519,sy=35044;var Dy="300 es",Ei=2e3,fa=2001;function E1(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function w1(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Jc(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function sE(){let n=Jc("canvas");return n.style.display="block",n}var nM={},pa=null;function Iy(...n){let e="THREE."+n.shift();pa?pa("log",e,...n):console.log(e,...n)}function oE(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function De(...n){n=oE(n);let e="THREE."+n.shift();if(pa)pa("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function Re(...n){n=oE(n);let e="THREE."+n.shift();if(pa)pa("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Lh(...n){let e=n.join(" ");e in nM||(nM[e]=!0,De(...n))}function aE(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var cE={[wh]:Th,[Ch]:Ah,[Dh]:Rh,[eo]:Ih,[Th]:wh,[Ah]:Ch,[Rh]:Dh,[Ih]:eo},Hi=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},xn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],iM=1234567,da=Math.PI/180,ma=180/Math.PI;function Ta(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(xn[n&255]+xn[n>>8&255]+xn[n>>16&255]+xn[n>>24&255]+"-"+xn[e&255]+xn[e>>8&255]+"-"+xn[e>>16&15|64]+xn[e>>24&255]+"-"+xn[t&63|128]+xn[t>>8&255]+"-"+xn[t>>16&255]+xn[t>>24&255]+xn[i&255]+xn[i>>8&255]+xn[i>>16&255]+xn[i>>24&255]).toLowerCase()}function tt(n,e,t){return Math.max(e,Math.min(t,n))}function Ay(n,e){return(n%e+e)%e}function T1(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function C1(n,e,t){return n!==e?(t-n)/(e-n):0}function qc(n,e,t){return(1-t)*n+t*e}function D1(n,e,t,i){return qc(n,e,1-Math.exp(-t*i))}function I1(n,e=1){return e-Math.abs(Ay(n,e*2)-e)}function A1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function R1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function N1(n,e){return n+Math.floor(Math.random()*(e-n+1))}function P1(n,e){return n+Math.random()*(e-n)}function O1(n){return n*(.5-Math.random())}function L1(n){n!==void 0&&(iM=n);let e=iM+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function F1(n){return n*da}function k1(n){return n*ma}function U1(n){return(n&n-1)===0&&n!==0}function B1(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function V1(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function H1(n,e,t,i,r){let s=Math.cos,o=Math.sin,a=s(t/2),c=o(t/2),l=s((e+i)/2),u=o((e+i)/2),d=s((e-i)/2),h=o((e-i)/2),f=s((i-e)/2),g=o((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*h,a*l);break;case"YZY":n.set(c*h,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*h,a*u,a*l);break;case"XZX":n.set(a*u,c*g,c*f,a*l);break;case"YXY":n.set(c*f,a*u,c*g,a*l);break;case"ZYZ":n.set(c*g,c*f,a*u,a*l);break;default:De("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function ua(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function Cn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var at={DEG2RAD:da,RAD2DEG:ma,generateUUID:Ta,clamp:tt,euclideanModulo:Ay,mapLinear:T1,inverseLerp:C1,lerp:qc,damp:D1,pingpong:I1,smoothstep:A1,smootherstep:R1,randInt:N1,randFloat:P1,randFloatSpread:O1,seededRandom:L1,degToRad:F1,radToDeg:k1,isPowerOfTwo:U1,ceilPowerOfTwo:B1,floorPowerOfTwo:V1,setQuaternionFromProperEuler:H1,normalize:Cn,denormalize:ua},rt=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=tt(this.x,e.x,t.x),this.y=tt(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=tt(this.x,e,t),this.y=tt(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(tt(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(tt(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},zi=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],h=s[o+0],f=s[o+1],g=s[o+2],y=s[o+3];if(d!==y||c!==h||l!==f||u!==g){let m=c*h+l*f+u*g+d*y;m<0&&(h=-h,f=-f,g=-g,y=-y,m=-m);let p=1-a;if(m<.9995){let b=Math.acos(m),w=Math.sin(b);p=Math.sin(p*b)/w,a=Math.sin(a*b)/w,c=c*p+h*a,l=l*p+f*a,u=u*p+g*a,d=d*p+y*a}else{c=c*p+h*a,l=l*p+f*a,u=u*p+g*a,d=d*p+y*a;let b=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=b,l*=b,u*=b,d*=b}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=s[o],h=s[o+1],f=s[o+2],g=s[o+3];return e[t]=a*g+u*d+c*f-l*h,e[t+1]=c*g+u*h+l*d-a*f,e[t+2]=l*g+u*f+a*h-c*d,e[t+3]=u*g-a*d-c*h-l*f,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(s/2),h=c(i/2),f=c(r/2),g=c(s/2);switch(o){case"XYZ":this._x=h*u*d+l*f*g,this._y=l*f*d-h*u*g,this._z=l*u*g+h*f*d,this._w=l*u*d-h*f*g;break;case"YXZ":this._x=h*u*d+l*f*g,this._y=l*f*d-h*u*g,this._z=l*u*g-h*f*d,this._w=l*u*d+h*f*g;break;case"ZXY":this._x=h*u*d-l*f*g,this._y=l*f*d+h*u*g,this._z=l*u*g+h*f*d,this._w=l*u*d-h*f*g;break;case"ZYX":this._x=h*u*d-l*f*g,this._y=l*f*d+h*u*g,this._z=l*u*g-h*f*d,this._w=l*u*d+h*f*g;break;case"YZX":this._x=h*u*d+l*f*g,this._y=l*f*d+h*u*g,this._z=l*u*g-h*f*d,this._w=l*u*d-h*f*g;break;case"XZY":this._x=h*u*d-l*f*g,this._y=l*f*d-h*u*g,this._z=l*u*g+h*f*d,this._w=l*u*d+h*f*g;break;default:De("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],h=i+a+d;if(h>0){let f=.5/Math.sqrt(h+1);this._w=.25/f,this._x=(u-c)*f,this._y=(s-l)*f,this._z=(o-r)*f}else if(i>a&&i>d){let f=2*Math.sqrt(1+i-a-d);this._w=(u-c)/f,this._x=.25*f,this._y=(r+o)/f,this._z=(s+l)/f}else if(a>d){let f=2*Math.sqrt(1+a-i-d);this._w=(s-l)/f,this._x=(r+o)/f,this._y=.25*f,this._z=(c+u)/f}else{let f=2*Math.sqrt(1+d-i-a);this._w=(o-r)/f,this._x=(s+l)/f,this._y=(c+u)/f,this._z=.25*f}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(tt(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},N=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(rM.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(rM.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),d=2*(s*i-o*t);return this.x=t+c*l+o*d-a*u,this.y=i+c*u+a*l-s*d,this.z=r+c*d+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=tt(this.x,e.x,t.x),this.y=tt(this.y,e.y,t.y),this.z=tt(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=tt(this.x,e,t),this.y=tt(this.y,e,t),this.z=tt(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(tt(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Iv.copy(this).projectOnVector(e),this.sub(Iv)}reflect(e){return this.sub(Iv.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(tt(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Iv=new N,rM=new zi,Fe=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],h=i[2],f=i[5],g=i[8],y=r[0],m=r[3],p=r[6],b=r[1],w=r[4],M=r[7],I=r[2],E=r[5],D=r[8];return s[0]=o*y+a*b+c*I,s[3]=o*m+a*w+c*E,s[6]=o*p+a*M+c*D,s[1]=l*y+u*b+d*I,s[4]=l*m+u*w+d*E,s[7]=l*p+u*M+d*D,s[2]=h*y+f*b+g*I,s[5]=h*m+f*w+g*E,s[8]=h*p+f*M+g*D,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*o-a*l,h=a*c-u*s,f=l*s-o*c,g=t*d+i*h+r*f;if(g===0)return this.set(0,0,0,0,0,0,0,0,0);let y=1/g;return e[0]=d*y,e[1]=(r*l-u*i)*y,e[2]=(a*i-r*o)*y,e[3]=h*y,e[4]=(u*t-r*c)*y,e[5]=(r*s-a*t)*y,e[6]=f*y,e[7]=(i*c-l*t)*y,e[8]=(o*t-i*s)*y,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Av.makeScale(e,t)),this}rotate(e){return this.premultiply(Av.makeRotation(-e)),this}translate(e,t){return this.premultiply(Av.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Av=new Fe,sM=new Fe().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),oM=new Fe().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function z1(){let n={enabled:!0,workingColorSpace:Zc,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===pt&&(r.r=yr(r.r),r.g=yr(r.g),r.b=yr(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===pt&&(r.r=ha(r.r),r.g=ha(r.g),r.b=ha(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===Sr?Kc:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return Lh("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return Lh("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Zc]:{primaries:e,whitePoint:i,transfer:Kc,toXYZ:sM,fromXYZ:oM,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:Dn},outputColorSpaceConfig:{drawingBufferColorSpace:Dn}},[Dn]:{primaries:e,whitePoint:i,transfer:pt,toXYZ:sM,fromXYZ:oM,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:Dn}}}),n}var et=z1();function yr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function ha(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var Jo,Fh=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{Jo===void 0&&(Jo=Jc("canvas")),Jo.width=e.width,Jo.height=e.height;let r=Jo.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=Jo}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Jc("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=yr(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(yr(t[i]/255)*255):t[i]=yr(t[i]);return{data:t,width:e.width,height:e.height}}else return De("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},G1=0,ga=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:G1++}),this.uuid=Ta(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(Rv(r[o].image)):s.push(Rv(r[o]))}else s=Rv(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function Rv(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?Fh.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(De("Texture: Unable to serialize Texture."),{})}var j1=0,Nv=new N,Mr=(()=>{class n extends Hi{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Bi,s=Bi,o=fn,a=cs,c=ui,l=Fn,u=n.DEFAULT_ANISOTROPY,d=Sr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:j1++}),this.uuid=Ta(),this.name="",this.source=new ga(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new rt(0,0),this.repeat=new rt(1,1),this.center=new rt(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Fe,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(Nv).x}get height(){return this.source.getSize(Nv).y}get depth(){return this.source.getSize(Nv).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){De(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){De(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Qv)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case Nh:t.x=t.x-Math.floor(t.x);break;case Bi:t.x=t.x<0?0:1;break;case Ph:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case Nh:t.y=t.y-Math.floor(t.y);break;case Bi:t.y=t.y<0?0:1;break;case Ph:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Qv,n.DEFAULT_ANISOTROPY=1,n})(),Ot=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],d=c[8],h=c[1],f=c[5],g=c[9],y=c[2],m=c[6],p=c[10];if(Math.abs(u-h)<.01&&Math.abs(d-y)<.01&&Math.abs(g-m)<.01){if(Math.abs(u+h)<.1&&Math.abs(d+y)<.1&&Math.abs(g+m)<.1&&Math.abs(l+f+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let w=(l+1)/2,M=(f+1)/2,I=(p+1)/2,E=(u+h)/4,D=(d+y)/4,_=(g+m)/4;return w>M&&w>I?w<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(w),r=E/i,s=D/i):M>I?M<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(M),i=E/r,s=_/r):I<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(I),i=D/s,r=_/s),this.set(i,r,s,t),this}let b=Math.sqrt((m-g)*(m-g)+(d-y)*(d-y)+(h-u)*(h-u));return Math.abs(b)<.001&&(b=1),this.x=(m-g)/b,this.y=(d-y)/b,this.z=(h-u)/b,this.w=Math.acos((l+f+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=tt(this.x,e.x,t.x),this.y=tt(this.y,e.y,t.y),this.z=tt(this.z,e.z,t.z),this.w=tt(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=tt(this.x,e,t),this.y=tt(this.y,e,t),this.z=tt(this.z,e,t),this.w=tt(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(tt(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},kh=class extends Hi{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:fn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new Ot(0,0,e,t),this.scissorTest=!1,this.viewport=new Ot(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new Mr(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:fn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new ga(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this.multiview=e.multiview,this}dispose(){this.dispatchEvent({type:"dispose"})}},Xn=class extends kh{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Qc=class extends Mr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=on,this.minFilter=on,this.wrapR=Bi,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var Uh=class extends Mr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=on,this.minFilter=on,this.wrapR=Bi,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Pt=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,d,h,f,g,y,m){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,d,h,f,g,y,m)}set(e,t,i,r,s,o,a,c,l,u,d,h,f,g,y,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=h,p[3]=f,p[7]=g,p[11]=y,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/Qo.setFromMatrixColumn(e,0).length(),s=1/Qo.setFromMatrixColumn(e,1).length(),o=1/Qo.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),d=Math.sin(s);if(e.order==="XYZ"){let h=o*u,f=o*d,g=a*u,y=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=f+g*l,t[5]=h-y*l,t[9]=-a*c,t[2]=y-h*l,t[6]=g+f*l,t[10]=o*c}else if(e.order==="YXZ"){let h=c*u,f=c*d,g=l*u,y=l*d;t[0]=h+y*a,t[4]=g*a-f,t[8]=o*l,t[1]=o*d,t[5]=o*u,t[9]=-a,t[2]=f*a-g,t[6]=y+h*a,t[10]=o*c}else if(e.order==="ZXY"){let h=c*u,f=c*d,g=l*u,y=l*d;t[0]=h-y*a,t[4]=-o*d,t[8]=g+f*a,t[1]=f+g*a,t[5]=o*u,t[9]=y-h*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let h=o*u,f=o*d,g=a*u,y=a*d;t[0]=c*u,t[4]=g*l-f,t[8]=h*l+y,t[1]=c*d,t[5]=y*l+h,t[9]=f*l-g,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let h=o*c,f=o*l,g=a*c,y=a*l;t[0]=c*u,t[4]=y-h*d,t[8]=g*d+f,t[1]=d,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=f*d+g,t[10]=h-y*d}else if(e.order==="XZY"){let h=o*c,f=o*l,g=a*c,y=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=h*d+y,t[5]=o*u,t[9]=f*d-g,t[2]=g*d-f,t[6]=a*u,t[10]=y*d+h}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(W1,e,$1)}lookAt(e,t,i){let r=this.elements;return Gn.subVectors(e,t),Gn.lengthSq()===0&&(Gn.z=1),Gn.normalize(),Zr.crossVectors(i,Gn),Zr.lengthSq()===0&&(Math.abs(i.z)===1?Gn.x+=1e-4:Gn.z+=1e-4,Gn.normalize(),Zr.crossVectors(i,Gn)),Zr.normalize(),Kd.crossVectors(Gn,Zr),r[0]=Zr.x,r[4]=Kd.x,r[8]=Gn.x,r[1]=Zr.y,r[5]=Kd.y,r[9]=Gn.y,r[2]=Zr.z,r[6]=Kd.z,r[10]=Gn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],h=i[9],f=i[13],g=i[2],y=i[6],m=i[10],p=i[14],b=i[3],w=i[7],M=i[11],I=i[15],E=r[0],D=r[4],_=r[8],T=r[12],O=r[1],C=r[5],F=r[9],G=r[13],W=r[2],P=r[6],H=r[10],U=r[14],Q=r[3],ee=r[7],ue=r[11],xe=r[15];return s[0]=o*E+a*O+c*W+l*Q,s[4]=o*D+a*C+c*P+l*ee,s[8]=o*_+a*F+c*H+l*ue,s[12]=o*T+a*G+c*U+l*xe,s[1]=u*E+d*O+h*W+f*Q,s[5]=u*D+d*C+h*P+f*ee,s[9]=u*_+d*F+h*H+f*ue,s[13]=u*T+d*G+h*U+f*xe,s[2]=g*E+y*O+m*W+p*Q,s[6]=g*D+y*C+m*P+p*ee,s[10]=g*_+y*F+m*H+p*ue,s[14]=g*T+y*G+m*U+p*xe,s[3]=b*E+w*O+M*W+I*Q,s[7]=b*D+w*C+M*P+I*ee,s[11]=b*_+w*F+M*H+I*ue,s[15]=b*T+w*G+M*U+I*xe,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],h=e[10],f=e[14],g=e[3],y=e[7],m=e[11],p=e[15],b=c*f-l*h,w=a*f-l*d,M=a*h-c*d,I=o*f-l*u,E=o*h-c*u,D=o*d-a*u;return t*(y*b-m*w+p*M)-i*(g*b-m*I+p*E)+r*(g*w-y*I+p*D)-s*(g*M-y*E+m*D)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],h=e[10],f=e[11],g=e[12],y=e[13],m=e[14],p=e[15],b=t*a-i*o,w=t*c-r*o,M=t*l-s*o,I=i*c-r*a,E=i*l-s*a,D=r*l-s*c,_=u*y-d*g,T=u*m-h*g,O=u*p-f*g,C=d*m-h*y,F=d*p-f*y,G=h*p-f*m,W=b*G-w*F+M*C+I*O-E*T+D*_;if(W===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let P=1/W;return e[0]=(a*G-c*F+l*C)*P,e[1]=(r*F-i*G-s*C)*P,e[2]=(y*D-m*E+p*I)*P,e[3]=(h*E-d*D-f*I)*P,e[4]=(c*O-o*G-l*T)*P,e[5]=(t*G-r*O+s*T)*P,e[6]=(m*M-g*D-p*w)*P,e[7]=(u*D-h*M+f*w)*P,e[8]=(o*F-a*O+l*_)*P,e[9]=(i*O-t*F-s*_)*P,e[10]=(g*E-y*M+p*b)*P,e[11]=(d*M-u*E-f*b)*P,e[12]=(a*T-o*C-c*_)*P,e[13]=(t*C-i*T+r*_)*P,e[14]=(y*w-g*I-m*b)*P,e[15]=(u*I-d*w+h*b)*P,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,d=a+a,h=s*l,f=s*u,g=s*d,y=o*u,m=o*d,p=a*d,b=c*l,w=c*u,M=c*d,I=i.x,E=i.y,D=i.z;return r[0]=(1-(y+p))*I,r[1]=(f+M)*I,r[2]=(g-w)*I,r[3]=0,r[4]=(f-M)*E,r[5]=(1-(h+p))*E,r[6]=(m+b)*E,r[7]=0,r[8]=(g+w)*D,r[9]=(m-b)*D,r[10]=(1-(h+y))*D,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=Qo.set(r[0],r[1],r[2]).length(),a=Qo.set(r[4],r[5],r[6]).length(),c=Qo.set(r[8],r[9],r[10]).length();s<0&&(o=-o),bi.copy(this);let l=1/o,u=1/a,d=1/c;return bi.elements[0]*=l,bi.elements[1]*=l,bi.elements[2]*=l,bi.elements[4]*=u,bi.elements[5]*=u,bi.elements[6]*=u,bi.elements[8]*=d,bi.elements[9]*=d,bi.elements[10]*=d,t.setFromRotationMatrix(bi),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=Ei,c=!1){let l=this.elements,u=2*s/(t-e),d=2*s/(i-r),h=(t+e)/(t-e),f=(i+r)/(i-r),g,y;if(c)g=s/(o-s),y=o*s/(o-s);else if(a===Ei)g=-(o+s)/(o-s),y=-2*o*s/(o-s);else if(a===fa)g=-o/(o-s),y=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=h,l[12]=0,l[1]=0,l[5]=d,l[9]=f,l[13]=0,l[2]=0,l[6]=0,l[10]=g,l[14]=y,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=Ei,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),h=-(t+e)/(t-e),f=-(i+r)/(i-r),g,y;if(c)g=1/(o-s),y=o/(o-s);else if(a===Ei)g=-2/(o-s),y=-(o+s)/(o-s);else if(a===fa)g=-1/(o-s),y=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=h,l[1]=0,l[5]=d,l[9]=0,l[13]=f,l[2]=0,l[6]=0,l[10]=g,l[14]=y,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Qo=new N,bi=new Pt,W1=new N(0,0,0),$1=new N(1,1,1),Zr=new N,Kd=new N,Gn=new N,aM=new Pt,cM=new zi,va=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],d=s[9],h=s[2],f=s[6],g=s[10];switch(i){case"XYZ":this._y=Math.asin(tt(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,g),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(f,u),this._z=0);break;case"YXZ":this._x=Math.asin(-tt(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,g),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-h,o),this._z=0);break;case"ZXY":this._x=Math.asin(tt(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(-h,g),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-tt(h,-1,1)),Math.abs(h)<.9999999?(this._x=Math.atan2(f,g),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(tt(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-h,o)):(this._x=0,this._y=Math.atan2(c,g));break;case"XZY":this._z=Math.asin(-tt(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(f,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-d,g),this._y=0);break;default:De("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return aM.makeRotationFromQuaternion(t),this.setFromRotationMatrix(aM,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return cM.setFromEuler(this),this.setFromQuaternion(cM,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),el=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},X1=0,lM=new N,ea=new zi,hr=new Pt,Jd=new N,Gc=new N,q1=new N,Y1=new zi,uM=new N(1,0,0),dM=new N(0,1,0),hM=new N(0,0,1),fM={type:"added"},Z1={type:"removed"},ta={type:"childadded",child:null},Pv={type:"childremoved",child:null},ci=(()=>{class n extends Hi{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:X1++}),this.uuid=Ta(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new N,i=new va,r=new zi,s=new N(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Pt},normalMatrix:{value:new Fe}}),this.matrix=new Pt,this.matrixWorld=new Pt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new el,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return ea.setFromAxisAngle(t,i),this.quaternion.multiply(ea),this}rotateOnWorldAxis(t,i){return ea.setFromAxisAngle(t,i),this.quaternion.premultiply(ea),this}rotateX(t){return this.rotateOnAxis(uM,t)}rotateY(t){return this.rotateOnAxis(dM,t)}rotateZ(t){return this.rotateOnAxis(hM,t)}translateOnAxis(t,i){return lM.copy(t).applyQuaternion(this.quaternion),this.position.add(lM.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(uM,t)}translateY(t){return this.translateOnAxis(dM,t)}translateZ(t){return this.translateOnAxis(hM,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(hr.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?Jd.copy(t):Jd.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),Gc.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?hr.lookAt(Gc,Jd,this.up):hr.lookAt(Jd,Gc,this.up),this.quaternion.setFromRotationMatrix(hr),s&&(hr.extractRotation(s.matrixWorld),ea.setFromRotationMatrix(hr),this.quaternion.premultiply(ea.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(Re("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(fM),ta.child=t,this.dispatchEvent(ta),ta.child=null):Re("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(Z1),Pv.child=t,this.dispatchEvent(Pv),Pv.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),hr.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),hr.multiply(t.parent.matrixWorld)),t.applyMatrix4(hr),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(fM),ta.child=t,this.dispatchEvent(ta),ta.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Gc,t,q1),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(Gc,Y1,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>ot(fe({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>fe({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let h=l[u];o(t.shapes,h)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),h=a(t.shapes),f=a(t.skeletons),g=a(t.animations),y=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),h.length>0&&(r.shapes=h),f.length>0&&(r.skeletons=f),g.length>0&&(r.animations=g),y.length>0&&(r.nodes=y)}return r.object=s,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new N(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),Wn=class extends ci{constructor(){super(),this.isGroup=!0,this.type="Group"}},K1={type:"move"},ya=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new Wn,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new Wn,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new N,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new N),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new Wn,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new N,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new N,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let y of e.hand.values()){let m=t.getJointPose(y,i),p=this._getHandJoint(l,y);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],h=u.position.distanceTo(d.position),f=.02,g=.005;l.inputState.pinching&&h>f+g?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&h<=f-g&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(K1)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new Wn;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},lE={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Kr={h:0,s:0,l:0},Qd={h:0,s:0,l:0};function Ov(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var qe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=Dn){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,et.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=et.workingColorSpace){return this.r=e,this.g=t,this.b=i,et.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=et.workingColorSpace){if(e=Ay(e,1),t=tt(t,0,1),i=tt(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=Ov(o,s,e+1/3),this.g=Ov(o,s,e),this.b=Ov(o,s,e-1/3)}return et.colorSpaceToWorking(this,r),this}setStyle(e,t=Dn){function i(s){s!==void 0&&parseFloat(s)<1&&De("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:De("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);De("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=Dn){let i=lE[e.toLowerCase()];return i!==void 0?this.setHex(i,t):De("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=yr(e.r),this.g=yr(e.g),this.b=yr(e.b),this}copyLinearToSRGB(e){return this.r=ha(e.r),this.g=ha(e.g),this.b=ha(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=Dn){return et.workingToColorSpace(bn.copy(this),e),Math.round(tt(bn.r*255,0,255))*65536+Math.round(tt(bn.g*255,0,255))*256+Math.round(tt(bn.b*255,0,255))}getHexString(e=Dn){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=et.workingColorSpace){et.workingToColorSpace(bn.copy(this),t);let i=bn.r,r=bn.g,s=bn.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let d=o-a;switch(l=u<=.5?d/(o+a):d/(2-o-a),o){case i:c=(r-s)/d+(r<s?6:0);break;case r:c=(s-i)/d+2;break;case s:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=et.workingColorSpace){return et.workingToColorSpace(bn.copy(this),t),e.r=bn.r,e.g=bn.g,e.b=bn.b,e}getStyle(e=Dn){et.workingToColorSpace(bn.copy(this),e);let t=bn.r,i=bn.g,r=bn.b;return e!==Dn?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Kr),this.setHSL(Kr.h+e,Kr.s+t,Kr.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Kr),e.getHSL(Qd);let i=qc(Kr.h,Qd.h,t),r=qc(Kr.s,Qd.s,t),s=qc(Kr.l,Qd.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},bn=new qe;qe.NAMES=lE;var tl=class n{constructor(e,t=25e-5){this.isFogExp2=!0,this.name="",this.color=new qe(e),this.density=t}clone(){return new n(this.color,this.density)}toJSON(){return{type:"FogExp2",name:this.name,color:this.color.getHex(),density:this.density}}};var nl=class extends ci{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new va,this.environmentIntensity=1,this.environmentRotation=new va,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},Si=new N,fr=new N,Lv=new N,pr=new N,na=new N,ia=new N,pM=new N,Fv=new N,kv=new N,Uv=new N,Bv=new Ot,Vv=new Ot,Hv=new Ot,vr=class n{constructor(e=new N,t=new N,i=new N){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Si.subVectors(e,t),r.cross(Si);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Si.subVectors(r,t),fr.subVectors(i,t),Lv.subVectors(e,t);let o=Si.dot(Si),a=Si.dot(fr),c=Si.dot(Lv),l=fr.dot(fr),u=fr.dot(Lv),d=o*l-a*a;if(d===0)return s.set(0,0,0),null;let h=1/d,f=(l*c-a*u)*h,g=(o*u-a*c)*h;return s.set(1-f-g,g,f)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,pr)===null?!1:pr.x>=0&&pr.y>=0&&pr.x+pr.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,pr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,pr.x),c.addScaledVector(o,pr.y),c.addScaledVector(a,pr.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return Bv.setScalar(0),Vv.setScalar(0),Hv.setScalar(0),Bv.fromBufferAttribute(e,t),Vv.fromBufferAttribute(e,i),Hv.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(Bv,s.x),o.addScaledVector(Vv,s.y),o.addScaledVector(Hv,s.z),o}static isFrontFacing(e,t,i,r){return Si.subVectors(i,t),fr.subVectors(e,t),Si.cross(fr).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Si.subVectors(this.c,this.b),fr.subVectors(this.a,this.b),Si.cross(fr).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;na.subVectors(r,i),ia.subVectors(s,i),Fv.subVectors(e,i);let c=na.dot(Fv),l=ia.dot(Fv);if(c<=0&&l<=0)return t.copy(i);kv.subVectors(e,r);let u=na.dot(kv),d=ia.dot(kv);if(u>=0&&d<=u)return t.copy(r);let h=c*d-u*l;if(h<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(na,o);Uv.subVectors(e,s);let f=na.dot(Uv),g=ia.dot(Uv);if(g>=0&&f<=g)return t.copy(s);let y=f*l-c*g;if(y<=0&&l>=0&&g<=0)return a=l/(l-g),t.copy(i).addScaledVector(ia,a);let m=u*g-f*d;if(m<=0&&d-u>=0&&f-g>=0)return pM.subVectors(s,r),a=(d-u)/(d-u+(f-g)),t.copy(r).addScaledVector(pM,a);let p=1/(m+y+h);return o=y*p,a=h*p,t.copy(i).addScaledVector(na,o).addScaledVector(ia,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},li=class{constructor(e=new N(1/0,1/0,1/0),t=new N(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Mi.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Mi.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Mi.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Mi):Mi.fromBufferAttribute(s,o),Mi.applyMatrix4(e.matrixWorld),this.expandByPoint(Mi);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),eh.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),eh.copy(i.boundingBox)),eh.applyMatrix4(e.matrixWorld),this.union(eh)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Mi),Mi.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(jc),th.subVectors(this.max,jc),ra.subVectors(e.a,jc),sa.subVectors(e.b,jc),oa.subVectors(e.c,jc),Jr.subVectors(sa,ra),Qr.subVectors(oa,sa),qs.subVectors(ra,oa);let t=[0,-Jr.z,Jr.y,0,-Qr.z,Qr.y,0,-qs.z,qs.y,Jr.z,0,-Jr.x,Qr.z,0,-Qr.x,qs.z,0,-qs.x,-Jr.y,Jr.x,0,-Qr.y,Qr.x,0,-qs.y,qs.x,0];return!zv(t,ra,sa,oa,th)||(t=[1,0,0,0,1,0,0,0,1],!zv(t,ra,sa,oa,th))?!1:(nh.crossVectors(Jr,Qr),t=[nh.x,nh.y,nh.z],zv(t,ra,sa,oa,th))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Mi).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Mi).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(mr[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),mr[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),mr[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),mr[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),mr[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),mr[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),mr[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),mr[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(mr),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},mr=[new N,new N,new N,new N,new N,new N,new N,new N],Mi=new N,eh=new li,ra=new N,sa=new N,oa=new N,Jr=new N,Qr=new N,qs=new N,jc=new N,th=new N,nh=new N,Ys=new N;function zv(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Ys.fromArray(n,s);let a=r.x*Math.abs(Ys.x)+r.y*Math.abs(Ys.y)+r.z*Math.abs(Ys.z),c=e.dot(Ys),l=t.dot(Ys),u=i.dot(Ys);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var Wt=new N,ih=new rt,J1=0,$n=class extends Hi{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:J1++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=sy,this.updateRanges=[],this.gpuType=Ci,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)ih.fromBufferAttribute(this,t),ih.applyMatrix3(e),this.setXY(t,ih.x,ih.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.applyMatrix3(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.applyMatrix4(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.applyNormalMatrix(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)Wt.fromBufferAttribute(this,t),Wt.transformDirection(e),this.setXYZ(t,Wt.x,Wt.y,Wt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=ua(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=Cn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=ua(t,this.array)),t}setX(e,t){return this.normalized&&(t=Cn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=ua(t,this.array)),t}setY(e,t){return this.normalized&&(t=Cn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=ua(t,this.array)),t}setZ(e,t){return this.normalized&&(t=Cn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=ua(t,this.array)),t}setW(e,t){return this.normalized&&(t=Cn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=Cn(t,this.array),i=Cn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=Cn(t,this.array),i=Cn(i,this.array),r=Cn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=Cn(t,this.array),i=Cn(i,this.array),r=Cn(r,this.array),s=Cn(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==sy&&(e.usage=this.usage),e}dispose(){this.dispatchEvent({type:"dispose"})}};var il=class extends $n{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var rl=class extends $n{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var At=class extends $n{constructor(e,t,i){super(new Float32Array(e),t,i)}},Q1=new li,Wc=new N,Gv=new N,to=class{constructor(e=new N,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):Q1.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Wc.subVectors(e,this.center);let t=Wc.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Wc,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(Gv.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Wc.copy(e.center).add(Gv)),this.expandByPoint(Wc.copy(e.center).sub(Gv))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},eN=0,ai=new Pt,jv=new ci,aa=new N,jn=new li,$c=new li,sn=new N,Sn=class n extends Hi{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:eN++}),this.uuid=Ta(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(E1(e)?rl:il)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Fe().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return ai.makeRotationFromQuaternion(e),this.applyMatrix4(ai),this}rotateX(e){return ai.makeRotationX(e),this.applyMatrix4(ai),this}rotateY(e){return ai.makeRotationY(e),this.applyMatrix4(ai),this}rotateZ(e){return ai.makeRotationZ(e),this.applyMatrix4(ai),this}translate(e,t,i){return ai.makeTranslation(e,t,i),this.applyMatrix4(ai),this}scale(e,t,i){return ai.makeScale(e,t,i),this.applyMatrix4(ai),this}lookAt(e){return jv.lookAt(e),jv.updateMatrix(),this.applyMatrix4(jv.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(aa).negate(),this.translate(aa.x,aa.y,aa.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new At(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&De("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new li);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new N(-1/0,-1/0,-1/0),new N(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];jn.setFromBufferAttribute(s),this.morphTargetsRelative?(sn.addVectors(this.boundingBox.min,jn.min),this.boundingBox.expandByPoint(sn),sn.addVectors(this.boundingBox.max,jn.max),this.boundingBox.expandByPoint(sn)):(this.boundingBox.expandByPoint(jn.min),this.boundingBox.expandByPoint(jn.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&Re('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new to);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){Re("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new N,1/0);return}if(e){let i=this.boundingSphere.center;if(jn.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];$c.setFromBufferAttribute(a),this.morphTargetsRelative?(sn.addVectors(jn.min,$c.min),jn.expandByPoint(sn),sn.addVectors(jn.max,$c.max),jn.expandByPoint(sn)):(jn.expandByPoint($c.min),jn.expandByPoint($c.max))}jn.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)sn.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(sn));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)sn.fromBufferAttribute(a,l),c&&(aa.fromBufferAttribute(e,l),sn.add(aa)),r=Math.max(r,i.distanceToSquared(sn))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&Re('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){Re("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new $n(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let _=0;_<i.count;_++)a[_]=new N,c[_]=new N;let l=new N,u=new N,d=new N,h=new rt,f=new rt,g=new rt,y=new N,m=new N;function p(_,T,O){l.fromBufferAttribute(i,_),u.fromBufferAttribute(i,T),d.fromBufferAttribute(i,O),h.fromBufferAttribute(s,_),f.fromBufferAttribute(s,T),g.fromBufferAttribute(s,O),u.sub(l),d.sub(l),f.sub(h),g.sub(h);let C=1/(f.x*g.y-g.x*f.y);isFinite(C)&&(y.copy(u).multiplyScalar(g.y).addScaledVector(d,-f.y).multiplyScalar(C),m.copy(d).multiplyScalar(f.x).addScaledVector(u,-g.x).multiplyScalar(C),a[_].add(y),a[T].add(y),a[O].add(y),c[_].add(m),c[T].add(m),c[O].add(m))}let b=this.groups;b.length===0&&(b=[{start:0,count:e.count}]);for(let _=0,T=b.length;_<T;++_){let O=b[_],C=O.start,F=O.count;for(let G=C,W=C+F;G<W;G+=3)p(e.getX(G+0),e.getX(G+1),e.getX(G+2))}let w=new N,M=new N,I=new N,E=new N;function D(_){I.fromBufferAttribute(r,_),E.copy(I);let T=a[_];w.copy(T),w.sub(I.multiplyScalar(I.dot(T))).normalize(),M.crossVectors(E,T);let C=M.dot(c[_])<0?-1:1;o.setXYZW(_,w.x,w.y,w.z,C)}for(let _=0,T=b.length;_<T;++_){let O=b[_],C=O.start,F=O.count;for(let G=C,W=C+F;G<W;G+=3)D(e.getX(G+0)),D(e.getX(G+1)),D(e.getX(G+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new $n(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let h=0,f=i.count;h<f;h++)i.setXYZ(h,0,0,0);let r=new N,s=new N,o=new N,a=new N,c=new N,l=new N,u=new N,d=new N;if(e)for(let h=0,f=e.count;h<f;h+=3){let g=e.getX(h+0),y=e.getX(h+1),m=e.getX(h+2);r.fromBufferAttribute(t,g),s.fromBufferAttribute(t,y),o.fromBufferAttribute(t,m),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),a.fromBufferAttribute(i,g),c.fromBufferAttribute(i,y),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(g,a.x,a.y,a.z),i.setXYZ(y,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let h=0,f=t.count;h<f;h+=3)r.fromBufferAttribute(t,h+0),s.fromBufferAttribute(t,h+1),o.fromBufferAttribute(t,h+2),u.subVectors(o,s),d.subVectors(r,s),u.cross(d),i.setXYZ(h+0,u.x,u.y,u.z),i.setXYZ(h+1,u.x,u.y,u.z),i.setXYZ(h+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)sn.fromBufferAttribute(e,t),sn.normalize(),e.setXYZ(t,sn.x,sn.y,sn.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,h=new l.constructor(c.length*u),f=0,g=0;for(let y=0,m=c.length;y<m;y++){a.isInterleavedBufferAttribute?f=c[y]*a.data.stride+a.offset:f=c[y]*u;for(let p=0;p<u;p++)h[g++]=l[f++]}return new $n(h,u,d)}if(this.index===null)return De("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,d=l.length;u<d;u++){let h=l[u],f=e(h,i);c.push(f)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,h=l.length;d<h;d++){let f=l[d];u.push(f.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],d=s[l];for(let h=0,f=d.length;h<f;h++)u.push(d[h].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let d=o[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var tN=0,xr=class extends Hi{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:tN++}),this.uuid=Ta(),this.name="",this.type="Material",this.blending=Qs,this.side=_r,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Mh,this.blendDst=Eh,this.blendEquation=ns,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new qe(0,0,0),this.blendAlpha=0,this.depthFunc=eo,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=ry,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Js,this.stencilZFail=Js,this.stencilZPass=Js,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){De(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){De(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Qs&&(i.blending=this.blending),this.side!==_r&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Mh&&(i.blendSrc=this.blendSrc),this.blendDst!==Eh&&(i.blendDst=this.blendDst),this.blendEquation!==ns&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==eo&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==ry&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Js&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Js&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Js&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var gr=new N,Wv=new N,rh=new N,es=new N,$v=new N,sh=new N,Xv=new N,sl=class{constructor(e=new N,t=new N(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,gr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=gr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(gr.copy(this.origin).addScaledVector(this.direction,t),gr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){Wv.copy(e).add(t).multiplyScalar(.5),rh.copy(t).sub(e).normalize(),es.copy(this.origin).sub(Wv);let s=e.distanceTo(t)*.5,o=-this.direction.dot(rh),a=es.dot(this.direction),c=-es.dot(rh),l=es.lengthSq(),u=Math.abs(1-o*o),d,h,f,g;if(u>0)if(d=o*c-a,h=o*a-c,g=s*u,d>=0)if(h>=-g)if(h<=g){let y=1/u;d*=y,h*=y,f=d*(d+o*h+2*a)+h*(o*d+h+2*c)+l}else h=s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*c)+l;else h=-s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*c)+l;else h<=-g?(d=Math.max(0,-(-o*s+a)),h=d>0?-s:Math.min(Math.max(-s,-c),s),f=-d*d+h*(h+2*c)+l):h<=g?(d=0,h=Math.min(Math.max(-s,-c),s),f=h*(h+2*c)+l):(d=Math.max(0,-(o*s+a)),h=d>0?s:Math.min(Math.max(-s,-c),s),f=-d*d+h*(h+2*c)+l);else h=o>0?-s:s,d=Math.max(0,-(o*h+a)),f=-d*d+h*(h+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(Wv).addScaledVector(rh,h),f}intersectSphere(e,t){gr.subVectors(e.center,this.origin);let i=gr.dot(this.direction),r=gr.dot(gr)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,h=this.origin;return l>=0?(i=(e.min.x-h.x)*l,r=(e.max.x-h.x)*l):(i=(e.max.x-h.x)*l,r=(e.min.x-h.x)*l),u>=0?(s=(e.min.y-h.y)*u,o=(e.max.y-h.y)*u):(s=(e.max.y-h.y)*u,o=(e.min.y-h.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),d>=0?(a=(e.min.z-h.z)*d,c=(e.max.z-h.z)*d):(a=(e.max.z-h.z)*d,c=(e.min.z-h.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,gr)!==null}intersectTriangle(e,t,i,r,s){$v.subVectors(t,e),sh.subVectors(i,e),Xv.crossVectors($v,sh);let o=this.direction.dot(Xv),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;es.subVectors(this.origin,e);let c=a*this.direction.dot(sh.crossVectors(es,sh));if(c<0)return null;let l=a*this.direction.dot($v.cross(es));if(l<0||c+l>o)return null;let u=-a*es.dot(Xv);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},no=class extends xr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new qe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new va,this.combine=py,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},mM=new Pt,Zs=new sl,oh=new to,gM=new N,ah=new N,ch=new N,lh=new N,qv=new N,uh=new N,vM=new N,dh=new N,dt=class extends ci{constructor(e=new Sn,t=new no){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){uh.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],d=s[c];u!==0&&(qv.fromBufferAttribute(d,e),o?uh.addScaledVector(qv,u):uh.addScaledVector(qv.sub(t),u))}t.add(uh)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),oh.copy(i.boundingSphere),oh.applyMatrix4(s),Zs.copy(e.ray).recast(e.near),!(oh.containsPoint(Zs.origin)===!1&&(Zs.intersectSphere(oh,gM)===null||Zs.origin.distanceToSquared(gM)>(e.far-e.near)**2))&&(mM.copy(s).invert(),Zs.copy(e.ray).applyMatrix4(mM),!(i.boundingBox!==null&&Zs.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Zs)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,d=s.attributes.normal,h=s.groups,f=s.drawRange;if(a!==null)if(Array.isArray(o))for(let g=0,y=h.length;g<y;g++){let m=h[g],p=o[m.materialIndex],b=Math.max(m.start,f.start),w=Math.min(a.count,Math.min(m.start+m.count,f.start+f.count));for(let M=b,I=w;M<I;M+=3){let E=a.getX(M),D=a.getX(M+1),_=a.getX(M+2);r=hh(this,p,e,i,l,u,d,E,D,_),r&&(r.faceIndex=Math.floor(M/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),y=Math.min(a.count,f.start+f.count);for(let m=g,p=y;m<p;m+=3){let b=a.getX(m),w=a.getX(m+1),M=a.getX(m+2);r=hh(this,o,e,i,l,u,d,b,w,M),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let g=0,y=h.length;g<y;g++){let m=h[g],p=o[m.materialIndex],b=Math.max(m.start,f.start),w=Math.min(c.count,Math.min(m.start+m.count,f.start+f.count));for(let M=b,I=w;M<I;M+=3){let E=M,D=M+1,_=M+2;r=hh(this,p,e,i,l,u,d,E,D,_),r&&(r.faceIndex=Math.floor(M/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let g=Math.max(0,f.start),y=Math.min(c.count,f.start+f.count);for(let m=g,p=y;m<p;m+=3){let b=m,w=m+1,M=m+2;r=hh(this,o,e,i,l,u,d,b,w,M),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function nN(n,e,t,i,r,s,o,a){let c;if(e.side===An?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===_r,a),c===null)return null;dh.copy(a),dh.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(dh);return l<t.near||l>t.far?null:{distance:l,point:dh.clone(),object:n}}function hh(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,ah),n.getVertexPosition(c,ch),n.getVertexPosition(l,lh);let u=nN(n,e,t,i,ah,ch,lh,vM);if(u){let d=new N;vr.getBarycoord(vM,ah,ch,lh,d),r&&(u.uv=vr.getInterpolatedAttribute(r,a,c,l,d,new rt)),s&&(u.uv1=vr.getInterpolatedAttribute(s,a,c,l,d,new rt)),o&&(u.normal=vr.getInterpolatedAttribute(o,a,c,l,d,new N),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let h={a,b:c,c:l,normal:new N,materialIndex:0};vr.getNormal(ah,ch,lh,h.normal),u.face=h,u.barycoord=d}return u}var Bh=class extends Mr{constructor(e=null,t=1,i=1,r,s,o,a,c,l=on,u=on,d,h){super(null,o,a,c,l,u,r,s,d,h),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Yv=new N,iN=new N,rN=new Fe,Ui=class{constructor(e=new N(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Yv.subVectors(i,t).cross(iN.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(Yv),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||rN.getNormalMatrix(e),r=this.coplanarPoint(Yv).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},Ks=new to,sN=new rt(.5,.5),fh=new N,_a=class{constructor(e=new Ui,t=new Ui,i=new Ui,r=new Ui,s=new Ui,o=new Ui){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=Ei,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],d=s[5],h=s[6],f=s[7],g=s[8],y=s[9],m=s[10],p=s[11],b=s[12],w=s[13],M=s[14],I=s[15];if(r[0].setComponents(l-o,f-u,p-g,I-b).normalize(),r[1].setComponents(l+o,f+u,p+g,I+b).normalize(),r[2].setComponents(l+a,f+d,p+y,I+w).normalize(),r[3].setComponents(l-a,f-d,p-y,I-w).normalize(),i)r[4].setComponents(c,h,m,M).normalize(),r[5].setComponents(l-c,f-h,p-m,I-M).normalize();else if(r[4].setComponents(l-c,f-h,p-m,I-M).normalize(),t===Ei)r[5].setComponents(l+c,f+h,p+m,I+M).normalize();else if(t===fa)r[5].setComponents(c,h,m,M).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),Ks.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),Ks.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(Ks)}intersectsSprite(e){Ks.center.set(0,0,0);let t=sN.distanceTo(e.center);return Ks.radius=.7071067811865476+t,Ks.applyMatrix4(e.matrixWorld),this.intersectsSphere(Ks)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(fh.x=r.normal.x>0?e.max.x:e.min.x,fh.y=r.normal.y>0?e.max.y:e.min.y,fh.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(fh)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var On=class extends xr{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new qe(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},Vh=new N,Hh=new N,yM=new Pt,Xc=new sl,ph=new to,Zv=new N,_M=new N,zh=class extends ci{constructor(e=new Sn,t=new On){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,s=t.count;r<s;r++)Vh.fromBufferAttribute(t,r-1),Hh.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=Vh.distanceTo(Hh);e.setAttribute("lineDistance",new At(i,1))}else De("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,s=e.params.Line.threshold,o=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),ph.copy(i.boundingSphere),ph.applyMatrix4(r),ph.radius+=s,e.ray.intersectsSphere(ph)===!1)return;yM.copy(r).invert(),Xc.copy(e.ray).applyMatrix4(yM);let a=s/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,h=i.attributes.position;if(u!==null){let f=Math.max(0,o.start),g=Math.min(u.count,o.start+o.count);for(let y=f,m=g-1;y<m;y+=l){let p=u.getX(y),b=u.getX(y+1),w=mh(this,e,Xc,c,p,b,y);w&&t.push(w)}if(this.isLineLoop){let y=u.getX(g-1),m=u.getX(f),p=mh(this,e,Xc,c,y,m,g-1);p&&t.push(p)}}else{let f=Math.max(0,o.start),g=Math.min(h.count,o.start+o.count);for(let y=f,m=g-1;y<m;y+=l){let p=mh(this,e,Xc,c,y,y+1,y);p&&t.push(p)}if(this.isLineLoop){let y=mh(this,e,Xc,c,g-1,f,g-1);y&&t.push(y)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}};function mh(n,e,t,i,r,s,o){let a=n.geometry.attributes.position;if(Vh.fromBufferAttribute(a,r),Hh.fromBufferAttribute(a,s),t.distanceSqToSegment(Vh,Hh,Zv,_M)>i)return;Zv.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(Zv);if(!(l<e.near||l>e.far))return{distance:l,point:_M.clone().applyMatrix4(n.matrixWorld),index:o,face:null,faceIndex:null,barycoord:null,object:n}}var xM=new N,bM=new N,Ln=class extends zh{constructor(e,t){super(e,t),this.isLineSegments=!0,this.type="LineSegments"}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[];for(let r=0,s=t.count;r<s;r+=2)xM.fromBufferAttribute(t,r),bM.fromBufferAttribute(t,r+1),i[r]=r===0?0:i[r-1],i[r+1]=i[r]+xM.distanceTo(bM);e.setAttribute("lineDistance",new At(i,1))}else De("LineSegments.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}};var ol=class extends Mr{constructor(e=[],t=as,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var br=class extends Mr{constructor(e,t,i=Ti,r,s,o,a=on,c=on,l,u=Vi,d=1){if(u!==Vi&&u!==ls)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let h={width:e,height:t,depth:d};super(h,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new ga(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Gh=class extends br{constructor(e,t=Ti,i=as,r,s,o=on,a=on,c,l=Vi){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},al=class extends Mr{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},$t=class n extends Sn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],d=[],h=0,f=0;g("z","y","x",-1,-1,i,t,e,o,s,0),g("z","y","x",1,-1,i,t,-e,o,s,1),g("x","z","y",1,1,e,i,t,r,o,2),g("x","z","y",1,-1,e,i,-t,r,o,3),g("x","y","z",1,-1,e,t,i,r,s,4),g("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new At(l,3)),this.setAttribute("normal",new At(u,3)),this.setAttribute("uv",new At(d,2));function g(y,m,p,b,w,M,I,E,D,_,T){let O=M/D,C=I/_,F=M/2,G=I/2,W=E/2,P=D+1,H=_+1,U=0,Q=0,ee=new N;for(let ue=0;ue<H;ue++){let xe=ue*C-G;for(let Te=0;Te<P;Te++){let nt=Te*O-F;ee[y]=nt*b,ee[m]=xe*w,ee[p]=W,l.push(ee.x,ee.y,ee.z),ee[y]=0,ee[m]=0,ee[p]=E>0?1:-1,u.push(ee.x,ee.y,ee.z),d.push(Te/D),d.push(1-ue/_),U+=1}}for(let ue=0;ue<_;ue++)for(let xe=0;xe<D;xe++){let Te=h+xe+P*ue,nt=h+xe+P*(ue+1),gt=h+(xe+1)+P*(ue+1),Ge=h+(xe+1)+P*ue;c.push(Te,nt,Ge),c.push(nt,gt,Ge),Q+=6}a.addGroup(f,Q,T),f+=Q,h+=U}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var cl=class n extends Sn{constructor(e=1,t=1,i=1,r=32,s=1,o=!1,a=0,c=Math.PI*2){super(),this.type="CylinderGeometry",this.parameters={radiusTop:e,radiusBottom:t,height:i,radialSegments:r,heightSegments:s,openEnded:o,thetaStart:a,thetaLength:c};let l=this;r=Math.floor(r),s=Math.floor(s);let u=[],d=[],h=[],f=[],g=0,y=[],m=i/2,p=0;b(),o===!1&&(e>0&&w(!0),t>0&&w(!1)),this.setIndex(u),this.setAttribute("position",new At(d,3)),this.setAttribute("normal",new At(h,3)),this.setAttribute("uv",new At(f,2));function b(){let M=new N,I=new N,E=0,D=(t-e)/i;for(let _=0;_<=s;_++){let T=[],O=_/s,C=O*(t-e)+e;for(let F=0;F<=r;F++){let G=F/r,W=G*c+a,P=Math.sin(W),H=Math.cos(W);I.x=C*P,I.y=-O*i+m,I.z=C*H,d.push(I.x,I.y,I.z),M.set(P,D,H).normalize(),h.push(M.x,M.y,M.z),f.push(G,1-O),T.push(g++)}y.push(T)}for(let _=0;_<r;_++)for(let T=0;T<s;T++){let O=y[T][_],C=y[T+1][_],F=y[T+1][_+1],G=y[T][_+1];(e>0||T!==0)&&(u.push(O,C,G),E+=3),(t>0||T!==s-1)&&(u.push(C,F,G),E+=3)}l.addGroup(p,E,0),p+=E}function w(M){let I=g,E=new rt,D=new N,_=0,T=M===!0?e:t,O=M===!0?1:-1;for(let F=1;F<=r;F++)d.push(0,m*O,0),h.push(0,O,0),f.push(.5,.5),g++;let C=g;for(let F=0;F<=r;F++){let W=F/r*c+a,P=Math.cos(W),H=Math.sin(W);D.x=T*H,D.y=m*O,D.z=T*P,d.push(D.x,D.y,D.z),h.push(0,O,0),E.x=P*.5+.5,E.y=H*.5*O+.5,f.push(E.x,E.y),g++}for(let F=0;F<r;F++){let G=I+F,W=C+F;M===!0?u.push(W,W+1,G):u.push(W+1,W,G),_+=3}l.addGroup(p,_,M===!0?1:2),p+=_}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radiusTop,e.radiusBottom,e.height,e.radialSegments,e.heightSegments,e.openEnded,e.thetaStart,e.thetaLength)}};var gh=new N,vh=new N,Kv=new N,yh=new vr,Gi=class extends Sn{constructor(e=null,t=1){if(super(),this.type="EdgesGeometry",this.parameters={geometry:e,thresholdAngle:t},e!==null){let r=Math.pow(10,4),s=Math.cos(da*t),o=e.getIndex(),a=e.getAttribute("position"),c=o?o.count:a.count,l=[0,0,0],u=["a","b","c"],d=new Array(3),h={},f=[];for(let g=0;g<c;g+=3){o?(l[0]=o.getX(g),l[1]=o.getX(g+1),l[2]=o.getX(g+2)):(l[0]=g,l[1]=g+1,l[2]=g+2);let{a:y,b:m,c:p}=yh;if(y.fromBufferAttribute(a,l[0]),m.fromBufferAttribute(a,l[1]),p.fromBufferAttribute(a,l[2]),yh.getNormal(Kv),d[0]=`${Math.round(y.x*r)},${Math.round(y.y*r)},${Math.round(y.z*r)}`,d[1]=`${Math.round(m.x*r)},${Math.round(m.y*r)},${Math.round(m.z*r)}`,d[2]=`${Math.round(p.x*r)},${Math.round(p.y*r)},${Math.round(p.z*r)}`,!(d[0]===d[1]||d[1]===d[2]||d[2]===d[0]))for(let b=0;b<3;b++){let w=(b+1)%3,M=d[b],I=d[w],E=yh[u[b]],D=yh[u[w]],_=`${M}_${I}`,T=`${I}_${M}`;T in h&&h[T]?(Kv.dot(h[T].normal)<=s&&(f.push(E.x,E.y,E.z),f.push(D.x,D.y,D.z)),h[T]=null):_ in h||(h[_]={index0:l[b],index1:l[w],normal:Kv.clone()})}}for(let g in h)if(h[g]){let{index0:y,index1:m}=h[g];gh.fromBufferAttribute(a,y),vh.fromBufferAttribute(a,m),f.push(gh.x,gh.y,gh.z),f.push(vh.x,vh.y,vh.z)}this.setAttribute("position",new At(f,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};var ll=class n extends Sn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,h=t/c,f=[],g=[],y=[],m=[];for(let p=0;p<u;p++){let b=p*h-o;for(let w=0;w<l;w++){let M=w*d-s;g.push(M,-b,0),y.push(0,0,1),m.push(w/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let b=0;b<a;b++){let w=b+l*p,M=b+l*(p+1),I=b+1+l*(p+1),E=b+1+l*p;f.push(w,M,E),f.push(M,I,E)}this.setIndex(f),this.setAttribute("position",new At(g,3)),this.setAttribute("normal",new At(y,3)),this.setAttribute("uv",new At(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};var ji=class n extends Sn{constructor(e=1,t=32,i=16,r=0,s=Math.PI*2,o=0,a=Math.PI){super(),this.type="SphereGeometry",this.parameters={radius:e,widthSegments:t,heightSegments:i,phiStart:r,phiLength:s,thetaStart:o,thetaLength:a},t=Math.max(3,Math.floor(t)),i=Math.max(2,Math.floor(i));let c=Math.min(o+a,Math.PI),l=0,u=[],d=new N,h=new N,f=[],g=[],y=[],m=[];for(let p=0;p<=i;p++){let b=[],w=p/i,M=0;p===0&&o===0?M=.5/t:p===i&&c===Math.PI&&(M=-.5/t);for(let I=0;I<=t;I++){let E=I/t;d.x=-e*Math.cos(r+E*s)*Math.sin(o+w*a),d.y=e*Math.cos(o+w*a),d.z=e*Math.sin(r+E*s)*Math.sin(o+w*a),g.push(d.x,d.y,d.z),h.copy(d).normalize(),y.push(h.x,h.y,h.z),m.push(E+M,1-w),b.push(l++)}u.push(b)}for(let p=0;p<i;p++)for(let b=0;b<t;b++){let w=u[p][b+1],M=u[p][b],I=u[p+1][b],E=u[p+1][b+1];(p!==0||o>0)&&f.push(w,M,E),(p!==i-1||c<Math.PI)&&f.push(M,I,E)}this.setIndex(f),this.setAttribute("position",new At(g,3)),this.setAttribute("normal",new At(y,3)),this.setAttribute("uv",new At(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.widthSegments,e.heightSegments,e.phiStart,e.phiLength,e.thetaStart,e.thetaLength)}};var xa=class n extends Sn{constructor(e=1,t=.4,i=12,r=48,s=Math.PI*2,o=0,a=Math.PI*2){super(),this.type="TorusGeometry",this.parameters={radius:e,tube:t,radialSegments:i,tubularSegments:r,arc:s,thetaStart:o,thetaLength:a},i=Math.floor(i),r=Math.floor(r);let c=[],l=[],u=[],d=[],h=new N,f=new N,g=new N;for(let y=0;y<=i;y++){let m=o+y/i*a;for(let p=0;p<=r;p++){let b=p/r*s;f.x=(e+t*Math.cos(m))*Math.cos(b),f.y=(e+t*Math.cos(m))*Math.sin(b),f.z=t*Math.sin(m),l.push(f.x,f.y,f.z),h.x=e*Math.cos(b),h.y=e*Math.sin(b),g.subVectors(f,h).normalize(),u.push(g.x,g.y,g.z),d.push(p/r),d.push(y/i)}}for(let y=1;y<=i;y++)for(let m=1;m<=r;m++){let p=(r+1)*y+m-1,b=(r+1)*(y-1)+m-1,w=(r+1)*(y-1)+m,M=(r+1)*y+m;c.push(p,b,M),c.push(b,w,M)}this.setIndex(c),this.setAttribute("position",new At(l,3)),this.setAttribute("normal",new At(u,3)),this.setAttribute("uv",new At(d,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.radius,e.tube,e.radialSegments,e.tubularSegments,e.arc)}};var is=class extends Sn{constructor(e=null){if(super(),this.type="WireframeGeometry",this.parameters={geometry:e},e!==null){let t=[],i=new Set,r=new N,s=new N;if(e.index!==null){let o=e.attributes.position,a=e.index,c=e.groups;c.length===0&&(c=[{start:0,count:a.count,materialIndex:0}]);for(let l=0,u=c.length;l<u;++l){let d=c[l],h=d.start,f=d.count;for(let g=h,y=h+f;g<y;g+=3)for(let m=0;m<3;m++){let p=a.getX(g+m),b=a.getX(g+(m+1)%3);r.fromBufferAttribute(o,p),s.fromBufferAttribute(o,b),SM(r,s,i)===!0&&(t.push(r.x,r.y,r.z),t.push(s.x,s.y,s.z))}}}else{let o=e.attributes.position;for(let a=0,c=o.count/3;a<c;a++)for(let l=0;l<3;l++){let u=3*a+l,d=3*a+(l+1)%3;r.fromBufferAttribute(o,u),s.fromBufferAttribute(o,d),SM(r,s,i)===!0&&(t.push(r.x,r.y,r.z),t.push(s.x,s.y,s.z))}}this.setAttribute("position",new At(t,3))}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}};function SM(n,e,t){let i=`${n.x},${n.y},${n.z}-${e.x},${e.y},${e.z}`,r=`${e.x},${e.y},${e.z}-${n.x},${n.y},${n.z}`;return t.has(i)===!0||t.has(r)===!0?!1:(t.add(i),t.add(r),!0)}function so(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(MM(r))r.isRenderTargetTexture?(De("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(MM(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function Mn(n){let e={};for(let t=0;t<n.length;t++){let i=so(n[t]);for(let r in i)e[r]=i[r]}return e}function MM(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function oN(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function Ry(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:et.workingColorSpace}var uE={clone:so,merge:Mn},aN=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,cN=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,qn=class extends xr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=aN,this.fragmentShader=cN,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=so(e.uniforms),this.uniformsGroups=oN(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},jh=class extends qn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},In=class extends xr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new qe(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new qe(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Wf,this.normalScale=new rt(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new va,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}};var Wh=class extends xr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=KM,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},$h=class extends xr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function _h(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var rs=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];n:{e:{let o;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break e}o=t.length;break t}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break e}o=i,i=0;break t}break n}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},Xh=class extends rs{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:ty,endingEnd:ty}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case ny:s=e,a=2*t-i;break;case iy:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case ny:o=e,c=2*i-t;break;case iy:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,h=this._weightPrev,f=this._weightNext,g=(i-t)/(r-t),y=g*g,m=y*g,p=-h*m+2*h*y-h*g,b=(1+h)*m+(-1.5-2*h)*y+(-.5+h)*g+1,w=(-1-f)*m+(1.5+f)*y+.5*g,M=f*m-f*y;for(let I=0;I!==a;++I)s[I]=p*o[u+I]+b*o[l+I]+w*o[c+I]+M*o[d+I];return s}},qh=class extends rs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let h=0;h!==a;++h)s[h]=o[l+h]*d+o[c+h]*u;return s}},Yh=class extends rs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},Zh=class extends rs{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,d=u.inTangents,h=u.outTangents;if(!d||!h){let y=(i-t)/(r-t),m=1-y;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*y;return s}let f=a*2,g=e-1;for(let y=0;y!==a;++y){let m=o[l+y],p=o[c+y],b=g*f+y*2,w=h[b],M=h[b+1],I=e*f+y*2,E=d[I],D=d[I+1],_=(i-t)/(r-t),T,O,C,F,G;for(let W=0;W<8;W++){T=_*_,O=T*_,C=1-_,F=C*C,G=F*C;let H=G*t+3*F*_*w+3*C*T*E+O*r-i;if(Math.abs(H)<1e-10)break;let U=3*F*(w-t)+6*C*_*(E-w)+3*T*(r-E);if(Math.abs(U)<1e-10)break;_=_-H/U,_=Math.max(0,Math.min(1,_))}s[y]=G*m+3*F*_*M+3*C*T*D+O*p}return s}},Yn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=_h(t,this.TimeBufferType),this.values=_h(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:_h(e.times,Array),values:_h(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new Yh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new qh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new Xh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new Zh(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case Yc:t=this.InterpolantFactoryMethodDiscrete;break;case Oh:t=this.InterpolantFactoryMethodLinear;break;case Sh:t=this.InterpolantFactoryMethodSmooth;break;case ey:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return De("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Yc;case this.InterpolantFactoryMethodLinear:return Oh;case this.InterpolantFactoryMethodSmooth:return Sh;case this.InterpolantFactoryMethodBezier:return ey}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(Re("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(Re("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){Re("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){Re("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&w1(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){Re("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Sh,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,h=d-i,f=d+i;for(let g=0;g!==i;++g){let y=t[d+g];if(y!==t[h+g]||y!==t[f+g]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let d=a*i,h=o*i;for(let f=0;f!==i;++f)t[h+f]=t[d+f]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};Yn.prototype.ValueTypeName="";Yn.prototype.TimeBufferType=Float32Array;Yn.prototype.ValueBufferType=Float32Array;Yn.prototype.DefaultInterpolation=Oh;var ss=class extends Yn{constructor(e,t,i){super(e,t,i)}};ss.prototype.ValueTypeName="bool";ss.prototype.ValueBufferType=Array;ss.prototype.DefaultInterpolation=Yc;ss.prototype.InterpolantFactoryMethodLinear=void 0;ss.prototype.InterpolantFactoryMethodSmooth=void 0;var Kh=class extends Yn{constructor(e,t,i,r){super(e,t,i,r)}};Kh.prototype.ValueTypeName="color";var Jh=class extends Yn{constructor(e,t,i,r){super(e,t,i,r)}};Jh.prototype.ValueTypeName="number";var Qh=class extends rs{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)zi.slerpFlat(s,0,o,l-a,o,l,c);return s}},ul=class extends Yn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new Qh(this.times,this.values,this.getValueSize(),e)}};ul.prototype.ValueTypeName="quaternion";ul.prototype.InterpolantFactoryMethodSmooth=void 0;var os=class extends Yn{constructor(e,t,i){super(e,t,i)}};os.prototype.ValueTypeName="string";os.prototype.ValueBufferType=Array;os.prototype.DefaultInterpolation=Yc;os.prototype.InterpolantFactoryMethodLinear=void 0;os.prototype.InterpolantFactoryMethodSmooth=void 0;var ef=class extends Yn{constructor(e,t,i,r){super(e,t,i,r)}};ef.prototype.ValueTypeName="vector";var ba=class extends ci{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new qe(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}},dl=class extends ba{constructor(e,t,i){super(e,i),this.isHemisphereLight=!0,this.type="HemisphereLight",this.position.copy(ci.DEFAULT_UP),this.updateMatrix(),this.groundColor=new qe(t)}copy(e,t){return super.copy(e,t),this.groundColor.copy(e.groundColor),this}toJSON(e){let t=super.toJSON(e);return t.object.groundColor=this.groundColor.getHex(),t}},Jv=new Pt,EM=new N,wM=new N,tf=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.biasNode=null,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new rt(512,512),this.mapType=Fn,this.map=null,this.mapPass=null,this.matrix=new Pt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new _a,this._frameExtents=new rt(1,1),this._viewportCount=1,this._viewports=[new Ot(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;EM.setFromMatrixPosition(e.matrixWorld),t.position.copy(EM),wM.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(wM),t.updateMatrixWorld(),Jv.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(Jv,t.coordinateSystem,t.reversedDepth),t.coordinateSystem===fa||t.reversedDepth?i.set(.5,0,0,.5,0,.5,0,.5,0,0,1,0,0,0,0,1):i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(Jv)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.autoUpdate=e.autoUpdate,this.needsUpdate=e.needsUpdate,this.normalBias=e.normalBias,this.blurSamples=e.blurSamples,this.mapSize.copy(e.mapSize),this.biasNode=e.biasNode,this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}},xh=new N,bh=new zi,ki=new N,hl=class extends ci{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Pt,this.projectionMatrix=new Pt,this.projectionMatrixInverse=new Pt,this.coordinateSystem=Ei,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(xh,bh,ki),ki.x===1&&ki.y===1&&ki.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(xh,bh,ki.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(xh,bh,ki),ki.x===1&&ki.y===1&&ki.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(xh,bh,ki.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},ts=new N,TM=new rt,CM=new rt,hn=class extends hl{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=ma*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(da*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return ma*2*Math.atan(Math.tan(da*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){ts.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(ts.x,ts.y).multiplyScalar(-e/ts.z),ts.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(ts.x,ts.y).multiplyScalar(-e/ts.z)}getViewSize(e,t){return this.getViewBounds(e,TM,CM),t.subVectors(CM,TM)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(da*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var oy=class extends tf{constructor(){super(new hn(90,1,.5,500)),this.isPointLightShadow=!0}},io=class extends ba{constructor(e,t,i=0,r=2){super(e,t),this.isPointLight=!0,this.type="PointLight",this.distance=i,this.decay=r,this.shadow=new oy}get power(){return this.intensity*4*Math.PI}set power(e){this.intensity=e/(4*Math.PI)}dispose(){super.dispose(),this.shadow.dispose()}copy(e,t){return super.copy(e,t),this.distance=e.distance,this.decay=e.decay,this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.distance=this.distance,t.object.decay=this.decay,t.object.shadow=this.shadow.toJSON(),t}},Sa=class extends hl{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},ay=class extends tf{constructor(){super(new Sa(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},fl=class extends ba{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(ci.DEFAULT_UP),this.updateMatrix(),this.target=new ci,this.shadow=new ay}dispose(){super.dispose(),this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.shadow=this.shadow.toJSON(),t.object.target=this.target.uuid,t}};var ca=-90,la=1,nf=class extends ci{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new hn(ca,la,e,t);r.layers=this.layers,this.add(r);let s=new hn(ca,la,e,t);s.layers=this.layers,this.add(s);let o=new hn(ca,la,e,t);o.layers=this.layers,this.add(o);let a=new hn(ca,la,e,t);a.layers=this.layers,this.add(a);let c=new hn(ca,la,e,t);c.layers=this.layers,this.add(c);let l=new hn(ca,la,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===Ei)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===fa)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,d=e.getRenderTarget(),h=e.getActiveCubeFace(),f=e.getActiveMipmapLevel(),g=e.xr.enabled;e.xr.enabled=!1;let y=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=y,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,h,f),e.xr.enabled=g,i.texture.needsPMREMUpdate=!0}},rf=class extends hn{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var Ny="\\[\\]\\.:\\/",lN=new RegExp("["+Ny+"]","g"),Py="[^"+Ny+"]",uN="[^"+Ny.replace("\\.","")+"]",dN=/((?:WC+[\/:])*)/.source.replace("WC",Py),hN=/(WCOD+)?/.source.replace("WCOD",uN),fN=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",Py),pN=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",Py),mN=new RegExp("^"+dN+hN+fN+pN+"$"),gN=["material","materials","bones","map"],cy=class{constructor(e,t,i){let r=i||Bt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Bt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(lN,"")}static parseTrackName(t){let i=mN.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);gN.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){De("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){Re("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){Re("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){Re("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){Re("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){Re("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){Re("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;Re("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){Re("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=cy,n})();Bt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Bt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Bt.prototype.GetterByBindingType=[Bt.prototype._getValue_direct,Bt.prototype._getValue_array,Bt.prototype._getValue_arrayElement,Bt.prototype._getValue_toArray];Bt.prototype.SetterByBindingTypeAndVersioning=[[Bt.prototype._setValue_direct,Bt.prototype._setValue_direct_setNeedsUpdate,Bt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Bt.prototype._setValue_array,Bt.prototype._setValue_array_setNeedsUpdate,Bt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Bt.prototype._setValue_arrayElement,Bt.prototype._setValue_arrayElement_setNeedsUpdate,Bt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Bt.prototype._setValue_fromArray,Bt.prototype._setValue_fromArray_setNeedsUpdate,Bt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var TX=new Float32Array(1);var ly=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};function Oy(n,e,t,i){let r=vN(i);switch(t){case wy:return n*e;case Cy:return n*e/r.components*r.byteLength;case hf:return n*e/r.components*r.byteLength;case us:return n*e*2/r.components*r.byteLength;case ff:return n*e*2/r.components*r.byteLength;case Ty:return n*e*3/r.components*r.byteLength;case ui:return n*e*4/r.components*r.byteLength;case pf:return n*e*4/r.components*r.byteLength;case yl:case _l:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case xl:case bl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case gf:case yf:return Math.max(n,16)*Math.max(e,8)/4;case mf:case vf:return Math.max(n,8)*Math.max(e,8)/2;case _f:case xf:case Sf:case Mf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case bf:case Sl:case Ef:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case wf:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Tf:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Cf:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Df:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case If:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Af:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Rf:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Nf:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Pf:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Of:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Lf:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Ff:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case kf:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case Uf:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case Bf:case Vf:case Hf:return Math.ceil(n/4)*Math.ceil(e/4)*16;case zf:case Gf:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Ml:case jf:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function vN(n){switch(n){case Fn:case by:return{byteLength:1,components:1};case Ea:case Sy:case Xi:return{byteLength:2,components:1};case uf:case df:return{byteLength:2,components:4};case Ti:case lf:case Ci:return{byteLength:4,components:1};case My:case Ey:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"184"}}));typeof window<"u"&&(window.__THREE__?De("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="184");function PE(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function _N(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,h=n.createBuffer();n.bindBuffer(c,h),n.bufferData(c,l,u),a.onUploadCallback();let f;if(l instanceof Float32Array)f=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)f=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?f=n.HALF_FLOAT:f=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)f=n.SHORT;else if(l instanceof Uint32Array)f=n.UNSIGNED_INT;else if(l instanceof Int32Array)f=n.INT;else if(l instanceof Int8Array)f=n.BYTE;else if(l instanceof Uint8Array)f=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)f=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:h,type:f,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((f,g)=>f.start-g.start);let h=0;for(let f=1;f<d.length;f++){let g=d[h],y=d[f];y.start<=g.start+g.count+1?g.count=Math.max(g.count,y.start+y.count-g.start):(++h,d[h]=y)}d.length=h+1;for(let f=0,g=d.length;f<g;f++){let y=d[f];n.bufferSubData(l,y.start*u.BYTES_PER_ELEMENT,u,y.start,y.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var xN=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,bN=`#ifdef USE_ALPHAHASH
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
#endif`,SN=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,MN=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,EN=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,wN=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,TN=`#ifdef USE_AOMAP
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
#endif`,CN=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,DN=`#ifdef USE_BATCHING
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
#endif`,IN=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,AN=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,RN=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,NN=`float G_BlinnPhong_Implicit( ) {
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
} // validated`,PN=`#ifdef USE_IRIDESCENCE
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
#endif`,ON=`#ifdef USE_BUMPMAP
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
#endif`,LN=`#if NUM_CLIPPING_PLANES > 0
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
#endif`,FN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,kN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,UN=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,BN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,VN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,HN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,zN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
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
#endif`,GN=`#define PI 3.141592653589793
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
} // validated`,jN=`#ifdef ENVMAP_TYPE_CUBE_UV
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
#endif`,WN=`vec3 transformedNormal = objectNormal;
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
#endif`,$N=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,XN=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,qN=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,YN=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,ZN="gl_FragColor = linearToOutputTexel( gl_FragColor );",KN=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,JN=`#ifdef USE_ENVMAP
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
#endif`,QN=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,eP=`#ifdef USE_ENVMAP
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
#endif`,tP=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,nP=`#ifdef USE_ENVMAP
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
#endif`,iP=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,rP=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,sP=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,oP=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,aP=`#ifdef USE_GRADIENTMAP
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
}`,cP=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,lP=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,uP=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,dP=`uniform bool receiveShadow;
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
#include <lightprobes_pars_fragment>`,hP=`#ifdef USE_ENVMAP
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
#endif`,fP=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,pP=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,mP=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,gP=`varying vec3 vViewPosition;
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
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,vP=`PhysicalMaterial material;
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
#endif`,yP=`uniform sampler2D dfgLUT;
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
}`,_P=`
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
#endif`,xP=`#if defined( RE_IndirectDiffuse )
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
#endif`,bP=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,SP=`#ifdef USE_LIGHT_PROBES_GRID
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
#endif`,MP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,EP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,wP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,TP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,CP=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,DP=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,IP=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
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
#endif`,AP=`#if defined( USE_POINTS_UV )
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
#endif`,RP=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,NP=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,PP=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,OP=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,LP=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,FP=`#ifdef USE_MORPHTARGETS
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
#endif`,kP=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,UP=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
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
vec3 nonPerturbedNormal = normal;`,BP=`#ifdef USE_NORMALMAP_OBJECTSPACE
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
#endif`,VP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,HP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,zP=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,GP=`#ifdef USE_NORMALMAP
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
#endif`,jP=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,WP=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,$P=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,XP=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,qP=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,YP=`vec3 packNormalToRGB( const in vec3 normal ) {
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
}`,ZP=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,KP=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,JP=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,QP=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,eO=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,tO=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,nO=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,iO=`#if NUM_SPOT_LIGHT_COORDS > 0
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
#endif`,rO=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
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
#endif`,sO=`float getShadowMask() {
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
}`,oO=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,aO=`#ifdef USE_SKINNING
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
#endif`,cO=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,lO=`#ifdef USE_SKINNING
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
#endif`,uO=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,dO=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,hO=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,fO=`#ifndef saturate
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
vec3 CustomToneMapping( vec3 color ) { return color; }`,pO=`#ifdef USE_TRANSMISSION
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
#endif`,mO=`#ifdef USE_TRANSMISSION
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
#endif`,gO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,vO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,yO=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
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
#endif`,_O=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,xO=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,bO=`uniform sampler2D t2D;
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
}`,SO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,MO=`#ifdef ENVMAP_TYPE_CUBE
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
}`,EO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,wO=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,TO=`#include <common>
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
}`,CO=`#if DEPTH_PACKING == 3200
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
}`,DO=`#define DISTANCE
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
}`,IO=`#define DISTANCE
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
}`,AO=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,RO=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,NO=`uniform float scale;
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
}`,PO=`uniform vec3 diffuse;
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
}`,OO=`#include <common>
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
}`,LO=`uniform vec3 diffuse;
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
}`,FO=`#define LAMBERT
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
}`,kO=`#define LAMBERT
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
}`,UO=`#define MATCAP
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
}`,BO=`#define MATCAP
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
}`,VO=`#define NORMAL
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
}`,HO=`#define NORMAL
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
}`,zO=`#define PHONG
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
}`,GO=`#define PHONG
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
}`,jO=`#define STANDARD
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
}`,WO=`#define STANDARD
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
}`,$O=`#define TOON
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
}`,XO=`#define TOON
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
}`,qO=`uniform float size;
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
}`,YO=`uniform vec3 diffuse;
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
}`,ZO=`#include <common>
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
}`,KO=`uniform vec3 color;
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
}`,JO=`uniform float rotation;
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
}`,QO=`uniform vec3 diffuse;
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
}`,$e={alphahash_fragment:xN,alphahash_pars_fragment:bN,alphamap_fragment:SN,alphamap_pars_fragment:MN,alphatest_fragment:EN,alphatest_pars_fragment:wN,aomap_fragment:TN,aomap_pars_fragment:CN,batching_pars_vertex:DN,batching_vertex:IN,begin_vertex:AN,beginnormal_vertex:RN,bsdfs:NN,iridescence_fragment:PN,bumpmap_pars_fragment:ON,clipping_planes_fragment:LN,clipping_planes_pars_fragment:FN,clipping_planes_pars_vertex:kN,clipping_planes_vertex:UN,color_fragment:BN,color_pars_fragment:VN,color_pars_vertex:HN,color_vertex:zN,common:GN,cube_uv_reflection_fragment:jN,defaultnormal_vertex:WN,displacementmap_pars_vertex:$N,displacementmap_vertex:XN,emissivemap_fragment:qN,emissivemap_pars_fragment:YN,colorspace_fragment:ZN,colorspace_pars_fragment:KN,envmap_fragment:JN,envmap_common_pars_fragment:QN,envmap_pars_fragment:eP,envmap_pars_vertex:tP,envmap_physical_pars_fragment:hP,envmap_vertex:nP,fog_vertex:iP,fog_pars_vertex:rP,fog_fragment:sP,fog_pars_fragment:oP,gradientmap_pars_fragment:aP,lightmap_pars_fragment:cP,lights_lambert_fragment:lP,lights_lambert_pars_fragment:uP,lights_pars_begin:dP,lights_toon_fragment:fP,lights_toon_pars_fragment:pP,lights_phong_fragment:mP,lights_phong_pars_fragment:gP,lights_physical_fragment:vP,lights_physical_pars_fragment:yP,lights_fragment_begin:_P,lights_fragment_maps:xP,lights_fragment_end:bP,lightprobes_pars_fragment:SP,logdepthbuf_fragment:MP,logdepthbuf_pars_fragment:EP,logdepthbuf_pars_vertex:wP,logdepthbuf_vertex:TP,map_fragment:CP,map_pars_fragment:DP,map_particle_fragment:IP,map_particle_pars_fragment:AP,metalnessmap_fragment:RP,metalnessmap_pars_fragment:NP,morphinstance_vertex:PP,morphcolor_vertex:OP,morphnormal_vertex:LP,morphtarget_pars_vertex:FP,morphtarget_vertex:kP,normal_fragment_begin:UP,normal_fragment_maps:BP,normal_pars_fragment:VP,normal_pars_vertex:HP,normal_vertex:zP,normalmap_pars_fragment:GP,clearcoat_normal_fragment_begin:jP,clearcoat_normal_fragment_maps:WP,clearcoat_pars_fragment:$P,iridescence_pars_fragment:XP,opaque_fragment:qP,packing:YP,premultiplied_alpha_fragment:ZP,project_vertex:KP,dithering_fragment:JP,dithering_pars_fragment:QP,roughnessmap_fragment:eO,roughnessmap_pars_fragment:tO,shadowmap_pars_fragment:nO,shadowmap_pars_vertex:iO,shadowmap_vertex:rO,shadowmask_pars_fragment:sO,skinbase_vertex:oO,skinning_pars_vertex:aO,skinning_vertex:cO,skinnormal_vertex:lO,specularmap_fragment:uO,specularmap_pars_fragment:dO,tonemapping_fragment:hO,tonemapping_pars_fragment:fO,transmission_fragment:pO,transmission_pars_fragment:mO,uv_pars_fragment:gO,uv_pars_vertex:vO,uv_vertex:yO,worldpos_vertex:_O,background_vert:xO,background_frag:bO,backgroundCube_vert:SO,backgroundCube_frag:MO,cube_vert:EO,cube_frag:wO,depth_vert:TO,depth_frag:CO,distance_vert:DO,distance_frag:IO,equirect_vert:AO,equirect_frag:RO,linedashed_vert:NO,linedashed_frag:PO,meshbasic_vert:OO,meshbasic_frag:LO,meshlambert_vert:FO,meshlambert_frag:kO,meshmatcap_vert:UO,meshmatcap_frag:BO,meshnormal_vert:VO,meshnormal_frag:HO,meshphong_vert:zO,meshphong_frag:GO,meshphysical_vert:jO,meshphysical_frag:WO,meshtoon_vert:$O,meshtoon_frag:XO,points_vert:qO,points_frag:YO,shadow_vert:ZO,shadow_frag:KO,sprite_vert:JO,sprite_frag:QO},le={common:{diffuse:{value:new qe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Fe},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Fe}},envmap:{envMap:{value:null},envMapRotation:{value:new Fe},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Fe}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Fe}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Fe},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Fe},normalScale:{value:new rt(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Fe},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Fe}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Fe}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Fe}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new qe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new N},probesMax:{value:new N},probesResolution:{value:new N}},points:{diffuse:{value:new qe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0},uvTransform:{value:new Fe}},sprite:{diffuse:{value:new qe(16777215)},opacity:{value:1},center:{value:new rt(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Fe},alphaMap:{value:null},alphaMapTransform:{value:new Fe},alphaTest:{value:0}}},Yi={basic:{uniforms:Mn([le.common,le.specularmap,le.envmap,le.aomap,le.lightmap,le.fog]),vertexShader:$e.meshbasic_vert,fragmentShader:$e.meshbasic_frag},lambert:{uniforms:Mn([le.common,le.specularmap,le.envmap,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.fog,le.lights,{emissive:{value:new qe(0)},envMapIntensity:{value:1}}]),vertexShader:$e.meshlambert_vert,fragmentShader:$e.meshlambert_frag},phong:{uniforms:Mn([le.common,le.specularmap,le.envmap,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.fog,le.lights,{emissive:{value:new qe(0)},specular:{value:new qe(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:$e.meshphong_vert,fragmentShader:$e.meshphong_frag},standard:{uniforms:Mn([le.common,le.envmap,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.roughnessmap,le.metalnessmap,le.fog,le.lights,{emissive:{value:new qe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:$e.meshphysical_vert,fragmentShader:$e.meshphysical_frag},toon:{uniforms:Mn([le.common,le.aomap,le.lightmap,le.emissivemap,le.bumpmap,le.normalmap,le.displacementmap,le.gradientmap,le.fog,le.lights,{emissive:{value:new qe(0)}}]),vertexShader:$e.meshtoon_vert,fragmentShader:$e.meshtoon_frag},matcap:{uniforms:Mn([le.common,le.bumpmap,le.normalmap,le.displacementmap,le.fog,{matcap:{value:null}}]),vertexShader:$e.meshmatcap_vert,fragmentShader:$e.meshmatcap_frag},points:{uniforms:Mn([le.points,le.fog]),vertexShader:$e.points_vert,fragmentShader:$e.points_frag},dashed:{uniforms:Mn([le.common,le.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:$e.linedashed_vert,fragmentShader:$e.linedashed_frag},depth:{uniforms:Mn([le.common,le.displacementmap]),vertexShader:$e.depth_vert,fragmentShader:$e.depth_frag},normal:{uniforms:Mn([le.common,le.bumpmap,le.normalmap,le.displacementmap,{opacity:{value:1}}]),vertexShader:$e.meshnormal_vert,fragmentShader:$e.meshnormal_frag},sprite:{uniforms:Mn([le.sprite,le.fog]),vertexShader:$e.sprite_vert,fragmentShader:$e.sprite_frag},background:{uniforms:{uvTransform:{value:new Fe},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:$e.background_vert,fragmentShader:$e.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Fe}},vertexShader:$e.backgroundCube_vert,fragmentShader:$e.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:$e.cube_vert,fragmentShader:$e.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:$e.equirect_vert,fragmentShader:$e.equirect_frag},distance:{uniforms:Mn([le.common,le.displacementmap,{referencePosition:{value:new N},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:$e.distance_vert,fragmentShader:$e.distance_frag},shadow:{uniforms:Mn([le.lights,le.fog,{color:{value:new qe(0)},opacity:{value:1}}]),vertexShader:$e.shadow_vert,fragmentShader:$e.shadow_frag}};Yi.physical={uniforms:Mn([Yi.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Fe},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Fe},clearcoatNormalScale:{value:new rt(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Fe},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Fe},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Fe},sheen:{value:0},sheenColor:{value:new qe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Fe},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Fe},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Fe},transmissionSamplerSize:{value:new rt},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Fe},attenuationDistance:{value:0},attenuationColor:{value:new qe(0)},specularColor:{value:new qe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Fe},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Fe},anisotropyVector:{value:new rt},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Fe}}]),vertexShader:$e.meshphysical_vert,fragmentShader:$e.meshphysical_frag};var qf={r:0,b:0,g:0},eL=new Pt,OE=new Fe;OE.set(-1,0,0,0,1,0,0,0,1);function tL(n,e,t,i,r,s){let o=new qe(0),a=r===!0?0:1,c,l,u=null,d=0,h=null;function f(b){let w=b.isScene===!0?b.background:null;if(w&&w.isTexture){let M=b.backgroundBlurriness>0;w=e.get(w,M)}return w}function g(b){let w=!1,M=f(b);M===null?m(o,a):M&&M.isColor&&(m(M,1),w=!0);let I=n.xr.getEnvironmentBlendMode();I==="additive"?t.buffers.color.setClear(0,0,0,1,s):I==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||w)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function y(b,w){let M=f(w);M&&(M.isCubeTexture||M.mapping===gl)?(l===void 0&&(l=new dt(new $t(1,1,1),new qn({name:"BackgroundCubeMaterial",uniforms:so(Yi.backgroundCube.uniforms),vertexShader:Yi.backgroundCube.vertexShader,fragmentShader:Yi.backgroundCube.fragmentShader,side:An,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(I,E,D){this.matrixWorld.copyPosition(D.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=M,l.material.uniforms.backgroundBlurriness.value=w.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(eL.makeRotationFromEuler(w.backgroundRotation)).transpose(),M.isCubeTexture&&M.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(OE),l.material.toneMapped=et.getTransfer(M.colorSpace)!==pt,(u!==M||d!==M.version||h!==n.toneMapping)&&(l.material.needsUpdate=!0,u=M,d=M.version,h=n.toneMapping),l.layers.enableAll(),b.unshift(l,l.geometry,l.material,0,0,null)):M&&M.isTexture&&(c===void 0&&(c=new dt(new ll(2,2),new qn({name:"BackgroundMaterial",uniforms:so(Yi.background.uniforms),vertexShader:Yi.background.vertexShader,fragmentShader:Yi.background.fragmentShader,side:_r,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=M,c.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,c.material.toneMapped=et.getTransfer(M.colorSpace)!==pt,M.matrixAutoUpdate===!0&&M.updateMatrix(),c.material.uniforms.uvTransform.value.copy(M.matrix),(u!==M||d!==M.version||h!==n.toneMapping)&&(c.material.needsUpdate=!0,u=M,d=M.version,h=n.toneMapping),c.layers.enableAll(),b.unshift(c,c.geometry,c.material,0,0,null))}function m(b,w){b.getRGB(qf,Ry(n)),t.buffers.color.setClear(qf.r,qf.g,qf.b,w,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(b,w=1){o.set(b),a=w,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(b){a=b,m(o,a)},render:g,addToRenderList:y,dispose:p}}function nL(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=h(null),s=r,o=!1;function a(C,F,G,W,P){let H=!1,U=d(C,W,G,F);s!==U&&(s=U,l(s.object)),H=f(C,W,G,P),H&&g(C,W,G,P),P!==null&&e.update(P,n.ELEMENT_ARRAY_BUFFER),(H||o)&&(o=!1,M(C,F,G,W),P!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(P).buffer))}function c(){return n.createVertexArray()}function l(C){return n.bindVertexArray(C)}function u(C){return n.deleteVertexArray(C)}function d(C,F,G,W){let P=W.wireframe===!0,H=i[F.id];H===void 0&&(H={},i[F.id]=H);let U=C.isInstancedMesh===!0?C.id:0,Q=H[U];Q===void 0&&(Q={},H[U]=Q);let ee=Q[G.id];ee===void 0&&(ee={},Q[G.id]=ee);let ue=ee[P];return ue===void 0&&(ue=h(c()),ee[P]=ue),ue}function h(C){let F=[],G=[],W=[];for(let P=0;P<t;P++)F[P]=0,G[P]=0,W[P]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:F,enabledAttributes:G,attributeDivisors:W,object:C,attributes:{},index:null}}function f(C,F,G,W){let P=s.attributes,H=F.attributes,U=0,Q=G.getAttributes();for(let ee in Q)if(Q[ee].location>=0){let xe=P[ee],Te=H[ee];if(Te===void 0&&(ee==="instanceMatrix"&&C.instanceMatrix&&(Te=C.instanceMatrix),ee==="instanceColor"&&C.instanceColor&&(Te=C.instanceColor)),xe===void 0||xe.attribute!==Te||Te&&xe.data!==Te.data)return!0;U++}return s.attributesNum!==U||s.index!==W}function g(C,F,G,W){let P={},H=F.attributes,U=0,Q=G.getAttributes();for(let ee in Q)if(Q[ee].location>=0){let xe=H[ee];xe===void 0&&(ee==="instanceMatrix"&&C.instanceMatrix&&(xe=C.instanceMatrix),ee==="instanceColor"&&C.instanceColor&&(xe=C.instanceColor));let Te={};Te.attribute=xe,xe&&xe.data&&(Te.data=xe.data),P[ee]=Te,U++}s.attributes=P,s.attributesNum=U,s.index=W}function y(){let C=s.newAttributes;for(let F=0,G=C.length;F<G;F++)C[F]=0}function m(C){p(C,0)}function p(C,F){let G=s.newAttributes,W=s.enabledAttributes,P=s.attributeDivisors;G[C]=1,W[C]===0&&(n.enableVertexAttribArray(C),W[C]=1),P[C]!==F&&(n.vertexAttribDivisor(C,F),P[C]=F)}function b(){let C=s.newAttributes,F=s.enabledAttributes;for(let G=0,W=F.length;G<W;G++)F[G]!==C[G]&&(n.disableVertexAttribArray(G),F[G]=0)}function w(C,F,G,W,P,H,U){U===!0?n.vertexAttribIPointer(C,F,G,P,H):n.vertexAttribPointer(C,F,G,W,P,H)}function M(C,F,G,W){y();let P=W.attributes,H=G.getAttributes(),U=F.defaultAttributeValues;for(let Q in H){let ee=H[Q];if(ee.location>=0){let ue=P[Q];if(ue===void 0&&(Q==="instanceMatrix"&&C.instanceMatrix&&(ue=C.instanceMatrix),Q==="instanceColor"&&C.instanceColor&&(ue=C.instanceColor)),ue!==void 0){let xe=ue.normalized,Te=ue.itemSize,nt=e.get(ue);if(nt===void 0)continue;let gt=nt.buffer,Ge=nt.type,Y=nt.bytesPerElement,pe=Ge===n.INT||Ge===n.UNSIGNED_INT||ue.gpuType===lf;if(ue.isInterleavedBufferAttribute){let re=ue.data,Ae=re.stride,ke=ue.offset;if(re.isInstancedInterleavedBuffer){for(let Ne=0;Ne<ee.locationSize;Ne++)p(ee.location+Ne,re.meshPerAttribute);C.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=re.meshPerAttribute*re.count)}else for(let Ne=0;Ne<ee.locationSize;Ne++)m(ee.location+Ne);n.bindBuffer(n.ARRAY_BUFFER,gt);for(let Ne=0;Ne<ee.locationSize;Ne++)w(ee.location+Ne,Te/ee.locationSize,Ge,xe,Ae*Y,(ke+Te/ee.locationSize*Ne)*Y,pe)}else{if(ue.isInstancedBufferAttribute){for(let re=0;re<ee.locationSize;re++)p(ee.location+re,ue.meshPerAttribute);C.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=ue.meshPerAttribute*ue.count)}else for(let re=0;re<ee.locationSize;re++)m(ee.location+re);n.bindBuffer(n.ARRAY_BUFFER,gt);for(let re=0;re<ee.locationSize;re++)w(ee.location+re,Te/ee.locationSize,Ge,xe,Te*Y,Te/ee.locationSize*re*Y,pe)}}else if(U!==void 0){let xe=U[Q];if(xe!==void 0)switch(xe.length){case 2:n.vertexAttrib2fv(ee.location,xe);break;case 3:n.vertexAttrib3fv(ee.location,xe);break;case 4:n.vertexAttrib4fv(ee.location,xe);break;default:n.vertexAttrib1fv(ee.location,xe)}}}}b()}function I(){T();for(let C in i){let F=i[C];for(let G in F){let W=F[G];for(let P in W){let H=W[P];for(let U in H)u(H[U].object),delete H[U];delete W[P]}}delete i[C]}}function E(C){if(i[C.id]===void 0)return;let F=i[C.id];for(let G in F){let W=F[G];for(let P in W){let H=W[P];for(let U in H)u(H[U].object),delete H[U];delete W[P]}}delete i[C.id]}function D(C){for(let F in i){let G=i[F];for(let W in G){let P=G[W];if(P[C.id]===void 0)continue;let H=P[C.id];for(let U in H)u(H[U].object),delete H[U];delete P[C.id]}}}function _(C){for(let F in i){let G=i[F],W=C.isInstancedMesh===!0?C.id:0,P=G[W];if(P!==void 0){for(let H in P){let U=P[H];for(let Q in U)u(U[Q].object),delete U[Q];delete P[H]}delete G[W],Object.keys(G).length===0&&delete i[F]}}}function T(){O(),o=!0,s!==r&&(s=r,l(s.object))}function O(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:T,resetDefaultState:O,dispose:I,releaseStatesOfGeometry:E,releaseStatesOfObject:_,releaseStatesOfProgram:D,initAttributes:y,enableAttribute:m,disableUnusedAttributes:b}}function iL(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let h=0;for(let f=0;f<u;f++)h+=l[f];t.update(h,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function rL(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let D=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(D.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(D){return!(D!==ui&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(D){let _=D===Xi&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(D!==Fn&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&D!==Ci&&!_)}function c(D){if(D==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";D="mediump"}return D==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(De("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,h=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&h===!1&&De("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let f=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),g=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),y=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),b=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),w=n.getParameter(n.MAX_VARYING_VECTORS),M=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),I=n.getParameter(n.MAX_SAMPLES),E=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:h,maxTextures:f,maxVertexTextures:g,maxTextureSize:y,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:b,maxVaryings:w,maxFragmentUniforms:M,maxSamples:I,samples:E}}function sL(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Ui,a=new Fe,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,h){let f=d.length!==0||h||i!==0||r;return r=h,i=d.length,f},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(d,h){t=u(d,h,0)},this.setState=function(d,h,f){let g=d.clippingPlanes,y=d.clipIntersection,m=d.clipShadows,p=n.get(d);if(!r||g===null||g.length===0||s&&!m)s?u(null):l();else{let b=s?0:i,w=b*4,M=p.clippingState||null;c.value=M,M=u(g,h,w,f);for(let I=0;I!==w;++I)M[I]=t[I];p.clippingState=M,this.numIntersection=y?this.numPlanes:0,this.numPlanes+=b}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,h,f,g){let y=d!==null?d.length:0,m=null;if(y!==0){if(m=c.value,g!==!0||m===null){let p=f+y*4,b=h.matrixWorldInverse;a.getNormalMatrix(b),(m===null||m.length<p)&&(m=new Float32Array(p));for(let w=0,M=f;w!==y;++w,M+=4)o.copy(d[w]).applyMatrix4(b,a),o.normal.toArray(m,M),m[M+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=y,e.numIntersection=0,m}}var ds=4,dE=[.125,.215,.35,.446,.526,.582],oo=20,oL=256,El=new Sa,hE=new qe,Ly=null,Fy=0,ky=0,Uy=!1,aL=new N,Zf=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=aL}=s;Ly=this._renderer.getRenderTarget(),Fy=this._renderer.getActiveCubeFace(),ky=this._renderer.getActiveMipmapLevel(),Uy=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=mE(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=pE(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(Ly,Fy,ky),this._renderer.xr.enabled=Uy,e.scissorTest=!1,Ca(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===as||e.mapping===ro?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),Ly=this._renderer.getRenderTarget(),Fy=this._renderer.getActiveCubeFace(),ky=this._renderer.getActiveMipmapLevel(),Uy=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:fn,minFilter:fn,generateMipmaps:!1,type:Xi,format:ui,colorSpace:Zc,depthBuffer:!1},r=fE(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=fE(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=cL(s)),this._blurMaterial=uL(s,e,t),this._ggxMaterial=lL(s,e,t)}return r}_compileMaterial(e){let t=new dt(new Sn,e);this._renderer.compile(t,El)}_sceneToCubeUV(e,t,i,r,s){let c=new hn(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,h=d.autoClear,f=d.toneMapping;d.getClearColor(hE),d.toneMapping=wi,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new dt(new $t,new no({name:"PMREM.Background",side:An,depthWrite:!1,depthTest:!1})));let y=this._backgroundBox,m=y.material,p=!1,b=e.background;b?b.isColor&&(m.color.copy(b),e.background=null,p=!0):(m.color.copy(hE),p=!0);for(let w=0;w<6;w++){let M=w%3;M===0?(c.up.set(0,l[w],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[w],s.y,s.z)):M===1?(c.up.set(0,0,l[w]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[w],s.z)):(c.up.set(0,l[w],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[w]));let I=this._cubeSize;Ca(r,M*I,w>2?I:0,I,I),d.setRenderTarget(r),p&&d.render(y,c),d.render(e,c)}d.toneMapping=f,d.autoClear=h,e.background=b}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===as||e.mapping===ro;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=mE()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=pE());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Ca(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,El)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),h=0+l*1.25,f=d*h,{_lodMax:g}=this,y=this._sizeLods[i],m=3*y*(i>g-ds?i-g+ds:0),p=4*(this._cubeSize-y);c.envMap.value=e.texture,c.roughness.value=f,c.mipInt.value=g-t,Ca(s,m,p,3*y,2*y),r.setRenderTarget(s),r.render(a,El),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=g-i,Ca(e,m,p,3*y,2*y),r.setRenderTarget(e),r.render(a,El)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&Re("blur direction must be either latitudinal or longitudinal!");let u=3,d=this._lodMeshes[r];d.material=l;let h=l.uniforms,f=this._sizeLods[i]-1,g=isFinite(s)?Math.PI/(2*f):2*Math.PI/(2*oo-1),y=s/g,m=isFinite(s)?1+Math.floor(u*y):oo;m>oo&&De(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${oo}`);let p=[],b=0;for(let D=0;D<oo;++D){let _=D/y,T=Math.exp(-_*_/2);p.push(T),D===0?b+=T:D<m&&(b+=2*T)}for(let D=0;D<p.length;D++)p[D]=p[D]/b;h.envMap.value=e.texture,h.samples.value=m,h.weights.value=p,h.latitudinal.value=o==="latitudinal",a&&(h.poleAxis.value=a);let{_lodMax:w}=this;h.dTheta.value=g,h.mipInt.value=w-i;let M=this._sizeLods[r],I=3*M*(r>w-ds?r-w+ds:0),E=4*(this._cubeSize-M);Ca(t,I,E,3*M,2*M),c.setRenderTarget(t),c.render(d,El)}};function cL(n){let e=[],t=[],i=[],r=n,s=n-ds+1+dE.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-ds?c=dE[o-n+ds-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,d=1+l,h=[u,u,d,u,d,d,u,u,d,d,u,d],f=6,g=6,y=3,m=2,p=1,b=new Float32Array(y*g*f),w=new Float32Array(m*g*f),M=new Float32Array(p*g*f);for(let E=0;E<f;E++){let D=E%3*2/3-1,_=E>2?0:-1,T=[D,_,0,D+2/3,_,0,D+2/3,_+1,0,D,_,0,D+2/3,_+1,0,D,_+1,0];b.set(T,y*g*E),w.set(h,m*g*E);let O=[E,E,E,E,E,E];M.set(O,p*g*E)}let I=new Sn;I.setAttribute("position",new $n(b,y)),I.setAttribute("uv",new $n(w,m)),I.setAttribute("faceIndex",new $n(M,p)),i.push(new dt(I,null)),r>ds&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function fE(n,e,t){let i=new Xn(n,e,t);return i.texture.mapping=gl,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Ca(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function lL(n,e,t){return new qn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:oL,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:Qf(),fragmentShader:`

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
		`,blending:$i,depthTest:!1,depthWrite:!1})}function uL(n,e,t){let i=new Float32Array(oo),r=new N(0,1,0);return new qn({name:"SphericalGaussianBlur",defines:{n:oo,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:Qf(),fragmentShader:`

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
		`,blending:$i,depthTest:!1,depthWrite:!1})}function pE(){return new qn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:Qf(),fragmentShader:`

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
		`,blending:$i,depthTest:!1,depthWrite:!1})}function mE(){return new qn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:Qf(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:$i,depthTest:!1,depthWrite:!1})}function Qf(){return`

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
	`}var Kf=class extends Xn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new ol(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

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
			`},r=new $t(5,5,5),s=new qn({name:"CubemapFromEquirect",uniforms:so(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:An,blending:$i});s.uniforms.tEquirect.value=t;let o=new dt(r,s),a=t.minFilter;return t.minFilter===cs&&(t.minFilter=fn),new nf(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function dL(n){let e=new WeakMap,t=new WeakMap,i=null;function r(h,f=!1){return h==null?null:f?o(h):s(h)}function s(h){if(h&&h.isTexture){let f=h.mapping;if(f===of||f===af)if(e.has(h)){let g=e.get(h).texture;return a(g,h.mapping)}else{let g=h.image;if(g&&g.height>0){let y=new Kf(g.height);return y.fromEquirectangularTexture(n,h),e.set(h,y),h.addEventListener("dispose",l),a(y.texture,h.mapping)}else return null}}return h}function o(h){if(h&&h.isTexture){let f=h.mapping,g=f===of||f===af,y=f===as||f===ro;if(g||y){let m=t.get(h),p=m!==void 0?m.texture.pmremVersion:0;if(h.isRenderTargetTexture&&h.pmremVersion!==p)return i===null&&(i=new Zf(n)),m=g?i.fromEquirectangular(h,m):i.fromCubemap(h,m),m.texture.pmremVersion=h.pmremVersion,t.set(h,m),m.texture;if(m!==void 0)return m.texture;{let b=h.image;return g&&b&&b.height>0||y&&b&&c(b)?(i===null&&(i=new Zf(n)),m=g?i.fromEquirectangular(h):i.fromCubemap(h),m.texture.pmremVersion=h.pmremVersion,t.set(h,m),h.addEventListener("dispose",u),m.texture):null}}}return h}function a(h,f){return f===of?h.mapping=as:f===af&&(h.mapping=ro),h}function c(h){let f=0,g=6;for(let y=0;y<g;y++)h[y]!==void 0&&f++;return f===g}function l(h){let f=h.target;f.removeEventListener("dispose",l);let g=e.get(f);g!==void 0&&(e.delete(f),g.dispose())}function u(h){let f=h.target;f.removeEventListener("dispose",u);let g=t.get(f);g!==void 0&&(t.delete(f),g.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function hL(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Lh("WebGLRenderer: "+i+" extension not supported."),r}}}function fL(n,e,t,i){let r={},s=new WeakMap;function o(d){let h=d.target;h.index!==null&&e.remove(h.index);for(let g in h.attributes)e.remove(h.attributes[g]);h.removeEventListener("dispose",o),delete r[h.id];let f=s.get(h);f&&(e.remove(f),s.delete(h)),i.releaseStatesOfGeometry(h),h.isInstancedBufferGeometry===!0&&delete h._maxInstanceCount,t.memory.geometries--}function a(d,h){return r[h.id]===!0||(h.addEventListener("dispose",o),r[h.id]=!0,t.memory.geometries++),h}function c(d){let h=d.attributes;for(let f in h)e.update(h[f],n.ARRAY_BUFFER)}function l(d){let h=[],f=d.index,g=d.attributes.position,y=0;if(g===void 0)return;if(f!==null){let b=f.array;y=f.version;for(let w=0,M=b.length;w<M;w+=3){let I=b[w+0],E=b[w+1],D=b[w+2];h.push(I,E,E,D,D,I)}}else{let b=g.array;y=g.version;for(let w=0,M=b.length/3-1;w<M;w+=3){let I=w+0,E=w+1,D=w+2;h.push(I,E,E,D,D,I)}}let m=new(g.count>=65535?rl:il)(h,1);m.version=y;let p=s.get(d);p&&e.remove(p),s.set(d,m)}function u(d){let h=s.get(d);if(h){let f=d.index;f!==null&&h.version<f.version&&l(d)}else l(d);return s.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function pL(n,e,t){let i;function r(d){i=d}let s,o;function a(d){s=d.type,o=d.bytesPerElement}function c(d,h){n.drawElements(i,h,s,d*o),t.update(h,i,1)}function l(d,h,f){f!==0&&(n.drawElementsInstanced(i,h,s,d*o,f),t.update(h,i,f))}function u(d,h,f){if(f===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,s,d,0,f);let y=0;for(let m=0;m<f;m++)y+=h[m];t.update(y,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function mL(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:Re("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function gL(n,e,t){let i=new WeakMap,r=new Ot;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,h=i.get(a);if(h===void 0||h.count!==d){let O=function(){_.dispose(),i.delete(a),a.removeEventListener("dispose",O)};var f=O;h!==void 0&&h.texture.dispose();let g=a.morphAttributes.position!==void 0,y=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],b=a.morphAttributes.normal||[],w=a.morphAttributes.color||[],M=0;g===!0&&(M=1),y===!0&&(M=2),m===!0&&(M=3);let I=a.attributes.position.count*M,E=1;I>e.maxTextureSize&&(E=Math.ceil(I/e.maxTextureSize),I=e.maxTextureSize);let D=new Float32Array(I*E*4*d),_=new Qc(D,I,E,d);_.type=Ci,_.needsUpdate=!0;let T=M*4;for(let C=0;C<d;C++){let F=p[C],G=b[C],W=w[C],P=I*E*4*C;for(let H=0;H<F.count;H++){let U=H*T;g===!0&&(r.fromBufferAttribute(F,H),D[P+U+0]=r.x,D[P+U+1]=r.y,D[P+U+2]=r.z,D[P+U+3]=0),y===!0&&(r.fromBufferAttribute(G,H),D[P+U+4]=r.x,D[P+U+5]=r.y,D[P+U+6]=r.z,D[P+U+7]=0),m===!0&&(r.fromBufferAttribute(W,H),D[P+U+8]=r.x,D[P+U+9]=r.y,D[P+U+10]=r.z,D[P+U+11]=W.itemSize===4?r.w:1)}}h={count:d,texture:_,size:new rt(I,E)},i.set(a,h),a.addEventListener("dispose",O)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let g=0;for(let m=0;m<l.length;m++)g+=l[m];let y=a.morphTargetsRelative?1:1-g;c.getUniforms().setValue(n,"morphTargetBaseInfluence",y),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",h.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",h.size)}return{update:s}}function vL(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,d=l.geometry,h=e.get(l,d);if(s.get(h)!==u&&(e.update(h),s.set(h,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let f=l.skeleton;s.get(f)!==u&&(f.update(),s.set(f,u))}return h}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var yL={[my]:"LINEAR_TONE_MAPPING",[gy]:"REINHARD_TONE_MAPPING",[vy]:"CINEON_TONE_MAPPING",[ml]:"ACES_FILMIC_TONE_MAPPING",[_y]:"AGX_TONE_MAPPING",[xy]:"NEUTRAL_TONE_MAPPING",[yy]:"CUSTOM_TONE_MAPPING"};function _L(n,e,t,i,r){let s=new Xn(e,t,{type:n,depthBuffer:i,stencilBuffer:r,depthTexture:i?new br(e,t):void 0}),o=new Xn(e,t,{type:Xi,depthBuffer:!1,stencilBuffer:!1}),a=new Sn;a.setAttribute("position",new At([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new At([0,2,0,0,2,0],2));let c=new jh({uniforms:{tDiffuse:{value:null}},vertexShader:`
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
			}`,depthTest:!1,depthWrite:!1}),l=new dt(a,c),u=new Sa(-1,1,1,-1,0,1),d=null,h=null,f=!1,g,y=null,m=[],p=!1;this.setSize=function(b,w){s.setSize(b,w),o.setSize(b,w);for(let M=0;M<m.length;M++){let I=m[M];I.setSize&&I.setSize(b,w)}},this.setEffects=function(b){m=b,p=m.length>0&&m[0].isRenderPass===!0;let w=s.width,M=s.height;for(let I=0;I<m.length;I++){let E=m[I];E.setSize&&E.setSize(w,M)}},this.begin=function(b,w){if(f||b.toneMapping===wi&&m.length===0)return!1;if(y=w,w!==null){let M=w.width,I=w.height;(s.width!==M||s.height!==I)&&this.setSize(M,I)}return p===!1&&b.setRenderTarget(s),g=b.toneMapping,b.toneMapping=wi,!0},this.hasRenderPass=function(){return p},this.end=function(b,w){b.toneMapping=g,f=!0;let M=s,I=o;for(let E=0;E<m.length;E++){let D=m[E];if(D.enabled!==!1&&(D.render(b,I,M,w),D.needsSwap!==!1)){let _=M;M=I,I=_}}if(d!==b.outputColorSpace||h!==b.toneMapping){d=b.outputColorSpace,h=b.toneMapping,c.defines={},et.getTransfer(d)===pt&&(c.defines.SRGB_TRANSFER="");let E=yL[h];E&&(c.defines[E]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=M.texture,b.setRenderTarget(y),b.render(l,u),y=null,f=!1},this.isCompositing=function(){return f},this.dispose=function(){s.depthTexture&&s.depthTexture.dispose(),s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var LE=new Mr,Hy=new br(1,1),FE=new Qc,kE=new Uh,UE=new ol,gE=[],vE=[],yE=new Float32Array(16),_E=new Float32Array(9),xE=new Float32Array(4);function Ia(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=gE[r];if(s===void 0&&(s=new Float32Array(r),gE[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function Jt(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Qt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function ep(n,e){let t=vE[e];t===void 0&&(t=new Int32Array(e),vE[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function xL(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function bL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Jt(t,e))return;n.uniform2fv(this.addr,e),Qt(t,e)}}function SL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(Jt(t,e))return;n.uniform3fv(this.addr,e),Qt(t,e)}}function ML(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Jt(t,e))return;n.uniform4fv(this.addr,e),Qt(t,e)}}function EL(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Jt(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Qt(t,e)}else{if(Jt(t,i))return;xE.set(i),n.uniformMatrix2fv(this.addr,!1,xE),Qt(t,i)}}function wL(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Jt(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Qt(t,e)}else{if(Jt(t,i))return;_E.set(i),n.uniformMatrix3fv(this.addr,!1,_E),Qt(t,i)}}function TL(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(Jt(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Qt(t,e)}else{if(Jt(t,i))return;yE.set(i),n.uniformMatrix4fv(this.addr,!1,yE),Qt(t,i)}}function CL(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function DL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Jt(t,e))return;n.uniform2iv(this.addr,e),Qt(t,e)}}function IL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Jt(t,e))return;n.uniform3iv(this.addr,e),Qt(t,e)}}function AL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Jt(t,e))return;n.uniform4iv(this.addr,e),Qt(t,e)}}function RL(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function NL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(Jt(t,e))return;n.uniform2uiv(this.addr,e),Qt(t,e)}}function PL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(Jt(t,e))return;n.uniform3uiv(this.addr,e),Qt(t,e)}}function OL(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(Jt(t,e))return;n.uniform4uiv(this.addr,e),Qt(t,e)}}function LL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(Hy.compareFunction=t.isReversedDepthBuffer()?Xf:$f,s=Hy):s=LE,t.setTexture2D(e||s,r)}function FL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||kE,r)}function kL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||UE,r)}function UL(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||FE,r)}function BL(n){switch(n){case 5126:return xL;case 35664:return bL;case 35665:return SL;case 35666:return ML;case 35674:return EL;case 35675:return wL;case 35676:return TL;case 5124:case 35670:return CL;case 35667:case 35671:return DL;case 35668:case 35672:return IL;case 35669:case 35673:return AL;case 5125:return RL;case 36294:return NL;case 36295:return PL;case 36296:return OL;case 35678:case 36198:case 36298:case 36306:case 35682:return LL;case 35679:case 36299:case 36307:return FL;case 35680:case 36300:case 36308:case 36293:return kL;case 36289:case 36303:case 36311:case 36292:return UL}}function VL(n,e){n.uniform1fv(this.addr,e)}function HL(n,e){let t=Ia(e,this.size,2);n.uniform2fv(this.addr,t)}function zL(n,e){let t=Ia(e,this.size,3);n.uniform3fv(this.addr,t)}function GL(n,e){let t=Ia(e,this.size,4);n.uniform4fv(this.addr,t)}function jL(n,e){let t=Ia(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function WL(n,e){let t=Ia(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function $L(n,e){let t=Ia(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function XL(n,e){n.uniform1iv(this.addr,e)}function qL(n,e){n.uniform2iv(this.addr,e)}function YL(n,e){n.uniform3iv(this.addr,e)}function ZL(n,e){n.uniform4iv(this.addr,e)}function KL(n,e){n.uniform1uiv(this.addr,e)}function JL(n,e){n.uniform2uiv(this.addr,e)}function QL(n,e){n.uniform3uiv(this.addr,e)}function eF(n,e){n.uniform4uiv(this.addr,e)}function tF(n,e,t){let i=this.cache,r=e.length,s=ep(t,r);Jt(i,s)||(n.uniform1iv(this.addr,s),Qt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=Hy:o=LE;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function nF(n,e,t){let i=this.cache,r=e.length,s=ep(t,r);Jt(i,s)||(n.uniform1iv(this.addr,s),Qt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||kE,s[o])}function iF(n,e,t){let i=this.cache,r=e.length,s=ep(t,r);Jt(i,s)||(n.uniform1iv(this.addr,s),Qt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||UE,s[o])}function rF(n,e,t){let i=this.cache,r=e.length,s=ep(t,r);Jt(i,s)||(n.uniform1iv(this.addr,s),Qt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||FE,s[o])}function sF(n){switch(n){case 5126:return VL;case 35664:return HL;case 35665:return zL;case 35666:return GL;case 35674:return jL;case 35675:return WL;case 35676:return $L;case 5124:case 35670:return XL;case 35667:case 35671:return qL;case 35668:case 35672:return YL;case 35669:case 35673:return ZL;case 5125:return KL;case 36294:return JL;case 36295:return QL;case 36296:return eF;case 35678:case 36198:case 36298:case 36306:case 35682:return tF;case 35679:case 36299:case 36307:return nF;case 35680:case 36300:case 36308:case 36293:return iF;case 36289:case 36303:case 36311:case 36292:return rF}}var zy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=BL(t.type)}},Gy=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=sF(t.type)}},jy=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},By=/(\w+)(\])?(\[|\.)?/g;function bE(n,e){n.seq.push(e),n.map[e.id]=e}function oF(n,e,t){let i=n.name,r=i.length;for(By.lastIndex=0;;){let s=By.exec(i),o=By.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){bE(t,l===void 0?new zy(a,n,e):new Gy(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new jy(a),bE(t,d)),t=d}}}var Da=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);oF(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function SE(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var aF=37297,cF=0;function lF(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var ME=new Fe;function uF(n){et._getMatrix(ME,et.workingColorSpace,n);let e=`mat3( ${ME.elements.map(t=>t.toFixed(4))} )`;switch(et.getTransfer(n)){case Kc:return[e,"LinearTransferOETF"];case pt:return[e,"sRGBTransferOETF"];default:return De("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function EE(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+lF(n.getShaderSource(e),a)}else return s}function dF(n,e){let t=uF(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var hF={[my]:"Linear",[gy]:"Reinhard",[vy]:"Cineon",[ml]:"ACESFilmic",[_y]:"AgX",[xy]:"Neutral",[yy]:"Custom"};function fF(n,e){let t=hF[e];return t===void 0?(De("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var Yf=new N;function pF(){et.getLuminanceCoefficients(Yf);let n=Yf.x.toFixed(4),e=Yf.y.toFixed(4),t=Yf.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function mF(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Tl).join(`
`)}function gF(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function vF(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function Tl(n){return n!==""}function wE(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function TE(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var yF=/^[ \t]*#include +<([\w\d./]+)>/gm;function Wy(n){return n.replace(yF,xF)}var _F=new Map;function xF(n,e){let t=$e[e];if(t===void 0){let i=_F.get(e);if(i!==void 0)t=$e[i],De('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Wy(t)}var bF=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function CE(n){return n.replace(bF,SF)}function SF(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function DE(n){let e=`precision ${n.precision} float;
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
#define LOW_PRECISION`),e}var MF={[pl]:"SHADOWMAP_TYPE_PCF",[Ma]:"SHADOWMAP_TYPE_VSM"};function EF(n){return MF[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var wF={[as]:"ENVMAP_TYPE_CUBE",[ro]:"ENVMAP_TYPE_CUBE",[gl]:"ENVMAP_TYPE_CUBE_UV"};function TF(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":wF[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var CF={[ro]:"ENVMAP_MODE_REFRACTION"};function DF(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":CF[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var IF={[py]:"ENVMAP_BLENDING_MULTIPLY",[qM]:"ENVMAP_BLENDING_MIX",[YM]:"ENVMAP_BLENDING_ADD"};function AF(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":IF[n.combine]||"ENVMAP_BLENDING_NONE"}function RF(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function NF(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=EF(t),l=TF(t),u=DF(t),d=AF(t),h=RF(t),f=mF(t),g=gF(s),y=r.createProgram(),m,p,b=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Tl).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g].filter(Tl).join(`
`),p.length>0&&(p+=`
`)):(m=[DE(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Tl).join(`
`),p=[DE(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,g,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",h?"#define CUBEUV_TEXEL_WIDTH "+h.texelWidth:"",h?"#define CUBEUV_TEXEL_HEIGHT "+h.texelHeight:"",h?"#define CUBEUV_MAX_MIP "+h.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==wi?"#define TONE_MAPPING":"",t.toneMapping!==wi?$e.tonemapping_pars_fragment:"",t.toneMapping!==wi?fF("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",$e.colorspace_pars_fragment,dF("linearToOutputTexel",t.outputColorSpace),pF(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Tl).join(`
`)),o=Wy(o),o=wE(o,t),o=TE(o,t),a=Wy(a),a=wE(a,t),a=TE(a,t),o=CE(o),a=CE(a),t.isRawShaderMaterial!==!0&&(b=`#version 300 es
`,m=[f,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===Dy?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Dy?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let w=b+m+o,M=b+p+a,I=SE(r,r.VERTEX_SHADER,w),E=SE(r,r.FRAGMENT_SHADER,M);r.attachShader(y,I),r.attachShader(y,E),t.index0AttributeName!==void 0?r.bindAttribLocation(y,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(y,0,"position"),r.linkProgram(y);function D(C){if(n.debug.checkShaderErrors){let F=r.getProgramInfoLog(y)||"",G=r.getShaderInfoLog(I)||"",W=r.getShaderInfoLog(E)||"",P=F.trim(),H=G.trim(),U=W.trim(),Q=!0,ee=!0;if(r.getProgramParameter(y,r.LINK_STATUS)===!1)if(Q=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,y,I,E);else{let ue=EE(r,I,"vertex"),xe=EE(r,E,"fragment");Re("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(y,r.VALIDATE_STATUS)+`

Material Name: `+C.name+`
Material Type: `+C.type+`

Program Info Log: `+P+`
`+ue+`
`+xe)}else P!==""?De("WebGLProgram: Program Info Log:",P):(H===""||U==="")&&(ee=!1);ee&&(C.diagnostics={runnable:Q,programLog:P,vertexShader:{log:H,prefix:m},fragmentShader:{log:U,prefix:p}})}r.deleteShader(I),r.deleteShader(E),_=new Da(r,y),T=vF(r,y)}let _;this.getUniforms=function(){return _===void 0&&D(this),_};let T;this.getAttributes=function(){return T===void 0&&D(this),T};let O=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return O===!1&&(O=r.getProgramParameter(y,aF)),O},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(y),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=cF++,this.cacheKey=e,this.usedTimes=1,this.program=y,this.vertexShader=I,this.fragmentShader=E,this}var PF=0,$y=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new Xy(e),t.set(e,i)),i}},Xy=class{constructor(e){this.id=PF++,this.code=e,this.usedTimes=0}};function OF(n){return n===us||n===Sl||n===Ml}function LF(n,e,t,i,r,s){let o=new el,a=new $y,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,h=i.precision,f={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function g(_){return c.add(_),_===0?"uv":`uv${_}`}function y(_,T,O,C,F,G){let W=C.fog,P=F.geometry,H=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?C.environment:null,U=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap,Q=e.get(_.envMap||H,U),ee=Q&&Q.mapping===gl?Q.image.height:null,ue=f[_.type];_.precision!==null&&(h=i.getMaxPrecision(_.precision),h!==_.precision&&De("WebGLProgram.getParameters:",_.precision,"not supported, using",h,"instead."));let xe=P.morphAttributes.position||P.morphAttributes.normal||P.morphAttributes.color,Te=xe!==void 0?xe.length:0,nt=0;P.morphAttributes.position!==void 0&&(nt=1),P.morphAttributes.normal!==void 0&&(nt=2),P.morphAttributes.color!==void 0&&(nt=3);let gt,Ge,Y,pe;if(ue){let Be=Yi[ue];gt=Be.vertexShader,Ge=Be.fragmentShader}else gt=_.vertexShader,Ge=_.fragmentShader,a.update(_),Y=a.getVertexShaderID(_),pe=a.getFragmentShaderID(_);let re=n.getRenderTarget(),Ae=n.state.buffers.depth.getReversed(),ke=F.isInstancedMesh===!0,Ne=F.isBatchedMesh===!0,Rt=!!_.map,Ke=!!_.matcap,vt=!!Q,It=!!_.aoMap,Ye=!!_.lightMap,Xt=!!_.bumpMap,Nt=!!_.normalMap,kn=!!_.displacementMap,R=!!_.emissiveMap,qt=!!_.metalnessMap,Je=!!_.roughnessMap,Tt=_.anisotropy>0,ce=_.clearcoat>0,Lt=_.dispersion>0,S=_.iridescence>0,v=_.sheen>0,k=_.transmission>0,X=Tt&&!!_.anisotropyMap,K=ce&&!!_.clearcoatMap,te=ce&&!!_.clearcoatNormalMap,ae=ce&&!!_.clearcoatRoughnessMap,j=S&&!!_.iridescenceMap,q=S&&!!_.iridescenceThicknessMap,me=v&&!!_.sheenColorMap,ye=v&&!!_.sheenRoughnessMap,se=!!_.specularMap,ne=!!_.specularColorMap,Oe=!!_.specularIntensityMap,je=k&&!!_.transmissionMap,st=k&&!!_.thicknessMap,A=!!_.gradientMap,ie=!!_.alphaMap,$=_.alphaTest>0,ge=!!_.alphaHash,oe=!!_.extensions,Z=wi;_.toneMapped&&(re===null||re.isXRRenderTarget===!0)&&(Z=n.toneMapping);let Se={shaderID:ue,shaderType:_.type,shaderName:_.name,vertexShader:gt,fragmentShader:Ge,defines:_.defines,customVertexShaderID:Y,customFragmentShaderID:pe,isRawShaderMaterial:_.isRawShaderMaterial===!0,glslVersion:_.glslVersion,precision:h,batching:Ne,batchingColor:Ne&&F._colorsTexture!==null,instancing:ke,instancingColor:ke&&F.instanceColor!==null,instancingMorph:ke&&F.morphTexture!==null,outputColorSpace:re===null?n.outputColorSpace:re.isXRRenderTarget===!0?re.texture.colorSpace:et.workingColorSpace,alphaToCoverage:!!_.alphaToCoverage,map:Rt,matcap:Ke,envMap:vt,envMapMode:vt&&Q.mapping,envMapCubeUVHeight:ee,aoMap:It,lightMap:Ye,bumpMap:Xt,normalMap:Nt,displacementMap:kn,emissiveMap:R,normalMapObjectSpace:Nt&&_.normalMapType===JM,normalMapTangentSpace:Nt&&_.normalMapType===Wf,packedNormalMap:Nt&&_.normalMapType===Wf&&OF(_.normalMap.format),metalnessMap:qt,roughnessMap:Je,anisotropy:Tt,anisotropyMap:X,clearcoat:ce,clearcoatMap:K,clearcoatNormalMap:te,clearcoatRoughnessMap:ae,dispersion:Lt,iridescence:S,iridescenceMap:j,iridescenceThicknessMap:q,sheen:v,sheenColorMap:me,sheenRoughnessMap:ye,specularMap:se,specularColorMap:ne,specularIntensityMap:Oe,transmission:k,transmissionMap:je,thicknessMap:st,gradientMap:A,opaque:_.transparent===!1&&_.blending===Qs&&_.alphaToCoverage===!1,alphaMap:ie,alphaTest:$,alphaHash:ge,combine:_.combine,mapUv:Rt&&g(_.map.channel),aoMapUv:It&&g(_.aoMap.channel),lightMapUv:Ye&&g(_.lightMap.channel),bumpMapUv:Xt&&g(_.bumpMap.channel),normalMapUv:Nt&&g(_.normalMap.channel),displacementMapUv:kn&&g(_.displacementMap.channel),emissiveMapUv:R&&g(_.emissiveMap.channel),metalnessMapUv:qt&&g(_.metalnessMap.channel),roughnessMapUv:Je&&g(_.roughnessMap.channel),anisotropyMapUv:X&&g(_.anisotropyMap.channel),clearcoatMapUv:K&&g(_.clearcoatMap.channel),clearcoatNormalMapUv:te&&g(_.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:ae&&g(_.clearcoatRoughnessMap.channel),iridescenceMapUv:j&&g(_.iridescenceMap.channel),iridescenceThicknessMapUv:q&&g(_.iridescenceThicknessMap.channel),sheenColorMapUv:me&&g(_.sheenColorMap.channel),sheenRoughnessMapUv:ye&&g(_.sheenRoughnessMap.channel),specularMapUv:se&&g(_.specularMap.channel),specularColorMapUv:ne&&g(_.specularColorMap.channel),specularIntensityMapUv:Oe&&g(_.specularIntensityMap.channel),transmissionMapUv:je&&g(_.transmissionMap.channel),thicknessMapUv:st&&g(_.thicknessMap.channel),alphaMapUv:ie&&g(_.alphaMap.channel),vertexTangents:!!P.attributes.tangent&&(Nt||Tt),vertexNormals:!!P.attributes.normal,vertexColors:_.vertexColors,vertexAlphas:_.vertexColors===!0&&!!P.attributes.color&&P.attributes.color.itemSize===4,pointsUvs:F.isPoints===!0&&!!P.attributes.uv&&(Rt||ie),fog:!!W,useFog:_.fog===!0,fogExp2:!!W&&W.isFogExp2,flatShading:_.wireframe===!1&&(_.flatShading===!0||P.attributes.normal===void 0&&Nt===!1&&(_.isMeshLambertMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isMeshPhysicalMaterial)),sizeAttenuation:_.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:Ae,skinning:F.isSkinnedMesh===!0,morphTargets:P.morphAttributes.position!==void 0,morphNormals:P.morphAttributes.normal!==void 0,morphColors:P.morphAttributes.color!==void 0,morphTargetsCount:Te,morphTextureStride:nt,numDirLights:T.directional.length,numPointLights:T.point.length,numSpotLights:T.spot.length,numSpotLightMaps:T.spotLightMap.length,numRectAreaLights:T.rectArea.length,numHemiLights:T.hemi.length,numDirLightShadows:T.directionalShadowMap.length,numPointLightShadows:T.pointShadowMap.length,numSpotLightShadows:T.spotShadowMap.length,numSpotLightShadowsWithMaps:T.numSpotLightShadowsWithMaps,numLightProbes:T.numLightProbes,numLightProbeGrids:G.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:_.dithering,shadowMapEnabled:n.shadowMap.enabled&&O.length>0,shadowMapType:n.shadowMap.type,toneMapping:Z,decodeVideoTexture:Rt&&_.map.isVideoTexture===!0&&et.getTransfer(_.map.colorSpace)===pt,decodeVideoTextureEmissive:R&&_.emissiveMap.isVideoTexture===!0&&et.getTransfer(_.emissiveMap.colorSpace)===pt,premultipliedAlpha:_.premultipliedAlpha,doubleSided:_.side===Wi,flipSided:_.side===An,useDepthPacking:_.depthPacking>=0,depthPacking:_.depthPacking||0,index0AttributeName:_.index0AttributeName,extensionClipCullDistance:oe&&_.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(oe&&_.extensions.multiDraw===!0||Ne)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:_.customProgramCacheKey()};return Se.vertexUv1s=c.has(1),Se.vertexUv2s=c.has(2),Se.vertexUv3s=c.has(3),c.clear(),Se}function m(_){let T=[];if(_.shaderID?T.push(_.shaderID):(T.push(_.customVertexShaderID),T.push(_.customFragmentShaderID)),_.defines!==void 0)for(let O in _.defines)T.push(O),T.push(_.defines[O]);return _.isRawShaderMaterial===!1&&(p(T,_),b(T,_),T.push(n.outputColorSpace)),T.push(_.customProgramCacheKey),T.join()}function p(_,T){_.push(T.precision),_.push(T.outputColorSpace),_.push(T.envMapMode),_.push(T.envMapCubeUVHeight),_.push(T.mapUv),_.push(T.alphaMapUv),_.push(T.lightMapUv),_.push(T.aoMapUv),_.push(T.bumpMapUv),_.push(T.normalMapUv),_.push(T.displacementMapUv),_.push(T.emissiveMapUv),_.push(T.metalnessMapUv),_.push(T.roughnessMapUv),_.push(T.anisotropyMapUv),_.push(T.clearcoatMapUv),_.push(T.clearcoatNormalMapUv),_.push(T.clearcoatRoughnessMapUv),_.push(T.iridescenceMapUv),_.push(T.iridescenceThicknessMapUv),_.push(T.sheenColorMapUv),_.push(T.sheenRoughnessMapUv),_.push(T.specularMapUv),_.push(T.specularColorMapUv),_.push(T.specularIntensityMapUv),_.push(T.transmissionMapUv),_.push(T.thicknessMapUv),_.push(T.combine),_.push(T.fogExp2),_.push(T.sizeAttenuation),_.push(T.morphTargetsCount),_.push(T.morphAttributeCount),_.push(T.numDirLights),_.push(T.numPointLights),_.push(T.numSpotLights),_.push(T.numSpotLightMaps),_.push(T.numHemiLights),_.push(T.numRectAreaLights),_.push(T.numDirLightShadows),_.push(T.numPointLightShadows),_.push(T.numSpotLightShadows),_.push(T.numSpotLightShadowsWithMaps),_.push(T.numLightProbes),_.push(T.shadowMapType),_.push(T.toneMapping),_.push(T.numClippingPlanes),_.push(T.numClipIntersection),_.push(T.depthPacking)}function b(_,T){o.disableAll(),T.instancing&&o.enable(0),T.instancingColor&&o.enable(1),T.instancingMorph&&o.enable(2),T.matcap&&o.enable(3),T.envMap&&o.enable(4),T.normalMapObjectSpace&&o.enable(5),T.normalMapTangentSpace&&o.enable(6),T.clearcoat&&o.enable(7),T.iridescence&&o.enable(8),T.alphaTest&&o.enable(9),T.vertexColors&&o.enable(10),T.vertexAlphas&&o.enable(11),T.vertexUv1s&&o.enable(12),T.vertexUv2s&&o.enable(13),T.vertexUv3s&&o.enable(14),T.vertexTangents&&o.enable(15),T.anisotropy&&o.enable(16),T.alphaHash&&o.enable(17),T.batching&&o.enable(18),T.dispersion&&o.enable(19),T.batchingColor&&o.enable(20),T.gradientMap&&o.enable(21),T.packedNormalMap&&o.enable(22),T.vertexNormals&&o.enable(23),_.push(o.mask),o.disableAll(),T.fog&&o.enable(0),T.useFog&&o.enable(1),T.flatShading&&o.enable(2),T.logarithmicDepthBuffer&&o.enable(3),T.reversedDepthBuffer&&o.enable(4),T.skinning&&o.enable(5),T.morphTargets&&o.enable(6),T.morphNormals&&o.enable(7),T.morphColors&&o.enable(8),T.premultipliedAlpha&&o.enable(9),T.shadowMapEnabled&&o.enable(10),T.doubleSided&&o.enable(11),T.flipSided&&o.enable(12),T.useDepthPacking&&o.enable(13),T.dithering&&o.enable(14),T.transmission&&o.enable(15),T.sheen&&o.enable(16),T.opaque&&o.enable(17),T.pointsUvs&&o.enable(18),T.decodeVideoTexture&&o.enable(19),T.decodeVideoTextureEmissive&&o.enable(20),T.alphaToCoverage&&o.enable(21),T.numLightProbeGrids>0&&o.enable(22),_.push(o.mask)}function w(_){let T=f[_.type],O;if(T){let C=Yi[T];O=uE.clone(C.uniforms)}else O=_.uniforms;return O}function M(_,T){let O=u.get(T);return O!==void 0?++O.usedTimes:(O=new NF(n,T,_,r),l.push(O),u.set(T,O)),O}function I(_){if(--_.usedTimes===0){let T=l.indexOf(_);l[T]=l[l.length-1],l.pop(),u.delete(_.cacheKey),_.destroy()}}function E(_){a.remove(_)}function D(){a.dispose()}return{getParameters:y,getProgramCacheKey:m,getUniforms:w,acquireProgram:M,releaseProgram:I,releaseShaderCache:E,programs:l,dispose:D}}function FF(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function kF(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function IE(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function AE(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(h){let f=0;return h.isInstancedMesh&&(f+=2),h.isSkinnedMesh&&(f+=1),f}function a(h,f,g,y,m,p){let b=n[e];return b===void 0?(b={id:h.id,object:h,geometry:f,material:g,materialVariant:o(h),groupOrder:y,renderOrder:h.renderOrder,z:m,group:p},n[e]=b):(b.id=h.id,b.object=h,b.geometry=f,b.material=g,b.materialVariant=o(h),b.groupOrder=y,b.renderOrder=h.renderOrder,b.z=m,b.group=p),e++,b}function c(h,f,g,y,m,p){let b=a(h,f,g,y,m,p);g.transmission>0?i.push(b):g.transparent===!0?r.push(b):t.push(b)}function l(h,f,g,y,m,p){let b=a(h,f,g,y,m,p);g.transmission>0?i.unshift(b):g.transparent===!0?r.unshift(b):t.unshift(b)}function u(h,f){t.length>1&&t.sort(h||kF),i.length>1&&i.sort(f||IE),r.length>1&&r.sort(f||IE)}function d(){for(let h=e,f=n.length;h<f;h++){let g=n[h];if(g.id===null)break;g.id=null,g.object=null,g.geometry=null,g.material=null,g.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:d,sort:u}}function UF(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new AE,n.set(i,[o])):r>=s.length?(o=new AE,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function BF(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new N,color:new qe};break;case"SpotLight":t={position:new N,direction:new N,color:new qe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new N,color:new qe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new N,skyColor:new qe,groundColor:new qe};break;case"RectAreaLight":t={color:new qe,position:new N,halfWidth:new N,halfHeight:new N};break}return n[e.id]=t,t}}}function VF(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new rt};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new rt};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new rt,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var HF=0;function zF(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function GF(n){let e=new BF,t=VF(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new N);let r=new N,s=new Pt,o=new Pt;function a(l){let u=0,d=0,h=0;for(let T=0;T<9;T++)i.probe[T].set(0,0,0);let f=0,g=0,y=0,m=0,p=0,b=0,w=0,M=0,I=0,E=0,D=0;l.sort(zF);for(let T=0,O=l.length;T<O;T++){let C=l[T],F=C.color,G=C.intensity,W=C.distance,P=null;if(C.shadow&&C.shadow.map&&(C.shadow.map.texture.format===us?P=C.shadow.map.texture:P=C.shadow.map.depthTexture||C.shadow.map.texture),C.isAmbientLight)u+=F.r*G,d+=F.g*G,h+=F.b*G;else if(C.isLightProbe){for(let H=0;H<9;H++)i.probe[H].addScaledVector(C.sh.coefficients[H],G);D++}else if(C.isDirectionalLight){let H=e.get(C);if(H.color.copy(C.color).multiplyScalar(C.intensity),C.castShadow){let U=C.shadow,Q=t.get(C);Q.shadowIntensity=U.intensity,Q.shadowBias=U.bias,Q.shadowNormalBias=U.normalBias,Q.shadowRadius=U.radius,Q.shadowMapSize=U.mapSize,i.directionalShadow[f]=Q,i.directionalShadowMap[f]=P,i.directionalShadowMatrix[f]=C.shadow.matrix,b++}i.directional[f]=H,f++}else if(C.isSpotLight){let H=e.get(C);H.position.setFromMatrixPosition(C.matrixWorld),H.color.copy(F).multiplyScalar(G),H.distance=W,H.coneCos=Math.cos(C.angle),H.penumbraCos=Math.cos(C.angle*(1-C.penumbra)),H.decay=C.decay,i.spot[y]=H;let U=C.shadow;if(C.map&&(i.spotLightMap[I]=C.map,I++,U.updateMatrices(C),C.castShadow&&E++),i.spotLightMatrix[y]=U.matrix,C.castShadow){let Q=t.get(C);Q.shadowIntensity=U.intensity,Q.shadowBias=U.bias,Q.shadowNormalBias=U.normalBias,Q.shadowRadius=U.radius,Q.shadowMapSize=U.mapSize,i.spotShadow[y]=Q,i.spotShadowMap[y]=P,M++}y++}else if(C.isRectAreaLight){let H=e.get(C);H.color.copy(F).multiplyScalar(G),H.halfWidth.set(C.width*.5,0,0),H.halfHeight.set(0,C.height*.5,0),i.rectArea[m]=H,m++}else if(C.isPointLight){let H=e.get(C);if(H.color.copy(C.color).multiplyScalar(C.intensity),H.distance=C.distance,H.decay=C.decay,C.castShadow){let U=C.shadow,Q=t.get(C);Q.shadowIntensity=U.intensity,Q.shadowBias=U.bias,Q.shadowNormalBias=U.normalBias,Q.shadowRadius=U.radius,Q.shadowMapSize=U.mapSize,Q.shadowCameraNear=U.camera.near,Q.shadowCameraFar=U.camera.far,i.pointShadow[g]=Q,i.pointShadowMap[g]=P,i.pointShadowMatrix[g]=C.shadow.matrix,w++}i.point[g]=H,g++}else if(C.isHemisphereLight){let H=e.get(C);H.skyColor.copy(C.color).multiplyScalar(G),H.groundColor.copy(C.groundColor).multiplyScalar(G),i.hemi[p]=H,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=le.LTC_FLOAT_1,i.rectAreaLTC2=le.LTC_FLOAT_2):(i.rectAreaLTC1=le.LTC_HALF_1,i.rectAreaLTC2=le.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=h;let _=i.hash;(_.directionalLength!==f||_.pointLength!==g||_.spotLength!==y||_.rectAreaLength!==m||_.hemiLength!==p||_.numDirectionalShadows!==b||_.numPointShadows!==w||_.numSpotShadows!==M||_.numSpotMaps!==I||_.numLightProbes!==D)&&(i.directional.length=f,i.spot.length=y,i.rectArea.length=m,i.point.length=g,i.hemi.length=p,i.directionalShadow.length=b,i.directionalShadowMap.length=b,i.pointShadow.length=w,i.pointShadowMap.length=w,i.spotShadow.length=M,i.spotShadowMap.length=M,i.directionalShadowMatrix.length=b,i.pointShadowMatrix.length=w,i.spotLightMatrix.length=M+I-E,i.spotLightMap.length=I,i.numSpotLightShadowsWithMaps=E,i.numLightProbes=D,_.directionalLength=f,_.pointLength=g,_.spotLength=y,_.rectAreaLength=m,_.hemiLength=p,_.numDirectionalShadows=b,_.numPointShadows=w,_.numSpotShadows=M,_.numSpotMaps=I,_.numLightProbes=D,i.version=HF++)}function c(l,u){let d=0,h=0,f=0,g=0,y=0,m=u.matrixWorldInverse;for(let p=0,b=l.length;p<b;p++){let w=l[p];if(w.isDirectionalLight){let M=i.directional[d];M.direction.setFromMatrixPosition(w.matrixWorld),r.setFromMatrixPosition(w.target.matrixWorld),M.direction.sub(r),M.direction.transformDirection(m),d++}else if(w.isSpotLight){let M=i.spot[f];M.position.setFromMatrixPosition(w.matrixWorld),M.position.applyMatrix4(m),M.direction.setFromMatrixPosition(w.matrixWorld),r.setFromMatrixPosition(w.target.matrixWorld),M.direction.sub(r),M.direction.transformDirection(m),f++}else if(w.isRectAreaLight){let M=i.rectArea[g];M.position.setFromMatrixPosition(w.matrixWorld),M.position.applyMatrix4(m),o.identity(),s.copy(w.matrixWorld),s.premultiply(m),o.extractRotation(s),M.halfWidth.set(w.width*.5,0,0),M.halfHeight.set(0,w.height*.5,0),M.halfWidth.applyMatrix4(o),M.halfHeight.applyMatrix4(o),g++}else if(w.isPointLight){let M=i.point[h];M.position.setFromMatrixPosition(w.matrixWorld),M.position.applyMatrix4(m),h++}else if(w.isHemisphereLight){let M=i.hemi[y];M.direction.setFromMatrixPosition(w.matrixWorld),M.direction.transformDirection(m),y++}}}return{setup:a,setupView:c,state:i}}function RE(n){let e=new GF(n),t=[],i=[],r=[];function s(h){d.camera=h,t.length=0,i.length=0,r.length=0}function o(h){t.push(h)}function a(h){i.push(h)}function c(h){r.push(h)}function l(){e.setup(t)}function u(h){e.setupView(t,h)}let d={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:d,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function jF(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new RE(n),e.set(r,[a])):s>=o.length?(a=new RE(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var WF=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,$F=`uniform sampler2D shadow_pass;
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
}`,XF=[new N(1,0,0),new N(-1,0,0),new N(0,1,0),new N(0,-1,0),new N(0,0,1),new N(0,0,-1)],qF=[new N(0,-1,0),new N(0,-1,0),new N(0,0,1),new N(0,0,-1),new N(0,-1,0),new N(0,-1,0)],NE=new Pt,wl=new N,Vy=new N;function YF(n,e,t){let i=new _a,r=new rt,s=new rt,o=new Ot,a=new Wh,c=new $h,l={},u=t.maxTextureSize,d={[_r]:An,[An]:_r,[Wi]:Wi},h=new qn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new rt},radius:{value:4}},vertexShader:WF,fragmentShader:$F}),f=h.clone();f.defines.HORIZONTAL_PASS=1;let g=new Sn;g.setAttribute("position",new $n(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let y=new dt(g,h),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=pl;let p=this.type;this.render=function(E,D,_){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||E.length===0)return;this.type===sf&&(De("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=pl);let T=n.getRenderTarget(),O=n.getActiveCubeFace(),C=n.getActiveMipmapLevel(),F=n.state;F.setBlending($i),F.buffers.depth.getReversed()===!0?F.buffers.color.setClear(0,0,0,0):F.buffers.color.setClear(1,1,1,1),F.buffers.depth.setTest(!0),F.setScissorTest(!1);let G=p!==this.type;G&&D.traverse(function(W){W.material&&(Array.isArray(W.material)?W.material.forEach(P=>P.needsUpdate=!0):W.material.needsUpdate=!0)});for(let W=0,P=E.length;W<P;W++){let H=E[W],U=H.shadow;if(U===void 0){De("WebGLShadowMap:",H,"has no shadow.");continue}if(U.autoUpdate===!1&&U.needsUpdate===!1)continue;r.copy(U.mapSize);let Q=U.getFrameExtents();r.multiply(Q),s.copy(U.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/Q.x),r.x=s.x*Q.x,U.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/Q.y),r.y=s.y*Q.y,U.mapSize.y=s.y));let ee=n.state.buffers.depth.getReversed();if(U.camera._reversedDepth=ee,U.map===null||G===!0){if(U.map!==null&&(U.map.depthTexture!==null&&(U.map.depthTexture.dispose(),U.map.depthTexture=null),U.map.dispose()),this.type===Ma){if(H.isPointLight){De("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}U.map=new Xn(r.x,r.y,{format:us,type:Xi,minFilter:fn,magFilter:fn,generateMipmaps:!1}),U.map.texture.name=H.name+".shadowMap",U.map.depthTexture=new br(r.x,r.y,Ci),U.map.depthTexture.name=H.name+".shadowMapDepth",U.map.depthTexture.format=Vi,U.map.depthTexture.compareFunction=null,U.map.depthTexture.minFilter=on,U.map.depthTexture.magFilter=on}else H.isPointLight?(U.map=new Kf(r.x),U.map.depthTexture=new Gh(r.x,Ti)):(U.map=new Xn(r.x,r.y),U.map.depthTexture=new br(r.x,r.y,Ti)),U.map.depthTexture.name=H.name+".shadowMap",U.map.depthTexture.format=Vi,this.type===pl?(U.map.depthTexture.compareFunction=ee?Xf:$f,U.map.depthTexture.minFilter=fn,U.map.depthTexture.magFilter=fn):(U.map.depthTexture.compareFunction=null,U.map.depthTexture.minFilter=on,U.map.depthTexture.magFilter=on);U.camera.updateProjectionMatrix()}let ue=U.map.isWebGLCubeRenderTarget?6:1;for(let xe=0;xe<ue;xe++){if(U.map.isWebGLCubeRenderTarget)n.setRenderTarget(U.map,xe),n.clear();else{xe===0&&(n.setRenderTarget(U.map),n.clear());let Te=U.getViewport(xe);o.set(s.x*Te.x,s.y*Te.y,s.x*Te.z,s.y*Te.w),F.viewport(o)}if(H.isPointLight){let Te=U.camera,nt=U.matrix,gt=H.distance||Te.far;gt!==Te.far&&(Te.far=gt,Te.updateProjectionMatrix()),wl.setFromMatrixPosition(H.matrixWorld),Te.position.copy(wl),Vy.copy(Te.position),Vy.add(XF[xe]),Te.up.copy(qF[xe]),Te.lookAt(Vy),Te.updateMatrixWorld(),nt.makeTranslation(-wl.x,-wl.y,-wl.z),NE.multiplyMatrices(Te.projectionMatrix,Te.matrixWorldInverse),U._frustum.setFromProjectionMatrix(NE,Te.coordinateSystem,Te.reversedDepth)}else U.updateMatrices(H);i=U.getFrustum(),M(D,_,U.camera,H,this.type)}U.isPointLightShadow!==!0&&this.type===Ma&&b(U,_),U.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(T,O,C)};function b(E,D){let _=e.update(y);h.defines.VSM_SAMPLES!==E.blurSamples&&(h.defines.VSM_SAMPLES=E.blurSamples,f.defines.VSM_SAMPLES=E.blurSamples,h.needsUpdate=!0,f.needsUpdate=!0),E.mapPass===null&&(E.mapPass=new Xn(r.x,r.y,{format:us,type:Xi})),h.uniforms.shadow_pass.value=E.map.depthTexture,h.uniforms.resolution.value=E.mapSize,h.uniforms.radius.value=E.radius,n.setRenderTarget(E.mapPass),n.clear(),n.renderBufferDirect(D,null,_,h,y,null),f.uniforms.shadow_pass.value=E.mapPass.texture,f.uniforms.resolution.value=E.mapSize,f.uniforms.radius.value=E.radius,n.setRenderTarget(E.map),n.clear(),n.renderBufferDirect(D,null,_,f,y,null)}function w(E,D,_,T){let O=null,C=_.isPointLight===!0?E.customDistanceMaterial:E.customDepthMaterial;if(C!==void 0)O=C;else if(O=_.isPointLight===!0?c:a,n.localClippingEnabled&&D.clipShadows===!0&&Array.isArray(D.clippingPlanes)&&D.clippingPlanes.length!==0||D.displacementMap&&D.displacementScale!==0||D.alphaMap&&D.alphaTest>0||D.map&&D.alphaTest>0||D.alphaToCoverage===!0){let F=O.uuid,G=D.uuid,W=l[F];W===void 0&&(W={},l[F]=W);let P=W[G];P===void 0&&(P=O.clone(),W[G]=P,D.addEventListener("dispose",I)),O=P}if(O.visible=D.visible,O.wireframe=D.wireframe,T===Ma?O.side=D.shadowSide!==null?D.shadowSide:D.side:O.side=D.shadowSide!==null?D.shadowSide:d[D.side],O.alphaMap=D.alphaMap,O.alphaTest=D.alphaToCoverage===!0?.5:D.alphaTest,O.map=D.map,O.clipShadows=D.clipShadows,O.clippingPlanes=D.clippingPlanes,O.clipIntersection=D.clipIntersection,O.displacementMap=D.displacementMap,O.displacementScale=D.displacementScale,O.displacementBias=D.displacementBias,O.wireframeLinewidth=D.wireframeLinewidth,O.linewidth=D.linewidth,_.isPointLight===!0&&O.isMeshDistanceMaterial===!0){let F=n.properties.get(O);F.light=_}return O}function M(E,D,_,T,O){if(E.visible===!1)return;if(E.layers.test(D.layers)&&(E.isMesh||E.isLine||E.isPoints)&&(E.castShadow||E.receiveShadow&&O===Ma)&&(!E.frustumCulled||i.intersectsObject(E))){E.modelViewMatrix.multiplyMatrices(_.matrixWorldInverse,E.matrixWorld);let G=e.update(E),W=E.material;if(Array.isArray(W)){let P=G.groups;for(let H=0,U=P.length;H<U;H++){let Q=P[H],ee=W[Q.materialIndex];if(ee&&ee.visible){let ue=w(E,ee,T,O);E.onBeforeShadow(n,E,D,_,G,ue,Q),n.renderBufferDirect(_,null,G,ue,E,Q),E.onAfterShadow(n,E,D,_,G,ue,Q)}}}else if(W.visible){let P=w(E,W,T,O);E.onBeforeShadow(n,E,D,_,G,P,null),n.renderBufferDirect(_,null,G,P,E,null),E.onAfterShadow(n,E,D,_,G,P,null)}}let F=E.children;for(let G=0,W=F.length;G<W;G++)M(F[G],D,_,T,O)}function I(E){E.target.removeEventListener("dispose",I);for(let _ in l){let T=l[_],O=E.target.uuid;O in T&&(T[O].dispose(),delete T[O])}}}function ZF(n,e){function t(){let A=!1,ie=new Ot,$=null,ge=new Ot(0,0,0,0);return{setMask:function(oe){$!==oe&&!A&&(n.colorMask(oe,oe,oe,oe),$=oe)},setLocked:function(oe){A=oe},setClear:function(oe,Z,Se,Be,Vt){Vt===!0&&(oe*=Be,Z*=Be,Se*=Be),ie.set(oe,Z,Se,Be),ge.equals(ie)===!1&&(n.clearColor(oe,Z,Se,Be),ge.copy(ie))},reset:function(){A=!1,$=null,ge.set(-1,0,0,0)}}}function i(){let A=!1,ie=!1,$=null,ge=null,oe=null;return{setReversed:function(Z){if(ie!==Z){let Se=e.get("EXT_clip_control");Z?Se.clipControlEXT(Se.LOWER_LEFT_EXT,Se.ZERO_TO_ONE_EXT):Se.clipControlEXT(Se.LOWER_LEFT_EXT,Se.NEGATIVE_ONE_TO_ONE_EXT),ie=Z;let Be=oe;oe=null,this.setClear(Be)}},getReversed:function(){return ie},setTest:function(Z){Z?re(n.DEPTH_TEST):Ae(n.DEPTH_TEST)},setMask:function(Z){$!==Z&&!A&&(n.depthMask(Z),$=Z)},setFunc:function(Z){if(ie&&(Z=cE[Z]),ge!==Z){switch(Z){case wh:n.depthFunc(n.NEVER);break;case Th:n.depthFunc(n.ALWAYS);break;case Ch:n.depthFunc(n.LESS);break;case eo:n.depthFunc(n.LEQUAL);break;case Dh:n.depthFunc(n.EQUAL);break;case Ih:n.depthFunc(n.GEQUAL);break;case Ah:n.depthFunc(n.GREATER);break;case Rh:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}ge=Z}},setLocked:function(Z){A=Z},setClear:function(Z){oe!==Z&&(oe=Z,ie&&(Z=1-Z),n.clearDepth(Z))},reset:function(){A=!1,$=null,ge=null,oe=null,ie=!1}}}function r(){let A=!1,ie=null,$=null,ge=null,oe=null,Z=null,Se=null,Be=null,Vt=null;return{setTest:function(yt){A||(yt?re(n.STENCIL_TEST):Ae(n.STENCIL_TEST))},setMask:function(yt){ie!==yt&&!A&&(n.stencilMask(yt),ie=yt)},setFunc:function(yt,Zi,Di){($!==yt||ge!==Zi||oe!==Di)&&(n.stencilFunc(yt,Zi,Di),$=yt,ge=Zi,oe=Di)},setOp:function(yt,Zi,Di){(Z!==yt||Se!==Zi||Be!==Di)&&(n.stencilOp(yt,Zi,Di),Z=yt,Se=Zi,Be=Di)},setLocked:function(yt){A=yt},setClear:function(yt){Vt!==yt&&(n.clearStencil(yt),Vt=yt)},reset:function(){A=!1,ie=null,$=null,ge=null,oe=null,Z=null,Se=null,Be=null,Vt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},h={},f=new WeakMap,g=[],y=null,m=!1,p=null,b=null,w=null,M=null,I=null,E=null,D=null,_=new qe(0,0,0),T=0,O=!1,C=null,F=null,G=null,W=null,P=null,H=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),U=!1,Q=0,ee=n.getParameter(n.VERSION);ee.indexOf("WebGL")!==-1?(Q=parseFloat(/^WebGL (\d)/.exec(ee)[1]),U=Q>=1):ee.indexOf("OpenGL ES")!==-1&&(Q=parseFloat(/^OpenGL ES (\d)/.exec(ee)[1]),U=Q>=2);let ue=null,xe={},Te=n.getParameter(n.SCISSOR_BOX),nt=n.getParameter(n.VIEWPORT),gt=new Ot().fromArray(Te),Ge=new Ot().fromArray(nt);function Y(A,ie,$,ge){let oe=new Uint8Array(4),Z=n.createTexture();n.bindTexture(A,Z),n.texParameteri(A,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(A,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let Se=0;Se<$;Se++)A===n.TEXTURE_3D||A===n.TEXTURE_2D_ARRAY?n.texImage3D(ie,0,n.RGBA,1,1,ge,0,n.RGBA,n.UNSIGNED_BYTE,oe):n.texImage2D(ie+Se,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,oe);return Z}let pe={};pe[n.TEXTURE_2D]=Y(n.TEXTURE_2D,n.TEXTURE_2D,1),pe[n.TEXTURE_CUBE_MAP]=Y(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),pe[n.TEXTURE_2D_ARRAY]=Y(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),pe[n.TEXTURE_3D]=Y(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),re(n.DEPTH_TEST),o.setFunc(eo),Xt(!1),Nt(uy),re(n.CULL_FACE),It($i);function re(A){u[A]!==!0&&(n.enable(A),u[A]=!0)}function Ae(A){u[A]!==!1&&(n.disable(A),u[A]=!1)}function ke(A,ie){return h[A]!==ie?(n.bindFramebuffer(A,ie),h[A]=ie,A===n.DRAW_FRAMEBUFFER&&(h[n.FRAMEBUFFER]=ie),A===n.FRAMEBUFFER&&(h[n.DRAW_FRAMEBUFFER]=ie),!0):!1}function Ne(A,ie){let $=g,ge=!1;if(A){$=f.get(ie),$===void 0&&($=[],f.set(ie,$));let oe=A.textures;if($.length!==oe.length||$[0]!==n.COLOR_ATTACHMENT0){for(let Z=0,Se=oe.length;Z<Se;Z++)$[Z]=n.COLOR_ATTACHMENT0+Z;$.length=oe.length,ge=!0}}else $[0]!==n.BACK&&($[0]=n.BACK,ge=!0);ge&&n.drawBuffers($)}function Rt(A){return y!==A?(n.useProgram(A),y=A,!0):!1}let Ke={[ns]:n.FUNC_ADD,[RM]:n.FUNC_SUBTRACT,[NM]:n.FUNC_REVERSE_SUBTRACT};Ke[PM]=n.MIN,Ke[OM]=n.MAX;let vt={[LM]:n.ZERO,[FM]:n.ONE,[kM]:n.SRC_COLOR,[Mh]:n.SRC_ALPHA,[GM]:n.SRC_ALPHA_SATURATE,[HM]:n.DST_COLOR,[BM]:n.DST_ALPHA,[UM]:n.ONE_MINUS_SRC_COLOR,[Eh]:n.ONE_MINUS_SRC_ALPHA,[zM]:n.ONE_MINUS_DST_COLOR,[VM]:n.ONE_MINUS_DST_ALPHA,[jM]:n.CONSTANT_COLOR,[WM]:n.ONE_MINUS_CONSTANT_COLOR,[$M]:n.CONSTANT_ALPHA,[XM]:n.ONE_MINUS_CONSTANT_ALPHA};function It(A,ie,$,ge,oe,Z,Se,Be,Vt,yt){if(A===$i){m===!0&&(Ae(n.BLEND),m=!1);return}if(m===!1&&(re(n.BLEND),m=!0),A!==AM){if(A!==p||yt!==O){if((b!==ns||I!==ns)&&(n.blendEquation(n.FUNC_ADD),b=ns,I=ns),yt)switch(A){case Qs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case dy:n.blendFunc(n.ONE,n.ONE);break;case hy:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case fy:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:Re("WebGLState: Invalid blending: ",A);break}else switch(A){case Qs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case dy:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case hy:Re("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case fy:Re("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:Re("WebGLState: Invalid blending: ",A);break}w=null,M=null,E=null,D=null,_.set(0,0,0),T=0,p=A,O=yt}return}oe=oe||ie,Z=Z||$,Se=Se||ge,(ie!==b||oe!==I)&&(n.blendEquationSeparate(Ke[ie],Ke[oe]),b=ie,I=oe),($!==w||ge!==M||Z!==E||Se!==D)&&(n.blendFuncSeparate(vt[$],vt[ge],vt[Z],vt[Se]),w=$,M=ge,E=Z,D=Se),(Be.equals(_)===!1||Vt!==T)&&(n.blendColor(Be.r,Be.g,Be.b,Vt),_.copy(Be),T=Vt),p=A,O=!1}function Ye(A,ie){A.side===Wi?Ae(n.CULL_FACE):re(n.CULL_FACE);let $=A.side===An;ie&&($=!$),Xt($),A.blending===Qs&&A.transparent===!1?It($i):It(A.blending,A.blendEquation,A.blendSrc,A.blendDst,A.blendEquationAlpha,A.blendSrcAlpha,A.blendDstAlpha,A.blendColor,A.blendAlpha,A.premultipliedAlpha),o.setFunc(A.depthFunc),o.setTest(A.depthTest),o.setMask(A.depthWrite),s.setMask(A.colorWrite);let ge=A.stencilWrite;a.setTest(ge),ge&&(a.setMask(A.stencilWriteMask),a.setFunc(A.stencilFunc,A.stencilRef,A.stencilFuncMask),a.setOp(A.stencilFail,A.stencilZFail,A.stencilZPass)),R(A.polygonOffset,A.polygonOffsetFactor,A.polygonOffsetUnits),A.alphaToCoverage===!0?re(n.SAMPLE_ALPHA_TO_COVERAGE):Ae(n.SAMPLE_ALPHA_TO_COVERAGE)}function Xt(A){C!==A&&(A?n.frontFace(n.CW):n.frontFace(n.CCW),C=A)}function Nt(A){A!==DM?(re(n.CULL_FACE),A!==F&&(A===uy?n.cullFace(n.BACK):A===IM?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Ae(n.CULL_FACE),F=A}function kn(A){A!==G&&(U&&n.lineWidth(A),G=A)}function R(A,ie,$){A?(re(n.POLYGON_OFFSET_FILL),(W!==ie||P!==$)&&(W=ie,P=$,o.getReversed()&&(ie=-ie),n.polygonOffset(ie,$))):Ae(n.POLYGON_OFFSET_FILL)}function qt(A){A?re(n.SCISSOR_TEST):Ae(n.SCISSOR_TEST)}function Je(A){A===void 0&&(A=n.TEXTURE0+H-1),ue!==A&&(n.activeTexture(A),ue=A)}function Tt(A,ie,$){$===void 0&&(ue===null?$=n.TEXTURE0+H-1:$=ue);let ge=xe[$];ge===void 0&&(ge={type:void 0,texture:void 0},xe[$]=ge),(ge.type!==A||ge.texture!==ie)&&(ue!==$&&(n.activeTexture($),ue=$),n.bindTexture(A,ie||pe[A]),ge.type=A,ge.texture=ie)}function ce(){let A=xe[ue];A!==void 0&&A.type!==void 0&&(n.bindTexture(A.type,null),A.type=void 0,A.texture=void 0)}function Lt(){try{n.compressedTexImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function S(){try{n.compressedTexImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function v(){try{n.texSubImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function k(){try{n.texSubImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function X(){try{n.compressedTexSubImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function K(){try{n.compressedTexSubImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function te(){try{n.texStorage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function ae(){try{n.texStorage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function j(){try{n.texImage2D(...arguments)}catch(A){Re("WebGLState:",A)}}function q(){try{n.texImage3D(...arguments)}catch(A){Re("WebGLState:",A)}}function me(A){return d[A]!==void 0?d[A]:n.getParameter(A)}function ye(A,ie){d[A]!==ie&&(n.pixelStorei(A,ie),d[A]=ie)}function se(A){gt.equals(A)===!1&&(n.scissor(A.x,A.y,A.z,A.w),gt.copy(A))}function ne(A){Ge.equals(A)===!1&&(n.viewport(A.x,A.y,A.z,A.w),Ge.copy(A))}function Oe(A,ie){let $=l.get(ie);$===void 0&&($=new WeakMap,l.set(ie,$));let ge=$.get(A);ge===void 0&&(ge=n.getUniformBlockIndex(ie,A.name),$.set(A,ge))}function je(A,ie){let ge=l.get(ie).get(A);c.get(ie)!==ge&&(n.uniformBlockBinding(ie,ge,A.__bindingPointIndex),c.set(ie,ge))}function st(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},d={},ue=null,xe={},h={},f=new WeakMap,g=[],y=null,m=!1,p=null,b=null,w=null,M=null,I=null,E=null,D=null,_=new qe(0,0,0),T=0,O=!1,C=null,F=null,G=null,W=null,P=null,gt.set(0,0,n.canvas.width,n.canvas.height),Ge.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:re,disable:Ae,bindFramebuffer:ke,drawBuffers:Ne,useProgram:Rt,setBlending:It,setMaterial:Ye,setFlipSided:Xt,setCullFace:Nt,setLineWidth:kn,setPolygonOffset:R,setScissorTest:qt,activeTexture:Je,bindTexture:Tt,unbindTexture:ce,compressedTexImage2D:Lt,compressedTexImage3D:S,texImage2D:j,texImage3D:q,pixelStorei:ye,getParameter:me,updateUBOMapping:Oe,uniformBlockBinding:je,texStorage2D:te,texStorage3D:ae,texSubImage2D:v,texSubImage3D:k,compressedTexSubImage2D:X,compressedTexSubImage3D:K,scissor:se,viewport:ne,reset:st}}function KF(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new rt,u=new WeakMap,d=new Set,h,f=new WeakMap,g=!1;try{g=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch(S){}function y(S,v){return g?new OffscreenCanvas(S,v):Jc("canvas")}function m(S,v,k){let X=1,K=Lt(S);if((K.width>k||K.height>k)&&(X=k/Math.max(K.width,K.height)),X<1)if(typeof HTMLImageElement<"u"&&S instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&S instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&S instanceof ImageBitmap||typeof VideoFrame<"u"&&S instanceof VideoFrame){let te=Math.floor(X*K.width),ae=Math.floor(X*K.height);h===void 0&&(h=y(te,ae));let j=v?y(te,ae):h;return j.width=te,j.height=ae,j.getContext("2d").drawImage(S,0,0,te,ae),De("WebGLRenderer: Texture has been resized from ("+K.width+"x"+K.height+") to ("+te+"x"+ae+")."),j}else return"data"in S&&De("WebGLRenderer: Image in DataTexture is too big ("+K.width+"x"+K.height+")."),S;return S}function p(S){return S.generateMipmaps}function b(S){n.generateMipmap(S)}function w(S){return S.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:S.isWebGL3DRenderTarget?n.TEXTURE_3D:S.isWebGLArrayRenderTarget||S.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function M(S,v,k,X,K,te=!1){if(S!==null){if(n[S]!==void 0)return n[S];De("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+S+"'")}let ae;X&&(ae=e.get("EXT_texture_norm16"),ae||De("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let j=v;if(v===n.RED&&(k===n.FLOAT&&(j=n.R32F),k===n.HALF_FLOAT&&(j=n.R16F),k===n.UNSIGNED_BYTE&&(j=n.R8),k===n.UNSIGNED_SHORT&&ae&&(j=ae.R16_EXT),k===n.SHORT&&ae&&(j=ae.R16_SNORM_EXT)),v===n.RED_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.R8UI),k===n.UNSIGNED_SHORT&&(j=n.R16UI),k===n.UNSIGNED_INT&&(j=n.R32UI),k===n.BYTE&&(j=n.R8I),k===n.SHORT&&(j=n.R16I),k===n.INT&&(j=n.R32I)),v===n.RG&&(k===n.FLOAT&&(j=n.RG32F),k===n.HALF_FLOAT&&(j=n.RG16F),k===n.UNSIGNED_BYTE&&(j=n.RG8),k===n.UNSIGNED_SHORT&&ae&&(j=ae.RG16_EXT),k===n.SHORT&&ae&&(j=ae.RG16_SNORM_EXT)),v===n.RG_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.RG8UI),k===n.UNSIGNED_SHORT&&(j=n.RG16UI),k===n.UNSIGNED_INT&&(j=n.RG32UI),k===n.BYTE&&(j=n.RG8I),k===n.SHORT&&(j=n.RG16I),k===n.INT&&(j=n.RG32I)),v===n.RGB_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.RGB8UI),k===n.UNSIGNED_SHORT&&(j=n.RGB16UI),k===n.UNSIGNED_INT&&(j=n.RGB32UI),k===n.BYTE&&(j=n.RGB8I),k===n.SHORT&&(j=n.RGB16I),k===n.INT&&(j=n.RGB32I)),v===n.RGBA_INTEGER&&(k===n.UNSIGNED_BYTE&&(j=n.RGBA8UI),k===n.UNSIGNED_SHORT&&(j=n.RGBA16UI),k===n.UNSIGNED_INT&&(j=n.RGBA32UI),k===n.BYTE&&(j=n.RGBA8I),k===n.SHORT&&(j=n.RGBA16I),k===n.INT&&(j=n.RGBA32I)),v===n.RGB&&(k===n.UNSIGNED_SHORT&&ae&&(j=ae.RGB16_EXT),k===n.SHORT&&ae&&(j=ae.RGB16_SNORM_EXT),k===n.UNSIGNED_INT_5_9_9_9_REV&&(j=n.RGB9_E5),k===n.UNSIGNED_INT_10F_11F_11F_REV&&(j=n.R11F_G11F_B10F)),v===n.RGBA){let q=te?Kc:et.getTransfer(K);k===n.FLOAT&&(j=n.RGBA32F),k===n.HALF_FLOAT&&(j=n.RGBA16F),k===n.UNSIGNED_BYTE&&(j=q===pt?n.SRGB8_ALPHA8:n.RGBA8),k===n.UNSIGNED_SHORT&&ae&&(j=ae.RGBA16_EXT),k===n.SHORT&&ae&&(j=ae.RGBA16_SNORM_EXT),k===n.UNSIGNED_SHORT_4_4_4_4&&(j=n.RGBA4),k===n.UNSIGNED_SHORT_5_5_5_1&&(j=n.RGB5_A1)}return(j===n.R16F||j===n.R32F||j===n.RG16F||j===n.RG32F||j===n.RGBA16F||j===n.RGBA32F)&&e.get("EXT_color_buffer_float"),j}function I(S,v){let k;return S?v===null||v===Ti||v===wa?k=n.DEPTH24_STENCIL8:v===Ci?k=n.DEPTH32F_STENCIL8:v===Ea&&(k=n.DEPTH24_STENCIL8,De("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===Ti||v===wa?k=n.DEPTH_COMPONENT24:v===Ci?k=n.DEPTH_COMPONENT32F:v===Ea&&(k=n.DEPTH_COMPONENT16),k}function E(S,v){return p(S)===!0||S.isFramebufferTexture&&S.minFilter!==on&&S.minFilter!==fn?Math.log2(Math.max(v.width,v.height))+1:S.mipmaps!==void 0&&S.mipmaps.length>0?S.mipmaps.length:S.isCompressedTexture&&Array.isArray(S.image)?v.mipmaps.length:1}function D(S){let v=S.target;v.removeEventListener("dispose",D),T(v),v.isVideoTexture&&u.delete(v),v.isHTMLTexture&&d.delete(v)}function _(S){let v=S.target;v.removeEventListener("dispose",_),C(v)}function T(S){let v=i.get(S);if(v.__webglInit===void 0)return;let k=S.source,X=f.get(k);if(X){let K=X[v.__cacheKey];K.usedTimes--,K.usedTimes===0&&O(S),Object.keys(X).length===0&&f.delete(k)}i.remove(S)}function O(S){let v=i.get(S);n.deleteTexture(v.__webglTexture);let k=S.source,X=f.get(k);delete X[v.__cacheKey],o.memory.textures--}function C(S){let v=i.get(S);if(S.depthTexture&&(S.depthTexture.dispose(),i.remove(S.depthTexture)),S.isWebGLCubeRenderTarget)for(let X=0;X<6;X++){if(Array.isArray(v.__webglFramebuffer[X]))for(let K=0;K<v.__webglFramebuffer[X].length;K++)n.deleteFramebuffer(v.__webglFramebuffer[X][K]);else n.deleteFramebuffer(v.__webglFramebuffer[X]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[X])}else{if(Array.isArray(v.__webglFramebuffer))for(let X=0;X<v.__webglFramebuffer.length;X++)n.deleteFramebuffer(v.__webglFramebuffer[X]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let X=0;X<v.__webglColorRenderbuffer.length;X++)v.__webglColorRenderbuffer[X]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[X]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let k=S.textures;for(let X=0,K=k.length;X<K;X++){let te=i.get(k[X]);te.__webglTexture&&(n.deleteTexture(te.__webglTexture),o.memory.textures--),i.remove(k[X])}i.remove(S)}let F=0;function G(){F=0}function W(){return F}function P(S){F=S}function H(){let S=F;return S>=r.maxTextures&&De("WebGLTextures: Trying to use "+S+" texture units while this GPU supports only "+r.maxTextures),F+=1,S}function U(S){let v=[];return v.push(S.wrapS),v.push(S.wrapT),v.push(S.wrapR||0),v.push(S.magFilter),v.push(S.minFilter),v.push(S.anisotropy),v.push(S.internalFormat),v.push(S.format),v.push(S.type),v.push(S.generateMipmaps),v.push(S.premultiplyAlpha),v.push(S.flipY),v.push(S.unpackAlignment),v.push(S.colorSpace),v.join()}function Q(S,v){let k=i.get(S);if(S.isVideoTexture&&Tt(S),S.isRenderTargetTexture===!1&&S.isExternalTexture!==!0&&S.version>0&&k.__version!==S.version){let X=S.image;if(X===null)De("WebGLRenderer: Texture marked for update but no image data found.");else if(X.complete===!1)De("WebGLRenderer: Texture marked for update but image is incomplete");else{Ae(k,S,v);return}}else S.isExternalTexture&&(k.__webglTexture=S.sourceTexture?S.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,k.__webglTexture,n.TEXTURE0+v)}function ee(S,v){let k=i.get(S);if(S.isRenderTargetTexture===!1&&S.version>0&&k.__version!==S.version){Ae(k,S,v);return}else S.isExternalTexture&&(k.__webglTexture=S.sourceTexture?S.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,k.__webglTexture,n.TEXTURE0+v)}function ue(S,v){let k=i.get(S);if(S.isRenderTargetTexture===!1&&S.version>0&&k.__version!==S.version){Ae(k,S,v);return}t.bindTexture(n.TEXTURE_3D,k.__webglTexture,n.TEXTURE0+v)}function xe(S,v){let k=i.get(S);if(S.isCubeDepthTexture!==!0&&S.version>0&&k.__version!==S.version){ke(k,S,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,k.__webglTexture,n.TEXTURE0+v)}let Te={[Nh]:n.REPEAT,[Bi]:n.CLAMP_TO_EDGE,[Ph]:n.MIRRORED_REPEAT},nt={[on]:n.NEAREST,[ZM]:n.NEAREST_MIPMAP_NEAREST,[vl]:n.NEAREST_MIPMAP_LINEAR,[fn]:n.LINEAR,[cf]:n.LINEAR_MIPMAP_NEAREST,[cs]:n.LINEAR_MIPMAP_LINEAR},gt={[QM]:n.NEVER,[rE]:n.ALWAYS,[eE]:n.LESS,[$f]:n.LEQUAL,[tE]:n.EQUAL,[Xf]:n.GEQUAL,[nE]:n.GREATER,[iE]:n.NOTEQUAL};function Ge(S,v){if(v.type===Ci&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===fn||v.magFilter===cf||v.magFilter===vl||v.magFilter===cs||v.minFilter===fn||v.minFilter===cf||v.minFilter===vl||v.minFilter===cs)&&De("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(S,n.TEXTURE_WRAP_S,Te[v.wrapS]),n.texParameteri(S,n.TEXTURE_WRAP_T,Te[v.wrapT]),(S===n.TEXTURE_3D||S===n.TEXTURE_2D_ARRAY)&&n.texParameteri(S,n.TEXTURE_WRAP_R,Te[v.wrapR]),n.texParameteri(S,n.TEXTURE_MAG_FILTER,nt[v.magFilter]),n.texParameteri(S,n.TEXTURE_MIN_FILTER,nt[v.minFilter]),v.compareFunction&&(n.texParameteri(S,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(S,n.TEXTURE_COMPARE_FUNC,gt[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===on||v.minFilter!==vl&&v.minFilter!==cs||v.type===Ci&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let k=e.get("EXT_texture_filter_anisotropic");n.texParameterf(S,k.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function Y(S,v){let k=!1;S.__webglInit===void 0&&(S.__webglInit=!0,v.addEventListener("dispose",D));let X=v.source,K=f.get(X);K===void 0&&(K={},f.set(X,K));let te=U(v);if(te!==S.__cacheKey){K[te]===void 0&&(K[te]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,k=!0),K[te].usedTimes++;let ae=K[S.__cacheKey];ae!==void 0&&(K[S.__cacheKey].usedTimes--,ae.usedTimes===0&&O(v)),S.__cacheKey=te,S.__webglTexture=K[te].texture}return k}function pe(S,v,k){return Math.floor(Math.floor(S/k)/v)}function re(S,v,k,X){let te=S.updateRanges;if(te.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,k,X,v.data);else{te.sort((ye,se)=>ye.start-se.start);let ae=0;for(let ye=1;ye<te.length;ye++){let se=te[ae],ne=te[ye],Oe=se.start+se.count,je=pe(ne.start,v.width,4),st=pe(se.start,v.width,4);ne.start<=Oe+1&&je===st&&pe(ne.start+ne.count-1,v.width,4)===je?se.count=Math.max(se.count,ne.start+ne.count-se.start):(++ae,te[ae]=ne)}te.length=ae+1;let j=t.getParameter(n.UNPACK_ROW_LENGTH),q=t.getParameter(n.UNPACK_SKIP_PIXELS),me=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let ye=0,se=te.length;ye<se;ye++){let ne=te[ye],Oe=Math.floor(ne.start/4),je=Math.ceil(ne.count/4),st=Oe%v.width,A=Math.floor(Oe/v.width),ie=je,$=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,st),t.pixelStorei(n.UNPACK_SKIP_ROWS,A),t.texSubImage2D(n.TEXTURE_2D,0,st,A,ie,$,k,X,v.data)}S.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,j),t.pixelStorei(n.UNPACK_SKIP_PIXELS,q),t.pixelStorei(n.UNPACK_SKIP_ROWS,me)}}function Ae(S,v,k){let X=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&(X=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&(X=n.TEXTURE_3D);let K=Y(S,v),te=v.source;t.bindTexture(X,S.__webglTexture,n.TEXTURE0+k);let ae=i.get(te);if(te.version!==ae.__version||K===!0){if(t.activeTexture(n.TEXTURE0+k),(typeof ImageBitmap<"u"&&v.image instanceof ImageBitmap)===!1){let $=et.getPrimaries(et.workingColorSpace),ge=v.colorSpace===Sr?null:et.getPrimaries(v.colorSpace),oe=v.colorSpace===Sr||$===ge?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,oe)}t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment);let q=m(v.image,!1,r.maxTextureSize);q=ce(v,q);let me=s.convert(v.format,v.colorSpace),ye=s.convert(v.type),se=M(v.internalFormat,me,ye,v.normalized,v.colorSpace,v.isVideoTexture);Ge(X,v);let ne,Oe=v.mipmaps,je=v.isVideoTexture!==!0,st=ae.__version===void 0||K===!0,A=te.dataReady,ie=E(v,q);if(v.isDepthTexture)se=I(v.format===ls,v.type),st&&(je?t.texStorage2D(n.TEXTURE_2D,1,se,q.width,q.height):t.texImage2D(n.TEXTURE_2D,0,se,q.width,q.height,0,me,ye,null));else if(v.isDataTexture)if(Oe.length>0){je&&st&&t.texStorage2D(n.TEXTURE_2D,ie,se,Oe[0].width,Oe[0].height);for(let $=0,ge=Oe.length;$<ge;$++)ne=Oe[$],je?A&&t.texSubImage2D(n.TEXTURE_2D,$,0,0,ne.width,ne.height,me,ye,ne.data):t.texImage2D(n.TEXTURE_2D,$,se,ne.width,ne.height,0,me,ye,ne.data);v.generateMipmaps=!1}else je?(st&&t.texStorage2D(n.TEXTURE_2D,ie,se,q.width,q.height),A&&re(v,q,me,ye)):t.texImage2D(n.TEXTURE_2D,0,se,q.width,q.height,0,me,ye,q.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){je&&st&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ie,se,Oe[0].width,Oe[0].height,q.depth);for(let $=0,ge=Oe.length;$<ge;$++)if(ne=Oe[$],v.format!==ui)if(me!==null)if(je){if(A)if(v.layerUpdates.size>0){let oe=Oy(ne.width,ne.height,v.format,v.type);for(let Z of v.layerUpdates){let Se=ne.data.subarray(Z*oe/ne.data.BYTES_PER_ELEMENT,(Z+1)*oe/ne.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,$,0,0,Z,ne.width,ne.height,1,me,Se)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,$,0,0,0,ne.width,ne.height,q.depth,me,ne.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,$,se,ne.width,ne.height,q.depth,0,ne.data,0,0);else De("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else je?A&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,$,0,0,0,ne.width,ne.height,q.depth,me,ye,ne.data):t.texImage3D(n.TEXTURE_2D_ARRAY,$,se,ne.width,ne.height,q.depth,0,me,ye,ne.data)}else{je&&st&&t.texStorage2D(n.TEXTURE_2D,ie,se,Oe[0].width,Oe[0].height);for(let $=0,ge=Oe.length;$<ge;$++)ne=Oe[$],v.format!==ui?me!==null?je?A&&t.compressedTexSubImage2D(n.TEXTURE_2D,$,0,0,ne.width,ne.height,me,ne.data):t.compressedTexImage2D(n.TEXTURE_2D,$,se,ne.width,ne.height,0,ne.data):De("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):je?A&&t.texSubImage2D(n.TEXTURE_2D,$,0,0,ne.width,ne.height,me,ye,ne.data):t.texImage2D(n.TEXTURE_2D,$,se,ne.width,ne.height,0,me,ye,ne.data)}else if(v.isDataArrayTexture)if(je){if(st&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ie,se,q.width,q.height,q.depth),A)if(v.layerUpdates.size>0){let $=Oy(q.width,q.height,v.format,v.type);for(let ge of v.layerUpdates){let oe=q.data.subarray(ge*$/q.data.BYTES_PER_ELEMENT,(ge+1)*$/q.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,ge,q.width,q.height,1,me,ye,oe)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,q.width,q.height,q.depth,me,ye,q.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,se,q.width,q.height,q.depth,0,me,ye,q.data);else if(v.isData3DTexture)je?(st&&t.texStorage3D(n.TEXTURE_3D,ie,se,q.width,q.height,q.depth),A&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,q.width,q.height,q.depth,me,ye,q.data)):t.texImage3D(n.TEXTURE_3D,0,se,q.width,q.height,q.depth,0,me,ye,q.data);else if(v.isFramebufferTexture){if(st)if(je)t.texStorage2D(n.TEXTURE_2D,ie,se,q.width,q.height);else{let $=q.width,ge=q.height;for(let oe=0;oe<ie;oe++)t.texImage2D(n.TEXTURE_2D,oe,se,$,ge,0,me,ye,null),$>>=1,ge>>=1}}else if(v.isHTMLTexture){if("texElementImage2D"in n){let $=n.canvas;if($.hasAttribute("layoutsubtree")||$.setAttribute("layoutsubtree","true"),q.parentNode!==$){$.appendChild(q),d.add(v),$.onpaint=Be=>{let Vt=Be.changedElements;for(let yt of d)Vt.includes(yt.image)&&(yt.needsUpdate=!0)},$.requestPaint();return}let ge=0,oe=n.RGBA,Z=n.RGBA,Se=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,ge,oe,Z,Se,q),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(Oe.length>0){if(je&&st){let $=Lt(Oe[0]);t.texStorage2D(n.TEXTURE_2D,ie,se,$.width,$.height)}for(let $=0,ge=Oe.length;$<ge;$++)ne=Oe[$],je?A&&t.texSubImage2D(n.TEXTURE_2D,$,0,0,me,ye,ne):t.texImage2D(n.TEXTURE_2D,$,se,me,ye,ne);v.generateMipmaps=!1}else if(je){if(st){let $=Lt(q);t.texStorage2D(n.TEXTURE_2D,ie,se,$.width,$.height)}A&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,me,ye,q)}else t.texImage2D(n.TEXTURE_2D,0,se,me,ye,q);p(v)&&b(X),ae.__version=te.version,v.onUpdate&&v.onUpdate(v)}S.__version=v.version}function ke(S,v,k){if(v.image.length!==6)return;let X=Y(S,v),K=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,S.__webglTexture,n.TEXTURE0+k);let te=i.get(K);if(K.version!==te.__version||X===!0){t.activeTexture(n.TEXTURE0+k);let ae=et.getPrimaries(et.workingColorSpace),j=v.colorSpace===Sr?null:et.getPrimaries(v.colorSpace),q=v.colorSpace===Sr||ae===j?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,q);let me=v.isCompressedTexture||v.image[0].isCompressedTexture,ye=v.image[0]&&v.image[0].isDataTexture,se=[];for(let Z=0;Z<6;Z++)!me&&!ye?se[Z]=m(v.image[Z],!0,r.maxCubemapSize):se[Z]=ye?v.image[Z].image:v.image[Z],se[Z]=ce(v,se[Z]);let ne=se[0],Oe=s.convert(v.format,v.colorSpace),je=s.convert(v.type),st=M(v.internalFormat,Oe,je,v.normalized,v.colorSpace),A=v.isVideoTexture!==!0,ie=te.__version===void 0||X===!0,$=K.dataReady,ge=E(v,ne);Ge(n.TEXTURE_CUBE_MAP,v);let oe;if(me){A&&ie&&t.texStorage2D(n.TEXTURE_CUBE_MAP,ge,st,ne.width,ne.height);for(let Z=0;Z<6;Z++){oe=se[Z].mipmaps;for(let Se=0;Se<oe.length;Se++){let Be=oe[Se];v.format!==ui?Oe!==null?A?$&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,0,0,Be.width,Be.height,Oe,Be.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,st,Be.width,Be.height,0,Be.data):De("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):A?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,0,0,Be.width,Be.height,Oe,je,Be.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se,st,Be.width,Be.height,0,Oe,je,Be.data)}}}else{if(oe=v.mipmaps,A&&ie){oe.length>0&&ge++;let Z=Lt(se[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,ge,st,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ye){A?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,se[Z].width,se[Z].height,Oe,je,se[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,st,se[Z].width,se[Z].height,0,Oe,je,se[Z].data);for(let Se=0;Se<oe.length;Se++){let Vt=oe[Se].image[Z].image;A?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,0,0,Vt.width,Vt.height,Oe,je,Vt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,st,Vt.width,Vt.height,0,Oe,je,Vt.data)}}else{A?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Oe,je,se[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,st,Oe,je,se[Z]);for(let Se=0;Se<oe.length;Se++){let Be=oe[Se];A?$&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,0,0,Oe,je,Be.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,Se+1,st,Oe,je,Be.image[Z])}}}p(v)&&b(n.TEXTURE_CUBE_MAP),te.__version=K.version,v.onUpdate&&v.onUpdate(v)}S.__version=v.version}function Ne(S,v,k,X,K,te){let ae=s.convert(k.format,k.colorSpace),j=s.convert(k.type),q=M(k.internalFormat,ae,j,k.normalized,k.colorSpace),me=i.get(v),ye=i.get(k);if(ye.__renderTarget=v,!me.__hasExternalTextures){let se=Math.max(1,v.width>>te),ne=Math.max(1,v.height>>te);K===n.TEXTURE_3D||K===n.TEXTURE_2D_ARRAY?t.texImage3D(K,te,q,se,ne,v.depth,0,ae,j,null):t.texImage2D(K,te,q,se,ne,0,ae,j,null)}t.bindFramebuffer(n.FRAMEBUFFER,S),Je(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,X,K,ye.__webglTexture,0,qt(v)):(K===n.TEXTURE_2D||K>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&K<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,X,K,ye.__webglTexture,te),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Rt(S,v,k){if(n.bindRenderbuffer(n.RENDERBUFFER,S),v.depthBuffer){let X=v.depthTexture,K=X&&X.isDepthTexture?X.type:null,te=I(v.stencilBuffer,K),ae=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Je(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,qt(v),te,v.width,v.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,qt(v),te,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,te,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ae,n.RENDERBUFFER,S)}else{let X=v.textures;for(let K=0;K<X.length;K++){let te=X[K],ae=s.convert(te.format,te.colorSpace),j=s.convert(te.type),q=M(te.internalFormat,ae,j,te.normalized,te.colorSpace);Je(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,qt(v),q,v.width,v.height):k?n.renderbufferStorageMultisample(n.RENDERBUFFER,qt(v),q,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,q,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ke(S,v,k){let X=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,S),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let K=i.get(v.depthTexture);if(K.__renderTarget=v,(!K.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),X){if(K.__webglInit===void 0&&(K.__webglInit=!0,v.depthTexture.addEventListener("dispose",D)),K.__webglTexture===void 0){K.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,K.__webglTexture),Ge(n.TEXTURE_CUBE_MAP,v.depthTexture);let me=s.convert(v.depthTexture.format),ye=s.convert(v.depthTexture.type),se;v.depthTexture.format===Vi?se=n.DEPTH_COMPONENT24:v.depthTexture.format===ls&&(se=n.DEPTH24_STENCIL8);for(let ne=0;ne<6;ne++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+ne,0,se,v.width,v.height,0,me,ye,null)}}else Q(v.depthTexture,0);let te=K.__webglTexture,ae=qt(v),j=X?n.TEXTURE_CUBE_MAP_POSITIVE_X+k:n.TEXTURE_2D,q=v.depthTexture.format===ls?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===Vi)Je(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,j,te,0,ae):n.framebufferTexture2D(n.FRAMEBUFFER,q,j,te,0);else if(v.depthTexture.format===ls)Je(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,j,te,0,ae):n.framebufferTexture2D(n.FRAMEBUFFER,q,j,te,0);else throw new Error("Unknown depthTexture format")}function vt(S){let v=i.get(S),k=S.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==S.depthTexture){let X=S.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),X){let K=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,X.removeEventListener("dispose",K)};X.addEventListener("dispose",K),v.__depthDisposeCallback=K}v.__boundDepthTexture=X}if(S.depthTexture&&!v.__autoAllocateDepthBuffer)if(k)for(let X=0;X<6;X++)Ke(v.__webglFramebuffer[X],S,X);else{let X=S.texture.mipmaps;X&&X.length>0?Ke(v.__webglFramebuffer[0],S,0):Ke(v.__webglFramebuffer,S,0)}else if(k){v.__webglDepthbuffer=[];for(let X=0;X<6;X++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[X]),v.__webglDepthbuffer[X]===void 0)v.__webglDepthbuffer[X]=n.createRenderbuffer(),Rt(v.__webglDepthbuffer[X],S,!1);else{let K=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,te=v.__webglDepthbuffer[X];n.bindRenderbuffer(n.RENDERBUFFER,te),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,te)}}else{let X=S.texture.mipmaps;if(X&&X.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Rt(v.__webglDepthbuffer,S,!1);else{let K=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,te=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,te),n.framebufferRenderbuffer(n.FRAMEBUFFER,K,n.RENDERBUFFER,te)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function It(S,v,k){let X=i.get(S);v!==void 0&&Ne(X.__webglFramebuffer,S,S.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),k!==void 0&&vt(S)}function Ye(S){let v=S.texture,k=i.get(S),X=i.get(v);S.addEventListener("dispose",_);let K=S.textures,te=S.isWebGLCubeRenderTarget===!0,ae=K.length>1;if(ae||(X.__webglTexture===void 0&&(X.__webglTexture=n.createTexture()),X.__version=v.version,o.memory.textures++),te){k.__webglFramebuffer=[];for(let j=0;j<6;j++)if(v.mipmaps&&v.mipmaps.length>0){k.__webglFramebuffer[j]=[];for(let q=0;q<v.mipmaps.length;q++)k.__webglFramebuffer[j][q]=n.createFramebuffer()}else k.__webglFramebuffer[j]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){k.__webglFramebuffer=[];for(let j=0;j<v.mipmaps.length;j++)k.__webglFramebuffer[j]=n.createFramebuffer()}else k.__webglFramebuffer=n.createFramebuffer();if(ae)for(let j=0,q=K.length;j<q;j++){let me=i.get(K[j]);me.__webglTexture===void 0&&(me.__webglTexture=n.createTexture(),o.memory.textures++)}if(S.samples>0&&Je(S)===!1){k.__webglMultisampledFramebuffer=n.createFramebuffer(),k.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,k.__webglMultisampledFramebuffer);for(let j=0;j<K.length;j++){let q=K[j];k.__webglColorRenderbuffer[j]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,k.__webglColorRenderbuffer[j]);let me=s.convert(q.format,q.colorSpace),ye=s.convert(q.type),se=M(q.internalFormat,me,ye,q.normalized,q.colorSpace,S.isXRRenderTarget===!0),ne=qt(S);n.renderbufferStorageMultisample(n.RENDERBUFFER,ne,se,S.width,S.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+j,n.RENDERBUFFER,k.__webglColorRenderbuffer[j])}n.bindRenderbuffer(n.RENDERBUFFER,null),S.depthBuffer&&(k.__webglDepthRenderbuffer=n.createRenderbuffer(),Rt(k.__webglDepthRenderbuffer,S,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(te){t.bindTexture(n.TEXTURE_CUBE_MAP,X.__webglTexture),Ge(n.TEXTURE_CUBE_MAP,v);for(let j=0;j<6;j++)if(v.mipmaps&&v.mipmaps.length>0)for(let q=0;q<v.mipmaps.length;q++)Ne(k.__webglFramebuffer[j][q],S,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+j,q);else Ne(k.__webglFramebuffer[j],S,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0);p(v)&&b(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ae){for(let j=0,q=K.length;j<q;j++){let me=K[j],ye=i.get(me),se=n.TEXTURE_2D;(S.isWebGL3DRenderTarget||S.isWebGLArrayRenderTarget)&&(se=S.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(se,ye.__webglTexture),Ge(se,me),Ne(k.__webglFramebuffer,S,me,n.COLOR_ATTACHMENT0+j,se,0),p(me)&&b(se)}t.unbindTexture()}else{let j=n.TEXTURE_2D;if((S.isWebGL3DRenderTarget||S.isWebGLArrayRenderTarget)&&(j=S.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(j,X.__webglTexture),Ge(j,v),v.mipmaps&&v.mipmaps.length>0)for(let q=0;q<v.mipmaps.length;q++)Ne(k.__webglFramebuffer[q],S,v,n.COLOR_ATTACHMENT0,j,q);else Ne(k.__webglFramebuffer,S,v,n.COLOR_ATTACHMENT0,j,0);p(v)&&b(j),t.unbindTexture()}S.depthBuffer&&vt(S)}function Xt(S){let v=S.textures;for(let k=0,X=v.length;k<X;k++){let K=v[k];if(p(K)){let te=w(S),ae=i.get(K).__webglTexture;t.bindTexture(te,ae),b(te),t.unbindTexture()}}}let Nt=[],kn=[];function R(S){if(S.samples>0){if(Je(S)===!1){let v=S.textures,k=S.width,X=S.height,K=n.COLOR_BUFFER_BIT,te=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ae=i.get(S),j=v.length>1;if(j)for(let me=0;me<v.length;me++)t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ae.__webglMultisampledFramebuffer);let q=S.texture.mipmaps;q&&q.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ae.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ae.__webglFramebuffer);for(let me=0;me<v.length;me++){if(S.resolveDepthBuffer&&(S.depthBuffer&&(K|=n.DEPTH_BUFFER_BIT),S.stencilBuffer&&S.resolveStencilBuffer&&(K|=n.STENCIL_BUFFER_BIT)),j){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ae.__webglColorRenderbuffer[me]);let ye=i.get(v[me]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,ye,0)}n.blitFramebuffer(0,0,k,X,0,0,k,X,K,n.NEAREST),c===!0&&(Nt.length=0,kn.length=0,Nt.push(n.COLOR_ATTACHMENT0+me),S.depthBuffer&&S.resolveDepthBuffer===!1&&(Nt.push(te),kn.push(te),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,kn)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Nt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),j)for(let me=0;me<v.length;me++){t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.RENDERBUFFER,ae.__webglColorRenderbuffer[me]);let ye=i.get(v[me]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ae.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+me,n.TEXTURE_2D,ye,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ae.__webglMultisampledFramebuffer)}else if(S.depthBuffer&&S.resolveDepthBuffer===!1&&c){let v=S.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function qt(S){return Math.min(r.maxSamples,S.samples)}function Je(S){let v=i.get(S);return S.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function Tt(S){let v=o.render.frame;u.get(S)!==v&&(u.set(S,v),S.update())}function ce(S,v){let k=S.colorSpace,X=S.format,K=S.type;return S.isCompressedTexture===!0||S.isVideoTexture===!0||k!==Zc&&k!==Sr&&(et.getTransfer(k)===pt?(X!==ui||K!==Fn)&&De("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):Re("WebGLTextures: Unsupported texture color space:",k)),v}function Lt(S){return typeof HTMLImageElement<"u"&&S instanceof HTMLImageElement?(l.width=S.naturalWidth||S.width,l.height=S.naturalHeight||S.height):typeof VideoFrame<"u"&&S instanceof VideoFrame?(l.width=S.displayWidth,l.height=S.displayHeight):(l.width=S.width,l.height=S.height),l}this.allocateTextureUnit=H,this.resetTextureUnits=G,this.getTextureUnits=W,this.setTextureUnits=P,this.setTexture2D=Q,this.setTexture2DArray=ee,this.setTexture3D=ue,this.setTextureCube=xe,this.rebindTextures=It,this.setupRenderTarget=Ye,this.updateRenderTargetMipmap=Xt,this.updateMultisampleRenderTarget=R,this.setupDepthRenderbuffer=vt,this.setupFrameBufferTexture=Ne,this.useMultisampledRTT=Je,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function JF(n,e){function t(i,r=Sr){let s,o=et.getTransfer(r);if(i===Fn)return n.UNSIGNED_BYTE;if(i===uf)return n.UNSIGNED_SHORT_4_4_4_4;if(i===df)return n.UNSIGNED_SHORT_5_5_5_1;if(i===My)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Ey)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===by)return n.BYTE;if(i===Sy)return n.SHORT;if(i===Ea)return n.UNSIGNED_SHORT;if(i===lf)return n.INT;if(i===Ti)return n.UNSIGNED_INT;if(i===Ci)return n.FLOAT;if(i===Xi)return n.HALF_FLOAT;if(i===wy)return n.ALPHA;if(i===Ty)return n.RGB;if(i===ui)return n.RGBA;if(i===Vi)return n.DEPTH_COMPONENT;if(i===ls)return n.DEPTH_STENCIL;if(i===Cy)return n.RED;if(i===hf)return n.RED_INTEGER;if(i===us)return n.RG;if(i===ff)return n.RG_INTEGER;if(i===pf)return n.RGBA_INTEGER;if(i===yl||i===_l||i===xl||i===bl)if(o===pt)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===yl)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===_l)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===xl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===bl)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===yl)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===_l)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===xl)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===bl)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===mf||i===gf||i===vf||i===yf)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===mf)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===gf)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===vf)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===yf)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===_f||i===xf||i===bf||i===Sf||i===Mf||i===Sl||i===Ef)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===_f||i===xf)return o===pt?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===bf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Sf)return s.COMPRESSED_R11_EAC;if(i===Mf)return s.COMPRESSED_SIGNED_R11_EAC;if(i===Sl)return s.COMPRESSED_RG11_EAC;if(i===Ef)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===wf||i===Tf||i===Cf||i===Df||i===If||i===Af||i===Rf||i===Nf||i===Pf||i===Of||i===Lf||i===Ff||i===kf||i===Uf)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===wf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Tf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Cf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Df)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===If)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Af)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Rf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Nf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Pf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Of)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Lf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Ff)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===kf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===Uf)return o===pt?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===Bf||i===Vf||i===Hf)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===Bf)return o===pt?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===Vf)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Hf)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===zf||i===Gf||i===Ml||i===jf)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===zf)return s.COMPRESSED_RED_RGTC1_EXT;if(i===Gf)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Ml)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===jf)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===wa?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var QF=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,e2=`
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

}`,qy=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new al(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new qn({vertexShader:QF,fragmentShader:e2,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new dt(new ll(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},Yy=class extends Hi{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,d=null,h=null,f=null,g=null,y=typeof XRWebGLBinding<"u",m=new qy,p={},b=t.getContextAttributes(),w=null,M=null,I=[],E=[],D=new rt,_=null,T=new hn;T.viewport=new Ot;let O=new hn;O.viewport=new Ot;let C=[T,O],F=new rf,G=null,W=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(Y){let pe=I[Y];return pe===void 0&&(pe=new ya,I[Y]=pe),pe.getTargetRaySpace()},this.getControllerGrip=function(Y){let pe=I[Y];return pe===void 0&&(pe=new ya,I[Y]=pe),pe.getGripSpace()},this.getHand=function(Y){let pe=I[Y];return pe===void 0&&(pe=new ya,I[Y]=pe),pe.getHandSpace()};function P(Y){let pe=E.indexOf(Y.inputSource);if(pe===-1)return;let re=I[pe];re!==void 0&&(re.update(Y.inputSource,Y.frame,l||o),re.dispatchEvent({type:Y.type,data:Y.inputSource}))}function H(){r.removeEventListener("select",P),r.removeEventListener("selectstart",P),r.removeEventListener("selectend",P),r.removeEventListener("squeeze",P),r.removeEventListener("squeezestart",P),r.removeEventListener("squeezeend",P),r.removeEventListener("end",H),r.removeEventListener("inputsourceschange",U);for(let Y=0;Y<I.length;Y++){let pe=E[Y];pe!==null&&(E[Y]=null,I[Y].disconnect(pe))}G=null,W=null,m.reset();for(let Y in p)delete p[Y];e.setRenderTarget(w),f=null,h=null,d=null,r=null,M=null,Ge.stop(),i.isPresenting=!1,e.setPixelRatio(_),e.setSize(D.width,D.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(Y){s=Y,i.isPresenting===!0&&De("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(Y){a=Y,i.isPresenting===!0&&De("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(Y){l=Y},this.getBaseLayer=function(){return h!==null?h:f},this.getBinding=function(){return d===null&&y&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return g},this.getSession=function(){return r},this.setSession=function(Y){return mt(this,null,function*(){if(r=Y,r!==null){if(w=e.getRenderTarget(),r.addEventListener("select",P),r.addEventListener("selectstart",P),r.addEventListener("selectend",P),r.addEventListener("squeeze",P),r.addEventListener("squeezestart",P),r.addEventListener("squeezeend",P),r.addEventListener("end",H),r.addEventListener("inputsourceschange",U),b.xrCompatible!==!0&&(yield t.makeXRCompatible()),_=e.getPixelRatio(),e.getSize(D),y&&"createProjectionLayer"in XRWebGLBinding.prototype){let re=null,Ae=null,ke=null;b.depth&&(ke=b.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,re=b.stencil?ls:Vi,Ae=b.stencil?wa:Ti);let Ne={colorFormat:t.RGBA8,depthFormat:ke,scaleFactor:s};d=this.getBinding(),h=d.createProjectionLayer(Ne),r.updateRenderState({layers:[h]}),e.setPixelRatio(1),e.setSize(h.textureWidth,h.textureHeight,!1),M=new Xn(h.textureWidth,h.textureHeight,{format:ui,type:Fn,depthTexture:new br(h.textureWidth,h.textureHeight,Ae,void 0,void 0,void 0,void 0,void 0,void 0,re),stencilBuffer:b.stencil,colorSpace:e.outputColorSpace,samples:b.antialias?4:0,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}else{let re={antialias:b.antialias,alpha:!0,depth:b.depth,stencil:b.stencil,framebufferScaleFactor:s};f=new XRWebGLLayer(r,t,re),r.updateRenderState({baseLayer:f}),e.setPixelRatio(1),e.setSize(f.framebufferWidth,f.framebufferHeight,!1),M=new Xn(f.framebufferWidth,f.framebufferHeight,{format:ui,type:Fn,colorSpace:e.outputColorSpace,stencilBuffer:b.stencil,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1})}M.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=yield r.requestReferenceSpace(a),Ge.setContext(r),Ge.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}})},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function U(Y){for(let pe=0;pe<Y.removed.length;pe++){let re=Y.removed[pe],Ae=E.indexOf(re);Ae>=0&&(E[Ae]=null,I[Ae].disconnect(re))}for(let pe=0;pe<Y.added.length;pe++){let re=Y.added[pe],Ae=E.indexOf(re);if(Ae===-1){for(let Ne=0;Ne<I.length;Ne++)if(Ne>=E.length){E.push(re),Ae=Ne;break}else if(E[Ne]===null){E[Ne]=re,Ae=Ne;break}if(Ae===-1)break}let ke=I[Ae];ke&&ke.connect(re)}}let Q=new N,ee=new N;function ue(Y,pe,re){Q.setFromMatrixPosition(pe.matrixWorld),ee.setFromMatrixPosition(re.matrixWorld);let Ae=Q.distanceTo(ee),ke=pe.projectionMatrix.elements,Ne=re.projectionMatrix.elements,Rt=ke[14]/(ke[10]-1),Ke=ke[14]/(ke[10]+1),vt=(ke[9]+1)/ke[5],It=(ke[9]-1)/ke[5],Ye=(ke[8]-1)/ke[0],Xt=(Ne[8]+1)/Ne[0],Nt=Rt*Ye,kn=Rt*Xt,R=Ae/(-Ye+Xt),qt=R*-Ye;if(pe.matrixWorld.decompose(Y.position,Y.quaternion,Y.scale),Y.translateX(qt),Y.translateZ(R),Y.matrixWorld.compose(Y.position,Y.quaternion,Y.scale),Y.matrixWorldInverse.copy(Y.matrixWorld).invert(),ke[10]===-1)Y.projectionMatrix.copy(pe.projectionMatrix),Y.projectionMatrixInverse.copy(pe.projectionMatrixInverse);else{let Je=Rt+R,Tt=Ke+R,ce=Nt-qt,Lt=kn+(Ae-qt),S=vt*Ke/Tt*Je,v=It*Ke/Tt*Je;Y.projectionMatrix.makePerspective(ce,Lt,S,v,Je,Tt),Y.projectionMatrixInverse.copy(Y.projectionMatrix).invert()}}function xe(Y,pe){pe===null?Y.matrixWorld.copy(Y.matrix):Y.matrixWorld.multiplyMatrices(pe.matrixWorld,Y.matrix),Y.matrixWorldInverse.copy(Y.matrixWorld).invert()}this.updateCamera=function(Y){if(r===null)return;let pe=Y.near,re=Y.far;m.texture!==null&&(m.depthNear>0&&(pe=m.depthNear),m.depthFar>0&&(re=m.depthFar)),F.near=O.near=T.near=pe,F.far=O.far=T.far=re,(G!==F.near||W!==F.far)&&(r.updateRenderState({depthNear:F.near,depthFar:F.far}),G=F.near,W=F.far),F.layers.mask=Y.layers.mask|6,T.layers.mask=F.layers.mask&-5,O.layers.mask=F.layers.mask&-3;let Ae=Y.parent,ke=F.cameras;xe(F,Ae);for(let Ne=0;Ne<ke.length;Ne++)xe(ke[Ne],Ae);ke.length===2?ue(F,T,O):F.projectionMatrix.copy(T.projectionMatrix),Te(Y,F,Ae)};function Te(Y,pe,re){re===null?Y.matrix.copy(pe.matrixWorld):(Y.matrix.copy(re.matrixWorld),Y.matrix.invert(),Y.matrix.multiply(pe.matrixWorld)),Y.matrix.decompose(Y.position,Y.quaternion,Y.scale),Y.updateMatrixWorld(!0),Y.projectionMatrix.copy(pe.projectionMatrix),Y.projectionMatrixInverse.copy(pe.projectionMatrixInverse),Y.isPerspectiveCamera&&(Y.fov=ma*2*Math.atan(1/Y.projectionMatrix.elements[5]),Y.zoom=1)}this.getCamera=function(){return F},this.getFoveation=function(){if(!(h===null&&f===null))return c},this.setFoveation=function(Y){c=Y,h!==null&&(h.fixedFoveation=Y),f!==null&&f.fixedFoveation!==void 0&&(f.fixedFoveation=Y)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(F)},this.getCameraTexture=function(Y){return p[Y]};let nt=null;function gt(Y,pe){if(u=pe.getViewerPose(l||o),g=pe,u!==null){let re=u.views;f!==null&&(e.setRenderTargetFramebuffer(M,f.framebuffer),e.setRenderTarget(M));let Ae=!1;re.length!==F.cameras.length&&(F.cameras.length=0,Ae=!0);for(let Ke=0;Ke<re.length;Ke++){let vt=re[Ke],It=null;if(f!==null)It=f.getViewport(vt);else{let Xt=d.getViewSubImage(h,vt);It=Xt.viewport,Ke===0&&(e.setRenderTargetTextures(M,Xt.colorTexture,Xt.depthStencilTexture),e.setRenderTarget(M))}let Ye=C[Ke];Ye===void 0&&(Ye=new hn,Ye.layers.enable(Ke),Ye.viewport=new Ot,C[Ke]=Ye),Ye.matrix.fromArray(vt.transform.matrix),Ye.matrix.decompose(Ye.position,Ye.quaternion,Ye.scale),Ye.projectionMatrix.fromArray(vt.projectionMatrix),Ye.projectionMatrixInverse.copy(Ye.projectionMatrix).invert(),Ye.viewport.set(It.x,It.y,It.width,It.height),Ke===0&&(F.matrix.copy(Ye.matrix),F.matrix.decompose(F.position,F.quaternion,F.scale)),Ae===!0&&F.cameras.push(Ye)}let ke=r.enabledFeatures;if(ke&&ke.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&y){d=i.getBinding();let Ke=d.getDepthInformation(re[0]);Ke&&Ke.isValid&&Ke.texture&&m.init(Ke,r.renderState)}if(ke&&ke.includes("camera-access")&&y){e.state.unbindTexture(),d=i.getBinding();for(let Ke=0;Ke<re.length;Ke++){let vt=re[Ke].camera;if(vt){let It=p[vt];It||(It=new al,p[vt]=It);let Ye=d.getCameraImage(vt);It.sourceTexture=Ye}}}}for(let re=0;re<I.length;re++){let Ae=E[re],ke=I[re];Ae!==null&&ke!==void 0&&ke.update(Ae,pe,l||o)}nt&&nt(Y,pe),pe.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:pe}),g=null}let Ge=new PE;Ge.setAnimationLoop(gt),this.setAnimationLoop=function(Y){nt=Y},this.dispose=function(){}}},t2=new Pt,BE=new Fe;BE.set(-1,0,0,0,1,0,0,0,1);function n2(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,Ry(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,b,w,M){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),d(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),h(m,p),p.isMeshPhysicalMaterial&&f(m,p,M)):p.isMeshMatcapMaterial?(s(m,p),g(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),y(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,b,w):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===An&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===An&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let b=e.get(p),w=b.envMap,M=b.envMapRotation;w&&(m.envMap.value=w,m.envMapRotation.value.setFromMatrix4(t2.makeRotationFromEuler(M)).transpose(),w.isCubeTexture&&w.isRenderTargetTexture===!1&&m.envMapRotation.value.premultiply(BE),m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,b,w){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*b,m.scale.value=w*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function d(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function h(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function f(m,p,b){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===An&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=b.texture,m.transmissionSamplerSize.value.set(b.width,b.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function g(m,p){p.matcap&&(m.matcap.value=p.matcap)}function y(m,p){let b=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(b.matrixWorld),m.nearDistance.value=b.shadow.camera.near,m.farDistance.value=b.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function i2(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(b,w){let M=w.program;i.uniformBlockBinding(b,M)}function l(b,w){let M=r[b.id];M===void 0&&(g(b),M=u(b),r[b.id]=M,b.addEventListener("dispose",m));let I=w.program;i.updateUBOMapping(b,I);let E=e.render.frame;s[b.id]!==E&&(h(b),s[b.id]=E)}function u(b){let w=d();b.__bindingPointIndex=w;let M=n.createBuffer(),I=b.__size,E=b.usage;return n.bindBuffer(n.UNIFORM_BUFFER,M),n.bufferData(n.UNIFORM_BUFFER,I,E),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,w,M),M}function d(){for(let b=0;b<a;b++)if(o.indexOf(b)===-1)return o.push(b),b;return Re("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function h(b){let w=r[b.id],M=b.uniforms,I=b.__cache;n.bindBuffer(n.UNIFORM_BUFFER,w);for(let E=0,D=M.length;E<D;E++){let _=Array.isArray(M[E])?M[E]:[M[E]];for(let T=0,O=_.length;T<O;T++){let C=_[T];if(f(C,E,T,I)===!0){let F=C.__offset,G=Array.isArray(C.value)?C.value:[C.value],W=0;for(let P=0;P<G.length;P++){let H=G[P],U=y(H);typeof H=="number"||typeof H=="boolean"?(C.__data[0]=H,n.bufferSubData(n.UNIFORM_BUFFER,F+W,C.__data)):H.isMatrix3?(C.__data[0]=H.elements[0],C.__data[1]=H.elements[1],C.__data[2]=H.elements[2],C.__data[3]=0,C.__data[4]=H.elements[3],C.__data[5]=H.elements[4],C.__data[6]=H.elements[5],C.__data[7]=0,C.__data[8]=H.elements[6],C.__data[9]=H.elements[7],C.__data[10]=H.elements[8],C.__data[11]=0):ArrayBuffer.isView(H)?C.__data.set(new H.constructor(H.buffer,H.byteOffset,C.__data.length)):(H.toArray(C.__data,W),W+=U.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,F,C.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function f(b,w,M,I){let E=b.value,D=w+"_"+M;if(I[D]===void 0)return typeof E=="number"||typeof E=="boolean"?I[D]=E:ArrayBuffer.isView(E)?I[D]=E.slice():I[D]=E.clone(),!0;{let _=I[D];if(typeof E=="number"||typeof E=="boolean"){if(_!==E)return I[D]=E,!0}else{if(ArrayBuffer.isView(E))return!0;if(_.equals(E)===!1)return _.copy(E),!0}}return!1}function g(b){let w=b.uniforms,M=0,I=16;for(let D=0,_=w.length;D<_;D++){let T=Array.isArray(w[D])?w[D]:[w[D]];for(let O=0,C=T.length;O<C;O++){let F=T[O],G=Array.isArray(F.value)?F.value:[F.value];for(let W=0,P=G.length;W<P;W++){let H=G[W],U=y(H),Q=M%I,ee=Q%U.boundary,ue=Q+ee;M+=ee,ue!==0&&I-ue<U.storage&&(M+=I-ue),F.__data=new Float32Array(U.storage/Float32Array.BYTES_PER_ELEMENT),F.__offset=M,M+=U.storage}}}let E=M%I;return E>0&&(M+=I-E),b.__size=M,b.__cache={},this}function y(b){let w={boundary:0,storage:0};return typeof b=="number"||typeof b=="boolean"?(w.boundary=4,w.storage=4):b.isVector2?(w.boundary=8,w.storage=8):b.isVector3||b.isColor?(w.boundary=16,w.storage=12):b.isVector4?(w.boundary=16,w.storage=16):b.isMatrix3?(w.boundary=48,w.storage=48):b.isMatrix4?(w.boundary=64,w.storage=64):b.isTexture?De("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(b)?(w.boundary=16,w.storage=b.byteLength):De("WebGLRenderer: Unsupported uniform value type.",b),w}function m(b){let w=b.target;w.removeEventListener("dispose",m);let M=o.indexOf(w.__bindingPointIndex);o.splice(M,1),n.deleteBuffer(r[w.id]),delete r[w.id],delete s[w.id]}function p(){for(let b in r)n.deleteBuffer(r[b]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var r2=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),qi=null;function s2(){return qi===null&&(qi=new Bh(r2,16,16,us,Xi),qi.name="DFG_LUT",qi.minFilter=fn,qi.magFilter=fn,qi.wrapS=Bi,qi.wrapT=Bi,qi.generateMipmaps=!1,qi.needsUpdate=!0),qi}var Jf=class{constructor(e={}){let{canvas:t=sE(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:h=!1,outputBufferType:f=Fn}=e;this.isWebGLRenderer=!0;let g;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");g=i.getContextAttributes().alpha}else g=o;let y=f,m=new Set([pf,ff,hf]),p=new Set([Fn,Ti,Ea,wa,uf,df]),b=new Uint32Array(4),w=new Int32Array(4),M=new N,I=null,E=null,D=[],_=[],T=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=wi,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let O=this,C=!1,F=null;this._outputColorSpace=Dn;let G=0,W=0,P=null,H=-1,U=null,Q=new Ot,ee=new Ot,ue=null,xe=new qe(0),Te=0,nt=t.width,gt=t.height,Ge=1,Y=null,pe=null,re=new Ot(0,0,nt,gt),Ae=new Ot(0,0,nt,gt),ke=!1,Ne=new _a,Rt=!1,Ke=!1,vt=new Pt,It=new N,Ye=new Ot,Xt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Nt=!1;function kn(){return P===null?Ge:1}let R=i;function qt(x,L){return t.getContext(x,L)}try{let x={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"184"}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",Se,!1),t.addEventListener("webglcontextcreationerror",Be,!1),R===null){let L="webgl2";if(R=qt(L,x),R===null)throw qt(L)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw Re("WebGLRenderer: "+x.message),x}let Je,Tt,ce,Lt,S,v,k,X,K,te,ae,j,q,me,ye,se,ne,Oe,je,st,A,ie,$;function ge(){Je=new hL(R),Je.init(),A=new JF(R,Je),Tt=new rL(R,Je,e,A),ce=new ZF(R,Je),Tt.reversedDepthBuffer&&h&&ce.buffers.depth.setReversed(!0),Lt=new mL(R),S=new FF,v=new KF(R,Je,ce,S,Tt,A,Lt),k=new dL(O),X=new _N(R),ie=new nL(R,X),K=new fL(R,X,Lt,ie),te=new vL(R,K,X,ie,Lt),Oe=new gL(R,Tt,v),ye=new sL(S),ae=new LF(O,k,Je,Tt,ie,ye),j=new n2(O,S),q=new UF,me=new jF(Je),ne=new tL(O,k,ce,te,g,c),se=new YF(O,te,Tt),$=new i2(R,Lt,Tt,ce),je=new iL(R,Je,Lt),st=new pL(R,Je,Lt),Lt.programs=ae.programs,O.capabilities=Tt,O.extensions=Je,O.properties=S,O.renderLists=q,O.shadowMap=se,O.state=ce,O.info=Lt}ge(),y!==Fn&&(T=new _L(y,t.width,t.height,r,s));let oe=new Yy(O,R);this.xr=oe,this.getContext=function(){return R},this.getContextAttributes=function(){return R.getContextAttributes()},this.forceContextLoss=function(){let x=Je.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=Je.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return Ge},this.setPixelRatio=function(x){x!==void 0&&(Ge=x,this.setSize(nt,gt,!1))},this.getSize=function(x){return x.set(nt,gt)},this.setSize=function(x,L,z=!0){if(oe.isPresenting){De("WebGLRenderer: Can't change size while VR device is presenting.");return}nt=x,gt=L,t.width=Math.floor(x*Ge),t.height=Math.floor(L*Ge),z===!0&&(t.style.width=x+"px",t.style.height=L+"px"),T!==null&&T.setSize(t.width,t.height),this.setViewport(0,0,x,L)},this.getDrawingBufferSize=function(x){return x.set(nt*Ge,gt*Ge).floor()},this.setDrawingBufferSize=function(x,L,z){nt=x,gt=L,Ge=z,t.width=Math.floor(x*z),t.height=Math.floor(L*z),this.setViewport(0,0,x,L)},this.setEffects=function(x){if(y===Fn){Re("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let L=0;L<x.length;L++)if(x[L].isOutputPass===!0){De("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}T.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(Q)},this.getViewport=function(x){return x.copy(re)},this.setViewport=function(x,L,z,B){x.isVector4?re.set(x.x,x.y,x.z,x.w):re.set(x,L,z,B),ce.viewport(Q.copy(re).multiplyScalar(Ge).round())},this.getScissor=function(x){return x.copy(Ae)},this.setScissor=function(x,L,z,B){x.isVector4?Ae.set(x.x,x.y,x.z,x.w):Ae.set(x,L,z,B),ce.scissor(ee.copy(Ae).multiplyScalar(Ge).round())},this.getScissorTest=function(){return ke},this.setScissorTest=function(x){ce.setScissorTest(ke=x)},this.setOpaqueSort=function(x){Y=x},this.setTransparentSort=function(x){pe=x},this.getClearColor=function(x){return x.copy(ne.getClearColor())},this.setClearColor=function(){ne.setClearColor(...arguments)},this.getClearAlpha=function(){return ne.getClearAlpha()},this.setClearAlpha=function(){ne.setClearAlpha(...arguments)},this.clear=function(x=!0,L=!0,z=!0){let B=0;if(x){let V=!1;if(P!==null){let he=P.texture.format;V=m.has(he)}if(V){let he=P.texture.type,_e=p.has(he),de=ne.getClearColor(),be=ne.getClearAlpha(),we=de.r,Ve=de.g,Xe=de.b;_e?(b[0]=we,b[1]=Ve,b[2]=Xe,b[3]=be,R.clearBufferuiv(R.COLOR,0,b)):(w[0]=we,w[1]=Ve,w[2]=Xe,w[3]=be,R.clearBufferiv(R.COLOR,0,w))}else B|=R.COLOR_BUFFER_BIT}L&&(B|=R.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),z&&(B|=R.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),B!==0&&R.clear(B)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(x){x.setRenderer(this),F=x},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",Se,!1),t.removeEventListener("webglcontextcreationerror",Be,!1),ne.dispose(),q.dispose(),me.dispose(),S.dispose(),k.dispose(),te.dispose(),ie.dispose(),$.dispose(),ae.dispose(),oe.dispose(),oe.removeEventListener("sessionstart",Ky),oe.removeEventListener("sessionend",Jy),hs.stop()};function Z(x){x.preventDefault(),Iy("WebGLRenderer: Context Lost."),C=!0}function Se(){Iy("WebGLRenderer: Context Restored."),C=!1;let x=Lt.autoReset,L=se.enabled,z=se.autoUpdate,B=se.needsUpdate,V=se.type;ge(),Lt.autoReset=x,se.enabled=L,se.autoUpdate=z,se.needsUpdate=B,se.type=V}function Be(x){Re("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function Vt(x){let L=x.target;L.removeEventListener("dispose",Vt),yt(L)}function yt(x){Zi(x),S.remove(x)}function Zi(x){let L=S.get(x).programs;L!==void 0&&(L.forEach(function(z){ae.releaseProgram(z)}),x.isShaderMaterial&&ae.releaseShaderCache(x))}this.renderBufferDirect=function(x,L,z,B,V,he){L===null&&(L=Xt);let _e=V.isMesh&&V.matrixWorld.determinant()<0,de=WE(x,L,z,B,V);ce.setMaterial(B,_e);let be=z.index,we=1;if(B.wireframe===!0){if(be=K.getWireframeAttribute(z),be===void 0)return;we=2}let Ve=z.drawRange,Xe=z.attributes.position,Ce=Ve.start*we,_t=(Ve.start+Ve.count)*we;he!==null&&(Ce=Math.max(Ce,he.start*we),_t=Math.min(_t,(he.start+he.count)*we)),be!==null?(Ce=Math.max(Ce,0),_t=Math.min(_t,be.count)):Xe!=null&&(Ce=Math.max(Ce,0),_t=Math.min(_t,Xe.count));let Ht=_t-Ce;if(Ht<0||Ht===1/0)return;ie.setup(V,B,de,z,be);let Ft,Mt=je;if(be!==null&&(Ft=X.get(be),Mt=st,Mt.setIndex(Ft)),V.isMesh)B.wireframe===!0?(ce.setLineWidth(B.wireframeLinewidth*kn()),Mt.setMode(R.LINES)):Mt.setMode(R.TRIANGLES);else if(V.isLine){let pn=B.linewidth;pn===void 0&&(pn=1),ce.setLineWidth(pn*kn()),V.isLineSegments?Mt.setMode(R.LINES):V.isLineLoop?Mt.setMode(R.LINE_LOOP):Mt.setMode(R.LINE_STRIP)}else V.isPoints?Mt.setMode(R.POINTS):V.isSprite&&Mt.setMode(R.TRIANGLES);if(V.isBatchedMesh)if(Je.get("WEBGL_multi_draw"))Mt.renderMultiDraw(V._multiDrawStarts,V._multiDrawCounts,V._multiDrawCount);else{let pn=V._multiDrawStarts,ve=V._multiDrawCounts,Un=V._multiDrawCount,it=be?X.get(be).bytesPerElement:1,Zn=S.get(B).currentProgram.getUniforms();for(let Ii=0;Ii<Un;Ii++)Zn.setValue(R,"_gl_DrawID",Ii),Mt.render(pn[Ii]/it,ve[Ii])}else if(V.isInstancedMesh)Mt.renderInstances(Ce,Ht,V.count);else if(z.isInstancedBufferGeometry){let pn=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,ve=Math.min(z.instanceCount,pn);Mt.renderInstances(Ce,Ht,ve)}else Mt.render(Ce,Ht)};function Di(x,L,z){x.transparent===!0&&x.side===Wi&&x.forceSinglePass===!1?(x.side=An,x.needsUpdate=!0,Pl(x,L,z),x.side=_r,x.needsUpdate=!0,Pl(x,L,z),x.side=Wi):Pl(x,L,z)}this.compile=function(x,L,z=null){z===null&&(z=x),E=me.get(z),E.init(L),_.push(E),z.traverseVisible(function(V){V.isLight&&V.layers.test(L.layers)&&(E.pushLight(V),V.castShadow&&E.pushShadow(V))}),x!==z&&x.traverseVisible(function(V){V.isLight&&V.layers.test(L.layers)&&(E.pushLight(V),V.castShadow&&E.pushShadow(V))}),E.setupLights();let B=new Set;return x.traverse(function(V){if(!(V.isMesh||V.isPoints||V.isLine||V.isSprite))return;let he=V.material;if(he)if(Array.isArray(he))for(let _e=0;_e<he.length;_e++){let de=he[_e];Di(de,z,V),B.add(de)}else Di(he,z,V),B.add(he)}),E=_.pop(),B},this.compileAsync=function(x,L,z=null){let B=this.compile(x,L,z);return new Promise(V=>{function he(){if(B.forEach(function(_e){S.get(_e).currentProgram.isReady()&&B.delete(_e)}),B.size===0){V(x);return}setTimeout(he,10)}Je.get("KHR_parallel_shader_compile")!==null?he():setTimeout(he,10)})};let ip=null;function GE(x){ip&&ip(x)}function Ky(){hs.stop()}function Jy(){hs.start()}let hs=new PE;hs.setAnimationLoop(GE),typeof self<"u"&&hs.setContext(self),this.setAnimationLoop=function(x){ip=x,oe.setAnimationLoop(x),x===null?hs.stop():hs.start()},oe.addEventListener("sessionstart",Ky),oe.addEventListener("sessionend",Jy),this.render=function(x,L){if(L!==void 0&&L.isCamera!==!0){Re("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(C===!0)return;F!==null&&F.renderStart(x,L);let z=oe.enabled===!0&&oe.isPresenting===!0,B=T!==null&&(P===null||z)&&T.begin(O,P);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),L.parent===null&&L.matrixWorldAutoUpdate===!0&&L.updateMatrixWorld(),oe.enabled===!0&&oe.isPresenting===!0&&(T===null||T.isCompositing()===!1)&&(oe.cameraAutoUpdate===!0&&oe.updateCamera(L),L=oe.getCamera()),x.isScene===!0&&x.onBeforeRender(O,x,L,P),E=me.get(x,_.length),E.init(L),E.state.textureUnits=v.getTextureUnits(),_.push(E),vt.multiplyMatrices(L.projectionMatrix,L.matrixWorldInverse),Ne.setFromProjectionMatrix(vt,Ei,L.reversedDepth),Ke=this.localClippingEnabled,Rt=ye.init(this.clippingPlanes,Ke),I=q.get(x,D.length),I.init(),D.push(I),oe.enabled===!0&&oe.isPresenting===!0){let _e=O.xr.getDepthSensingMesh();_e!==null&&rp(_e,L,-1/0,O.sortObjects)}rp(x,L,0,O.sortObjects),I.finish(),O.sortObjects===!0&&I.sort(Y,pe),Nt=oe.enabled===!1||oe.isPresenting===!1||oe.hasDepthSensing()===!1,Nt&&ne.addToRenderList(I,x),this.info.render.frame++,Rt===!0&&ye.beginShadows();let V=E.state.shadowsArray;if(se.render(V,x,L),Rt===!0&&ye.endShadows(),this.info.autoReset===!0&&this.info.reset(),(B&&T.hasRenderPass())===!1){let _e=I.opaque,de=I.transmissive;if(E.setupLights(),L.isArrayCamera){let be=L.cameras;if(de.length>0)for(let we=0,Ve=be.length;we<Ve;we++){let Xe=be[we];e0(_e,de,x,Xe)}Nt&&ne.render(x);for(let we=0,Ve=be.length;we<Ve;we++){let Xe=be[we];Qy(I,x,Xe,Xe.viewport)}}else de.length>0&&e0(_e,de,x,L),Nt&&ne.render(x),Qy(I,x,L)}P!==null&&W===0&&(v.updateMultisampleRenderTarget(P),v.updateRenderTargetMipmap(P)),B&&T.end(O),x.isScene===!0&&x.onAfterRender(O,x,L),ie.resetDefaultState(),H=-1,U=null,_.pop(),_.length>0?(E=_[_.length-1],v.setTextureUnits(E.state.textureUnits),Rt===!0&&ye.setGlobalState(O.clippingPlanes,E.state.camera)):E=null,D.pop(),D.length>0?I=D[D.length-1]:I=null,F!==null&&F.renderEnd()};function rp(x,L,z,B){if(x.visible===!1)return;if(x.layers.test(L.layers)){if(x.isGroup)z=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(L);else if(x.isLightProbeGrid)E.pushLightProbeGrid(x);else if(x.isLight)E.pushLight(x),x.castShadow&&E.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||Ne.intersectsSprite(x)){B&&Ye.setFromMatrixPosition(x.matrixWorld).applyMatrix4(vt);let _e=te.update(x),de=x.material;de.visible&&I.push(x,_e,de,z,Ye.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||Ne.intersectsObject(x))){let _e=te.update(x),de=x.material;if(B&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),Ye.copy(x.boundingSphere.center)):(_e.boundingSphere===null&&_e.computeBoundingSphere(),Ye.copy(_e.boundingSphere.center)),Ye.applyMatrix4(x.matrixWorld).applyMatrix4(vt)),Array.isArray(de)){let be=_e.groups;for(let we=0,Ve=be.length;we<Ve;we++){let Xe=be[we],Ce=de[Xe.materialIndex];Ce&&Ce.visible&&I.push(x,_e,Ce,z,Ye.z,Xe)}}else de.visible&&I.push(x,_e,de,z,Ye.z,null)}}let he=x.children;for(let _e=0,de=he.length;_e<de;_e++)rp(he[_e],L,z,B)}function Qy(x,L,z,B){let{opaque:V,transmissive:he,transparent:_e}=x;E.setupLightsView(z),Rt===!0&&ye.setGlobalState(O.clippingPlanes,z),B&&ce.viewport(Q.copy(B)),V.length>0&&Nl(V,L,z),he.length>0&&Nl(he,L,z),_e.length>0&&Nl(_e,L,z),ce.buffers.depth.setTest(!0),ce.buffers.depth.setMask(!0),ce.buffers.color.setMask(!0),ce.setPolygonOffset(!1)}function e0(x,L,z,B){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(E.state.transmissionRenderTarget[B.id]===void 0){let Ce=Je.has("EXT_color_buffer_half_float")||Je.has("EXT_color_buffer_float");E.state.transmissionRenderTarget[B.id]=new Xn(1,1,{generateMipmaps:!0,type:Ce?Xi:Fn,minFilter:cs,samples:Math.max(4,Tt.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:et.workingColorSpace})}let he=E.state.transmissionRenderTarget[B.id],_e=B.viewport||Q;he.setSize(_e.z*O.transmissionResolutionScale,_e.w*O.transmissionResolutionScale);let de=O.getRenderTarget(),be=O.getActiveCubeFace(),we=O.getActiveMipmapLevel();O.setRenderTarget(he),O.getClearColor(xe),Te=O.getClearAlpha(),Te<1&&O.setClearColor(16777215,.5),O.clear(),Nt&&ne.render(z);let Ve=O.toneMapping;O.toneMapping=wi;let Xe=B.viewport;if(B.viewport!==void 0&&(B.viewport=void 0),E.setupLightsView(B),Rt===!0&&ye.setGlobalState(O.clippingPlanes,B),Nl(x,z,B),v.updateMultisampleRenderTarget(he),v.updateRenderTargetMipmap(he),Je.has("WEBGL_multisampled_render_to_texture")===!1){let Ce=!1;for(let _t=0,Ht=L.length;_t<Ht;_t++){let Ft=L[_t],{object:Mt,geometry:pn,material:ve,group:Un}=Ft;if(ve.side===Wi&&Mt.layers.test(B.layers)){let it=ve.side;ve.side=An,ve.needsUpdate=!0,t0(Mt,z,B,pn,ve,Un),ve.side=it,ve.needsUpdate=!0,Ce=!0}}Ce===!0&&(v.updateMultisampleRenderTarget(he),v.updateRenderTargetMipmap(he))}O.setRenderTarget(de,be,we),O.setClearColor(xe,Te),Xe!==void 0&&(B.viewport=Xe),O.toneMapping=Ve}function Nl(x,L,z){let B=L.isScene===!0?L.overrideMaterial:null;for(let V=0,he=x.length;V<he;V++){let _e=x[V],{object:de,geometry:be,group:we}=_e,Ve=_e.material;Ve.allowOverride===!0&&B!==null&&(Ve=B),de.layers.test(z.layers)&&t0(de,L,z,be,Ve,we)}}function t0(x,L,z,B,V,he){x.onBeforeRender(O,L,z,B,V,he),x.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),V.onBeforeRender(O,L,z,B,x,he),V.transparent===!0&&V.side===Wi&&V.forceSinglePass===!1?(V.side=An,V.needsUpdate=!0,O.renderBufferDirect(z,L,B,V,x,he),V.side=_r,V.needsUpdate=!0,O.renderBufferDirect(z,L,B,V,x,he),V.side=Wi):O.renderBufferDirect(z,L,B,V,x,he),x.onAfterRender(O,L,z,B,V,he)}function Pl(x,L,z){L.isScene!==!0&&(L=Xt);let B=S.get(x),V=E.state.lights,he=E.state.shadowsArray,_e=V.state.version,de=ae.getParameters(x,V.state,he,L,z,E.state.lightProbeGridArray),be=ae.getProgramCacheKey(de),we=B.programs;B.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?L.environment:null,B.fog=L.fog;let Ve=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;B.envMap=k.get(x.envMap||B.environment,Ve),B.envMapRotation=B.environment!==null&&x.envMap===null?L.environmentRotation:x.envMapRotation,we===void 0&&(x.addEventListener("dispose",Vt),we=new Map,B.programs=we);let Xe=we.get(be);if(Xe!==void 0){if(B.currentProgram===Xe&&B.lightsStateVersion===_e)return i0(x,de),Xe}else de.uniforms=ae.getUniforms(x),F!==null&&x.isNodeMaterial&&F.build(x,z,de),x.onBeforeCompile(de,O),Xe=ae.acquireProgram(de,be),we.set(be,Xe),B.uniforms=de.uniforms;let Ce=B.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(Ce.clippingPlanes=ye.uniform),i0(x,de),B.needsLights=XE(x),B.lightsStateVersion=_e,B.needsLights&&(Ce.ambientLightColor.value=V.state.ambient,Ce.lightProbe.value=V.state.probe,Ce.directionalLights.value=V.state.directional,Ce.directionalLightShadows.value=V.state.directionalShadow,Ce.spotLights.value=V.state.spot,Ce.spotLightShadows.value=V.state.spotShadow,Ce.rectAreaLights.value=V.state.rectArea,Ce.ltc_1.value=V.state.rectAreaLTC1,Ce.ltc_2.value=V.state.rectAreaLTC2,Ce.pointLights.value=V.state.point,Ce.pointLightShadows.value=V.state.pointShadow,Ce.hemisphereLights.value=V.state.hemi,Ce.directionalShadowMatrix.value=V.state.directionalShadowMatrix,Ce.spotLightMatrix.value=V.state.spotLightMatrix,Ce.spotLightMap.value=V.state.spotLightMap,Ce.pointShadowMatrix.value=V.state.pointShadowMatrix),B.lightProbeGrid=E.state.lightProbeGridArray.length>0,B.currentProgram=Xe,B.uniformsList=null,Xe}function n0(x){if(x.uniformsList===null){let L=x.currentProgram.getUniforms();x.uniformsList=Da.seqWithValue(L.seq,x.uniforms)}return x.uniformsList}function i0(x,L){let z=S.get(x);z.outputColorSpace=L.outputColorSpace,z.batching=L.batching,z.batchingColor=L.batchingColor,z.instancing=L.instancing,z.instancingColor=L.instancingColor,z.instancingMorph=L.instancingMorph,z.skinning=L.skinning,z.morphTargets=L.morphTargets,z.morphNormals=L.morphNormals,z.morphColors=L.morphColors,z.morphTargetsCount=L.morphTargetsCount,z.numClippingPlanes=L.numClippingPlanes,z.numIntersection=L.numClipIntersection,z.vertexAlphas=L.vertexAlphas,z.vertexTangents=L.vertexTangents,z.toneMapping=L.toneMapping}function jE(x,L){if(x.length===0)return null;if(x.length===1)return x[0].texture!==null?x[0]:null;M.setFromMatrixPosition(L.matrixWorld);for(let z=0,B=x.length;z<B;z++){let V=x[z];if(V.texture!==null&&V.boundingBox.containsPoint(M))return V}return null}function WE(x,L,z,B,V){L.isScene!==!0&&(L=Xt),v.resetTextureUnits();let he=L.fog,_e=B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial?L.environment:null,de=P===null?O.outputColorSpace:P.isXRRenderTarget===!0?P.texture.colorSpace:et.workingColorSpace,be=B.isMeshStandardMaterial||B.isMeshLambertMaterial&&!B.envMap||B.isMeshPhongMaterial&&!B.envMap,we=k.get(B.envMap||_e,be),Ve=B.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,Xe=!!z.attributes.tangent&&(!!B.normalMap||B.anisotropy>0),Ce=!!z.morphAttributes.position,_t=!!z.morphAttributes.normal,Ht=!!z.morphAttributes.color,Ft=wi;B.toneMapped&&(P===null||P.isXRRenderTarget===!0)&&(Ft=O.toneMapping);let Mt=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,pn=Mt!==void 0?Mt.length:0,ve=S.get(B),Un=E.state.lights;if(Rt===!0&&(Ke===!0||x!==U)){let Ct=x===U&&B.id===H;ye.setState(B,x,Ct)}let it=!1;B.version===ve.__version?(ve.needsLights&&ve.lightsStateVersion!==Un.state.version||ve.outputColorSpace!==de||V.isBatchedMesh&&ve.batching===!1||!V.isBatchedMesh&&ve.batching===!0||V.isBatchedMesh&&ve.batchingColor===!0&&V.colorTexture===null||V.isBatchedMesh&&ve.batchingColor===!1&&V.colorTexture!==null||V.isInstancedMesh&&ve.instancing===!1||!V.isInstancedMesh&&ve.instancing===!0||V.isSkinnedMesh&&ve.skinning===!1||!V.isSkinnedMesh&&ve.skinning===!0||V.isInstancedMesh&&ve.instancingColor===!0&&V.instanceColor===null||V.isInstancedMesh&&ve.instancingColor===!1&&V.instanceColor!==null||V.isInstancedMesh&&ve.instancingMorph===!0&&V.morphTexture===null||V.isInstancedMesh&&ve.instancingMorph===!1&&V.morphTexture!==null||ve.envMap!==we||B.fog===!0&&ve.fog!==he||ve.numClippingPlanes!==void 0&&(ve.numClippingPlanes!==ye.numPlanes||ve.numIntersection!==ye.numIntersection)||ve.vertexAlphas!==Ve||ve.vertexTangents!==Xe||ve.morphTargets!==Ce||ve.morphNormals!==_t||ve.morphColors!==Ht||ve.toneMapping!==Ft||ve.morphTargetsCount!==pn||!!ve.lightProbeGrid!=E.state.lightProbeGridArray.length>0)&&(it=!0):(it=!0,ve.__version=B.version);let Zn=ve.currentProgram;it===!0&&(Zn=Pl(B,L,V),F&&B.isNodeMaterial&&F.onUpdateProgram(B,Zn,ve));let Ii=!1,Er=!1,ao=!1,Et=Zn.getUniforms(),zt=ve.uniforms;if(ce.useProgram(Zn.program)&&(Ii=!0,Er=!0,ao=!0),B.id!==H&&(H=B.id,Er=!0),ve.needsLights){let Ct=jE(E.state.lightProbeGridArray,V);ve.lightProbeGrid!==Ct&&(ve.lightProbeGrid=Ct,Er=!0)}if(Ii||U!==x){ce.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),Et.setValue(R,"projectionMatrix",x.projectionMatrix),Et.setValue(R,"viewMatrix",x.matrixWorldInverse);let Tr=Et.map.cameraPosition;Tr!==void 0&&Tr.setValue(R,It.setFromMatrixPosition(x.matrixWorld)),Tt.logarithmicDepthBuffer&&Et.setValue(R,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(B.isMeshPhongMaterial||B.isMeshToonMaterial||B.isMeshLambertMaterial||B.isMeshBasicMaterial||B.isMeshStandardMaterial||B.isShaderMaterial)&&Et.setValue(R,"isOrthographic",x.isOrthographicCamera===!0),U!==x&&(U=x,Er=!0,ao=!0)}if(ve.needsLights&&(Un.state.directionalShadowMap.length>0&&Et.setValue(R,"directionalShadowMap",Un.state.directionalShadowMap,v),Un.state.spotShadowMap.length>0&&Et.setValue(R,"spotShadowMap",Un.state.spotShadowMap,v),Un.state.pointShadowMap.length>0&&Et.setValue(R,"pointShadowMap",Un.state.pointShadowMap,v)),V.isSkinnedMesh){Et.setOptional(R,V,"bindMatrix"),Et.setOptional(R,V,"bindMatrixInverse");let Ct=V.skeleton;Ct&&(Ct.boneTexture===null&&Ct.computeBoneTexture(),Et.setValue(R,"boneTexture",Ct.boneTexture,v))}V.isBatchedMesh&&(Et.setOptional(R,V,"batchingTexture"),Et.setValue(R,"batchingTexture",V._matricesTexture,v),Et.setOptional(R,V,"batchingIdTexture"),Et.setValue(R,"batchingIdTexture",V._indirectTexture,v),Et.setOptional(R,V,"batchingColorTexture"),V._colorsTexture!==null&&Et.setValue(R,"batchingColorTexture",V._colorsTexture,v));let wr=z.morphAttributes;if((wr.position!==void 0||wr.normal!==void 0||wr.color!==void 0)&&Oe.update(V,z,Zn),(Er||ve.receiveShadow!==V.receiveShadow)&&(ve.receiveShadow=V.receiveShadow,Et.setValue(R,"receiveShadow",V.receiveShadow)),(B.isMeshStandardMaterial||B.isMeshLambertMaterial||B.isMeshPhongMaterial)&&B.envMap===null&&L.environment!==null&&(zt.envMapIntensity.value=L.environmentIntensity),zt.dfgLUT!==void 0&&(zt.dfgLUT.value=s2()),Er){if(Et.setValue(R,"toneMappingExposure",O.toneMappingExposure),ve.needsLights&&$E(zt,ao),he&&B.fog===!0&&j.refreshFogUniforms(zt,he),j.refreshMaterialUniforms(zt,B,Ge,gt,E.state.transmissionRenderTarget[x.id]),ve.needsLights&&ve.lightProbeGrid){let Ct=ve.lightProbeGrid;zt.probesSH.value=Ct.texture,zt.probesMin.value.copy(Ct.boundingBox.min),zt.probesMax.value.copy(Ct.boundingBox.max),zt.probesResolution.value.copy(Ct.resolution)}Da.upload(R,n0(ve),zt,v)}if(B.isShaderMaterial&&B.uniformsNeedUpdate===!0&&(Da.upload(R,n0(ve),zt,v),B.uniformsNeedUpdate=!1),B.isSpriteMaterial&&Et.setValue(R,"center",V.center),Et.setValue(R,"modelViewMatrix",V.modelViewMatrix),Et.setValue(R,"normalMatrix",V.normalMatrix),Et.setValue(R,"modelMatrix",V.matrixWorld),B.uniformsGroups!==void 0){let Ct=B.uniformsGroups;for(let Tr=0,co=Ct.length;Tr<co;Tr++){let r0=Ct[Tr];$.update(r0,Zn),$.bind(r0,Zn)}}return Zn}function $E(x,L){x.ambientLightColor.needsUpdate=L,x.lightProbe.needsUpdate=L,x.directionalLights.needsUpdate=L,x.directionalLightShadows.needsUpdate=L,x.pointLights.needsUpdate=L,x.pointLightShadows.needsUpdate=L,x.spotLights.needsUpdate=L,x.spotLightShadows.needsUpdate=L,x.rectAreaLights.needsUpdate=L,x.hemisphereLights.needsUpdate=L}function XE(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return G},this.getActiveMipmapLevel=function(){return W},this.getRenderTarget=function(){return P},this.setRenderTargetTextures=function(x,L,z){let B=S.get(x);B.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,B.__autoAllocateDepthBuffer===!1&&(B.__useRenderToTexture=!1),S.get(x.texture).__webglTexture=L,S.get(x.depthTexture).__webglTexture=B.__autoAllocateDepthBuffer?void 0:z,B.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,L){let z=S.get(x);z.__webglFramebuffer=L,z.__useDefaultFramebuffer=L===void 0};let qE=R.createFramebuffer();this.setRenderTarget=function(x,L=0,z=0){P=x,G=L,W=z;let B=null,V=!1,he=!1;if(x){let de=S.get(x);if(de.__useDefaultFramebuffer!==void 0){ce.bindFramebuffer(R.FRAMEBUFFER,de.__webglFramebuffer),Q.copy(x.viewport),ee.copy(x.scissor),ue=x.scissorTest,ce.viewport(Q),ce.scissor(ee),ce.setScissorTest(ue),H=-1;return}else if(de.__webglFramebuffer===void 0)v.setupRenderTarget(x);else if(de.__hasExternalTextures)v.rebindTextures(x,S.get(x.texture).__webglTexture,S.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let Ve=x.depthTexture;if(de.__boundDepthTexture!==Ve){if(Ve!==null&&S.has(Ve)&&(x.width!==Ve.image.width||x.height!==Ve.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");v.setupDepthRenderbuffer(x)}}let be=x.texture;(be.isData3DTexture||be.isDataArrayTexture||be.isCompressedArrayTexture)&&(he=!0);let we=S.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(we[L])?B=we[L][z]:B=we[L],V=!0):x.samples>0&&v.useMultisampledRTT(x)===!1?B=S.get(x).__webglMultisampledFramebuffer:Array.isArray(we)?B=we[z]:B=we,Q.copy(x.viewport),ee.copy(x.scissor),ue=x.scissorTest}else Q.copy(re).multiplyScalar(Ge).floor(),ee.copy(Ae).multiplyScalar(Ge).floor(),ue=ke;if(z!==0&&(B=qE),ce.bindFramebuffer(R.FRAMEBUFFER,B)&&ce.drawBuffers(x,B),ce.viewport(Q),ce.scissor(ee),ce.setScissorTest(ue),V){let de=S.get(x.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_CUBE_MAP_POSITIVE_X+L,de.__webglTexture,z)}else if(he){let de=L;for(let be=0;be<x.textures.length;be++){let we=S.get(x.textures[be]);R.framebufferTextureLayer(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0+be,we.__webglTexture,z,de)}}else if(x!==null&&z!==0){let de=S.get(x.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,de.__webglTexture,z)}H=-1},this.readRenderTargetPixels=function(x,L,z,B,V,he,_e,de=0){if(!(x&&x.isWebGLRenderTarget)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let be=S.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(be=be[_e]),be){ce.bindFramebuffer(R.FRAMEBUFFER,be);try{let we=x.textures[de],Ve=we.format,Xe=we.type;if(x.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+de),!Tt.textureFormatReadable(Ve)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!Tt.textureTypeReadable(Xe)){Re("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}L>=0&&L<=x.width-B&&z>=0&&z<=x.height-V&&R.readPixels(L,z,B,V,A.convert(Ve),A.convert(Xe),he)}finally{let we=P!==null?S.get(P).__webglFramebuffer:null;ce.bindFramebuffer(R.FRAMEBUFFER,we)}}},this.readRenderTargetPixelsAsync=function(x,L,z,B,V,he,_e,de=0){return mt(this,null,function*(){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let be=S.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(be=be[_e]),be)if(L>=0&&L<=x.width-B&&z>=0&&z<=x.height-V){ce.bindFramebuffer(R.FRAMEBUFFER,be);let we=x.textures[de],Ve=we.format,Xe=we.type;if(x.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+de),!Tt.textureFormatReadable(Ve))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!Tt.textureTypeReadable(Xe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Ce=R.createBuffer();R.bindBuffer(R.PIXEL_PACK_BUFFER,Ce),R.bufferData(R.PIXEL_PACK_BUFFER,he.byteLength,R.STREAM_READ),R.readPixels(L,z,B,V,A.convert(Ve),A.convert(Xe),0);let _t=P!==null?S.get(P).__webglFramebuffer:null;ce.bindFramebuffer(R.FRAMEBUFFER,_t);let Ht=R.fenceSync(R.SYNC_GPU_COMMANDS_COMPLETE,0);return R.flush(),yield aE(R,Ht,4),R.bindBuffer(R.PIXEL_PACK_BUFFER,Ce),R.getBufferSubData(R.PIXEL_PACK_BUFFER,0,he),R.deleteBuffer(Ce),R.deleteSync(Ht),he}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")})},this.copyFramebufferToTexture=function(x,L=null,z=0){let B=Math.pow(2,-z),V=Math.floor(x.image.width*B),he=Math.floor(x.image.height*B),_e=L!==null?L.x:0,de=L!==null?L.y:0;v.setTexture2D(x,0),R.copyTexSubImage2D(R.TEXTURE_2D,z,0,0,_e,de,V,he),ce.unbindTexture()};let YE=R.createFramebuffer(),ZE=R.createFramebuffer();this.copyTextureToTexture=function(x,L,z=null,B=null,V=0,he=0){let _e,de,be,we,Ve,Xe,Ce,_t,Ht,Ft=x.isCompressedTexture?x.mipmaps[he]:x.image;if(z!==null)_e=z.max.x-z.min.x,de=z.max.y-z.min.y,be=z.isBox3?z.max.z-z.min.z:1,we=z.min.x,Ve=z.min.y,Xe=z.isBox3?z.min.z:0;else{let zt=Math.pow(2,-V);_e=Math.floor(Ft.width*zt),de=Math.floor(Ft.height*zt),x.isDataArrayTexture?be=Ft.depth:x.isData3DTexture?be=Math.floor(Ft.depth*zt):be=1,we=0,Ve=0,Xe=0}B!==null?(Ce=B.x,_t=B.y,Ht=B.z):(Ce=0,_t=0,Ht=0);let Mt=A.convert(L.format),pn=A.convert(L.type),ve;L.isData3DTexture?(v.setTexture3D(L,0),ve=R.TEXTURE_3D):L.isDataArrayTexture||L.isCompressedArrayTexture?(v.setTexture2DArray(L,0),ve=R.TEXTURE_2D_ARRAY):(v.setTexture2D(L,0),ve=R.TEXTURE_2D),ce.activeTexture(R.TEXTURE0),ce.pixelStorei(R.UNPACK_FLIP_Y_WEBGL,L.flipY),ce.pixelStorei(R.UNPACK_PREMULTIPLY_ALPHA_WEBGL,L.premultiplyAlpha),ce.pixelStorei(R.UNPACK_ALIGNMENT,L.unpackAlignment);let Un=ce.getParameter(R.UNPACK_ROW_LENGTH),it=ce.getParameter(R.UNPACK_IMAGE_HEIGHT),Zn=ce.getParameter(R.UNPACK_SKIP_PIXELS),Ii=ce.getParameter(R.UNPACK_SKIP_ROWS),Er=ce.getParameter(R.UNPACK_SKIP_IMAGES);ce.pixelStorei(R.UNPACK_ROW_LENGTH,Ft.width),ce.pixelStorei(R.UNPACK_IMAGE_HEIGHT,Ft.height),ce.pixelStorei(R.UNPACK_SKIP_PIXELS,we),ce.pixelStorei(R.UNPACK_SKIP_ROWS,Ve),ce.pixelStorei(R.UNPACK_SKIP_IMAGES,Xe);let ao=x.isDataArrayTexture||x.isData3DTexture,Et=L.isDataArrayTexture||L.isData3DTexture;if(x.isDepthTexture){let zt=S.get(x),wr=S.get(L),Ct=S.get(zt.__renderTarget),Tr=S.get(wr.__renderTarget);ce.bindFramebuffer(R.READ_FRAMEBUFFER,Ct.__webglFramebuffer),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,Tr.__webglFramebuffer);for(let co=0;co<be;co++)ao&&(R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,S.get(x).__webglTexture,V,Xe+co),R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,S.get(L).__webglTexture,he,Ht+co)),R.blitFramebuffer(we,Ve,_e,de,Ce,_t,_e,de,R.DEPTH_BUFFER_BIT,R.NEAREST);ce.bindFramebuffer(R.READ_FRAMEBUFFER,null),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else if(V!==0||x.isRenderTargetTexture||S.has(x)){let zt=S.get(x),wr=S.get(L);ce.bindFramebuffer(R.READ_FRAMEBUFFER,YE),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,ZE);for(let Ct=0;Ct<be;Ct++)ao?R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,zt.__webglTexture,V,Xe+Ct):R.framebufferTexture2D(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,zt.__webglTexture,V),Et?R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,wr.__webglTexture,he,Ht+Ct):R.framebufferTexture2D(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,wr.__webglTexture,he),V!==0?R.blitFramebuffer(we,Ve,_e,de,Ce,_t,_e,de,R.COLOR_BUFFER_BIT,R.NEAREST):Et?R.copyTexSubImage3D(ve,he,Ce,_t,Ht+Ct,we,Ve,_e,de):R.copyTexSubImage2D(ve,he,Ce,_t,we,Ve,_e,de);ce.bindFramebuffer(R.READ_FRAMEBUFFER,null),ce.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else Et?x.isDataTexture||x.isData3DTexture?R.texSubImage3D(ve,he,Ce,_t,Ht,_e,de,be,Mt,pn,Ft.data):L.isCompressedArrayTexture?R.compressedTexSubImage3D(ve,he,Ce,_t,Ht,_e,de,be,Mt,Ft.data):R.texSubImage3D(ve,he,Ce,_t,Ht,_e,de,be,Mt,pn,Ft):x.isDataTexture?R.texSubImage2D(R.TEXTURE_2D,he,Ce,_t,_e,de,Mt,pn,Ft.data):x.isCompressedTexture?R.compressedTexSubImage2D(R.TEXTURE_2D,he,Ce,_t,Ft.width,Ft.height,Mt,Ft.data):R.texSubImage2D(R.TEXTURE_2D,he,Ce,_t,_e,de,Mt,pn,Ft);ce.pixelStorei(R.UNPACK_ROW_LENGTH,Un),ce.pixelStorei(R.UNPACK_IMAGE_HEIGHT,it),ce.pixelStorei(R.UNPACK_SKIP_PIXELS,Zn),ce.pixelStorei(R.UNPACK_SKIP_ROWS,Ii),ce.pixelStorei(R.UNPACK_SKIP_IMAGES,Er),he===0&&L.generateMipmaps&&R.generateMipmap(ve),ce.unbindTexture()},this.initRenderTarget=function(x){S.get(x).__webglFramebuffer===void 0&&v.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?v.setTextureCube(x,0):x.isData3DTexture?v.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?v.setTexture2DArray(x,0):v.setTexture2D(x,0),ce.unbindTexture()},this.resetState=function(){G=0,W=0,P=null,ce.reset(),ie.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return Ei}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=et._getDrawingBufferColorSpace(e),t.unpackColorSpace=et._getUnpackColorSpace()}};var Cl=class n{obstacleBox=new li;closestPoint=new N;findTrackContact(e,t,i){let r=null,s=Number.POSITIVE_INFINITY;for(let o of i){if(e.z>o.nearZ+t*.9||e.z<o.farZ-t*.9)continue;let a=o.width*.5-t*.12,c=e.x-o.centerX;if(Math.abs(c)<=a){let l=at.clamp((o.nearZ-e.z)/o.length,0,1),u=at.lerp(o.nearY,o.farY,l),d=u+t,h=e.y-d,f=o.kind==="ramp"||o.kind==="launch"||o.kind==="brake"?t*3.1:t*1.65;if(h<-f)continue;let g=Math.abs(h)+Math.max(0,-h)*.35;if(g>=s)continue;s=g,r={segment:o,surfaceY:u,edgeDistance:a-Math.abs(c)}}}return r}resolveWorldCollision(e,t,i,r){let s=null,o=Number.NEGATIVE_INFINITY;for(let a of r){let c=a.box;if(e.z>c.max.z+i||e.z<c.min.z-i||e.x>c.max.x+i||e.x<c.min.x-i||e.y>c.max.y+i*1.25||e.y<c.min.y-i)continue;let l=a.kind==="solid"?i*.28:-i*.5,u=e.x>=c.min.x+l&&e.x<=c.max.x-l&&e.z>=c.min.z+l&&e.z<=c.max.z-l;if(a.kind!=="kill"&&u&&t.y>=c.max.y+i*.28&&e.y<=c.max.y+i*1.4&&c.max.y>o){o=c.max.y,s={type:a.kind==="boost"?"boostGround":"ground",surfaceY:c.max.y,bouncy:a.kind==="solid"};continue}if(this.closestPoint.copy(e).clamp(c.min,c.max),this.closestPoint.distanceToSquared(e)>i*i)continue;if(a.kind==="kill")return{type:"kill"};let h=i*.42,f=e.x>c.min.x+h&&e.x<c.max.x-h;if(a.kind==="solid"&&f&&t.z>=c.max.z+i*.12&&e.z<=c.max.z+i&&e.y<c.max.y+i*.35)return{type:"kill"};let y=(c.min.x+c.max.x)*.5,m=e.x>=y?1:-1;return e.x=m>0?c.max.x+i:c.min.x-i,{type:"bounce",normalX:m,bouncy:a.kind==="solid"}}return s}resolveTrackPillarCollision(e,t,i,r){for(let s of r){if(e.z>s.nearZ+i||e.z<s.farZ-i)continue;let o=at.clamp(e.z,s.farZ,s.nearZ),a=at.clamp((s.nearZ-o)/s.length,0,1),c=at.lerp(s.nearY,s.farY,a),l=c-95;if(e.y>c-i*.35||e.y<l-i)continue;let u=s.width*.5,d=e.x-s.centerX,h=i*1.55,f=i*.45;if(Math.abs(d)<=u+f&&t.z>=s.nearZ+i*.1&&e.z<=s.nearZ+h)return{type:"kill"};if(Math.abs(d)>u&&Math.abs(d)<=u+i*1.4){let y=Math.sign(d)||1;return e.x=s.centerX+y*(u+i),{type:"bounce",normalX:y}}}return null}hitObstacle(e,t,i){let r=t*t;for(let s of i)if(this.obstacleBox.setFromObject(s.mesh),this.closestPoint.copy(e).clamp(this.obstacleBox.min,this.obstacleBox.max),this.closestPoint.distanceToSquared(e)<=r)return s;return null}hitPowerup(e,t,i){for(let r of i){let s=t+r.radius,o=(r.group.position.x-e.x)**2+(r.group.position.z-e.z)**2,a=e.y-r.group.position.y;if(o<=s*s&&a>=-s&&a<=s+4.25)return r}return null}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var Dl=class n{frameId=0;lastTime=0;elapsed=0;running=!1;start(e){this.stop(),this.running=!0,this.elapsed=0,this.lastTime=performance.now();let t=i=>{if(!this.running)return;let r=Math.min((i-this.lastTime)/1e3,.05);this.lastTime=i,this.elapsed+=r,e(r,this.elapsed),this.frameId=requestAnimationFrame(t)};this.frameId=requestAnimationFrame(t)}stop(){this.frameId!==0&&(cancelAnimationFrame(this.frameId),this.frameId=0),this.running=!1}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var Il=class n{leftKeys=new Set(["KeyA","ArrowLeft"]);rightKeys=new Set(["KeyD","ArrowRight"]);forwardKeys=new Set(["KeyW","ArrowUp"]);backKeys=new Set(["KeyS","ArrowDown"]);pressedKeys=new Set;touchDirection=0;restartQueued=!1;pauseQueued=!1;keyboardTarget=null;documentTarget=null;keyDownHandler=e=>{if(this.leftKeys.has(e.code)||this.rightKeys.has(e.code)||this.forwardKeys.has(e.code)||this.backKeys.has(e.code)){this.pressedKeys.add(e.code),e.preventDefault();return}if(e.code==="KeyR"){this.restartQueued=!0,e.preventDefault();return}e.code==="Space"&&(this.pauseQueued=!0,e.preventDefault())};keyUpHandler=e=>{(this.leftKeys.has(e.code)||this.rightKeys.has(e.code)||this.forwardKeys.has(e.code)||this.backKeys.has(e.code))&&(this.pressedKeys.delete(e.code),e.preventDefault())};releaseHeldInput=()=>{this.pressedKeys.clear(),this.touchDirection=0};visibilityChangeHandler=()=>{this.documentTarget?.visibilityState==="hidden"&&this.releaseHeldInput()};attach(e=window){this.detach(),this.keyboardTarget=e,this.documentTarget=e.document,e.addEventListener("keydown",this.keyDownHandler,{passive:!1}),e.addEventListener("keyup",this.keyUpHandler,{passive:!1}),e.addEventListener("blur",this.releaseHeldInput),this.documentTarget.addEventListener("visibilitychange",this.visibilityChangeHandler)}detach(){this.keyboardTarget&&(this.keyboardTarget.removeEventListener("keydown",this.keyDownHandler),this.keyboardTarget.removeEventListener("keyup",this.keyUpHandler),this.keyboardTarget.removeEventListener("blur",this.releaseHeldInput),this.documentTarget?.removeEventListener("visibilitychange",this.visibilityChangeHandler),this.keyboardTarget=null,this.documentTarget=null,this.releaseHeldInput(),this.restartQueued=!1,this.pauseQueued=!1)}get steering(){let e=Number(this.isPressed(this.rightKeys))-Number(this.isPressed(this.leftKeys));return Math.max(-1,Math.min(1,e+this.touchDirection))}get throttle(){return Number(this.isPressed(this.forwardKeys))-Number(this.isPressed(this.backKeys))}setTouchDirection(e){this.touchDirection=Math.max(-1,Math.min(1,e))}releaseTouchDirection(e){this.touchDirection===e&&(this.touchDirection=0)}consumeRestart(){let e=this.restartQueued;return this.restartQueued=!1,e}consumePause(){let e=this.pauseQueued;return this.pauseQueued=!1,e}isPressed(e){for(let t of e)if(this.pressedKeys.has(t))return!0;return!1}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var VE="neon-slope-best-stage-score",Al=class n{score=Nn(0);speed=Nn(24);bestScore=Nn(this.readBestScore());elapsed=0;slowdownPenalty=0;baseSpeed=24;maxSpeed=82;reset(){this.elapsed=0,this.slowdownPenalty=0,this.score.set(0),this.speed.set(this.baseSpeed),this.bestScore.set(this.readBestScore())}update(e){this.elapsed+=e,this.slowdownPenalty=Math.max(0,this.slowdownPenalty-e*6.2)}applySlowdown(e){this.slowdownPenalty=Math.min(22,this.slowdownPenalty+e)}completeStages(e){e<=0||this.score.update(t=>t+e)}syncSpeed(e){this.speed.set(e)}slowdown(){return this.slowdownPenalty}commitBestScore(){let e=this.score();if(!(e<=this.bestScore())){this.bestScore.set(e);try{localStorage.setItem(VE,String(e))}catch(t){}}}difficulty(){return Math.min(1,(this.score()+this.elapsed*.075)/20)}minSpeed(){return this.baseSpeed*.75}maxSpeedLimit(){return this.maxSpeed}readBestScore(){try{let e=localStorage.getItem(VE);return e&&Number(e)||0}catch(e){return 0}}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var Rl=class n{leadDistance=390;cleanupDistance=300;trackThickness=95;downhillRate=.135;minRoadCenterGap=12.4;maxRoadCenterGap=26.5;scene=null;trackMaterial=null;rampMaterial=null;edgeMaterial=null;laneMaterial=null;obstacleMaterial=null;barMaterial=null;killMaterial=null;killLineMaterial=null;boostPlatformMaterial=null;powerupMaterials=new Map;powerupWireMaterials=new Map;buildingMaterial=null;wireMaterial=null;dimWireMaterial=null;gateMaterial=null;nextNearZ=28;nextNearY=0;lastCenterX=0;segmentIndex=0;generatedStageIndex=0;lastStagePattern=-1;activeRoadCount=1;activeRoadCenters=[0];activeRoadStates=[];activeNearbyIslands=[];nextIslandId=0;roadClampOffset=0;roadClampHalfRange=null;roadClampAbsoluteMinX=null;roadClampAbsoluteMaxX=null;stagePatternBag=[];chillStagePatternBag=[];nextStageShouldBeChill=!1;mutableSegments=[];mutableObstacles=[];mutablePowerups=[];mutableDecorations=[];mutableWorldColliders=[];stageGates=[];get segments(){return this.mutableSegments}get obstacles(){return this.mutableObstacles}get powerups(){return this.mutablePowerups}get worldColliders(){return this.mutableWorldColliders}init(e){this.scene=e,this.createMaterials(),this.reset()}reset(){for(this.clearDynamicObjects(),this.nextNearZ=28,this.nextNearY=0,this.lastCenterX=0,this.segmentIndex=0,this.generatedStageIndex=0,this.lastStagePattern=-1,this.activeRoadCount=1,this.activeRoadCenters=[0],this.activeRoadStates=[],this.activeNearbyIslands=[],this.nextIslandId=0,this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null,this.stagePatternBag=[],this.chillStagePatternBag=[],this.nextStageShouldBeChill=!1,this.stageGates.length=0,this.createStarterRunway();this.nextNearZ>-this.leadDistance;)this.addStage()}ensureAhead(e){for(;this.nextNearZ>e-this.leadDistance;)this.addStage();this.cleanupBehind(e)}consumePassedStages(e){let t=0;for(;this.stageGates.length>0&&e<=this.stageGates[0].z;)this.stageGates.shift(),t+=1;return t}collectPowerup(e){let t=this.mutablePowerups.indexOf(e);t!==-1&&(this.mutablePowerups.splice(t,1),this.disposeObject(e.group,!1))}update(e,t){this.edgeMaterial&&(this.edgeMaterial.emissiveIntensity=2.8+Math.sin(t*6.4)*.5),this.rampMaterial&&(this.rampMaterial.emissiveIntensity=.65+Math.sin(t*3)*.16);for(let i of this.mutableObstacles)i.moveAmplitude!==0&&(i.mesh.position.x=i.baseX+Math.sin(t*i.moveSpeed+i.movePhase)*i.moveAmplitude),i.moveYAmplitude!==0&&(i.mesh.position.y=i.baseY+Math.sin(t*i.moveSpeed+i.movePhase)*i.moveYAmplitude);for(let i of this.mutablePowerups)i.group.position.y=i.baseY+Math.sin(t*3.2+i.phase)*.28,i.group.rotation.y+=e*1.9,i.group.rotation.x=Math.sin(t*1.7+i.phase)*.22}surfaceYAt(e,t){let i=at.clamp((e.nearZ-t)/e.length,0,1);return at.lerp(e.nearY,e.farY,i)}lowestSurfaceYNearZ(e){let t=null,i=Number.POSITIVE_INFINITY,r=Number.POSITIVE_INFINITY;for(let o of this.mutableSegments){let a=e>o.nearZ?e-o.nearZ:e<o.farZ?o.farZ-e:0,c=at.clamp(e,o.farZ,o.nearZ),l=this.surfaceYAt(o,c);a<90&&(r=Math.min(r,l)),a<i&&(i=a,t=o)}if(Number.isFinite(r))return r;if(!t)return this.lowestActiveRoadY();let s=at.clamp(e,t.farZ,t.nearZ);return this.surfaceYAt(t,s)}highestSurfaceYNearZ(e){let t=null,i=Number.POSITIVE_INFINITY,r=Number.NEGATIVE_INFINITY;for(let o of this.mutableSegments){let a=e>o.nearZ?e-o.nearZ:e<o.farZ?o.farZ-e:0,c=at.clamp(e,o.farZ,o.nearZ),l=this.surfaceYAt(o,c);a<90&&(r=Math.max(r,l)),a<i&&(i=a,t=o)}if(Number.isFinite(r))return r;if(!t)return this.lowestActiveRoadY();let s=at.clamp(e,t.farZ,t.nearZ);return this.surfaceYAt(t,s)}fallLimitForZ(e){return this.lowestSurfaceYNearZ(e)-75}destroy(){this.clearDynamicObjects(),this.trackMaterial?.dispose(),this.rampMaterial?.dispose(),this.edgeMaterial?.dispose(),this.laneMaterial?.dispose(),this.obstacleMaterial?.dispose(),this.barMaterial?.dispose(),this.killMaterial?.dispose(),this.killLineMaterial?.dispose(),this.boostPlatformMaterial?.dispose();for(let e of this.powerupMaterials.values())e.dispose();for(let e of this.powerupWireMaterials.values())e.dispose();this.buildingMaterial?.dispose(),this.wireMaterial?.dispose(),this.dimWireMaterial?.dispose(),this.gateMaterial?.dispose(),this.trackMaterial=null,this.rampMaterial=null,this.edgeMaterial=null,this.laneMaterial=null,this.obstacleMaterial=null,this.barMaterial=null,this.killMaterial=null,this.killLineMaterial=null,this.boostPlatformMaterial=null,this.powerupMaterials.clear(),this.powerupWireMaterials.clear(),this.buildingMaterial=null,this.wireMaterial=null,this.dimWireMaterial=null,this.gateMaterial=null,this.scene=null}createStarterRunway(){for(let e=0;e<4;e+=1)this.addTrackSegment({centerX:0,farYOffset:-4.2,kind:"starter",length:38,stageIndex:-1,width:12})}addStage(){let e=this.generatedStageIndex,t=Math.min(1,e/18);e%5===0&&this.activateRoadSequence();let i=this.nextNearZ,r=this.lowestActiveRoadY(),s=this.random(2.8,5.6+t*1.2),o=this.random(3.4,6.4+t*1.8),a=this.nextStageShouldBeChill,c=!1;if(this.activeRoadCount===2||this.activeRoadCount===3)c=this.addParallelGeneratedRoads(e,t,this.activeRoadCount,s,o,a);else{let u=this.nextStagePattern(a);c=!this.isChillStagePattern(u),this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null;let d=this.mutableSegments.length;this.runStagePattern(u,e,t),this.maybeSpawnPowerupForStage(d),this.advanceGap(s,o),this.activeRoadCenters=[this.lastCenterX],this.activeRoadStates=[{centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY}]}let l=this.captureRoadCursor();this.maybeSpawnNearbyIsland(e,i,r),this.generateNearbyIslands(e,t,s,o,a),this.restoreRoadCursor(l),this.stageGates.push({stageIndex:e,z:this.nextNearZ}),this.addEnvironmentBand(i,this.nextNearZ,r,this.lowestActiveRoadY(),e),this.nextStageShouldBeChill=c,this.generatedStageIndex+=1}rollLaneCount(){let e=Math.random();return e<1/3?1:e<2/3?2:3}activateRoadSequence(){let e=this.activeRoadStates.length>0?this.activeRoadStates:[{centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY}],t=this.average(e.map(l=>l.centerX)),i=Math.max(...e.map(l=>l.nextNearZ)),r=this.average(e.map(l=>l.nextNearY)),s=Math.min(...e.map(l=>l.nextNearY));this.activeRoadCount=this.rollLaneCount(),this.activeRoadCenters=this.centersForRoadCount(this.activeRoadCount,t);let o=this.roadHeightOffsets(this.activeRoadCount),a=Math.max(...o),c=s-this.random(2.4,4.1);this.activeRoadStates=this.activeRoadCenters.map((l,u)=>({centerX:l,nextNearZ:i,nextNearY:this.activeRoadCount===1?r:c+o[u]-a})),this.lastCenterX=this.activeRoadCount===1?this.activeRoadCenters[0]:this.average(this.activeRoadCenters),this.syncCursorFromRoadStates()}centersForRoadCount(e,t){if(e===1)return[this.clampValue(t+this.randomSigned(1),-7,7)];if(e===2){let o=this.random(17.6,26.8),a=this.clampValue(t+this.randomSigned(1.2),-23+o*.5,23-o*.5);return[a-o*.5,a+o*.5]}let i=this.random(15.4,25.8),r=this.random(15.4,25.8),s=this.clampValue(t+this.randomSigned(1.4),-24+i,24-r);return[s-i,s,s+r]}roadHeightOffsets(e){if(e===1)return[0];let t=e===2?[-1.9,2.2]:[-3.2,.4,3.1];return this.shuffleNumbers(t).map(i=>i+this.randomSigned(.55))}nextStagePattern(e=!1){let t=e?this.chillStagePatternBag:this.stagePatternBag,i=e?[10,11,12,13,14]:[0,1,2,3,4,5,6,7,8,9];if(t.length===0&&(t=this.shuffleNumbers(i),e?this.chillStagePatternBag=t:this.stagePatternBag=t),t[0]===this.lastStagePattern&&t.length>1){let s=Math.floor(this.random(1,t.length));[t[0],t[s]]=[t[s],t[0]]}let r=t.shift()??i[0];return this.lastStagePattern=r,r}isChillStagePattern(e){return e>=10}runStagePattern(e,t,i){switch(e){case 0:this.addLaunchRampStage(t,i);break;case 1:this.addBrakeLaunchStage(t,i);break;case 2:this.addSkyDropStage(t,i);break;case 3:this.addMovingLaneStage(t,i);break;case 4:this.addRampChainStage(t,i);break;case 5:this.addSlalomStage(t,i);break;case 6:this.addNarrowLandingStage(t,i);break;case 8:this.addBoostTunnelStage(t,i);break;case 9:this.addDropBarStage(t,i);break;case 10:this.addPeacefulCruiseStage(t,i);break;case 11:this.addPeacefulSwerveStage(t,i);break;case 12:this.addPeacefulWaveStage(t,i);break;case 13:this.addPeacefulNarrowStage(t,i);break;case 14:this.addPeacefulStepStage(t,i);break;default:this.addLanePressureStage(t,i);break}}addParallelGeneratedRoads(e,t,i,r,s,o){if(this.activeRoadStates.length!==i){let d=this.centersForRoadCount(i,this.lastCenterX),h=this.roadHeightOffsets(i);this.activeRoadStates=d.map((f,g)=>({centerX:f,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY+h[g]}))}let a=[],c=!1,l=i===2?2.55:1.45,u=[...this.activeRoadStates].sort((d,h)=>d.centerX-h.centerX);for(let d of this.activeRoadStates){let h=u.indexOf(d),f=u[h-1],g=u[h+1];this.nextNearZ=d.nextNearZ,this.nextNearY=d.nextNearY,this.lastCenterX=d.centerX,this.roadClampOffset=d.centerX,this.roadClampHalfRange=l,this.roadClampAbsoluteMinX=f?f.centerX+this.minRoadCenterGap:-24,this.roadClampAbsoluteMaxX=g?g.centerX-this.minRoadCenterGap:24;let y=this.nextStagePattern(o);c||=!this.isChillStagePattern(y);let m=this.mutableSegments.length;this.runStagePattern(y,e,t),this.maybeSpawnPowerupForStage(m),this.advanceGap(Math.max(1.8,r+this.randomSigned(1.3)),Math.max(2.4,s+this.randomSigned(1.2))),a.push({centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY})}return this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null,a=this.settleRoadSpacing(a,i),this.activeRoadStates=a,this.activeRoadCenters=a.map(d=>d.centerX),this.lastCenterX=this.average(this.activeRoadCenters),this.syncCursorFromRoadStates(),c}maybeSpawnNearbyIsland(e,t,i){if(e<1||this.activeNearbyIslands.length>=2||Math.random()>.2)return;let r=this.chooseIslandPlacement();r&&(this.activeNearbyIslands.push({id:this.nextIslandId,state:{centerX:r.centerX,nextNearZ:t-this.random(2.5,6.5),nextNearY:i-this.random(1.2,4.4)},remainingStages:Math.floor(this.random(2,5)),farZ:t,minX:r.minX,maxX:r.maxX}),this.nextIslandId+=1)}generateNearbyIslands(e,t,i,r,s){for(let o of this.activeNearbyIslands){if(o.remainingStages<=0)continue;if(!this.refreshIslandBounds(o)){o.remainingStages=0;continue}this.nextNearZ=o.state.nextNearZ,this.nextNearY=o.state.nextNearY,this.lastCenterX=o.state.centerX,this.roadClampOffset=o.state.centerX,this.roadClampHalfRange=2.1,this.roadClampAbsoluteMinX=o.minX,this.roadClampAbsoluteMaxX=o.maxX;let a=this.mutableSegments.length;this.runStagePattern(this.randomIslandStagePattern(s),e,t),this.maybeSpawnPowerupForStage(a),this.advanceGap(Math.max(1.8,i+this.randomSigned(1.8)),Math.max(2.2,r+this.randomSigned(1.4))),o.state={centerX:this.clampValue(this.lastCenterX,o.minX,o.maxX),nextNearZ:this.nextNearZ,nextNearY:this.nextNearY},o.farZ=this.nextNearZ,o.remainingStages-=1}this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null}maybeSpawnPowerupForStage(e){if(!this.scene||Math.random()>=1/20)return;let t=this.mutableSegments.slice(e).filter(i=>i.length>=18&&i.width>=3.1);if(t.length!==0)for(let i=0;i<14;i+=1){let r=t[Math.floor(this.random(0,t.length))],s=this.random(.24,.78),o=at.lerp(r.nearZ,r.farZ,s),a=this.surfaceYAt(r,o),c=r.centerX+this.randomSigned(Math.min(2.1,r.width*.22)),l=new N(c,a+1.45,o);if(this.isPowerupSpotClear(l,r)){this.spawnPowerup(this.randomPowerupKind(),l);return}}}isPowerupSpotClear(e,t){for(let i of this.mutableObstacles){let r=Math.abs(i.mesh.position.z-e.z),s=Math.abs(i.mesh.position.x-e.x);if(r<8&&s<t.width*.42+1.6)return!1}for(let i of this.mutableWorldColliders)if(i.kind==="kill"&&i.box.distanceToPoint(e)<1.85)return!1;return!0}spawnPowerup(e,t){if(!this.scene)return;let i=this.powerupMaterials.get(e),r=this.powerupWireMaterials.get(e);if(!i||!r)return;let s=.58,o=new Wn;o.position.copy(t);let a=new dt(new ji(s,26,18),i);o.add(a);let c=new ji(s*1.12,16,10),l=new Ln(new is(c),r);c.dispose(),o.add(l);let u=new dt(new xa(s*1.32,.035,8,42),i);u.rotation.x=Math.PI*.5,o.add(u);let d=new dt(new xa(s*1.18,.028,8,38),i);d.rotation.y=Math.PI*.5,o.add(d);let h=new dt(new cl(.045,.045,4.25,8),i);h.position.y=2.15,o.add(h),o.add(new io(i.color,2.4,7)),this.scene.add(o),this.mutablePowerups.push({group:o,kind:e,radius:s,baseY:t.y,phase:Math.random()*Math.PI*2})}randomPowerupKind(){let e=Math.random();return e<.2?"speedShield":e<.5?"fling":e<.75?"crashGuard":"fallGuardian"}chooseIslandPlacement(){let t=[...this.activeRoadStates.length>0?this.activeRoadStates.map(d=>d.centerX):[this.lastCenterX]].sort((d,h)=>d-h),i=[...t,...this.activeNearbyIslands.map(d=>d.state.centerX)],r=[];for(let d=0;d<t.length-1;d+=1){let h=t[d],f=t[d+1],g=h+this.minRoadCenterGap,y=f-this.minRoadCenterGap;y-g>1.4&&r.push({centerX:this.random(g,y),minX:g,maxX:y})}let s=t[0],o=t[t.length-1],a=Math.max(-27,s-this.maxRoadCenterGap),c=Math.min(27,s-this.minRoadCenterGap),l=Math.max(-27,o+this.minRoadCenterGap),u=Math.min(27,o+this.maxRoadCenterGap);c-a>2.4&&r.push({centerX:this.random(a,c),minX:a,maxX:c}),u-l>2.4&&r.push({centerX:this.random(l,u),minX:l,maxX:u});for(let d of this.shuffleObjects(r)){let h=this.clampValue(d.centerX,d.minX,d.maxX);if(this.hasRoadClearance(h,i))return ot(fe({},d),{centerX:h})}return null}refreshIslandBounds(e){let t=this.activeNearbyIslands.filter(c=>c.id!==e.id).map(c=>c.state.centerX),i=this.activeRoadStates.length>0?[...this.activeRoadStates.map(c=>c.centerX)].sort((c,l)=>c-l):[this.lastCenterX],r=[...i].reverse().find(c=>c<e.state.centerX),s=i.find(c=>c>e.state.centerX),o=-27,a=27;if(r!==void 0&&(o=Math.max(o,r+this.minRoadCenterGap)),s!==void 0&&(a=Math.min(a,s-this.minRoadCenterGap)),r===void 0){let c=i[0];o=Math.max(o,c-this.maxRoadCenterGap),a=Math.min(a,c-this.minRoadCenterGap)}if(s===void 0){let c=i[i.length-1];o=Math.max(o,c+this.minRoadCenterGap),a=Math.min(a,c+this.maxRoadCenterGap)}for(let c of t)c<e.state.centerX?o=Math.max(o,c+this.minRoadCenterGap):a=Math.min(a,c-this.minRoadCenterGap);return a-o<1.1?!1:(e.minX=o,e.maxX=a,e.state.centerX=this.clampValue(e.state.centerX+this.randomSigned(1.15),o,a),!0)}hasRoadClearance(e,t){return t.every(i=>Math.abs(e-i)>=this.minRoadCenterGap)}settleRoadSpacing(e,t){let i=[...e].sort((d,h)=>d.centerX-h.centerX),r=t===2?this.maxRoadCenterGap:this.maxRoadCenterGap-1.2;for(let d=0;d<2;d+=1)for(let h=1;h<i.length;h+=1){let f=i[h-1],g=i[h],y=g.centerX-f.centerX;if(y<this.minRoadCenterGap){let m=(this.minRoadCenterGap-y)*.5;f.centerX-=m,g.centerX+=m}else if(y>r){let m=(y-r)*.5;f.centerX+=m,g.centerX-=m}}let s=this.average(i.map(d=>d.centerX)),o=-24,a=24,c=Math.min(...i.map(d=>d.centerX))-o,l=a-Math.max(...i.map(d=>d.centerX)),u=c<0?-c:l<0?l:-s*.08;return i.map(d=>ot(fe({},d),{centerX:this.clampValue(d.centerX+u,o,a)}))}addParallelRoadStage(e,t,i){let r=this.nextNearZ,s=this.nextNearY,o=this.nextStagePattern(),a=o===0||o===4||o===6,c=o===1,l=o===3||o===7,u=a?32+t*5:c?38+t*5:68+t*14,d=r-u,h=s+this.parallelStageYOffset(u,o,t),f=i===2?4.25-t*.25:3.45-t*.15,g=a?"launch":c?"brake":o===2?"ramp":"straight",y=this.centersForRoadCount(i,this.average(this.activeRoadCenters)),m=Math.floor(this.random(0,i));for(let p=0;p<i;p+=1){let b=this.createTrackSegment({centerX:y[p],farY:h,farZ:d,kind:g,length:u,nearY:s,nearZ:r,stageIndex:e,width:f});if(p!==m){let w=l||p%2===1?"slider":"cube";this.spawnObstacle(b,0,w,this.random(.38,.72))}else t>.6&&l&&this.spawnObstacle(b,Math.random()>.5?-1:1,"cube",.68)}this.nextNearZ=d,this.nextNearY=h,this.activeRoadCenters=y,this.lastCenterX=this.average(y),a&&this.advanceGap(24+t*8,10+t*4)}parallelStageYOffset(e,t,i){switch(t){case 0:case 4:case 6:return 6.5+i*3.5;case 1:return 4.5+i*2.2;case 2:return-e*(.18+i*.035);default:return-e*(.12+i*.025)}}addSplitLaneStage(e,t,i){let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(.9),-5.5,5.5),farYOffset:-5.8-t*2.2,kind:"straight",length:26,stageIndex:e,width:11.4-t});this.spawnObstacle(r,Math.random()>.5?-1:1,"cube",.54);let s=this.nextNearZ,o=this.nextNearY,a=i===2?78+t*12:88+t*14,c=s-a,l=o-a*(.12+t*.025),u=this.clamp(this.lastCenterX+this.randomSigned(1),i===3?-2.2:-4.8,i===3?2.2:4.8),d=i===2?[-3.25,3.25]:[-5.1,0,5.1],h=i===2?3.85-t*.2:3.25-t*.12,f=Math.floor(this.random(0,i)),g=[];for(let m=0;m<i;m+=1)g.push(this.createTrackSegment({centerX:u+d[m],farY:l,farZ:c,kind:"split",length:a,nearY:o,nearZ:s,stageIndex:e,width:h}));for(let m=0;m<g.length;m+=1){let p=g[m];m!==f&&this.spawnObstacle(p,0,m%2===0?"wall":"slider",.38+m*.18)}this.nextNearZ=c,this.nextNearY=l,this.lastCenterX=u,this.advanceGap(2.2+t*.8,2.5+t);let y=this.addTrackSegment({centerX:this.clamp(u+this.randomSigned(1.2),-6.5,6.5),farYOffset:-8-t*3.5,kind:"landing",length:38+t*8,stageIndex:e,width:10.2-t});t>.55&&this.spawnObstacle(y,Math.random()>.5?-1:1,"slider",.68)}addLaunchRampStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.2),-6.5,6.5),farYOffset:-8.5-t*3,kind:"straight",length:38,stageIndex:e,width:10.8-t});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.48),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.6+t),-7.2,7.2),farYOffset:8.5+t*4.5,kind:"launch",length:30+t*5,stageIndex:e,width:9.8-t*.9}),this.advanceGap(36+t*14,13+t*6);let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8+t*1.1),-7.4,7.4),farYOffset:-13-t*6,kind:"landing",length:58+t*10,stageIndex:e,width:10-t*1.25});this.spawnObstacle(r,0,"slider",.58)}addBrakeLaunchStage(e,t){this.addTrackSegment({centerX:this.lastCenterX,farYOffset:-7-t*2,kind:"straight",length:34,stageIndex:e,width:10.4-t});let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.2),-6.6,6.6),farYOffset:5.8+t*2.6,kind:"brake",length:28+t*4,stageIndex:e,width:9.6-t});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.42),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4+t),-7.2,7.2),farYOffset:7.2+t*3,kind:"launch",length:25+t*5,stageIndex:e,width:9.2-t}),this.advanceGap(30+t*12,11+t*5),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8),-7.4,7.4),farYOffset:-12-t*4,kind:"landing",length:48+t*12,stageIndex:e,width:8.8-t})}addSkyDropStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.4),-7.4,7.4),farYOffset:-22-t*12,kind:"ramp",length:82+t*12,stageIndex:e,width:10.5-t*1.4});this.spawnObstacle(i,-1,"wall",.36),this.spawnObstacle(i,1,"slider",.68),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.7),-7.5,7.5),farYOffset:5.8+t*2.8,kind:"launch",length:24+t*5,stageIndex:e,width:8.6-t}),this.advanceGap(28+t*10,12+t*5),this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2),-7.8,7.8),farYOffset:-10-t*5,kind:"landing",length:42+t*12,stageIndex:e,width:8.2-t*.7})}addMovingLaneStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8),-7,7),farYOffset:-(86+t*10)*(.12+t*.025),kind:"straight",length:86+t*10,stageIndex:e,width:11-t*1.2});this.spawnObstacle(i,-1,"slider",.22),this.spawnObstacle(i,1,"slider",.48),this.spawnObstacle(i,0,"slider",.75)}addDropBarStage(e,t){this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(.9),-6.4,6.4),farYOffset:-5.6-t*1.7,kind:"straight",length:24+t*4,stageIndex:e,width:10.8-t*.75});let i=78+t*16,r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.5+t),-7.2,7.2),farYOffset:-i*(.13+t*.028),kind:"straight",length:i,stageIndex:e,width:10.4-t*.85});this.spawnDropBarPair(r,.3),this.spawnObstacle(r,Math.random()>.5?-1:1,"cube",.66),t>.72&&this.spawnObstacle(r,Math.random()>.5?-1:1,"cube",.84)}spawnDropBarPair(e,t){let i=Math.random()*Math.PI*2,r=this.random(1.5,2.03),s=this.random(5.55,6.35);this.spawnObstacle(e,-1,"dropBar",t,{movePhase:i,moveSpeed:r,yTravel:s}),this.spawnObstacle(e,1,"dropBar",t,{movePhase:i+Math.PI,moveSpeed:r,yTravel:s})}addPeacefulCruiseStage(e,t){for(let i=0;i<3;i+=1){let r=30+i*4+t*4;this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1+t*.55),-6.8,6.8),farYOffset:-r*(.112+t*.012),kind:"straight",length:r,stageIndex:e,width:11.2-i*.25-t*.45})}}addPeacefulSwerveStage(e,t){let i=Math.random()>.5?1:-1;for(let r=0;r<4;r+=1){let s=i*(r%2===0?1.7+t*.4:-2.25-t*.45);this.addTrackSegment({centerX:this.clamp(this.lastCenterX+s,-7.4,7.4),farYOffset:-(23+t*3)*(.108+r*.008),kind:"chicane",length:23+t*3,stageIndex:e,width:10.5-t*.65})}}addPeacefulWaveStage(e,t){let i=[{kind:"straight",y:-4.8-t,length:24},{kind:"brake",y:2.8+t*.8,length:24},{kind:"ramp",y:-7.2-t*1.8,length:30},{kind:"straight",y:-5.4-t*1.2,length:28}];for(let r of i)this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.15+t*.55),-7.2,7.2),farYOffset:r.y,kind:r.kind,length:r.length+t*4,stageIndex:e,width:10.2-t*.55})}addPeacefulNarrowStage(e,t){let i=84+t*12;this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2+t),-7.5,7.5),farYOffset:-i*(.13+t*.018),kind:"straight",length:i,stageIndex:e,width:8.4-t*.75})}addPeacefulStepStage(e,t){for(let i=0;i<4;i+=1){let r=18+i*2+t*2;this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.25+t*.65),-7.2,7.2),farYOffset:i%2===0?-2.4-t*.6:-4.8-t,kind:"steps",length:r,stageIndex:e,width:9.8-t*.55})}}addRampChainStage(e,t){for(let r=0;r<3;r+=1)this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.6+t),-7.2,7.2),farYOffset:r===1?4.8+t*2:-8.2-t*2,kind:r===1?"launch":"ramp",length:28+r*5,stageIndex:e,width:9.6-t});this.advanceGap(26+t*8,10+t*5);let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.7+t*.8),-7.5,7.5),farYOffset:-10-t*4,kind:"landing",length:44+t*10,stageIndex:e,width:9-t});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.5)}addNarrowLandingStage(e,t){this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4),-6.4,6.4),farYOffset:8+t*3.8,kind:"launch",length:32+t*4,stageIndex:e,width:9.4-t*.8}),this.advanceGap(34+t*14,14+t*6);let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.2+t),-8,8),farYOffset:-12-t*6,kind:"landing",length:64+t*12,stageIndex:e,width:8-t*.8});this.spawnObstacle(i,Math.random()>.5?-1:1,"wall",.62)}addFastDropStage(e,t){let i=68+t*18,r=10.8-t*1.6,s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.6+t*2),-6.8,6.8),farYOffset:-i*(.18+t*.04),kind:"ramp",length:i,stageIndex:e,width:r});this.spawnObstacle(s,-1,"cube",.32),t>.25&&this.spawnObstacle(s,1,"slider",.68)}addSweepingTurnStage(e,t){let i=Math.random()>.5?1:-1,r=9.8-t*1.4;for(let s=0;s<4;s+=1){let o=(s+1)/4,a=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+i*(1.4+o*(1.35+t)),-8,8),farYOffset:-(19+t*4)*(.14+o*.035),kind:"chicane",length:19+t*4,stageIndex:e,width:r-s*.25});s===1&&this.spawnObstacle(a,-i,"wall",.48),s===3&&t>.35&&this.spawnObstacle(a,0,"slider",.58)}}addNarrowSlopeStage(e,t){let i=84+t*12,r=7.2-t*1.1,s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.2+t*1.4),-7.6,7.6),farYOffset:-i*(.15+t*.035),kind:"straight",length:i,stageIndex:e,width:r});this.spawnObstacle(s,-1,"wall",.28),this.spawnObstacle(s,1,"wall",.56),t>.55&&this.spawnObstacle(s,0,"slider",.78)}addRollingSlopeStage(e,t){for(let i=0;i<4;i+=1){let r=i%2===0?.18+t*.035:.09+t*.02,s=18+i*2+t*3,o=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.2+t*1.8),-7.2,7.2),farYOffset:-s*r,kind:i%2===0?"ramp":"straight",length:s,stageIndex:e,width:9.4-t*1.1});i===2&&this.spawnObstacle(o,Math.random()>.5?-1:1,"cube",.48)}}addLanePressureStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8+t*1.6),-7,7),farYOffset:-(72+t*14)*(.145+t*.04),kind:"straight",length:72+t*14,stageIndex:e,width:10-t*1.25}),r=Math.random()>.5?-1:1;this.spawnObstacle(i,r,"wall",.25),this.spawnObstacle(i,-r,"cube",.48),this.spawnObstacle(i,0,t>.45?"slider":"cube",.72)}addBrakeRampStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.3),-6.5,6.5),farYOffset:-5.2-t*1.5,kind:"straight",length:28,stageIndex:e,width:10.5-t*1.2});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.52);let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.1+t),-7,7),farYOffset:3.4+t*2.1,kind:"brake",length:30+t*7,stageIndex:e,width:9.4-t});this.spawnObstacle(r,0,"slider",.56);let s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(2.2+t*1.4),-7.8,7.8),farYOffset:-8.8-t*4.6,kind:"ramp",length:36+t*8,stageIndex:e,width:8.8-t*1.1});this.spawnObstacle(s,Math.random()>.5?-1:1,t>.4?"slider":"cube",.64)}addSlalomStage(e,t){let i=Math.random()>.5?1:-1;for(let r=0;r<5;r+=1){let s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+i*(r%2===0?2+t*.65:-2.65-t),-7.7,7.7),farYOffset:-(20+t*3)*(.115+r*.012),kind:"chicane",length:20+t*3,stageIndex:e,width:9.35-t*.85});r%2===1?this.spawnObstacle(s,0,"slider",.58):r>2&&this.spawnObstacle(s,-i,"wall",.48)}}addBoostTunnelStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.1),-6.4,6.4),farYOffset:-5.8-t*1.6,kind:"straight",length:28,stageIndex:e,width:10.6-t*.7});this.spawnObstacle(i,Math.random()>.5?-1:1,"cube",.52);let r=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(.7),-6.5,6.5),farYOffset:-(66+t*10)*(.165+t*.02),kind:"boost",length:66+t*10,stageIndex:e,width:10.2-t*.65});this.addTunnelVisual(r);let s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4+t),-7.1,7.1),farYOffset:-9.2-t*3.5,kind:"landing",length:38+t*6,stageIndex:e,width:9.6-t*.9});t>.45&&this.spawnObstacle(s,Math.random()>.5?-1:1,"slider",.62)}addPinballStage(e,t){let i=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.4),-6.4,6.4),farYOffset:-(82+t*8)*(.125+t*.03),kind:"straight",length:82+t*8,stageIndex:e,width:11.4-t*1.35});this.spawnObstacle(i,-1,"slider",.26),this.spawnObstacle(i,1,"slider",.52),this.spawnObstacle(i,0,t>.35?"slider":"cube",.76)}addRampJumpStage(e,t){let i=this.randomSigned(1.2+t*2.4),r=this.addTrackSegment({centerX:this.lastCenterX,kind:"straight",length:18,stageIndex:e,width:10.5-t*1.2});this.spawnObstacle(r,t>.35?-1:1,"cube",.45),this.addTrackSegment({centerX:this.lastCenterX+i*.4,farYOffset:1.75+t*.75,kind:"ramp",length:17,stageIndex:e,width:9.2-t}),this.advanceGap(5.2+t*2.8,1.35+t*.7);let s=this.addTrackSegment({centerX:this.lastCenterX+i,farYOffset:-1.4,kind:"landing",length:25,stageIndex:e,width:8.8-t*1.2});this.spawnObstacle(s,0,t>.45?"slider":"cube",.6)}addSplitBridgeStage(e,t){this.addTrackSegment({centerX:this.lastCenterX,kind:"straight",length:16,stageIndex:e,width:10.4-t}),this.advanceGap(3.5+t,.25);let i=this.nextNearZ,r=this.nextNearY,s=36,o=r-s*this.downhillRate,a=2.55+t*.55,c=3.15-t*.28,l=Math.random()>.5?-1:1,u=this.createTrackSegment({centerX:this.lastCenterX-a,farY:o,farZ:i-s,kind:"split",length:s,nearY:r,nearZ:i,stageIndex:e,width:c}),d=this.createTrackSegment({centerX:this.lastCenterX+a,farY:o,farZ:i-s,kind:"split",length:s,nearY:r,nearZ:i,stageIndex:e,width:c});this.spawnObstacle(l<0?d:u,0,t>.5?"slider":"cube",.5),this.nextNearZ=i-s,this.nextNearY=o,this.advanceGap(3.8+t*1.2,.35),this.addTrackSegment({centerX:this.lastCenterX,kind:"landing",length:20,stageIndex:e,width:9.4-t})}addStepDropStage(e,t){let i=3+(t>.45?1:0);for(let r=0;r<i;r+=1){let s=this.randomSigned(1.4+t*2.2),o=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+s,-7.2,7.2),farYOffset:-.75-t*.25,kind:"steps",length:15+r*1.5,stageIndex:e,width:7.5-t*1.5});(r>0||t>.35)&&this.spawnObstacle(o,r%2===0?-1:1,"wall",.48),this.advanceGap(2.5+t*2.4,.85+t*.45)}this.addTrackSegment({centerX:this.lastCenterX,farYOffset:-1.2,kind:"landing",length:22,stageIndex:e,width:9})}addChicaneStage(e,t){let i=Math.random()>.5?1:-1,r=7.6-t*1.2,s=[this.lastCenterX,this.clamp(this.lastCenterX+i*(3.2+t),-8,8),this.clamp(this.lastCenterX-i*(3.6+t*1.2),-8,8),this.clamp(this.lastCenterX+i*(2.2+t*1.4),-8,8)];for(let o=0;o<s.length;o+=1){let a=this.addTrackSegment({centerX:s[o],kind:"chicane",length:18,stageIndex:e,width:r});o>0&&this.spawnObstacle(a,-i,o%2===0?"cube":"wall",.52),o<s.length-1&&this.advanceGap(1.2+t*.9,.12)}}addWaveStage(e,t){for(let i=0;i<4;i+=1){let r=i%2===0,s=this.addTrackSegment({centerX:this.clamp(this.lastCenterX+this.randomSigned(1.8+t),-7,7),farYOffset:r?1.2+t*.4:-2-t*.35,kind:"ramp",length:17,stageIndex:e,width:8.6-t*.9});r||this.spawnObstacle(s,0,"slider",.58)}}addThreadTheNeedleStage(e,t){let i=this.addTrackSegment({centerX:this.lastCenterX+this.randomSigned(1.8),kind:"straight",length:48,stageIndex:e,width:10-t*1.2});this.spawnObstacle(i,-1,"wall",.3),this.spawnObstacle(i,1,"wall",.58),t>.45&&this.spawnObstacle(i,0,"slider",.78)}addTrackSegment(e){let t=this.nextNearZ,i=t-e.length,r=this.nextNearY,s=r+(e.farYOffset??-e.length*this.downhillRate),o=this.createTrackSegment({centerX:e.centerX,farY:s,farZ:i,kind:e.kind,length:e.length,nearY:r,nearZ:t,stageIndex:e.stageIndex,width:e.width});return this.nextNearZ=i,this.nextNearY=s,this.lastCenterX=e.centerX,o}createTrackSegment(e){if(!this.scene||!this.trackMaterial||!this.rampMaterial||!this.edgeMaterial||!this.laneMaterial)throw new Error("WorldService must be initialized before creating track segments.");let t=(e.nearZ+e.farZ)*.5,i=(e.nearY+e.farY)*.5,r=Math.asin((e.farY-e.nearY)/e.length),s=new Wn;s.position.set(e.centerX,i,t),s.rotation.x=r;let o=new $t(e.width,this.trackThickness,e.length),a=e.kind==="ramp"||e.kind==="launch"||e.kind==="brake"||e.kind==="boost"?this.rampMaterial:this.trackMaterial,c=new dt(o,a);c.position.y=-this.trackThickness*.5,c.receiveShadow=!0,s.add(c);let l=new $t(.12,.12,e.length),u=new dt(l,this.edgeMaterial);u.position.set(-e.width*.5,.08,0),s.add(u);let d=new dt(l.clone(),this.edgeMaterial);d.position.set(e.width*.5,.08,0),s.add(d);let h=e.width>5.6?2:1;for(let y=0;y<h;y+=1){let m=h===1?0:(y===0?-1:1)*e.width*.17,p=new dt(new $t(.045,.045,e.length*.86),this.laneMaterial);p.position.set(m,.105,0),s.add(p)}let f=Math.max(3,Math.floor(e.length/5));for(let y=0;y<f;y+=1){let m=-e.length*.38+y/Math.max(1,f-1)*e.length*.76,p=new dt(new $t(e.width*.78,.035,.035),this.laneMaterial);p.position.set(0,.11,m),s.add(p)}if(e.kind==="boost"){let y=new dt(new $t(e.width*.46,.055,e.length*.9),this.laneMaterial);y.position.set(0,.14,0),s.add(y)}this.scene.add(s);let g={group:s,centerX:e.centerX,centerZ:t,nearZ:e.nearZ,farZ:e.farZ,nearY:e.nearY,farY:e.farY,width:e.width,length:e.length,index:this.segmentIndex,stageIndex:e.stageIndex,kind:e.kind};return this.segmentIndex+=1,this.mutableSegments.push(g),g}addTunnelVisual(e){if(!this.edgeMaterial||!this.killMaterial||!this.killLineMaterial||!this.boostPlatformMaterial)return;let t=6,i=e.width*.5,r=.28,s=4.2,o=s+.15,a=e.width*.82,c=e.length*.84,l=(e.nearY+e.farY)*.5;for(let h=0;h<t;h+=1){let f=h/Math.max(1,t-1),g=-e.length*.42+f*e.length*.84,y=new dt(new $t(.18,s,r),this.edgeMaterial),m=new dt(new $t(.18,s,r),this.edgeMaterial);y.position.set(-i*.88,s*.5,g),m.position.set(i*.88,s*.5,g),e.group.add(y),e.group.add(m)}let u=new dt(new $t(a,.3,c),this.boostPlatformMaterial);u.position.set(0,o,0),u.receiveShadow=!0,e.group.add(u),this.registerWorldCollider(e.centerX,l+o,e.centerZ,a,.3,c,e.nearZ,"boost");for(let h of[-1,1]){let f=h*i*.93,g=new dt(new $t(.34,s,c),this.killMaterial);g.position.set(f,s*.5,0),g.receiveShadow=!0,e.group.add(g);let y=new Ln(new Gi(g.geometry),this.killLineMaterial);g.add(y),this.registerWorldCollider(e.centerX+f,l+s*.5,e.centerZ,.34,s,c,e.nearZ,"kill")}let d=new Ln(new Gi(u.geometry),this.edgeMaterial);u.add(d)}spawnObstacle(e,t,i,r,s={}){if(!this.scene||!this.obstacleMaterial||!this.barMaterial||!this.killLineMaterial)return;let o=this.sizeForKind(i,e.width),a=e.width*.28,c=at.lerp(e.nearZ,e.farZ,r),l=this.surfaceYAt(e,c),u=i==="dropBar"?s.yTravel??this.random(5.55,6.35):0,h=i==="dropBar"?e.centerX+t*(o.x*.5+.72*.5):e.centerX+t*a,f=i==="dropBar"?l+o.y*.5+u+.24:l+o.y*.5,g=new $t(o.x,o.y,o.z),y=this.obstacleMaterial,m=new dt(g,y);m.position.set(h,f,c),m.castShadow=!0,m.receiveShadow=!0;let p=new Ln(new Gi(g),this.killLineMaterial);m.add(p),this.scene.add(m),this.mutableObstacles.push({mesh:m,kind:i,baseX:h,baseY:f,moveAmplitude:i==="slider"?Math.max(0,e.width*.38-o.x*.35):0,moveYAmplitude:u,moveSpeed:s.moveSpeed??(i==="slider"?this.random(.675,1.125):i==="dropBar"?this.random(1.5,2.03):this.random(.8,1.25)),movePhase:s.movePhase??Math.random()*Math.PI*2})}addStageGate(e,t,i){if(!this.scene||!this.gateMaterial)return;let r=new Wn;r.userData.nearZ=e+1;let s=new $t(24+i%3*2,14,.35),o=new Gi(s);s.dispose();let a=new Ln(o,this.gateMaterial);a.position.set(0,t+6.7,e),r.add(a),this.scene.add(r),this.mutableDecorations.push(r)}addEnvironmentBand(e,t,i,r,s){if(!this.scene||!this.buildingMaterial||!this.wireMaterial||!this.dimWireMaterial||!this.gateMaterial)return;let o=new Wn;o.userData.nearZ=e;let a=Math.abs(e-t),c=Math.max(2,Math.floor(a/34));for(let l=0;l<=c;l+=1){let u=l/c,d=at.lerp(e,t,u),h=at.lerp(i,r,u);for(let f of[-1,1]){let g=h+this.random(14,42+s*.5),y=h-this.random(125,190),m=g-y,p=this.random(5.8,11.5),b=this.random(20,42),w=f*this.random(34,64),M=this.createSolidBlock(p,m,b,this.buildingMaterial,l%3===0?this.gateMaterial:this.wireMaterial),I=d+this.random(-4,4);if(M.position.set(w,y+m*.5,I),o.add(M),this.registerWorldCollider(w,y+m*.5,I,p,m,b,e,"solid"),l%3===1){let E=h+this.random(6,18),D=h-this.random(105,165),_=E-D,T=this.random(8,15),O=this.random(14,28),C=f*this.random(42,70),F=d+this.random(-5,5),G=this.createSolidBlock(T,_,O,this.buildingMaterial,this.dimWireMaterial);G.position.set(C,D+_*.5,F),o.add(G),this.registerWorldCollider(C,D+_*.5,F,T,_,O,e,"solid")}}}this.scene.add(o),this.mutableDecorations.push(o)}cleanupBehind(e){for(let t=this.activeNearbyIslands.length-1;t>=0;t-=1){let i=this.activeNearbyIslands[t];i.remainingStages<=0&&i.farZ>e+this.cleanupDistance&&this.activeNearbyIslands.splice(t,1)}for(;this.mutableSegments.length>0&&this.mutableSegments[0].nearZ>e+this.cleanupDistance;){let t=this.mutableSegments.shift();t&&this.disposeObject(t.group,!1)}for(let t=this.mutableObstacles.length-1;t>=0;t-=1){let i=this.mutableObstacles[t];i.mesh.position.z>e+this.cleanupDistance&&(this.mutableObstacles.splice(t,1),this.disposeObject(i.mesh,!1))}for(let t=this.mutablePowerups.length-1;t>=0;t-=1){let i=this.mutablePowerups[t];i.group.position.z>e+this.cleanupDistance&&(this.mutablePowerups.splice(t,1),this.disposeObject(i.group,!1))}for(let t=this.mutableDecorations.length-1;t>=0;t-=1){let i=this.mutableDecorations[t];Number(i.userData.nearZ??i.position.z)>e+this.cleanupDistance&&(this.mutableDecorations.splice(t,1),this.disposeObject(i,!1))}for(let t=this.mutableWorldColliders.length-1;t>=0;t-=1)this.mutableWorldColliders[t].nearZ>e+this.cleanupDistance&&this.mutableWorldColliders.splice(t,1)}clearDynamicObjects(){for(let e of this.mutableSegments)this.disposeObject(e.group,!1);for(let e of this.mutableObstacles)this.disposeObject(e.mesh,!1);for(let e of this.mutablePowerups)this.disposeObject(e.group,!1);for(let e of this.mutableDecorations)this.disposeObject(e,!1);this.mutableSegments.length=0,this.mutableObstacles.length=0,this.mutablePowerups.length=0,this.mutableDecorations.length=0,this.mutableWorldColliders.length=0}disposeObject(e,t){e.parent?e.parent.remove(e):this.scene&&this.scene.remove(e),e.traverse(i=>{let r=i;if(r.geometry?.dispose(),!(!t||!r.material))if(Array.isArray(r.material))for(let s of r.material)s.dispose();else r.material.dispose()})}createSolidBlock(e,t,i,r,s){let o=new Wn,a=new $t(e,t,i),c=new dt(a,r);c.receiveShadow=!0,o.add(c);let l=new Gi(a),u=new Ln(l,s);return o.add(u),o}registerWorldCollider(e,t,i,r,s,o,a,c){let l=r*.5,u=s*.5,d=o*.5;this.mutableWorldColliders.push({box:new li(new N(e-l,t-u,i-d),new N(e+l,t+u,i+d)),kind:c,nearZ:a})}createWireBox(e,t,i,r){let s=new $t(e,t,i),o=new Gi(s);return s.dispose(),new Ln(o,r)}createMaterials(){this.trackMaterial=new In({color:462859,roughness:.42,metalness:.34,emissive:267532,emissiveIntensity:.72}),this.rampMaterial=new In({color:661006,roughness:.34,metalness:.42,emissive:797975,emissiveIntensity:.72}),this.edgeMaterial=new In({color:3800852,roughness:.12,metalness:.1,emissive:3800852,emissiveIntensity:3}),this.laneMaterial=new no({color:7536443,transparent:!0,opacity:.66}),this.obstacleMaterial=new In({color:16724319,roughness:.16,metalness:.18,emissive:16716351,emissiveIntensity:2.45}),this.barMaterial=new In({color:9109294,roughness:.18,metalness:.25,emissive:4652818,emissiveIntensity:2.1}),this.killMaterial=new In({color:2162696,roughness:.22,metalness:.2,emissive:16717647,emissiveIntensity:1.35,transparent:!0,opacity:.86}),this.killLineMaterial=new On({color:16726632,transparent:!0,opacity:.95}),this.boostPlatformMaterial=new In({color:533517,roughness:.28,metalness:.35,emissive:3407658,emissiveIntensity:1.1,transparent:!0,opacity:.9});let e={speedShield:16724319,fling:3800852,crashGuard:3106559,fallGuardian:6744319};for(let[t,i]of Object.entries(e))this.powerupMaterials.set(t,new In({color:i,roughness:.12,metalness:.18,emissive:i,emissiveIntensity:2.65,transparent:!0,opacity:.92})),this.powerupWireMaterials.set(t,new On({color:i,transparent:!0,opacity:.95}));this.buildingMaterial=new In({color:463882,roughness:.72,metalness:.18,emissive:204553,emissiveIntensity:.42,transparent:!0,opacity:.92}),this.wireMaterial=new On({color:3800852,transparent:!0,opacity:.9}),this.dimWireMaterial=new On({color:2078782,transparent:!0,opacity:.5}),this.gateMaterial=new On({color:10223440,transparent:!0,opacity:.96})}sizeForKind(e,t){switch(e){case"slider":return new N(1.75,1.75,1.75);case"dropBar":return new N(t*.41,3.25,2.35);case"wall":return new N(Math.min(2.35,t*.25),2.35,.8);case"cube":return new N(1.5,1.5,1.5)}}advanceGap(e,t){this.nextNearZ-=e,this.nextNearY-=t}random(e,t){return e+Math.random()*(t-e)}randomSigned(e){return this.random(-e,e)}randomIslandStagePattern(e){let t=e?[10,11,12,13,14]:[0,1,2,3,4,5,6,7,8,9];return t[Math.floor(this.random(0,t.length))]}shuffleNumbers(e){let t=[...e];for(let i=t.length-1;i>0;i-=1){let r=Math.floor(this.random(0,i+1));[t[i],t[r]]=[t[r],t[i]]}return t}shuffleObjects(e){let t=[...e];for(let i=t.length-1;i>0;i-=1){let r=Math.floor(this.random(0,i+1));[t[i],t[r]]=[t[r],t[i]]}return t}average(e){return e.length===0?this.lastCenterX:e.reduce((t,i)=>t+i,0)/e.length}captureRoadCursor(){return{centerX:this.lastCenterX,nextNearZ:this.nextNearZ,nextNearY:this.nextNearY}}restoreRoadCursor(e){this.lastCenterX=e.centerX,this.nextNearZ=e.nextNearZ,this.nextNearY=e.nextNearY,this.roadClampOffset=0,this.roadClampHalfRange=null,this.roadClampAbsoluteMinX=null,this.roadClampAbsoluteMaxX=null}syncCursorFromRoadStates(){this.activeRoadStates.length!==0&&(this.nextNearZ=Math.max(...this.activeRoadStates.map(e=>e.nextNearZ)),this.nextNearY=this.lowestActiveRoadY())}lowestActiveRoadY(){return this.activeRoadStates.length===0?this.nextNearY:Math.min(...this.activeRoadStates.map(e=>e.nextNearY))}clampValue(e,t,i){return Math.max(t,Math.min(i,e))}clamp(e,t,i){let r=this.roadClampOffset;if(this.roadClampHalfRange!==null){let s=Math.max(t,-this.roadClampHalfRange),o=Math.min(i,this.roadClampHalfRange);return this.clampWithAbsoluteBounds(e,r+s,r+o)}return this.clampWithAbsoluteBounds(e,t+r,i+r)}clampWithAbsoluteBounds(e,t,i){let r=this.roadClampAbsoluteMinX===null?t:Math.max(t,this.roadClampAbsoluteMinX),s=this.roadClampAbsoluteMaxX===null?i:Math.min(i,this.roadClampAbsoluteMaxX);return r>s?(r+s)*.5:this.clampValue(e,r,s)}static \u0275fac=function(t){return new(t||n)};static \u0275prov=Ee({token:n,factory:n.\u0275fac})};var o2=["gameCanvas"];function a2(n,e){if(n&1){let t=hd();St(0,"div",11)(1,"div",15)(2,"span",16),jt(3,"Run ended"),Dt(),St(4,"h1"),jt(5,"Game Over"),Dt(),St(6,"div",17)(7,"div")(8,"span"),jt(9,"Stages cleared"),Dt(),St(10,"strong"),jt(11),Hs(12,"number"),Dt()(),St(13,"div")(14,"span"),jt(15,"Best"),Dt(),St(16,"strong"),jt(17),Hs(18,"number"),Dt()()(),St(19,"button",18),sr("click",function(){wo(t);let r=Wr();return To(r.restartGame())}),jt(20,"Restart"),Dt()()()}if(n&2){let t=Wr();_i(11),or(zs(12,2,t.score.score(),"1.0-0")),_i(6),or(zs(18,5,t.score.bestScore(),"1.0-0"))}}function c2(n,e){if(n&1&&(St(0,"div",19),jt(1),Dt()),n&2){let t=Wr(2);_i(),or(t.countdown())}}function l2(n,e){if(n&1){let t=hd();St(0,"div",20)(1,"span",16),jt(2,"Run paused"),Dt(),St(3,"h1"),jt(4,"Paused"),Dt(),St(5,"div",21)(6,"button",18),sr("click",function(){wo(t);let r=Wr(2);return To(r.startResumeCountdown())}),jt(7,"Resume"),Dt(),St(8,"button",22),sr("click",function(){wo(t);let r=Wr(2);return To(r.restartGame())}),jt(9,"Restart"),Dt()()()}}function u2(n,e){if(n&1&&(St(0,"div",11),uc(1,c2,2,1,"div",19)(2,l2,10,0,"div",20),Dt()),n&2){let t=Wr();_i(),dc(t.countdown()!==null?1:2)}}var tp=class n{canvasRef;input=J(Il);score=J(Al);gameOver=Nn(!1);paused=Nn(!1);countdown=Nn(null);collision=J(Cl);cdr=J(gc);gameLoop=J(Dl);hostRef=J(zr);ngZone=J(Ut);world=J(Rl);ballRadius=.62;gravity=-18.5;desiredCameraPosition=new N;cameraFocus=new N;previousBallPosition=new N;resizeHandler=()=>this.resizeRenderer();renderer=null;scene=null;camera=null;ball=null;speedShieldOutline=null;crashGuardOutline=null;fallGuardianOutline=null;followLight=null;forwardVelocity=24;sideVelocity=0;verticalVelocity=0;speedShieldTimer=0;crashGuardActive=!1;fallGuardianActive=!1;killClipTimer=0;wobblePhase=0;wasGrounded=!1;lastSurfaceY=0;lastGroundedSegment=null;launchedFromSegmentIndex=-1;fallRescueAttempted=!1;surfaceSnapLockTimer=0;countdownRemaining=0;hudAccumulator=0;destroyed=!1;ngAfterViewInit(){this.ngZone.runOutsideAngular(()=>{this.setupScene(),this.input.attach(window),window.addEventListener("resize",this.resizeHandler),this.gameLoop.start((e,t)=>this.tick(e,t))})}ngOnDestroy(){this.destroyed=!0,this.gameLoop.stop(),this.input.detach(),window.removeEventListener("resize",this.resizeHandler),this.ball&&(this.disposeObject(this.ball),this.ball=null),this.speedShieldOutline=null,this.crashGuardOutline=null,this.fallGuardianOutline=null,this.world.destroy(),this.renderer?.dispose(),this.renderer?.forceContextLoss(),this.renderer=null,this.scene=null,this.camera=null,this.followLight=null}restartGame(){if(!this.ball)return;this.world.reset(),this.score.reset(),this.forwardVelocity=24,this.sideVelocity=0,this.verticalVelocity=0,this.wobblePhase=0,this.wasGrounded=!0,this.lastGroundedSegment=null,this.launchedFromSegmentIndex=-1,this.fallRescueAttempted=!1,this.surfaceSnapLockTimer=0,this.clearPowerupState(),this.paused.set(!1),this.countdown.set(null),this.countdownRemaining=0;let e=12,i=(this.collision.findTrackContact(new N(0,0,e),this.ballRadius,this.world.segments)?.surfaceY??0)+this.ballRadius;this.lastSurfaceY=i,this.ball.position.set(0,i,e),this.previousBallPosition.copy(this.ball.position),this.ball.rotation.set(0,0,0),this.gameOver.set(!1),this.cdr.detectChanges()}setupScene(){let e=this.canvasRef.nativeElement,t=new Jf({canvas:e,antialias:!0,powerPreference:"high-performance"});t.setClearColor(197642,1),t.setPixelRatio(Math.min(window.devicePixelRatio,2)),t.outputColorSpace=Dn,t.toneMapping=ml,t.toneMappingExposure=1.12,t.shadowMap.enabled=!0,t.shadowMap.type=sf,this.renderer=t;let i=new nl;i.background=new qe(197642),i.fog=new tl(133382,.0085),this.scene=i,this.camera=new hn(68,1,.1,720),this.camera.position.set(0,6,24);let r=new dl(8781673,329482,1.15);i.add(r);let s=new fl(14155744,2.2);s.position.set(-7,12,9),s.castShadow=!0,s.shadow.mapSize.set(1024,1024),i.add(s),this.followLight=new io(3800852,7.4,34),i.add(this.followLight),this.world.init(i),this.createBall(i),this.restartGame(),this.resizeRenderer()}createBall(e){let t=new ji(this.ballRadius,42,28),i=new In({color:14680040,roughness:.18,metalness:.78,emissive:1193493,emissiveIntensity:.65});this.ball=new dt(t,i),this.ball.castShadow=!0;let r=new is(new ji(this.ballRadius*1.012,18,12)),s=new On({color:6160175,transparent:!0,opacity:.78}),o=new Ln(r,s);this.ball.add(o),this.speedShieldOutline=this.createBallOutline(16724319,1.1),this.crashGuardOutline=this.createBallOutline(3106559,1.2),this.fallGuardianOutline=this.createBallOutline(6744319,1.3),this.ball.add(this.speedShieldOutline,this.crashGuardOutline,this.fallGuardianOutline),e.add(this.ball)}createBallOutline(e,t){let i=new is(new ji(this.ballRadius*t,20,12)),r=new On({color:e,transparent:!0,opacity:.92,depthTest:!1}),s=new Ln(i,r);return s.visible=!1,s}tick(e,t){if(!(!this.renderer||!this.scene||!this.camera||!this.ball)){if(this.input.consumeRestart()){this.ngZone.run(()=>this.restartGame());return}if(this.input.consumePause()&&this.ngZone.run(()=>this.handlePauseInput()),this.paused()){this.updateCountdown(e),this.updateCamera(e),this.renderer.render(this.scene,this.camera),this.syncHud(e);return}this.gameOver()?this.world.update(e*.35,t):this.updatePlayer(e),this.updateCamera(e),this.renderer.render(this.scene,this.camera),this.syncHud(e)}}updatePlayer(e){if(!this.ball)return;let t=this.input.steering,i=this.ball.position.x,r=this.ball.position.z;this.previousBallPosition.copy(this.ball.position);let s=this.wasGrounded,o=this.lastGroundedSegment,a=o,c=this.forwardVelocity;this.surfaceSnapLockTimer=Math.max(0,this.surfaceSnapLockTimer-e),this.updatePowerupState(e);let l=a?(a.nearY-a.farY)/a.length:0;if(s&&a){let E=l*95,D=1.15+Math.abs(this.sideVelocity)*.025,_=a.kind==="brake"?24+this.score.slowdown()*1.6:0,T=a.kind==="boost"?34+this.score.difficulty()*12:0;this.forwardVelocity+=(E+4.2+T-D-_)*e,a.kind==="brake"&&this.score.applySlowdown(e*20),a.kind==="boost"&&(this.sideVelocity*=Math.max(.9,1-e*2.4))}else this.forwardVelocity+=(1.2-this.forwardVelocity*.012)*e;let u=this.input.throttle;if(u!==0){let E=s?1:.28,D=u>0?7.1:4.6;this.forwardVelocity+=u*D*E*e}let d=u>0?Number.POSITIVE_INFINITY:Math.max(this.score.maxSpeedLimit(),c);this.forwardVelocity=at.clamp(this.forwardVelocity,this.score.minSpeed(),d);let h=s?1:.28,f=(24+this.forwardVelocity*.46)*h,g=s?t===0?1.7:.66:.08,y=11.4+this.forwardVelocity*.1;this.wobblePhase+=e*(8.5+this.forwardVelocity*.15+Math.abs(t)*7);let m=Math.sin(this.wobblePhase)*Math.abs(t)*(1.6+this.score.difficulty()*2.2);if(this.sideVelocity+=(t*f+m)*e,this.sideVelocity-=this.sideVelocity*Math.min(.58,g*e),this.sideVelocity=at.clamp(this.sideVelocity,-y,y),this.ball.position.x+=this.sideVelocity*e,this.ball.position.z-=this.forwardVelocity*e,this.world.ensureAhead(this.ball.position.z),this.collectPowerups(),!s){let E=Math.pow(Math.max(0,this.forwardVelocity-152)/40,2)*.78,D=1+Math.min(2.2,Math.max(0,this.forwardVelocity-40)*.012)+Math.min(5.8,E);this.verticalVelocity+=this.gravity*D*e}this.ball.position.y+=this.verticalVelocity*e;let p=this.collision.resolveWorldCollision(this.ball.position,this.previousBallPosition,this.ballRadius,this.world.worldColliders),b=!1;if(p?.type==="kill"){if(!this.handleKillCollision())return}else if(p?.type==="bounce")this.applyBounce(p.normalX,p.normalZ,p.bouncy);else if(p&&(p.type==="ground"||p.type==="boostGround")&&p.surfaceY!==void 0){let E=p.bouncy?.18:0;this.ball.position.y=p.surfaceY+this.ballRadius+E,this.verticalVelocity=p.bouncy?at.clamp(7.8+Math.max(0,-this.verticalVelocity)*.28+this.forwardVelocity*.012,7.8,12.5):0,this.lastSurfaceY=this.ball.position.y,this.lastGroundedSegment=null,b=!p.bouncy,p.type==="boostGround"&&(this.forwardVelocity+=e*(28+this.forwardVelocity*.14))}let w=this.collision.resolveTrackPillarCollision(this.ball.position,this.previousBallPosition,this.ballRadius,this.world.segments);if(w?.type==="kill"){if(!this.handleKillCollision())return}else w?.type==="bounce"&&this.applyBounce(w.normalX,w.normalZ,w.bouncy);if(this.shouldReleaseFromRampCrest(s,o,r)){let E=o.farY-o.nearY,D=at.clamp(5.8+E*.2+this.forwardVelocity*.035,6.2,10.6);this.ball.position.y+=.32,this.verticalVelocity=Math.max(this.verticalVelocity,D),this.surfaceSnapLockTimer=.24,this.launchedFromSegmentIndex=o.index,this.lastGroundedSegment=null}let M=this.collision.findTrackContact(this.ball.position,this.ballRadius,this.world.segments);if(!b&&M&&!this.shouldIgnoreContactDuringSnapLock(M.segment)){let E=M.surfaceY+this.ballRadius,D=Math.max(0,-this.verticalVelocity*e),_=M.segment.kind==="ramp"||M.segment.kind==="launch"||M.segment.kind==="brake"||M.segment.kind==="boost",T=s?1.35:at.clamp((_?1.25:.9)+D,.9,_?3.1:2.45),O=s?1.42:at.clamp((_?1.55:1.05)+D*.5,1.05,_?2.65:1.75),C=this.ball.position.y>=E-O,F=this.verticalVelocity<=4.5||s||_&&this.ball.position.y<=E+.55+D,G=o?o.farY-o.nearY:0,W=M.segment.farY-M.segment.nearY;if(s&&o!==null&&M.segment.index!==o.index&&(o.kind==="launch"||o.kind==="ramp"||o.kind==="brake")&&G>1.2&&W<G*.45&&this.ball.position.z<=o.farZ+4.2){let H=at.clamp(4.8+G*.16+this.forwardVelocity*.025,5.2,8.8);this.ball.position.y=Math.max(this.ball.position.y,E+.16),this.verticalVelocity=Math.max(this.verticalVelocity,H),this.launchedFromSegmentIndex=o.index,this.lastSurfaceY=this.ball.position.y}else if(C&&this.ball.position.y<=E+T&&F){let H=this.wasGrounded?(E-this.lastSurfaceY)/Math.max(e,.001):0;if(this.ball.position.y=E,this.verticalVelocity=at.clamp(H*.35,-5,5),this.lastSurfaceY=E,b=!0,this.lastGroundedSegment=M.segment,M.segment.kind==="brake"&&(this.forwardVelocity=Math.max(this.score.minSpeed(),this.forwardVelocity-e*20),this.sideVelocity*=Math.max(.74,1-e*6)),M.segment.kind==="boost"&&(this.forwardVelocity+=e*(26+this.forwardVelocity*.12)),M.edgeDistance<.78){let U=Math.sign(this.ball.position.x-M.segment.centerX);this.sideVelocity+=U*(.78-M.edgeDistance)*4.5*e}}else if(!C&&this.ball.position.y<E-this.ballRadius*1.1){this.endGame();return}}if(b&&(this.fallRescueAttempted=!1,this.surfaceSnapLockTimer=0),!b&&s&&o?.kind==="launch"&&this.launchedFromSegmentIndex!==o.index){let E=at.clamp(9.2+this.forwardVelocity*.075+this.score.difficulty()*1.7,10.5,16.5);this.verticalVelocity=Math.max(this.verticalVelocity,E),this.forwardVelocity+=3.5,this.launchedFromSegmentIndex=o.index}this.wasGrounded=b,this.rollBall(this.ball.position.x-i,this.ball.position.z-r),this.world.update(e,performance.now()*.001),this.score.completeStages(this.world.consumePassedStages(this.ball.position.z)),this.score.update(e),this.score.syncSpeed(this.forwardVelocity);let I=this.world.fallLimitForZ(this.ball.position.z);this.ball.position.y<I+14&&!this.fallRescueAttempted&&!b&&(this.fallRescueAttempted=!0,(this.fallGuardianActive||Math.random()<1/15)&&(this.fallGuardianActive&&(this.fallGuardianActive=!1,this.syncPowerupOutlines()),this.applyFallRescue())),!(this.collision.hitObstacle(this.ball.position,this.ballRadius,this.world.obstacles)&&!this.handleKillCollision())&&this.ball.position.y<I&&this.endGame()}collectPowerups(){if(!this.ball)return;let e=this.collision.hitPowerup(this.ball.position,this.ballRadius,this.world.powerups);for(;e;)this.applyPowerup(e.kind),this.world.collectPowerup(e),e=this.collision.hitPowerup(this.ball.position,this.ballRadius,this.world.powerups)}applyPowerup(e){switch(e){case"speedShield":this.forwardVelocity+=30,this.speedShieldTimer=10;break;case"fling":this.applyFlingPowerup();break;case"crashGuard":this.crashGuardActive=!0;break;case"fallGuardian":this.fallGuardianActive=!0;break}this.syncPowerupOutlines()}applyFlingPowerup(){if(!this.ball)return;let e=this.world.highestSurfaceYNearZ(this.ball.position.z),t=this.world.fallLimitForZ(this.ball.position.z),i=Math.max(0,e-t),r=at.clamp(i,28,76),s=Math.sqrt(r*Math.abs(this.gravity)*2);this.ball.position.y=Math.max(this.ball.position.y,e+this.ballRadius+1.5),this.verticalVelocity=Math.max(this.verticalVelocity,s),this.forwardVelocity+=4,this.wasGrounded=!1,this.lastGroundedSegment=null,this.surfaceSnapLockTimer=.22,this.fallRescueAttempted=!1}applyFallRescue(){if(!this.ball)return;let e=this.world.lowestSurfaceYNearZ(this.ball.position.z);this.ball.position.y=Math.max(this.ball.position.y,e+this.ballRadius+2),this.verticalVelocity=Math.max(this.verticalVelocity,32),this.forwardVelocity+=8}handleKillCollision(){return this.speedShieldTimer>0||this.killClipTimer>0?!0:this.crashGuardActive?(this.crashGuardActive=!1,this.killClipTimer=.85,this.sideVelocity*=.35,this.verticalVelocity=Math.max(this.verticalVelocity,4),this.syncPowerupOutlines(),!0):(this.endGame(),!1)}updatePowerupState(e){this.speedShieldTimer=Math.max(0,this.speedShieldTimer-e),this.killClipTimer=Math.max(0,this.killClipTimer-e),this.syncPowerupOutlines()}clearPowerupState(){this.speedShieldTimer=0,this.crashGuardActive=!1,this.fallGuardianActive=!1,this.killClipTimer=0,this.syncPowerupOutlines()}syncPowerupOutlines(){let e=this.speedShieldTimer>0&&(this.speedShieldTimer>3||Math.floor(this.speedShieldTimer*8)%2===0);this.setOutlineVisible(this.speedShieldOutline,e,.94),this.setOutlineVisible(this.crashGuardOutline,this.crashGuardActive,.9),this.setOutlineVisible(this.fallGuardianOutline,this.fallGuardianActive,.9)}setOutlineVisible(e,t,i){e&&(e.visible=t,e.material instanceof On&&(e.material.opacity=i))}applyBounce(e,t,i=!1){if(e){let r=i?13:7,s=i?1.15:.78;this.sideVelocity=e*Math.max(r,Math.abs(this.sideVelocity)*s),this.wobblePhase+=.9}t&&(this.forwardVelocity=Math.max(this.score.minSpeed(),this.forwardVelocity*.38),this.verticalVelocity=Math.max(this.verticalVelocity,3.5))}shouldReleaseFromRampCrest(e,t,i){if(!e||!t||!this.ball||this.surfaceSnapLockTimer>0)return!1;let r=t.farY-t.nearY,s=t.kind==="launch"||t.kind==="ramp"||t.kind==="brake",o=i>=t.farZ-.4&&this.ball.position.z<=t.farZ+2.8;return s&&r>1.15&&o}shouldIgnoreContactDuringSnapLock(e){return this.surfaceSnapLockTimer<=0||this.verticalVelocity<=0?!1:e.kind!=="ramp"&&e.kind!=="launch"&&e.kind!=="brake"&&e.kind!=="boost"}handlePauseInput(){if(this.gameOver()){this.restartGame();return}if(!this.paused()){this.paused.set(!0),this.countdown.set(null),this.countdownRemaining=0,this.cdr.detectChanges();return}if(this.countdown()!==null){this.paused.set(!1),this.countdown.set(null),this.countdownRemaining=0,this.cdr.detectChanges();return}this.startResumeCountdown()}startResumeCountdown(){!this.paused()||this.gameOver()||(this.countdownRemaining=2,this.countdown.set(3),this.cdr.detectChanges())}toggleFullscreen(){let e=document,t=document.documentElement;if(document.fullscreenElement??e.webkitFullscreenElement??null){document.exitFullscreen?document.exitFullscreen().catch(()=>{}):e.webkitExitFullscreen?.();return}t.requestFullscreen?t.requestFullscreen().catch(()=>{}):t.webkitRequestFullscreen?.()}updateCountdown(e){if(this.countdown()===null)return;if(this.countdownRemaining-=e,this.countdownRemaining<=0){this.ngZone.run(()=>{this.paused.set(!1),this.countdown.set(null),this.countdownRemaining=0,this.cdr.detectChanges()});return}let t=Math.max(1,Math.ceil(this.countdownRemaining/(2/3)));t!==this.countdown()&&this.ngZone.run(()=>{this.countdown.set(t),this.cdr.detectChanges()})}rollBall(e,t){this.ball&&(this.ball.rotation.x-=t/this.ballRadius,this.ball.rotation.z-=e/this.ballRadius,this.ball.rotation.y+=this.sideVelocity*.004)}updateCamera(e){if(!this.camera||!this.ball)return;let t=at.clamp((this.score.speed()-this.score.minSpeed())/(this.score.maxSpeedLimit()-this.score.minSpeed()),0,1),i=at.clamp(1-Math.exp(-e*4.2),0,1),r=at.clamp(1-Math.exp(-e*9.5),0,1);this.desiredCameraPosition.set(this.ball.position.x*.9-this.sideVelocity*.04,this.ball.position.y+4.9+t*1.45,this.ball.position.z+11.6+t*2.15),this.camera.position.y+=(this.desiredCameraPosition.y-this.camera.position.y)*i,this.camera.position.z+=(this.desiredCameraPosition.z-this.camera.position.z)*i,this.camera.position.x+=(this.desiredCameraPosition.x-this.camera.position.x)*r,this.cameraFocus.set(this.ball.position.x*.92+this.sideVelocity*.015,this.ball.position.y+.8,this.ball.position.z-16-t*2.6),this.camera.lookAt(this.cameraFocus);let s=t*t;this.camera.rotation.z-=this.sideVelocity*(.0016+s*55e-5)+Math.sin(this.wobblePhase*(.68+s*.16))*(.0022+s*.0038);let o=66+t*14;this.camera.fov+=(o-this.camera.fov)*Math.min(1,e*2.5),this.camera.updateProjectionMatrix(),this.followLight&&this.followLight.position.set(this.ball.position.x,this.ball.position.y+4.5,this.ball.position.z+6)}endGame(){this.gameOver()||(this.score.commitBestScore(),this.ngZone.run(()=>{this.paused.set(!1),this.countdown.set(null),this.countdownRemaining=0,this.gameOver.set(!0),this.cdr.detectChanges()}))}resizeRenderer(){if(!this.renderer||!this.camera)return;let e=this.renderer.domElement,t=e.clientWidth||this.hostRef.nativeElement.clientWidth||window.innerWidth,i=e.clientHeight||this.hostRef.nativeElement.clientHeight||window.innerHeight;this.renderer.setSize(t,i,!1),this.camera.aspect=t/i,this.camera.updateProjectionMatrix()}syncHud(e){this.hudAccumulator+=e,!(this.destroyed||this.hudAccumulator<.1)&&(this.hudAccumulator=0,this.ngZone.run(()=>this.cdr.detectChanges()))}disposeObject(e){e.parent&&e.parent.remove(e),e.traverse(t=>{let i=t;if(i.geometry?.dispose(),Array.isArray(i.material))for(let r of i.material)r.dispose();else i.material?.dispose()})}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Us({type:n,selectors:[["app-game"]],viewQuery:function(t,i){if(t&1&&pd(o2,7),t&2){let r;Lg(r=Fg())&&(i.canvasRef=r.first)}},features:[Ug([Cl,Dl,Il,Al,Rl])],decls:33,vars:14,consts:[["gameCanvas",""],[1,"game-shell"],["aria-label","Neon rolling ball endless runner",1,"game-canvas"],["aria-live","polite",1,"hud"],[1,"stat"],["type","button","aria-label","Toggle fullscreen",1,"fullscreen-button",3,"click"],["viewBox","0 0 24 24","fill","none","xmlns","http://www.w3.org/2000/svg","aria-hidden","true","focusable","false",1,"fullscreen-icon"],["d","M2 7V2H7","stroke","currentColor","stroke-width","2","stroke-linecap","round","stroke-linejoin","round"],["d","M22 7V2H17","stroke","currentColor","stroke-width","2","stroke-linecap","round","stroke-linejoin","round"],["d","M7 22L2 22L2 17","stroke","currentColor","stroke-width","2","stroke-linecap","round","stroke-linejoin","round"],["d","M17 22L22 22L22 17","stroke","currentColor","stroke-width","2","stroke-linecap","round","stroke-linejoin","round"],[1,"overlay"],["aria-hidden","true",1,"touch-steer-zones"],[1,"touch-zone","touch-zone-left",3,"pointerdown","pointerup","pointercancel","pointerleave"],[1,"touch-zone","touch-zone-right",3,"pointerdown","pointerup","pointercancel","pointerleave"],[1,"game-over-panel"],[1,"panel-kicker"],[1,"final-stats"],["type","button",1,"restart-button",3,"click"],[1,"countdown"],[1,"game-over-panel","pause-panel"],[1,"pause-actions"],["type","button",1,"restart-button","secondary",3,"click"]],template:function(t,i){t&1&&(St(0,"section",1),hc(1,"canvas",2,0),St(3,"div",3)(4,"div",4)(5,"span"),jt(6,"Stage"),Dt(),St(7,"strong"),jt(8),Hs(9,"number"),Dt()(),St(10,"div",4)(11,"span"),jt(12,"Speed"),Dt(),St(13,"strong"),jt(14),Hs(15,"number"),Dt()(),St(16,"div",4)(17,"span"),jt(18,"Best"),Dt(),St(19,"strong"),jt(20),Hs(21,"number"),Dt()()(),St(22,"button",5),sr("click",function(){return i.toggleFullscreen()}),Cu(),St(23,"svg",6),hc(24,"path",7)(25,"path",8)(26,"path",9)(27,"path",10),Dt()(),uc(28,a2,21,8,"div",11),uc(29,u2,3,1,"div",11),Du(),St(30,"div",12)(31,"div",13),sr("pointerdown",function(){return i.input.setTouchDirection(-1)})("pointerup",function(){return i.input.releaseTouchDirection(-1)})("pointercancel",function(){return i.input.releaseTouchDirection(-1)})("pointerleave",function(){return i.input.releaseTouchDirection(-1)}),Dt(),St(32,"div",14),sr("pointerdown",function(){return i.input.setTouchDirection(1)})("pointerup",function(){return i.input.releaseTouchDirection(1)})("pointercancel",function(){return i.input.releaseTouchDirection(1)})("pointerleave",function(){return i.input.releaseTouchDirection(1)}),Dt()()()),t&2&&(_i(8),or(zs(9,5,i.score.score(),"1.0-0")),_i(6),or(zs(15,8,i.score.speed(),"1.0-0")),_i(6),or(zs(21,11,i.score.bestScore(),"1.0-0")),_i(8),dc(i.gameOver()?28:-1),_i(),dc(i.paused()&&!i.gameOver()?29:-1))},dependencies:[$g],styles:["[_nghost-%COMP%]{display:block;width:100vw;height:100vh;overflow:hidden;background:#03040a}.game-shell[_ngcontent-%COMP%]{position:relative;width:100%;height:100%;isolation:isolate}.game-canvas[_ngcontent-%COMP%]{display:block;width:100%;height:100%;background:radial-gradient(circle at 50% 18%,#0b1c10,#041008 42%,#010403);outline:none}.hud[_ngcontent-%COMP%]{position:absolute;top:18px;left:18px;z-index:2;display:flex;gap:10px;max-width:calc(100% - 36px)}.stat[_ngcontent-%COMP%], .game-over-panel[_ngcontent-%COMP%]{border:1px solid rgba(88,255,55,.36);border-radius:8px;background:#010805ad;box-shadow:0 0 24px #39ff1424,inset 0 0 18px #61ff4712;-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px)}.stat[_ngcontent-%COMP%]{min-width:104px;padding:10px 12px}.stat[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .final-stats[_ngcontent-%COMP%]   span[_ngcontent-%COMP%], .panel-kicker[_ngcontent-%COMP%]{display:block;color:#92ff82;font-size:.7rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase}.stat[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{display:block;margin-top:2px;color:#f6fff3;font-size:1.32rem;font-variant-numeric:tabular-nums;line-height:1;text-shadow:0 0 14px rgba(57,255,20,.36)}.overlay[_ngcontent-%COMP%]{position:absolute;inset:0;z-index:4;display:grid;place-items:center;padding:18px;background:linear-gradient(#02030842,#020308a3),radial-gradient(circle at center,rgba(57,255,20,.12),transparent 44%)}.game-over-panel[_ngcontent-%COMP%]{width:min(420px,100%);padding:26px;text-align:center}.game-over-panel[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin:8px 0 18px;color:#fff;font-size:clamp(2rem,7vw,4rem);font-weight:900;line-height:.95;text-shadow:0 0 14px rgba(255,49,95,.72),0 0 28px rgba(57,255,20,.24)}.final-stats[_ngcontent-%COMP%]{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-bottom:20px}.final-stats[_ngcontent-%COMP%]   div[_ngcontent-%COMP%]{border:1px solid rgba(146,255,130,.2);border-radius:8px;padding:12px;background:#ffffff0b}.final-stats[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{display:block;margin-top:4px;color:#39ff14;font-size:1.42rem;font-variant-numeric:tabular-nums}.restart-button[_ngcontent-%COMP%], .fullscreen-button[_ngcontent-%COMP%]{border:1px solid rgba(57,255,20,.5);border-radius:8px;color:#041006;background:linear-gradient(135deg,#39ff14,#b7ff58);box-shadow:0 0 20px #39ff143d;cursor:pointer}.restart-button[_ngcontent-%COMP%]{min-width:150px;min-height:44px;padding:0 18px;font:inherit;font-weight:900;text-transform:uppercase}.fullscreen-button[_ngcontent-%COMP%]{position:absolute;top:18px;right:18px;z-index:6;display:grid;place-items:center;width:44px;min-height:40px;padding:0;font:inherit}.fullscreen-icon[_ngcontent-%COMP%]{width:22px;height:22px;filter:drop-shadow(0 0 5px rgba(4,16,6,.22))}.secondary[_ngcontent-%COMP%]{color:#f6fff3;background:#ff315f29;border-color:#ff315f9e;box-shadow:0 0 18px #ff315f38}.pause-panel[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{text-shadow:0 0 14px rgba(57,255,20,.72),0 0 28px rgba(57,255,20,.24)}.pause-actions[_ngcontent-%COMP%]{display:flex;justify-content:center;gap:10px}.countdown[_ngcontent-%COMP%]{color:#f6fff3;font-size:clamp(5rem,22vw,12rem);font-weight:900;line-height:.9;text-shadow:0 0 20px rgba(57,255,20,.86),0 0 46px rgba(57,255,20,.44)}.touch-steer-zones[_ngcontent-%COMP%]{position:absolute;inset:0;z-index:3;display:none;pointer-events:none}.touch-zone[_ngcontent-%COMP%]{position:absolute;top:0;bottom:0;width:50%;touch-action:none;-webkit-user-select:none;user-select:none;pointer-events:auto}.touch-zone-left[_ngcontent-%COMP%]{left:0}.touch-zone-right[_ngcontent-%COMP%]{right:0}.restart-button[_ngcontent-%COMP%]:hover, .fullscreen-button[_ngcontent-%COMP%]:hover{filter:brightness(1.08)}.restart-button[_ngcontent-%COMP%]:active, .fullscreen-button[_ngcontent-%COMP%]:active{transform:translateY(1px)}@media(pointer:coarse){.touch-steer-zones[_ngcontent-%COMP%]{display:block}}@media(max-width:620px){.hud[_ngcontent-%COMP%]{top:10px;left:10px;gap:6px;max-width:calc(100% - 20px)}.stat[_ngcontent-%COMP%]{min-width:0;padding:8px 9px}.stat[_ngcontent-%COMP%]   strong[_ngcontent-%COMP%]{font-size:1rem}.fullscreen-button[_ngcontent-%COMP%]{top:10px;right:10px;width:40px;min-height:36px}.fullscreen-icon[_ngcontent-%COMP%]{width:20px;height:20px}}"]})};var HE=[{path:"",component:tp},{path:"**",redirectTo:""}];var zE={providers:[Pm(),Vb({eventCoalescing:!0}),Dv(HE)]};var np=class n{static \u0275fac=function(t){return new(t||n)};static \u0275cmp=Us({type:n,selectors:[["app-root"]],decls:1,vars:0,template:function(t,i){t&1&&Uo(0,"router-outlet")},dependencies:[Bc],styles:["[_nghost-%COMP%]{display:block;min-height:100vh}"]})};tv(np,zE).catch(n=>console.error(n));

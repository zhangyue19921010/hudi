"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[87557],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>m});var r=n(67294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),c=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},u=function(e){var t=c(e.components);return r.createElement(s.Provider,{value:t},e.children)},p="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),p=c(n),d=o,m=p["".concat(s,".").concat(d)]||p[d]||g[d]||a;return n?r.createElement(m,i(i({ref:t},u),{},{components:n})):r.createElement(m,i({ref:t},u))}));function m(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[p]="string"==typeof e?e:o,i[1]=l;for(var c=2;c<a;c++)i[c]=n[c];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},9008:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>g,frontMatter:()=>a,metadata:()=>l,toc:()=>c});var r=n(87462),o=(n(67294),n(3905));const a={title:"Asynchronous Clustering using Hudi",excerpt:"How to setup Hudi for asynchronous clustering",author:"codope",category:"blog",image:"/assets/images/blog/clustering/example_perf_improvement.png",tags:["design","clustering","apache hudi"]},i=void 0,l={permalink:"/blog/2021/08/23/async-clustering",editUrl:"https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2021-08-23-async-clustering.md",source:"@site/blog/2021-08-23-async-clustering.md",title:"Asynchronous Clustering using Hudi",description:"In one of the previous blog posts, we introduced a new",date:"2021-08-23T00:00:00.000Z",formattedDate:"August 23, 2021",tags:[{label:"design",permalink:"/blog/tags/design"},{label:"clustering",permalink:"/blog/tags/clustering"},{label:"apache hudi",permalink:"/blog/tags/apache-hudi"}],readingTime:6.055,truncated:!0,authors:[{name:"codope"}],prevItem:{title:"Building an ExaByte-level Data Lake Using Apache Hudi at ByteDance",permalink:"/blog/2021/09/01/building-eb-level-data-lake-using-hudi-at-bytedance"},nextItem:{title:"Reliable ingestion from AWS S3 using Hudi",permalink:"/blog/2021/08/23/s3-events-source"}},s={authorsImageUrls:[void 0]},c=[],u={toc:c},p="wrapper";function g(e){let{components:t,...n}=e;return(0,o.kt)(p,(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"In one of the ",(0,o.kt)("a",{parentName:"p",href:"/blog/2021/01/27/hudi-clustering-intro"},"previous blog")," posts, we introduced a new\nkind of table service called clustering to reorganize data for improved query performance without compromising on\ningestion speed. We learnt how to setup inline clustering. In this post, we will discuss what has changed since then and\nsee how asynchronous clustering can be setup using HoodieClusteringJob as well as DeltaStreamer utility."))}g.isMDXComponent=!0}}]);
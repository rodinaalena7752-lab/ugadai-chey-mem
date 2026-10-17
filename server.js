const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const multer=require('multer');
const crypto=require('crypto');
const fs=require('fs');
const path=require('path');
const app=express(); const server=http.createServer(app); const io=new Server(server,{maxHttpBufferSize:8e6});
// Optional durable storage: one private Supabase Storage bucket. Keep keys on Render only.
const SB_URL=(process.env.SUPABASE_URL||'').replace(/\/$/,'');
const SB_KEY=process.env.SUPABASE_SERVICE_ROLE_KEY||'';
const SB_BUCKET=process.env.SUPABASE_BUCKET||'meme-data';
const DURABLE=!!(SB_URL&&SB_KEY);
function sbPath(name){return SB_URL+'/storage/v1/object/'+SB_BUCKET+'/'+name;}
function sbHeaders(type){return {apikey:SB_KEY,Authorization:'Bearer '+SB_KEY,...(type?{'Content-Type':type}:{})};}
async function sbUpload(name,data,type){const res=await fetch(sbPath(name),{method:'POST',headers:{...sbHeaders(type),'x-upsert':'true'},body:data});if(!res.ok)throw Error('Supabase upload '+res.status+': '+(await res.text()).slice(0,250));}
async function sbGet(name){const res=await fetch(SB_URL+'/storage/v1/object/authenticated/'+SB_BUCKET+'/'+name,{headers:sbHeaders()});if(res.status===404||res.status===400)return null;if(!res.ok)throw Error('Supabase read '+res.status);return res;}
let remoteWrites=Promise.resolve();
const DATA=process.env.DATA_DIR||path.join(__dirname,'data'); const UP=path.join(DATA,'uploads');
fs.mkdirSync(UP,{recursive:true});
const DB=path.join(DATA,'rooms.json'); let rooms={};
try {rooms=JSON.parse(fs.readFileSync(DB,'utf8'));}catch{}
// Never persist live socket identifiers.
for(const r of Object.values(rooms)){r.phase=r.phase==='playing'?'reveal':r.phase;r.votes={};for(const p of r.players)p.online=false;}
function save(){const serialized=JSON.stringify(rooms);const tmp=DB+'.tmp';fs.writeFileSync(tmp,serialized);fs.renameSync(tmp,DB);if(DURABLE){remoteWrites=remoteWrites.catch(e=>console.error('Previous snapshot failed:',e.message)).then(()=>sbUpload('rooms.json',serialized,'application/json'));remoteWrites.catch(e=>console.error('Snapshot error:',e.message));}}
async function initialize(){if(!DURABLE)return;const response=await sbGet('rooms.json');if(response){rooms=JSON.parse(await response.text());for(const r of Object.values(rooms)){if(r.phase==='playing')r.phase='reveal';r.votes={};for(const p of r.players){p.online=false;p.sid=null;}r.history??=[];}console.log('Durable rooms restored:',Object.keys(rooms).length);}else console.log('No previous snapshot; starting empty.');}
function id(n=16){return crypto.randomBytes(n).toString('hex');}
function code(){let c;do{c=crypto.randomBytes(4).toString('hex').toUpperCase().slice(0,6);}while(rooms[c]);return c;}
function fail(socket,msg){socket.emit('notice',{error:msg});}
function auth(socket,roomCode,token){const r=rooms[String(roomCode||'').toUpperCase()];const p=r?.players.find(p=>p.token===token);if(!r||!p){fail(socket,'Комната или участник не найдены.');return null;}return {r,p};}
function publicState(r,p){const show=r.phase==='reveal'||r.phase==='done'; const card=r.deck[r.index];const isHost=p.id===r.hostId;return {code:r.code,me:p.id,isHost,hostId:r.hostId,phase:r.phase,index:r.index,total:r.deck.length,mode:r.mode,settings:r.settings,players:r.players.map(x=>({id:x.id,name:x.name,online:x.online,score:x.score,uploaded:r.cards.filter(c=>c.owner===x.id).length})),myCards:r.cards.filter(c=>c.owner===p.id||(p.id===r.hostId&&c.imposter)).map(c=>({id:c.id,url:'/uploads/'+c.file,imposter:c.imposter})),card:card?{id:card.id,url:'/uploads/'+card.file,imposter:show?!!card.imposter:undefined,owner:show?card.owner:null}:null,voted:!!r.votes[p.id],voteCount:Object.keys(r.votes).length,voteTotal:r.players.filter(x=>x.id!==card?.owner || r.mode==='who').length,results:show?r.results:null,history:(r.phase==='done'||show)?(r.history||[]):[],durable:DURABLE,canStart:r.players.every(x=>r.cards.some(c=>c.owner===x.id)) && r.players.length>=3};}
function broadcast(r){for(const p of r.players)if(p.sid)io.to(p.sid).emit('state',publicState(r,p));}
function onlyHost(r,p,s){if(p.id!==r.hostId){fail(s,'Это действие доступно ведущему.');return false;}return true;}
function calculate(r){
 const c=r.deck[r.index],votes=r.votes,tally={},details=[];
 const before=Object.fromEntries(r.players.map(p=>[p.id,p.score]));
 for(const [pid,v] of Object.entries(votes)){
  const voter=r.players.find(p=>p.id===pid);if(!voter)continue;
  tally[v.choice]=(tally[v.choice]||0)+1;
  let change=0,correct=null;
  if(r.mode==='owner'||r.mode==='imposter'){
   correct=c.imposter?v.choice==='nobody':v.choice===c.owner;
   const proposed=correct?(v.stake?4:2):(v.stake?-1:0);
   voter.score=Math.max(0,voter.score+proposed);
   change=voter.score-before[pid];
  }
  details.push({pid,name:voter.name,choice:v.choice,stake:v.stake,change,correct});
 }
 let ownerBonus=0;
 if(r.mode==='owner'&&!c.imposter){const owner=r.players.find(p=>p.id===c.owner);if(owner){ownerBonus=details.filter(d=>d.choice!==c.owner).length;owner.score+=ownerBonus;}}
 const scores=r.players.map(p=>({id:p.id,name:p.name,before:before[p.id],after:p.score,delta:p.score-before[p.id],votePoints:details.find(d=>d.pid===p.id)?.change||0,ownerBonus:p.id===c.owner?ownerBonus:0}));
 const sorted=Object.entries(tally).sort((a,b)=>b[1]-a[1]);
 r.results={details,tally,majority:sorted.length?sorted[0][0]:null,ownerName:r.players.find(p=>p.id===c.owner)?.name||'Мемный самозванец',isImposter:!!c.imposter,ownerBonus,scores};
 r.history??=[];r.history.push({round:r.index+1,mode:r.mode,cardUrl:'/uploads/'+c.file,...r.results});
 r.phase='reveal';r.votes={};save();broadcast(r);
}
const upload=multer({storage:multer.diskStorage({destination:UP,filename:(req,file,cb)=>{const ext=file.mimetype==='image/gif'?'.gif':file.mimetype==='image/png'?'.png':file.mimetype==='image/webp'?'.webp':'.jpg';cb(null,id(18)+ext);}}),limits:{fileSize:6*1024*1024,files:1},fileFilter:(req,file,cb)=>cb(null,['image/jpeg','image/png','image/webp','image/gif'].includes(file.mimetype))});
app.use(express.json({limit:'50kb'}));app.use(express.static(path.join(__dirname,'public')));app.get('/uploads/:file',async(req,res)=>{const file=req.params.file;if(!/^[a-f0-9]{36}\.(gif|png|webp|jpg)$/.test(file))return res.sendStatus(404);if(!DURABLE)return res.sendFile(path.join(UP,file));try{const response=await sbGet('uploads/'+file);if(!response)return res.sendStatus(404);res.set('Content-Type',response.headers.get('content-type')||'image/jpeg');res.set('Cache-Control','private, max-age=3600');res.send(Buffer.from(await response.arrayBuffer()));}catch(e){console.error(e);res.sendStatus(502);}});
app.post('/api/upload', (req,res)=>upload.single('file')(req,res,async err=>{if(err)return res.status(400).json({error:'Поддерживаются JPG, PNG, WebP, GIF до 6 МБ.'});const f=req.file;if(!f)return res.status(400).json({error:'Не удалось получить изображение.'});const r=rooms[String(req.body.code||'').toUpperCase()];const p=r?.players.find(p=>p.token===req.body.token);if(!p||r.phase!=='lobby'){fs.unlinkSync(f.path);return res.status(403).json({error:'Загрузка закрыта или участник не найден.'});}if(r.cards.filter(c=>c.owner===p.id || (p.id===r.hostId&&c.imposter)).length>=12){fs.unlinkSync(f.path);return res.status(400).json({error:'Максимум 10 мемов от участника.'});}try{if(DURABLE)await sbUpload('uploads/'+f.filename,fs.readFileSync(f.path),f.mimetype);}catch(e){console.error(e);fs.unlinkSync(f.path);return res.status(503).json({error:'Не удалось сохранить мем в постоянное хранилище.'});}r.cards.push({id:id(8),owner:req.body.imposter==='true'&&p.id===r.hostId?null:p.id,file:f.filename,imposter:req.body.imposter==='true'&&p.id===r.hostId});save();broadcast(r);res.json({ok:true});}));
const socketAuth=new Map();
io.on('connection',socket=>{
 socket.on('create',({name},cb)=>{name=String(name||'').trim().slice(0,24);if(!name)return cb({error:'Введи ник.'});const c=code(),token=id(),pid=id(8);const p={id:pid,token,name,score:0,online:true,sid:socket.id};rooms[c]={code:c,hostId:pid,phase:'lobby',mode:'owner',settings:{imposter:true,who:true,shame:true,stake:true},players:[p],cards:[],deck:[],index:0,votes:{},results:null,history:[]};socketAuth.set(socket.id,{code:c,pid});save();cb({code:c,token});broadcast(rooms[c]);});
 socket.on('join',({code:raw,name,token},cb)=>{const c=String(raw||'').trim().toUpperCase();const r=rooms[c];if(!r)return cb({error:'Комната не найдена.'});let p=token?r.players.find(x=>x.token===token):null;if(!p){name=String(name||'').trim().slice(0,24);if(!name)return cb({error:'Введи ник.'});if(r.phase!=='lobby')return cb({error:'Игра уже началась.'});if(r.players.length>=5)return cb({error:'Максимум 5 игроков.'});if(r.players.some(x=>x.name.toLowerCase()===name.toLowerCase()))return cb({error:'Такой ник уже есть.'});token=id();p={id:id(8),token,name,score:0,online:true};r.players.push(p);}p.online=true;p.sid=socket.id;socketAuth.set(socket.id,{code:c,pid:p.id});save();cb({code:c,token});broadcast(r);});
 socket.on('settings',({code,token,settings})=>{const a=auth(socket,code,token);if(!a||!onlyHost(a.r,a.p,socket)||a.r.phase!=='lobby')return;for(const k of ['imposter','who','shame','stake'])if(typeof settings?.[k]==='boolean')a.r.settings[k]=settings[k];save();broadcast(a.r);});
 socket.on('start',({code,token})=>{const a=auth(socket,code,token);if(!a||!onlyHost(a.r,a.p,socket))return;const r=a.r;if(r.phase!=='lobby')return;if(r.players.length<3||!r.players.every(x=>r.cards.some(c=>c.owner===x.id)))return fail(socket,'Нужно 3–5 игроков и хотя бы по одному мему от каждого.');const deck=r.cards.filter(c=>!c.imposter||r.settings.imposter).map(c=>({...c}));for(let i=deck.length-1;i>0;i--){const j=crypto.randomInt(i+1);[deck[i],deck[j]]=[deck[j],deck[i]];}r.deck=deck;r.index=0;r.mode='owner';r.phase='playing';r.votes={};r.results=null;save();broadcast(r);});
 socket.on('mode',({code,token,mode})=>{const a=auth(socket,code,token);if(!a||!onlyHost(a.r,a.p,socket)||a.r.phase!=='playing')return;const c=a.r.deck[a.r.index];if(mode==='who'&&!a.r.settings.who)return;if(!['owner','who','imposter'].includes(mode))return;a.r.mode=mode;a.r.votes={};save();broadcast(a.r);});
 socket.on('vote',({code,token,choice,stake})=>{const a=auth(socket,code,token);if(!a||a.r.phase!=='playing')return;const r=a.r,c=r.deck[r.index];if(r.votes[a.p.id])return fail(socket,'Ты уже проголосовал(а).');if(r.mode!=='who'&&c.owner===a.p.id&&!c.imposter)return fail(socket,'За собственный мем нельзя голосовать.');if(choice!=='nobody'&&!r.players.some(x=>x.id===choice))return; if(choice==='nobody'&&r.mode==='who')return; r.votes[a.p.id]={choice,stake:!!stake&&r.settings.stake};save();broadcast(r);});
 socket.on('reveal',({code,token})=>{const a=auth(socket,code,token);if(!a||!onlyHost(a.r,a.p,socket)||a.r.phase!=='playing')return;calculate(a.r);});
 socket.on('next',({code,token})=>{const a=auth(socket,code,token);if(!a||!onlyHost(a.r,a.p,socket)||a.r.phase!=='reveal')return;const r=a.r;r.index++;r.phase=r.index>=r.deck.length?'done':'playing';r.votes={};r.results=null;r.mode='owner';save();broadcast(r);});
 socket.on('disconnect',()=>{const a=socketAuth.get(socket.id);socketAuth.delete(socket.id);const p=rooms[a?.code]?.players.find(x=>x.id===a.pid);if(p&&p.sid===socket.id){p.online=false;p.sid=null;broadcast(rooms[a.code]);}});
});
const port=Number(process.env.PORT||3000);initialize().then(()=>server.listen(port,'0.0.0.0',()=>console.log('Мемная игра: http://localhost:'+port+(DURABLE?' (Supabase durable storage)':' (local temporary storage)')))).catch(err=>{console.error('Durable storage startup failed:',err);process.exit(1)});

// Real-layout regression checks without a browser automation dependency.
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const chrome = spawn(input.chrome, ['--headless', '--no-sandbox', '--disable-gpu',
  '--remote-debugging-port=0', `--user-data-dir=${input.profile}`, 'about:blank'],
  {stdio: ['ignore', 'ignore', 'pipe']});
let socket;
let sequence = 0;
const pending = new Map();
const endpoint = new Promise((resolve, reject) => {
  let stderr = '';
  chrome.stderr.on('data', data => {
    stderr += data;
    const found = stderr.match(/DevTools listening on (ws:\/\/\S+)/);
    if (found) resolve(found[1]);
  });
  chrome.on('exit', code => reject(new Error(`Chrome exited ${code}: ${stderr}`)));
});
function request(method, params = {}, sessionId) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    pending.set(id, {resolve, reject});
    socket.send(JSON.stringify({id, method, params, ...(sessionId ? {sessionId} : {})}));
  });
}
async function evaluate(session, expression) {
  const result = await request('Runtime.evaluate', {expression, awaitPromise: true, returnByValue: true}, session);
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}
(async () => {
  socket = new WebSocket(await endpoint);
  await new Promise(resolve => socket.addEventListener('open', resolve, {once: true}));
  socket.addEventListener('message', event => {
    const message = JSON.parse(event.data);
    const entry = pending.get(message.id);
    if (!entry) return;
    pending.delete(message.id);
    if (message.error) entry.reject(new Error(JSON.stringify(message.error)));
    else entry.resolve(message.result);
  });
  const {targetId} = await request('Target.createTarget', {url: 'about:blank'});
  const {sessionId: session} = await request('Target.attachToTarget', {targetId, flatten: true});
  await request('Page.enable', {}, session);
  await request('Page.addScriptToEvaluateOnNewDocument', {source: `
    window.streams=[];
    window.mockRun=null;
    window.mockRuns=[];window.archiveWrites=[];window.failArchive=false;
    window.cvWrites=[];window.bulkCvWrites=[];
    window.batchWrites=[];window.stopWrites=[];window.mockStop=null;window.failStop=false;
    window.mockJobs=[];window.statusWrites=[];window.failStatus=false;
    window.EventSource=class {constructor(url){this.url=url;streams.push(this)}close(){this.closed=true}};
    window.fetch=async (url,options={})=>{
      if(String(url)==='/api/cvs/generate-new'){
        bulkCvWrites.push(JSON.parse(options.body));
        return {ok:true,status:202,json:async()=>({queued:2,ready:1,already_active:1,changed:0,concurrency:4,runs:[]})};
      }
      if(String(url)==='/api/listings/refresh-preview'){
        const body=JSON.parse(options.body),opts={freshness_hours:24,limit:10,max_ai_fallbacks:2,include_tracked:false,...body};
        const jobs=mockJobs.filter(j=>['new','shortlisted'].includes(j.application_status)||(opts.include_tracked&&['applied','interviewing'].includes(j.application_status)));
        return {ok:true,status:200,json:async()=>({options:opts,eligible_count:jobs.length,jobs:structuredClone(jobs)})};
      }
      if(String(url)==='/api/listings/refresh-eligible'){
        const body=JSON.parse(options.body);batchWrites.push(body);
        const children=body.job_ids.map((id,i)=>({id:'run_child_'+i,task_type:'refresh_listing',job_id:id,batch_id:'run_batch',state:'queued',display_state:'queued',company:mockJobs.find(j=>j.id===id).company,job_title:mockJobs.find(j=>j.id===id).title,events:[],approvals:[],usage:{}}));
        mockRun={id:'run_batch',task_type:'refresh_bulk',state:'queued',counts:{queued:children.length,total:children.length},children,events:[],approvals:[],usage:{}};mockRuns=[mockRun,...children];
        return {ok:true,status:202,json:async()=>({run:structuredClone(mockRun)})};
      }
      if(String(url)==='/api/runs/stop-all'||String(url).endsWith('/stop-batch')){
        stopWrites.push({url,body:JSON.parse(options.body)});
        if(failStop)return {ok:false,status:500,json:async()=>({error:'Mock interrupt unavailable'})};
        let queued=0,active=0;for(const r of mockRuns){if(r.state==='queued'){r.state='cancelled';r.display_state='cancelled';r.cancel_requested=true;queued++}else if(r.state==='running'){r.display_state='stopping';r.cancel_requested=true;active++}}
        if(mockRun?.counts)mockRun.counts={...mockRun.counts,queued:0,running:0,stopping:active,cancelled:queued};
        mockStop={id:'stop_mock',state:active?'stopping':'completed',cancelled_queued:queued,interrupted_active:0,stopping:active,failures:[],finished_before_stop:0};
        return {ok:true,status:202,json:async()=>structuredClone(mockStop)};
      }
      if(String(url).startsWith('/api/stops/'))return {ok:true,status:200,json:async()=>structuredClone(mockStop)};
      if(String(url).endsWith('/generate-cv')){
        const job=mockJobs.find(j=>String(url).includes('/'+j.id+'/'));
        cvWrites.push({job_id:job.id,body:JSON.parse(options.body)});
        mockRun={id:'run_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',task_type:'generate_cv',state:'queued',job_id:job.id,
          queue_position:2,waiting_for:{id:'run_busy',task_type:'search'},events:[{id:1,kind:'queued',message:'Task queued'}],approvals:[],artifacts:[]};
        mockRuns=[mockRun];job.cv={state:'queued',run_id:mockRun.id,queue_position:2,waiting_for:mockRun.waiting_for};
        return {ok:true,status:202,json:async()=>({run:structuredClone(mockRun)})};
      }
      if(String(url).startsWith('/api/jobs/')&&String(url).split('/').length===4&&!String(url).includes('?')){
        const job=mockJobs.find(j=>String(url).endsWith('/'+j.id));
        return {ok:true,status:200,json:async()=>({...structuredClone(job),history:[],artifacts:job.cv?.url?[{kind:'CV',url:job.cv.url}]:[]})};
      }
      if(String(url)==='/api/runs/archive-history'){
        archiveWrites.push(JSON.parse(options.body));
        if(failArchive)return {ok:false,status:500,json:async()=>({error:'Mock archive failed'})};
        let count=0;for(const run of mockRuns)if(!run.archived_at&&!['queued','running'].includes(run.state)){run.archived_at='2026-10-05T12:00:00Z';count++}
        return {ok:true,status:200,json:async()=>({archived_count:count})};
      }
      if(String(url)==='/api/runs'||String(url).startsWith('/api/runs?')){
        const includeArchived=String(url).includes('include_archived=1');
        return {ok:true,status:200,json:async()=>({
          runs:structuredClone(mockRuns.filter(r=>includeArchived||!r.archived_at||['queued','running'].includes(r.state))),
          history:{clearable:mockRuns.filter(r=>!r.archived_at&&!['queued','running'].includes(r.state)).length,
            archived:mockRuns.filter(r=>r.archived_at).length}})};
      }
      if(String(url).endsWith('/status')){
        const body=JSON.parse(options.body);statusWrites.push({url,body});
        if(failStatus)return {ok:false,status:500,json:async()=>({error:'Mock save failed'})};
        const job=mockJobs.find(j=>String(url).includes('/'+j.id+'/'));job.application_status=body.status;job.version++;
      }
      return {ok:true,status:200,json:async()=>
        String(url).startsWith('/api/runs/') ? mockRun :
        {jobs:structuredClone(mockJobs),runs:[],documents:[],conversations:[],authenticated:true,auth_required:false}};
    };
  `}, session);
  await request('Page.navigate', {url: 'data:text/html;base64,' + Buffer.from(input.html).toString('base64')}, session);
  for (let i = 0; i < 100; i++) {
    if (await evaluate(session, "typeof sortedRunEvents==='function'")) break;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  const reports = [];
  for (const width of [1440, 390]) {
    await request('Emulation.setDeviceMetricsOverride', {width, height: 844, deviceScaleFactor: 1, mobile: width < 500}, session);
    const report = await evaluate(session, `(async()=>{
      function check(value,message){if(!value)throw new Error(message)}
      const event=id=>({id,kind:'activity',message:'Log '+id+' with useful detail',created_at:'2026-10-05T10:00:00Z'});
      const ids=()=>[...$('run-events').children].map(e=>Number(e.dataset.eventId));
      const descending=()=>{const values=ids();return values.every((v,i)=>!i||values[i-1]>v)};
      const original=Array.from({length:100},(_,i)=>event(i+1));
      mockRun={id:'run_browser_'+innerWidth,task_type:'search',state:'running',events:original,approvals:[],artifacts:[]};
      await openRun(mockRun.id);
      check(ids()[0]===100&&ids().at(-1)===1,'initial order');
      check(original[0].id===1,'stored order changed');
      check(document.documentElement.scrollWidth<=innerWidth,'horizontal overflow');
      const log=$('run-events');log.scrollTop=900;
      const anchor=captureRunLogScroll(log);
      const stream=S.runSubscriptions.get(mockRun.id).source;
      stream.onmessage({lastEventId:'101',data:JSON.stringify({event:event(101),terminal:false})});
      check(ids()[0]===101,'live event did not appear first');
      const afterLive=captureRunLogScroll(log);
      check(afterLive.id===anchor.id&&Math.abs(afterLive.offset-anchor.offset)<1,'live scroll anchor moved');
      // Replay/out-of-order delivery must not corrupt descending display order.
      prependRunEvent(event(50.5));prependRunEvent(event(101));
      check(descending()&&ids().length===102,'replay order/deduplication');
      mockRun={...mockRun,events:[...original,event(101),event(102)]};
      stream.onerror();
      await new Promise(resolve=>setTimeout(resolve,50));
      check(ids()[0]===102&&descending(),'polling recovery order '+JSON.stringify({first:ids()[0],cursor:S.runCursors.get(mockRun.id),sub:S.runSubscriptions.has(mockRun.id)}));
      check(S.runCursors.get(mockRun.id)===102,'SSE cursor changed with display order');
      const afterPoll=captureRunLogScroll(log);
      check(afterPoll.id===anchor.id&&Math.abs(afterPoll.offset-anchor.offset)<1,'poll scroll anchor moved');
      const sub=S.runSubscriptions.get(mockRun.id);
      clearTimeout(sub.timer);sub.timer=null;connectRunSubscription(sub);
      check(sub.source.url.endsWith('after=102'),'reconnect lost cursor');
      check(S.runSubscriptions.size===1,'duplicate subscription');
      // A full detail refresh/reopen uses the same display order.
      const refreshAnchor=captureRunLogScroll(log);
      mockRun={...mockRun,events:[...mockRun.events,event(103)]};
      await refreshRunDrawer(mockRun.id);
      const afterRefresh=captureRunLogScroll($('run-events'));
      check(ids()[0]===103&&descending(),'refresh order');
      check(afterRefresh.id===refreshAnchor.id&&Math.abs(afterRefresh.offset-refreshAnchor.offset)<1,'refresh scroll anchor moved');
      closeDrawer();await openRun(mockRun.id);
      check(ids()[0]===103&&descending(),'reopen order');
      check(S.runSubscriptions.size===1,'reopen subscription leak');
      closeDrawer();
      renderConversationMessages({title:'Test',messages:[{role:'user',content:'first'},{role:'assistant',content:'second'}]});
      check($('messages').children[0].textContent==='first','chat order changed');
      return {width:innerWidth,initial:true,live:true,polling:true,reconnect:true,scrollAnchoring:true,chatOrder:true};
    })()`);
    reports.push(report);
  }
  const kanbanReports=[];
  for(const [width,height] of [[1440,900],[1024,768],[900,560],[768,844],[390,844]]){
    await request('Emulation.setDeviceMetricsOverride',{width,height,deviceScaleFactor:1,mobile:width<=820},session);
    const setup=await evaluate(session,`(async()=>{
      showView('jobs');window.scrollTo(0,0);S.archive=false;S.query='';
      mockJobs=boardStatuses.flatMap((status,index)=>Array.from({length:80},(_,i)=>({
        id:'job_board_'+index+'_'+i,company:'Example '+i,title:'Technical Implementation Specialist '+i,
        application_status:status,location:'Israel',work_arrangement:'unknown',qualification_status:'recommended',
        fit_recommendation:'Customer delivery and troubleshooting',version:1
      })));
      await loadJobs();
      function check(value,message){if(!value)throw new Error(message)}
      const board=$('kanban'),lists=[...board.querySelectorAll('.column-list')];
      if(innerWidth<=820){
        check(getComputedStyle(board).display==='none','mobile board visible');
        check(document.documentElement.scrollHeight>innerHeight*2,'mobile page scrolling constrained');
        window.scrollTo(0,600);check(scrollY>0,'mobile page cannot scroll');window.scrollTo(0,0);
        return {mobile:true,width:innerWidth,pageScrolling:true,cards:mockJobs.length};
      }
      check(document.documentElement.scrollHeight<=innerHeight+1,'board stretches page');
      check(document.documentElement.scrollWidth<=innerWidth,'page horizontal overflow');
      const bounds=board.getBoundingClientRect();
      check(bounds.bottom<=innerHeight&&bounds.height>100,'board not viewport constrained');
      check(lists.every(list=>list.scrollHeight>list.clientHeight*3),'many cards do not scroll');
      check(lists.every(list=>getComputedStyle(list).minHeight==='0px'&&getComputedStyle(list).overflowY==='auto'),'card-list overflow rules');
      lists.forEach(list=>list.scrollTop=0);board.scrollLeft=0;
      const header=board.querySelector('.column header');window.boardHeaderTop=header.getBoundingClientRect().top;
      lists[0].scrollTop=320;
      check(lists[1].scrollTop===0,'columns scroll together');
      check(header.getBoundingClientRect().top===boardHeaderTop,'header scrolls with cards');
      renderJobs();
      check($('kanban').querySelector('.column-list').scrollTop===320,'refresh reset column scroll');
      const rect=$('kanban').querySelector('.column-list').getBoundingClientRect();
      return {mobile:false,width:innerWidth,height:innerHeight,cards:mockJobs.length,x:rect.left+80,y:rect.top+100};
    })()`);
    if(setup.mobile){kanbanReports.push(setup);continue;}
    await request('Input.dispatchMouseEvent',{type:'mouseWheel',x:setup.x,y:setup.y,deltaX:0,deltaY:180},session);
    await new Promise(resolve=>setTimeout(resolve,100));
    const report=await evaluate(session,`(async()=>{
      function check(value,message){if(!value)throw new Error(message)}
      const board=$('kanban'),columns=[...board.querySelectorAll('.column')],first=columns[0].querySelector('.column-list');
      check(first.scrollTop>320,'native wheel did not scroll card list');
      check(columns[1].querySelector('.column-list').scrollTop===0,'wheel affected another column');
      check(scrollY===0&&columns[0].querySelector('header').getBoundingClientRect().top===boardHeaderTop,'wheel moved page/header');
      const source=first.querySelector('.job-card'),transfer=new DataTransfer();
      const sourceRect=source.getBoundingClientRect();
      source.dispatchEvent(new DragEvent('dragstart',{bubbles:true,dataTransfer:transfer,clientX:sourceRect.left+30,clientY:sourceRect.top+30}));
      const target=columns[1],list=target.querySelector('.column-list'),rect=list.getBoundingClientRect();
      const dragover=y=>target.dispatchEvent(new DragEvent('dragover',{bubbles:true,cancelable:true,dataTransfer:transfer,clientX:rect.left+40,clientY:y}));
      dragover(rect.bottom-3);
      await new Promise(resolve=>setTimeout(resolve,160));
      check(list.scrollTop>0,'drag bottom-edge autoscroll failed');
      const bottomTop=list.scrollTop;
      // Background refreshes must keep the native source/target mounted during dragging.
      renderJobs();check(source.isConnected&&pendingJobRender,'refresh detached dragged card');
      dragover(rect.top+3);
      await new Promise(resolve=>setTimeout(resolve,160));
      check(list.scrollTop<bottomTop,'drag top-edge autoscroll failed');
      target.querySelector('header').dispatchEvent(new DragEvent('drop',{bubbles:true,cancelable:true,dataTransfer:transfer,clientX:rect.left+40,clientY:rect.top-10}));
      await new Promise(resolve=>setTimeout(resolve,30));
      check(!boardDrag&&!pendingJobRender,'drop leaked drag state');
      check(statusWrites.at(-1).body.status==='shortlisted','drop did not persist status');
      check(S.jobs.find(j=>j.id===source.dataset.id).application_status==='shortlisted','drop state not loaded');
      check(board.querySelector('[data-column="new"] header .badge').textContent==='79'&&
            board.querySelector('[data-column="shortlisted"] header .badge').textContent==='81','saved drop counts incorrect');
      // At tablet widths, drag near the horizontal edge to reveal later columns.
      let horizontal=true;
      if(board.scrollWidth>board.clientWidth){
        const card=board.querySelector('.job-card'),bounds=board.getBoundingClientRect();
        card.dispatchEvent(new DragEvent('dragstart',{bubbles:true,dataTransfer:transfer,clientX:bounds.left+30,clientY:bounds.top+100}));
        board.dispatchEvent(new DragEvent('dragover',{bubbles:true,cancelable:true,dataTransfer:transfer,clientX:bounds.right-2,clientY:bounds.top+100}));
        await new Promise(resolve=>setTimeout(resolve,160));
        check(board.scrollLeft>0,'drag horizontal-edge autoscroll failed');
        const left=board.scrollLeft;stopBoardDrag();renderJobs();
        check(board.scrollLeft===left,'refresh reset horizontal scroll');
      }
      // Failed saves must revert the moved card and retain independent scroll areas.
      failStatus=true;const id=S.jobs.find(j=>j.application_status==='new').id;
      await changeStatus(id,'offer');failStatus=false;
      check(S.jobs.find(j=>j.id===id).application_status==='new','failed drop/status save did not roll back');
      check($('flash').textContent==='Mock save failed','save failure feedback missing');
      S.archive=true;mockJobs.push(...Array.from({length:40},(_,i)=>({id:'archive_'+i,company:'Archived',title:'Role',application_status:'rejected_employer',version:1})));
      await loadJobs();check(document.documentElement.scrollHeight<=innerHeight+1,'archive stretches board page');
      check($('archive-section').scrollHeight>$('archive-section').clientHeight,'archive is not bounded');
      S.archive=false;await loadJobs();
      // Other portal tabs retain their existing page scrolling.
      showView('documents');check(!document.querySelector('.shell').classList.contains('board-view'),'board height leaked into other views');
      showView('jobs');
      return {width:innerWidth,cards:400,independentScrolling:true,fixedHeaders:true,nativeWheel:true,
        edgeAutoscroll:true,horizontalScrolling:horizontal,persistentDrop:true,rollback:true,archiveBounded:true};
    })()`);
    kanbanReports.push(report);
  }
  const runReports=[];
  for(const [width,height] of [[1440,900],[1024,768],[900,560],[768,844],[390,844]]){
    await request('Emulation.setDeviceMetricsOverride',{width,height,deviceScaleFactor:1,mobile:width<500},session);
    const setup=await evaluate(session,`(async()=>{
      function check(value,message){if(!value)throw new Error(message)}
      window.scrollTo(0,0);$('show-archived-runs').checked=false;
      const states=['completed','failed','cancelled','interrupted'];
      mockRuns=Array.from({length:200},(_,i)=>({id:'run_'+i.toString(16).padStart(32,'0'),task_type:'search',state:states[i%4],
        created_at:'2026-10-05T10:00:00Z',result_summary:'Result '+i+' with enough detailed content to make the run cards realistic',archived_at:null}));
      mockRuns.unshift(...['running','queued'].map((state,i)=>({id:'run_'+(i+200).toString(16).padStart(32,'0'),task_type:'chat',state,
        created_at:'2026-10-05T09:00:00Z',result_summary:'Active work',archived_at:null,cancel_requested:false})));
      window.activeRunsBefore=JSON.stringify(mockRuns.slice(0,2));
      showView('runs');await loadRuns();
      check($('runs').children.length===202,'initial run history');
      check(!$('clear-runs').disabled,'clear disabled with finished runs');
      check(document.documentElement.scrollWidth<=innerWidth,'runs horizontal overflow');
      const list=$('runs'),rect=list.getBoundingClientRect();
      window.runToolbarTop=document.querySelector('#view-runs .toolbar').getBoundingClientRect().top;
      if(innerWidth<=820){
        check(document.documentElement.scrollHeight>innerHeight*3,'mobile page not scrolling');
        check(getComputedStyle(list).overflowY!=='auto','mobile run list traps scrolling');
        window.scrollTo(0,400);check(scrollY>0,'mobile page scroll failed');window.scrollTo(0,0);
        return {mobile:true};
      }
      check(document.documentElement.scrollHeight<=innerHeight+1,'run history stretched page');
      check(rect.bottom<=innerHeight&&rect.height>100,'run list not viewport constrained');
      check(list.scrollHeight>list.clientHeight*3,'run list lacks independent scrolling');
      check(getComputedStyle(list).minHeight==='0px'&&getComputedStyle(list).overflowY==='auto','run list overflow rules');
      list.scrollTop=320;await loadRuns();check(list.scrollTop===320,'refresh resets run scroll');
      return {mobile:false,x:rect.left+100,y:rect.top+100};
    })()`);
    if(!setup.mobile){
      await request('Input.dispatchMouseEvent',{type:'mouseWheel',x:setup.x,y:setup.y,deltaX:0,deltaY:180},session);
      await new Promise(resolve=>setTimeout(resolve,100));
      await evaluate(session,`(()=>{
        if($('runs').scrollTop<=320||scrollY!==0||document.querySelector('#view-runs .toolbar').getBoundingClientRect().top!==runToolbarTop)
          throw new Error('run wheel moved toolbar/page or failed to scroll list');
      })()`);
    }
    const report=await evaluate(session,`(async()=>{
      function check(value,message){if(!value)throw new Error(message)}
      const writes=archiveWrites.length;
      // Repeated clicks cannot submit multiple archival sweeps.
      await Promise.all([clearRunHistory(),clearRunHistory()]);
      check(archiveWrites.length===writes+1,'duplicate archive clicks');
      check($('runs').children.length===2&&S.runs.every(r=>['queued','running'].includes(r.state)),'clearing hid active work');
      check(JSON.stringify(mockRuns.slice(0,2))===activeRunsBefore,'clearing mutated active work');
      check($('clear-runs').disabled,'clear enabled without eligible runs');
      check($('runs-feedback').textContent.includes('200 runs cleared'),'cleared count missing');
      $('show-archived-runs').checked=true;await loadRuns();
      check($('runs').children.length===202&&S.runs.filter(r=>r.archived_at).length===200,'archived toggle lost history');
      check($('clear-runs').disabled,'archived runs considered clearable');
      mockRun={...mockRuns[2],events:[{id:1,kind:'activity',message:'Preserved log',created_at:mockRuns[2].created_at}],approvals:[],artifacts:[]};
      renderConversationMessages({title:'Preserved conversation',messages:[{role:'assistant',content:'Preserved reply',run_id:mockRun.id}]});
      $('messages').querySelector('[data-run-details]').click();await new Promise(resolve=>setTimeout(resolve,20));
      check(S.drawerRun===mockRun.id&&$('run-events').textContent.includes('Preserved log'),'chat cannot open archived run');
      closeDrawer();
      $('show-archived-runs').checked=false;await loadRuns();
      check(S.runs.length===2,'hidden archives reappeared on refresh');
      // A failed request keeps visible history and reports an actionable error.
      mockRuns.push({id:'run_ffffffffffffffffffffffffffffffff',state:'completed',task_type:'search',created_at:'2026-10-05T12:00:00Z',archived_at:null});
      await loadRuns();failArchive=true;await clearRunHistory();failArchive=false;
      check(S.runs.length===3&&!$('clear-runs').disabled&&$('runs-feedback').textContent==='Mock archive failed','archive failure not recoverable');
      // A delayed old load must not overwrite the selected archive filter.
      const fetchBefore=window.fetch;let release;
      window.fetch=async(url,options)=>String(url)==='/api/runs'?await new Promise(resolve=>release=resolve):fetchBefore(url,options);
      const stale=loadRuns();$('show-archived-runs').checked=true;await loadRuns();
      release({ok:true,status:200,json:async()=>({runs:[],history:{clearable:0,archived:0}})});await stale;window.fetch=fetchBefore;
      check(S.runs.length===203,'stale request overwrote archived view');
      showView('documents');check(!document.querySelector('.shell').classList.contains('runs-view'),'run layout leaked to other tabs');
      return {width:innerWidth,cards:202,independentScrolling:innerWidth>820,pageScrolling:innerWidth<=820,
        fixedToolbar:true,archiving:true,activePreserved:true,chatAccess:true,refresh:true,rollback:true};
    })()`);
    runReports.push(report);
  }
  const cvReports=[];
  for(const width of [1440,390]){
    await request('Emulation.setDeviceMetricsOverride',{width,height:844,deviceScaleFactor:1,mobile:width<500},session);
    const report=await evaluate(session,`(async()=>{
      function check(value,message){if(!value)throw new Error(message)}
      showView('jobs');S.query='';S.archive=false;S.discoveries=false;
      mockJobs=[{id:'job_fixture_cv',company:'AcmeCloud',title:'Partner Technical Account Manager',application_status:'new',
        qualification_status:'recommended',qualification_reasons:['Partner/customer implementation'],qualification_reviewed:true,
        source_url:'https://example.com/acmecloud',version:1,cv:{state:'not_generated'}}];
      await loadJobs();const root=innerWidth<=820?$('mobile-jobs'):$('kanban');
      const control=()=>root.querySelector('.cv-controls');
      check(control().textContent.includes('Not generated'),'CV initial state hidden');
      const posting=root.querySelector('[data-job-posting]');
      check(posting?.getAttribute('href')===mockJobs[0].source_url&&posting.textContent.includes('Open job posting'),'job posting link missing');
      check(Boolean(posting.compareDocumentPosition(control())&Node.DOCUMENT_POSITION_FOLLOWING),'job posting hidden below CV controls');
      check(posting.target==='_blank'&&posting.rel.includes('noopener'),'job posting link not safely opened');
      check(control().querySelector('[data-cv-generate]')&&!control().querySelector('[data-cv-generate]').disabled,'New job cannot generate');
      const writes=cvWrites.length,statuses=statusWrites.length;
      control().querySelector('[data-cv-generate]').click();await new Promise(resolve=>setTimeout(resolve,50));
      check(cvWrites.length===writes+1&&cvWrites.at(-1).job_id===mockJobs[0].id,'CV click not scoped to stable ID');
      check(control().textContent.includes('Queued')&&control().textContent.includes('Waiting behind search'),'CV queue information hidden');
      check(control().querySelector('[data-cv-generate]').disabled,'duplicate CV click possible');
      check(control().querySelector('[data-cv-cancel]')&&control().querySelector('[data-run-details]'),'active CV controls missing');
      check($('drawer-panel').textContent.includes('Queue position 2')&&$('drawer-panel').textContent.includes('Waiting behind search'),'drawer queue information missing');
      mockJobs[0].cv={...mockJobs[0].cv,state:'generating',progress:'Compiling and inspecting one-page PDF'};
      mockRun.state='running';await loadJobs();
      check(control().textContent.includes('Generating')&&control().textContent.includes('Compiling'),'generation progress hidden');
      const sub=S.runSubscriptions.get(mockRun.id);
      const started={id:2,kind:'running',message:'Task started',created_at:'2026-10-06T10:00:00Z'};mockRun.events.push(started);
      sub.source.onmessage({lastEventId:'2',data:JSON.stringify({event:started,terminal:false})});
      await new Promise(resolve=>setTimeout(resolve,160));
      check($('drawer-panel').querySelector('.badge.running')&&!$('drawer-panel').textContent.includes('Waiting behind search'),'drawer still queued after task started');
      sub.source.onmessage({lastEventId:'3',data:JSON.stringify({event:{id:3,kind:'command_output',message:'Compiler finished',created_at:'2026-10-06T10:00:00Z'},terminal:false})});
      check($('run-events').textContent.includes('Compiler finished'),'live compiler progress hidden');
      mockJobs[0].cv={state:'ready',run_id:mockRun.id,url:'/files/cv/acmecloud.pdf'};mockRun.state='completed';
      mockRun.artifacts=[{kind:'CV',url:'/files/cv/acmecloud.pdf'}];
      sub.source.onmessage({lastEventId:'4',data:JSON.stringify({terminal:true,state:'completed',run:mockRun})});
      await new Promise(resolve=>setTimeout(resolve,180));
      check(control().textContent.includes('Ready'),'completion state missing');
      check(control().querySelector('a[href="/files/cv/acmecloud.pdf"]')&&control().querySelector('a[href="/files/cv/acmecloud.pdf?download=1"]'),'card CV links missing');
      closeDrawer();await openJob(mockJobs[0].id);
      check($('drawer-panel').querySelector('[data-job-posting]')?.getAttribute('href')===mockJobs[0].source_url,'detail job posting link missing');
      check($('drawer-panel').querySelector('.cv-controls a[href="/files/cv/acmecloud.pdf"]'),'detail Open CV missing');
      check($('drawer-panel').querySelector('.cv-controls a[href="/files/cv/acmecloud.pdf?download=1"]'),'detail Download CV missing');
      closeDrawer();mockJobs[0].cv={state:'failed',run_id:mockRun.id,error:'PDF validation failed'};await loadJobs();
      check(control().textContent.includes('Failed')&&control().querySelector('[data-cv-retry]'),'CV failure/retry hidden');
      check(!control().querySelector('a'),'unvalidated PDF exposed as ready');
      mockJobs[0].source_url='';await loadJobs();
      check(root.textContent.includes('Job posting link unavailable')&&!root.querySelector('[data-job-posting]'),'missing source silently omitted or invented');
      check(statusWrites.length===statuses&&mockJobs[0].application_status==='new','CV generation changed application status');
      check(document.documentElement.scrollWidth<=innerWidth,'CV controls overflow phone');
      renderConversationMessages({title:'Explicit request',messages:[{role:'assistant',content:'Queued CV task',cv_runs:[{id:mockRun.id,company:'AcmeCloud',title:mockJobs[0].title}]}]});
      check($('messages').querySelector('[data-run-details]')?.dataset.runDetails===mockRun.id,'dedicated chat task link missing');
      return {width:innerWidth,newJobAction:true,queued:true,generating:true,progress:true,ready:true,failed:true,chatLink:true,statusPreserved:true};
    })()`);
    cvReports.push(report);
  }
  const refreshReports=[];
  for(const width of [1440,390]){
    await request('Emulation.setDeviceMetricsOverride',{width,height:844,deviceScaleFactor:1,mobile:width<500},session);
    refreshReports.push(await evaluate(session,`(async()=>{
      function check(value,message){if(!value)throw new Error(message)}
      mockJobs=[{id:'job_refresh_new',company:'Refresh New',title:'Implementation Engineer',application_status:'new'},
        {id:'job_refresh_applied',company:'Tracked application',title:'Technical Account Manager',application_status:'applied'}];
      const oldWrites=batchWrites.length;await refreshAll();
      check(batchWrites.length===oldWrites,'preview already started work');
      check(refreshSelection.size===1&&$('refresh-preview-count').textContent.includes('1 eligible'),'preview selection missing');
      $('refresh-tracked').checked=true;await loadRefreshPreview(false);
      check(refreshPreview.eligible_count===2&&refreshSelection.size===1&&!refreshSelection.has('job_refresh_applied'),'tracked applications implicitly selected');
      const started=submitRefreshBatch();await submitRefreshBatch();await started;
      check(batchWrites.length===oldWrites+1&&batchWrites.at(-1).job_ids.join(',')==='job_refresh_new','duplicate or wrong batch selection');
      showView('runs');await loadRuns();
      check($('runs').querySelectorAll(':scope > .run-card').length===1,'batch children flooded history');
      const batch=$('runs').querySelector('[data-batch-expand]');batch.open=true;
      check(batch.textContent.includes('Refresh New')&&batch.textContent.includes('Implementation Engineer'),'child identity missing');
      check($('runs').textContent.includes('Queued')&&!$('runs').textContent.includes('Working…'),'queued label wrong');
      await loadRuns();check($('runs').querySelector('[data-batch-expand]').open,'expanded batch collapsed after polling');
      mockRun.state='running';mockRun.children[0].state='running';mockRun.children[0].display_state='running';mockRun.counts={total:1,running:1,queued:0};await loadRuns();
      const stopped=stopRuns(mockRun.id,$('runs').querySelector('[data-stop-batch]'));
      check($('runs-feedback').textContent==='Stopping…','stop lacks immediate feedback');await stopped;
      check(stopWrites.at(-1).url.endsWith('/stop-batch')&&$('runs').textContent.includes('Stopping'),'stop batch state not displayed');
      for(const r of mockRuns){r.state='interrupted';r.display_state='interrupted'}mockRun.counts={total:1,interrupted:1};
      mockStop={...mockStop,state:'completed',interrupted_active:2,stopping:0};
      await new Promise(resolve=>setTimeout(resolve,400));
      check($('runs-feedback').textContent.includes('2 active interrupted')&&$('stop-all-runs').disabled,'terminal stop counts wrong');
      mockRuns=[{id:'run_independent',task_type:'search',state:'queued'}];await loadRuns();
      failStop=true;await stopRuns();failStop=false;
      check($('runs-feedback').textContent.includes('Failed to request stop')&&!$('stop-all-runs').disabled,'stop failure cannot recover');
      await stopRuns();check(stopWrites.at(-1).url==='/api/runs/stop-all'&&mockRuns[0].state==='cancelled','stop all failed');
      check(document.documentElement.scrollWidth<=innerWidth,'refresh controls overflow phone');
      return {width:innerWidth,preview:true,grouping:true,explicitSelection:true,dedup:true,stopping:true,accurateStates:true,stopAll:true};
    })()`));
  }
  const bulkCvReports=[];
  for(const width of [1440,390]){
    await request('Emulation.setDeviceMetricsOverride',{width,height:844,deviceScaleFactor:1,mobile:width<500},session);
    bulkCvReports.push(await evaluate(session,`(async()=>{
      showView('jobs');closeDrawer();const before=JSON.stringify(mockJobs.map(j=>j.application_status));
      const button=$('generate-new-cvs');if(!button||!button.offsetWidth)throw new Error('Bulk CV action missing');
      const old=bulkCvWrites.length;const first=generateNewCvs();await generateNewCvs();await first;
      if(bulkCvWrites.length!==old+1||button.disabled)throw new Error('Bulk repeat-click guard failed');
      if(!$('flash').textContent.includes('2 CVs queued')||JSON.stringify(mockJobs.map(j=>j.application_status))!==before)throw new Error('Bulk CV feedback/status failed');
      if(!$('flash').textContent.includes('up to 4 at a time'))throw new Error('Configured concurrency feedback incorrect');
      if(document.documentElement.scrollWidth>innerWidth)throw new Error('Bulk CV toolbar overflows');
      return {width:innerWidth,button:true,dedup:true,feedback:true,statusPreserved:true};
    })()`));
  }
  console.log(JSON.stringify({logs:reports,kanban:kanbanReports,runs:runReports,cv:cvReports,refresh:refreshReports,bulkCv:bulkCvReports}));
})().catch(error => {console.error(error);process.exitCode=1;}).finally(() => {
  if (socket) socket.close();
  chrome.kill('SIGTERM');
});

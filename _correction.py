#!/usr/bin/env python3
"""Correction pass: revert bad choices, keep good ones, apply restrained design."""
import re

FILE = r'C:\Users\Utilisateur\Dev-RAIKU\raiku-revenue-model\raiku_revenue_simulator.html'

with open(FILE, 'r', encoding='utf-8') as f:
    content = f.read()

# ============================================================
# STEP 1: Replace CSS color variables with restrained palette
# ============================================================
old_colors = """    /* Economic category colors — semantic hierarchy */
    --clr-gross: #F6F8FA;       /* White — neutral aggregate */
    --clr-jit: #4ECDC4;         /* Teal — JIT source revenue */
    --clr-aot: #A89F91;         /* Warm gray — AOT source revenue */
    --clr-validator: #5B8DEF;   /* Blue — Validator base revenue */
    --clr-val-bonus: #3A6BD4;   /* Darker blue — AOT validator bonus */
    --clr-rebate: #F5A623;      /* Orange — customer rebates */
    --clr-rebate-jit: #EDBE5A;  /* Lighter orange — JIT rebate */
    --clr-protocol: #C0FF38;    /* Lime — Protocol Treasury */"""

new_colors = """    /* Economic category colors — restrained palette */
    --clr-gross: #F5F6D0;       /* Pale yellow / pale lime — total gross */
    --clr-jit: #FFFFFF;         /* White — JIT revenue */
    --clr-aot: #FFFFFF;         /* White — AOT revenue */
    --clr-validator: #5B8DEF;   /* Blue — Validator family */
    --clr-val-bonus: #4178DE;   /* Slightly darker blue — AOT validator bonus */
    --clr-rebate: #4CAF50;      /* Green — customer rebates */
    --clr-rebate-jit: #66BB6A;  /* Lighter green — JIT rebate */
    --clr-protocol: #C0FF38;    /* Lime — Protocol Treasury */"""

content = content.replace(old_colors, new_colors)
print("STEP 1: Color palette replaced")

# ============================================================
# STEP 2: Remove JIT module CSS
# ============================================================
jit_module_css = """/* JIT Data Module */
.jit-module { background:var(--card); border:1px solid var(--card-border); border-radius:var(--radius); padding:20px; margin-bottom:var(--gap); border-left:3px solid var(--clr-jit); }
.jit-module h2 { font-size:13px; font-weight:600; color:var(--clr-jit); margin-bottom:12px; text-transform:uppercase; letter-spacing:2px; font-family:var(--font-mono); }
.jit-headline { display:flex; align-items:baseline; gap:12px; margin-bottom:14px; }
.jit-headline .jit-sol { font-size:28px; font-weight:700; color:var(--clr-jit); font-family:var(--font-mono); }
.jit-headline .jit-usd { font-size:16px; color:var(--text2); font-family:var(--font-mono); }
.jit-meta-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:8px 16px; margin-bottom:14px; }
.jit-meta { display:flex; flex-direction:column; gap:2px; }
.jit-meta .jm-label { font-size:9px; color:var(--text2); text-transform:uppercase; letter-spacing:0.8px; }
.jit-meta .jm-value { font-size:12px; color:var(--text); }
.jit-sensitivity { margin-top:12px; }
.jit-sensitivity h3 { font-size:11px; color:var(--text-mid); text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }
.jit-scope-note { font-size:10px; color:var(--text2); font-style:italic; margin-top:8px; padding:6px 10px; background:#ffffff04; border-radius:4px; border-left:2px solid var(--clr-jit); line-height:1.5; }"""

content = content.replace(jit_module_css, '')
print("STEP 2: JIT module CSS removed")

# Also remove responsive rule for jit-headline
content = content.replace("    .jit-headline { flex-direction:column; gap:4px; }\n", "")

# ============================================================
# STEP 3: Remove JIT Market Assumption hero block from HTML
# ============================================================
jit_hero_html = """<!-- JIT MARKET ASSUMPTION MODULE -->
<div class="jit-module fade-in fade-in-3">
    <h2>JIT Market Assumption</h2>
    <div class="jit-headline">
        <span class="jit-sol" id="jit-m-sol">&mdash;</span>
        <span class="jit-usd" id="jit-m-usd"></span>
    </div>
    <div class="jit-meta-grid">
        <div class="jit-meta"><span class="jm-label">Source</span><span class="jm-value">Dune Analytics — Jito MEV Tips</span></div>
        <div class="jit-meta"><span class="jm-label">Period</span><span class="jm-value" id="jit-m-period">&mdash;</span></div>
        <div class="jit-meta"><span class="jm-label">Duration</span><span class="jm-value" id="jit-m-duration">&mdash;</span></div>
        <div class="jit-meta"><span class="jm-label">Annualization</span><span class="jm-value" id="jit-m-method">&mdash;</span></div>
    </div>
    <div class="jit-scope-note">
        <strong>Scope:</strong> Jito tips only. Does not include other JIT auction providers (e.g. Harmonic) or private MEV flows. The actual addressable JIT market is likely larger than this observed figure.
    </div>
    <div class="jit-sensitivity">
        <h3>Protocol Treasury at Different JIT Market Sizes</h3>
        <div style="max-width:600px;"><canvas id="chart-jit-sens" height="140"></canvas></div>
    </div>
</div>"""

content = content.replace(jit_hero_html, '')
print("STEP 3: JIT hero block removed from HTML")

# ============================================================
# STEP 4: Fix Rebate card colors (green instead of orange)
# ============================================================
content = content.replace(
    '<span style="color:var(--clr-rebate);">AOT Rebate</span>',
    '<span style="color:var(--clr-rebate);">AOT Rebate</span>'
)  # Already uses var, no change needed since we changed the var

# ============================================================
# STEP 5: Redesign Sankey SVG — taller, cleaner, readable
# ============================================================
old_sankey = """    <!-- Sankey Flow Chart — redesigned for readability -->
    <div class="chart-box" style="min-height:360px;padding:20px;">
        <h3 style="font-size:13px;margin-bottom:12px;">Revenue Flow — JIT + AOT → Distribution</h3>
        <svg id="flow-sankey" viewBox="0 0 1000 340" style="width:100%;height:320px;font-family:var(--font-mono);">
            <!-- Left: Sources -->
            <rect id="fk-jit" x="0" y="30" width="24" height="100" rx="4" fill="var(--clr-jit)"/>
            <text id="fk-jit-label" x="32" y="70" fill="var(--clr-jit)" font-size="14" font-weight="700">JIT Revenue</text>
            <text id="fk-jit-v" x="32" y="90" fill="#A0A3A9" font-size="12">—</text>
            <rect id="fk-aot" x="0" y="170" width="24" height="130" rx="4" fill="var(--clr-aot)"/>
            <text id="fk-aot-label" x="32" y="220" fill="var(--clr-aot)" font-size="14" font-weight="700">AOT Revenue</text>
            <text id="fk-aot-v" x="32" y="242" fill="#A0A3A9" font-size="12">—</text>
            <!-- Center: Total Revenue -->
            <rect id="fk-total" x="340" y="15" width="24" height="310" rx="4" fill="var(--clr-gross)"/>
            <text x="372" y="160" fill="var(--clr-gross)" font-size="14" font-weight="700">Total Revenue</text>
            <text id="fk-total-v" x="372" y="180" fill="#A0A3A9" font-size="12">—</text>
            <!-- Right: 5 Distribution outputs -->
            <rect id="fk-valbase" x="700" y="10" width="24" rx="4" fill="var(--clr-validator)"/>
            <text id="fk-valbase-label" x="732" fill="var(--clr-validator)" font-size="13" font-weight="600">Validator Base</text>
            <text id="fk-valbase-v" x="732" fill="#A0A3A9" font-size="11">—</text>
            <rect id="fk-vb" x="700" rx="4" fill="var(--clr-val-bonus)"/>
            <text id="fk-vb-label" x="732" fill="var(--clr-val-bonus)" font-size="13" font-weight="600">AOT Val. Bonus</text>
            <text id="fk-vb-paren" x="875" fill="var(--clr-val-bonus)" font-size="9" opacity="0.7">(AOT only)</text>
            <text id="fk-vb-v" x="732" fill="#A0A3A9" font-size="11">—</text>
            <rect id="fk-reb-aot" x="700" rx="4" fill="var(--clr-rebate)"/>
            <text id="fk-reb-aot-label" x="732" fill="var(--clr-rebate)" font-size="13" font-weight="600">AOT Rebate</text>
            <text id="fk-reb-aot-v" x="732" fill="#A0A3A9" font-size="11">—</text>
            <rect id="fk-reb-jit" x="700" rx="4" fill="var(--clr-rebate-jit)"/>
            <text id="fk-reb-jit-label" x="732" fill="var(--clr-rebate-jit)" font-size="13" font-weight="600">JIT Rebate</text>
            <text id="fk-reb-jit-v" x="732" fill="#A0A3A9" font-size="11">—</text>
            <rect id="fk-proto" x="700" rx="4" fill="var(--clr-protocol)"/>
            <text id="fk-proto-label" x="732" fill="var(--clr-protocol)" font-size="13" font-weight="700">Protocol Treasury</text>
            <text id="fk-proto-v" x="732" fill="var(--clr-protocol)" font-size="11">—</text>
            <!-- Flow paths (drawn dynamically) -->
            <g id="fk-paths" opacity="0.35"></g>
        </svg>
    </div>"""

new_sankey = """    <!-- Sankey Flow Chart — clean, tall, readable -->
    <div class="chart-box" style="min-height:520px;padding:24px;">
        <h3 style="font-size:14px;margin-bottom:16px;color:var(--text-mid);">Revenue Flow — JIT + AOT → Distribution</h3>
        <svg id="flow-sankey" viewBox="0 0 1000 480" style="width:100%;height:480px;font-family:var(--font-mono);">
            <!-- Left: Sources -->
            <rect id="fk-jit" x="0" y="40" width="20" height="140" rx="3" fill="#FFFFFF" opacity="0.9"/>
            <text id="fk-jit-label" x="30" y="100" fill="#FFFFFF" font-size="15" font-weight="700">JIT Revenue</text>
            <text id="fk-jit-v" x="30" y="122" fill="#A0A3A9" font-size="13">—</text>
            <rect id="fk-aot" x="0" y="240" width="20" height="140" rx="3" fill="#FFFFFF" opacity="0.9"/>
            <text id="fk-aot-label" x="30" y="300" fill="#FFFFFF" font-size="15" font-weight="700">AOT Revenue</text>
            <text id="fk-aot-v" x="30" y="322" fill="#A0A3A9" font-size="13">—</text>
            <!-- Center: Total Revenue -->
            <rect id="fk-total" x="370" y="20" width="20" height="440" rx="3" fill="#F5F6D0" opacity="0.85"/>
            <text x="400" y="230" fill="#F5F6D0" font-size="15" font-weight="700">Total Revenue</text>
            <text id="fk-total-v" x="400" y="252" fill="#A0A3A9" font-size="13">—</text>
            <!-- Right: 5 Distribution outputs — evenly spaced, large -->
            <rect id="fk-valbase" x="720" y="20" width="18" height="80" rx="3" fill="#5B8DEF"/>
            <text id="fk-valbase-label" x="746" y="52" fill="#5B8DEF" font-size="15" font-weight="700">Validator Base</text>
            <text id="fk-valbase-v" x="746" y="74" fill="#A0A3A9" font-size="13">—</text>
            <rect id="fk-vb" x="720" y="115" width="18" height="60" rx="3" fill="#4178DE"/>
            <text id="fk-vb-label" x="746" y="140" fill="#4178DE" font-size="14" font-weight="600">AOT Val. Bonus</text>
            <text id="fk-vb-paren" x="882" y="140" fill="#4178DE" font-size="10" opacity="0.7">(AOT only)</text>
            <text id="fk-vb-v" x="746" y="160" fill="#A0A3A9" font-size="13">—</text>
            <rect id="fk-reb-aot" x="720" y="200" width="18" height="60" rx="3" fill="#4CAF50"/>
            <text id="fk-reb-aot-label" x="746" y="225" fill="#4CAF50" font-size="14" font-weight="600">AOT Rebate</text>
            <text id="fk-reb-aot-v" x="746" y="245" fill="#A0A3A9" font-size="13">—</text>
            <rect id="fk-reb-jit" x="720" y="285" width="18" height="60" rx="3" fill="#66BB6A"/>
            <text id="fk-reb-jit-label" x="746" y="310" fill="#66BB6A" font-size="14" font-weight="600">JIT Rebate</text>
            <text id="fk-reb-jit-v" x="746" y="330" fill="#A0A3A9" font-size="13">—</text>
            <rect id="fk-proto" x="720" y="370" width="18" height="80" rx="3" fill="#C0FF38"/>
            <text id="fk-proto-label" x="746" y="402" fill="#C0FF38" font-size="15" font-weight="700">Protocol Treasury</text>
            <text id="fk-proto-v" x="746" y="424" fill="#C0FF38" font-size="13">—</text>
            <!-- Flow paths (drawn dynamically) -->
            <g id="fk-paths" opacity="0.3"></g>
        </svg>
    </div>"""

content = content.replace(old_sankey, new_sankey)
print("STEP 5: Sankey redesigned — taller, cleaner, evenly spaced outputs")

# ============================================================
# STEP 6: Enrich Data Sources section with full JIT data module
# ============================================================
old_datasources = """<!-- DATA SOURCES -->
<div class="section fade-in fade-in-7" style="margin-top:var(--gap);">
    <h2>Data Sources & Methodology</h2>
    <details style="margin-bottom:10px;">
        <summary style="font-size:12px;color:var(--lime-soft);cursor:pointer;font-weight:600;padding:6px 0;">JIT Market Data</summary>
        <div style="font-size:11px;color:var(--text2);padding:8px 0 4px 12px;line-height:1.6;">
            <strong>Source:</strong> Dune Analytics — Jito MEV tips aggregated over 36 epochs (Feb–Mar 2026).<br>
            <strong>Annualization:</strong> Sum of per-epoch tips × (365.25 / epoch_days) = ~810,778 SOL/yr.<br>
            <strong>Scope:</strong> Jito tips only. Excludes Harmonic, private MEV, and future JIT providers. Actual addressable market is larger.
        </div>
    </details>
    <details>
        <summary style="font-size:12px;color:var(--lime-soft);cursor:pointer;font-weight:600;padding:6px 0;">Model Constants & Setup</summary>
        <div style="font-size:11px;color:var(--text2);padding:8px 0 4px 12px;line-height:1.6;">
            <strong>Slots/year (SY):</strong> 78,408,000 (~2.5 slots/sec × 86400 × 365.25)<br>
            <strong>CU/block (CB):</strong> 60,000,000 (Solana max compute units per block)<br>
            <strong>Lamports/SOL (LS):</strong> 1,000,000,000<br>
            <strong>SOL price default:</strong> Real average from last 36 epochs (~$109)<br>
            <strong>Commission:</strong> Dynamic from slider (default 5%). Validator base = 1 – commission.<br>
            <strong>Two-waterfall model:</strong> AOT and JIT streams are calculated independently, then aggregated. Validator bonus applies only to AOT.
        </div>
    </details>
</div>"""

new_datasources = """<!-- DATA SOURCES & METHODOLOGY — includes full JIT data module -->
<div class="section fade-in fade-in-7" style="margin-top:var(--gap);">
    <h2>Data Sources & Methodology</h2>

    <!-- JIT Market Data — primary data transparency module -->
    <div style="background:#0d0e0f;border:1px solid var(--card-border);border-radius:8px;padding:16px;margin-bottom:12px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
            <span style="font-size:12px;font-weight:600;color:var(--lime-soft);text-transform:uppercase;letter-spacing:1px;">JIT Market Assumption</span>
            <span style="font-size:10px;color:var(--text2);font-style:italic;" id="jit-ds-badge">computed from raw data</span>
        </div>
        <div style="display:flex;align-items:baseline;gap:10px;margin-bottom:12px;">
            <span style="font-size:22px;font-weight:700;color:var(--text);font-family:var(--font-mono);" id="jit-m-sol">&mdash;</span>
            <span style="font-size:13px;color:var(--text2);font-family:var(--font-mono);" id="jit-m-usd"></span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(180px, 1fr));gap:6px 14px;font-size:11px;">
            <div><span style="color:var(--text2);font-size:9px;text-transform:uppercase;letter-spacing:0.5px;">Source</span><br><span style="color:var(--text);">Dune Analytics — Jito MEV Tips</span></div>
            <div><span style="color:var(--text2);font-size:9px;text-transform:uppercase;letter-spacing:0.5px;">Period</span><br><span style="color:var(--text);" id="jit-m-period">&mdash;</span></div>
            <div><span style="color:var(--text2);font-size:9px;text-transform:uppercase;letter-spacing:0.5px;">Window</span><br><span style="color:var(--text);" id="jit-m-duration">&mdash;</span></div>
            <div><span style="color:var(--text2);font-size:9px;text-transform:uppercase;letter-spacing:0.5px;">Annualization</span><br><span style="color:var(--text);" id="jit-m-method">&mdash;</span></div>
        </div>
        <div style="font-size:10px;color:var(--text2);font-style:italic;margin-top:10px;line-height:1.5;padding-top:8px;border-top:1px solid var(--card-border);">
            <strong>Scope:</strong> Jito tips only. Excludes Harmonic, private MEV, and future JIT providers. Actual addressable JIT market is likely larger than this observed figure.
        </div>
        <details style="margin-top:10px;">
            <summary style="font-size:11px;color:var(--lime-soft);cursor:pointer;font-weight:600;padding:4px 0;">Inspect JIT dataset details</summary>
            <div style="font-size:10px;color:var(--text2);padding:8px 0 4px 12px;line-height:1.6;">
                <strong>Raw dataset:</strong> <code>jito_mev_rewards.csv</code> — <span id="jit-ds-total-epochs">—</span> epochs total (epoch <span id="jit-ds-first-epoch">—</span> → <span id="jit-ds-last-epoch">—</span>)<br>
                <strong>Window used:</strong> Most recent <span id="jit-ds-window-n">—</span> epochs with non-zero MEV rewards<br>
                <strong>Total SOL in window:</strong> <span id="jit-ds-total-sol">—</span><br>
                <strong>Formula:</strong> (total SOL in window / total days in window) × 365.25<br>
                <strong>Validation:</strong> Result closely matches pre-aggregated reference value (~810,778 SOL/yr from D.a.mev)
            </div>
        </details>
    </div>

    <details style="margin-bottom:10px;">
        <summary style="font-size:12px;color:var(--lime-soft);cursor:pointer;font-weight:600;padding:6px 0;">Model Constants & Setup</summary>
        <div style="font-size:11px;color:var(--text2);padding:8px 0 4px 12px;line-height:1.6;">
            <strong>Slots/year (SY):</strong> 78,408,000 (~2.5 slots/sec × 86400 × 365.25)<br>
            <strong>CU/block (CB):</strong> 60,000,000 (Solana max compute units per block)<br>
            <strong>Lamports/SOL (LS):</strong> 1,000,000,000<br>
            <strong>SOL price default:</strong> Real average from last 36 epochs (~$109)<br>
            <strong>Commission:</strong> Dynamic from slider (default 5%). Validator base = 1 – commission.<br>
            <strong>Two-waterfall model:</strong> AOT and JIT streams are calculated independently, then aggregated. Validator bonus applies only to AOT.
        </div>
    </details>
</div>"""

content = content.replace(old_datasources, new_datasources)
print("STEP 6: Data Sources enriched with full JIT data module")

# ============================================================
# STEP 7: Rewrite updFlow() for new taller Sankey (viewBox 1000×480)
# ============================================================
old_updflow = '''function updFlow(p,r){
    const jU=r.jitGrossUSD, aU=r.aotGrossUSD, totalU=r.totalGrossUSD||1;
    const vbU=r.totalValBase*p.p, abU=r.aotValBonus*p.p;
    const raU=r.aotRebate*p.p, rjU=r.jitRebate*p.p, prU=r.totalProtocolUSD;

    document.getElementById('fk-jit-v').textContent=fmtShort(jU);
    document.getElementById('fk-aot-v').textContent=fmtShort(aU);
    document.getElementById('fk-total-v').textContent=fmtShort(totalU)+' (100%)';

    const outputs=[
        {id:'valbase', v:vbU, color:'#5B8DEF', label:'Validator Base'},
        {id:'vb',      v:abU, color:'#3A6BD4', label:'AOT Val. Bonus'},
        {id:'reb-aot', v:raU, color:'#F5A623', label:'AOT Rebate'},
        {id:'reb-jit', v:rjU, color:'#EDBE5A', label:'JIT Rebate'},
        {id:'proto',   v:prU, color:'#C0FF38', label:'Protocol Treasury'},
    ];
    const totalOut=outputs.reduce((a,o)=>a+o.v,0)||1;
    const svgH=300, svgTop=10, minBarH=16, barGap=6;
    let yPos=svgTop;
    outputs.forEach(o=>{
        const h=Math.max(minBarH, (o.v/totalOut)*svgH);
        const el=document.getElementById('fk-'+o.id);
        if(el){el.setAttribute('y',yPos);el.setAttribute('height',h);el.setAttribute('width',24);}
        const lbl=document.getElementById('fk-'+o.id+'-label');
        if(lbl) lbl.setAttribute('y',yPos+h/2-2);
        const val=document.getElementById('fk-'+o.id+'-v');
        if(val){val.textContent=fmtShort(o.v)+' ('+Math.round(o.v/totalOut*100)+'%)';val.setAttribute('y',yPos+h/2+13);}
        const paren=document.getElementById('fk-'+o.id+'-paren');
        if(paren) paren.setAttribute('y',yPos+h/2-2);
        o.yMid=yPos+h/2;
        yPos+=h+barGap;
    });

    const maxH=260, minH=16;
    const jH=Math.max(minH,jU/totalU*maxH), aH=Math.max(minH,aU/totalU*maxH);
    const jY=20, aY=jY+jH+24;
    const jitRect=document.getElementById('fk-jit');
    jitRect.setAttribute('height',jH); jitRect.setAttribute('y',jY);
    const aotRect=document.getElementById('fk-aot');
    aotRect.setAttribute('y',aY); aotRect.setAttribute('height',aH);
    document.getElementById('fk-jit-label').setAttribute('y',jY+jH/2-4);
    document.getElementById('fk-jit-v').setAttribute('y',jY+jH/2+14);
    document.getElementById('fk-aot-label').setAttribute('y',aY+aH/2-4);
    document.getElementById('fk-aot-v').setAttribute('y',aY+aH/2+14);

    const totalEl=document.getElementById('fk-total');
    totalEl.setAttribute('y',svgTop);
    totalEl.setAttribute('height',yPos-svgTop);
    const cMid=svgTop+(yPos-svgTop)/2;

    const paths=document.getElementById('fk-paths');
    let svg='';
    svg+=`<path d="M24,${jY+jH/2} C180,${jY+jH/2} 260,${cMid} 340,${cMid}" stroke="#4ECDC4" stroke-width="${Math.max(2,jH/4)}" fill="none"/>`;
    svg+=`<path d="M24,${aY+aH/2} C180,${aY+aH/2} 260,${cMid} 340,${cMid}" stroke="#A89F91" stroke-width="${Math.max(2,aH/4)}" fill="none"/>`;
    outputs.forEach(o=>{
        const w=Math.max(1.2, o.v/totalOut*12);
        svg+=`<path d="M364,${cMid} C530,${cMid} 620,${o.yMid} 700,${o.yMid}" stroke="${o.color}" stroke-width="${w}" fill="none"/>`;
    });
    paths.innerHTML=svg;
}'''

new_updflow = '''function updFlow(p,r){
    const jU=r.jitGrossUSD, aU=r.aotGrossUSD, totalU=r.totalGrossUSD||1;
    const vbU=r.totalValBase*p.p, abU=r.aotValBonus*p.p;
    const raU=r.aotRebate*p.p, rjU=r.jitRebate*p.p, prU=r.totalProtocolUSD;

    // Update value labels
    document.getElementById('fk-jit-v').textContent=fmtShort(jU);
    document.getElementById('fk-aot-v').textContent=fmtShort(aU);
    document.getElementById('fk-total-v').textContent=fmtShort(totalU)+' (100%)';

    // Right outputs — proportional sizing with generous minimum, evenly spaced
    const outputs=[
        {id:'valbase', v:vbU, color:'#5B8DEF'},
        {id:'vb',      v:abU, color:'#4178DE'},
        {id:'reb-aot', v:raU, color:'#4CAF50'},
        {id:'reb-jit', v:rjU, color:'#66BB6A'},
        {id:'proto',   v:prU, color:'#C0FF38'},
    ];
    const totalOut=outputs.reduce((a,o)=>a+o.v,0)||1;
    const svgH=440, svgTop=20, minBarH=40, barGap=15;
    const availH=svgH-barGap*(outputs.length-1);
    let yPos=svgTop;
    outputs.forEach(o=>{
        const h=Math.max(minBarH, (o.v/totalOut)*availH);
        const el=document.getElementById('fk-'+o.id);
        if(el){el.setAttribute('y',yPos);el.setAttribute('height',h);el.setAttribute('width',18);}
        const lbl=document.getElementById('fk-'+o.id+'-label');
        if(lbl) lbl.setAttribute('y',yPos+h/2-4);
        const val=document.getElementById('fk-'+o.id+'-v');
        if(val){val.textContent=fmtShort(o.v)+' ('+Math.round(o.v/totalOut*100)+'%)';val.setAttribute('y',yPos+h/2+14);}
        const paren=document.getElementById('fk-'+o.id+'-paren');
        if(paren) paren.setAttribute('y',yPos+h/2-4);
        o.yMid=yPos+h/2;
        yPos+=h+barGap;
    });

    // Left inputs — proportional, generous min
    const leftH=380, minH=60;
    const jH=Math.max(minH, jU/totalU*leftH), aH=Math.max(minH, aU/totalU*leftH);
    const jY=40, aY=jY+jH+40;
    const jitRect=document.getElementById('fk-jit');
    jitRect.setAttribute('height',jH); jitRect.setAttribute('y',jY);
    const aotRect=document.getElementById('fk-aot');
    aotRect.setAttribute('y',aY); aotRect.setAttribute('height',aH);
    document.getElementById('fk-jit-label').setAttribute('y',jY+jH/2-6);
    document.getElementById('fk-jit-v').setAttribute('y',jY+jH/2+14);
    document.getElementById('fk-aot-label').setAttribute('y',aY+aH/2-6);
    document.getElementById('fk-aot-v').setAttribute('y',aY+aH/2+14);

    // Center bar
    const totalEl=document.getElementById('fk-total');
    totalEl.setAttribute('y',svgTop);
    totalEl.setAttribute('height',yPos-svgTop-barGap);
    const cMid=svgTop+(yPos-svgTop-barGap)/2;
    // Center labels
    const cLbl=document.querySelector('#flow-sankey text[x="400"][font-weight="700"]');
    if(cLbl) cLbl.setAttribute('y',cMid-8);
    const cVal=document.getElementById('fk-total-v');
    if(cVal) cVal.setAttribute('y',cMid+12);

    // Flow paths — thin, elegant curves
    const paths=document.getElementById('fk-paths');
    let svg='';
    const sw1=Math.max(3,jH/5), sw2=Math.max(3,aH/5);
    svg+=`<path d="M20,${jY+jH/2} C190,${jY+jH/2} 280,${cMid} 370,${cMid}" stroke="#FFFFFF" stroke-width="${sw1}" fill="none" opacity="0.5"/>`;
    svg+=`<path d="M20,${aY+aH/2} C190,${aY+aH/2} 280,${cMid} 370,${cMid}" stroke="#FFFFFF" stroke-width="${sw2}" fill="none" opacity="0.5"/>`;
    outputs.forEach(o=>{
        const w=Math.max(2, o.v/totalOut*16);
        svg+=`<path d="M390,${cMid} C550,${cMid} 640,${o.yMid} 720,${o.yMid}" stroke="${o.color}" stroke-width="${w}" fill="none" opacity="0.6"/>`;
    });
    paths.innerHTML=svg;
}'''

content = content.replace(old_updflow, new_updflow)
print("STEP 7: updFlow() rewritten for taller Sankey")

# ============================================================
# STEP 8: Remove JIT sensitivity chart from updCharts and initCharts
# ============================================================
# Remove cJS from initCharts
old_jit_chart_init = """    const jitEl=document.getElementById('chart-jit-sens');
    if(jitEl){
        cJS=new Chart(jitEl,{type:'bar',data:{labels:['2%','5%','10%','15%','20%','30%','50%'],datasets:[{label:'Protocol Treasury from JIT',data:Array(7).fill(0),backgroundColor:'rgba(78,205,196,0.5)',borderColor:'#4ECDC4',borderWidth:1}]},options:{...o,plugins:{...o.plugins,legend:{display:false}},scales:{...o.scales,x:{...o.scales.x,title:{...o.scales.x.title,text:'RAIKU JIT Market Share'}},y:{...o.scales.y,title:{...o.scales.y.title,text:'Protocol Treasury (USD/yr)'}}}}});
    }"""
content = content.replace(old_jit_chart_init, '')

# Also change chart color for epoch chart from teal to white
content = content.replace("borderColor:'#4ECDC4'", "borderColor:'#FFFFFF'")
content = content.replace("backgroundColor:'rgba(78,205,196,0.1)'", "backgroundColor:'rgba(255,255,255,0.05)'")

# Remove cJS usage from updCharts
old_jit_updcharts = """    if(cJS){
        const jM=getJitStats().annualized;
        [0.02,0.05,0.10,0.15,0.20,0.30,0.50].forEach((j,i)=>{
            const jG=jM*j;
            const jP=Math.max(0, jG*cR - jG*(p.rj/100));
            cJS.data.datasets[0].data[i]=jP*p.p;
        }); cJS.update();
    }"""
content = content.replace(old_jit_updcharts, '')

# Remove cJS variable declaration
content = content.replace('let cE,cA,cP,cF,cJS;', 'let cE,cA,cP,cF;')

print("STEP 8: JIT sensitivity chart removed from JS")

# ============================================================
# STEP 9: Remove JIT module population from update() and add
#          Data Sources population instead
# ============================================================
old_jit_update = """    // === JIT Module (populated from real data) ===
    const js=getJitStats();
    document.getElementById('jit-m-sol').textContent=fmt(js.annualized,'S')+'/yr';
    document.getElementById('jit-m-usd').textContent=fmt(js.annualized*p.p,'$')+'/yr';
    document.getElementById('jit-m-period').textContent=js.firstDate+' \\u2192 '+js.lastDate;
    document.getElementById('jit-m-duration').textContent=js.epochs+' epochs ('+Math.round(js.totalDays)+' days)';
    document.getElementById('jit-m-method').textContent='('+fmt(js.totalSol,'S')+' / '+Math.round(js.totalDays)+'d) \\u00d7 365.25';"""

new_jit_update = """    // === JIT Data (in Data Sources section) ===
    const js=getJitStats();
    document.getElementById('jit-m-sol').textContent=fmt(js.annualized,'S')+'/yr';
    document.getElementById('jit-m-usd').textContent=fmt(js.annualized*p.p,'$')+'/yr';
    document.getElementById('jit-m-period').textContent=js.firstDate+' \\u2192 '+js.lastDate;
    document.getElementById('jit-m-duration').textContent=js.epochs+' epochs ('+Math.round(js.totalDays)+' days)';
    document.getElementById('jit-m-method').textContent='('+fmt(js.totalSol,'S')+' / '+Math.round(js.totalDays)+'d) \\u00d7 365.25';
    // JIT dataset detail fields
    const allJit=D_JITO.filter(r=>r[1]>0);
    const dsTE=document.getElementById('jit-ds-total-epochs');if(dsTE) dsTE.textContent=allJit.length;
    const dsFE=document.getElementById('jit-ds-first-epoch');if(dsFE) dsFE.textContent=allJit[0]?allJit[0][0]:'?';
    const dsLE=document.getElementById('jit-ds-last-epoch');if(dsLE) dsLE.textContent=allJit.length?allJit[allJit.length-1][0]:'?';
    const dsWN=document.getElementById('jit-ds-window-n');if(dsWN) dsWN.textContent=js.epochs;
    const dsTS=document.getElementById('jit-ds-total-sol');if(dsTS) dsTS.textContent=fmt(js.totalSol,'S');"""

content = content.replace(old_jit_update, new_jit_update)
print("STEP 9: JIT data now populates Data Sources section")

# ============================================================
# STEP 10: Fix hardcoded teal/orange colors in updFlow color references
# ============================================================
# These were in the old updFlow which we already replaced.
# But also fix any remaining hardcoded teal references elsewhere.

# In cE epoch chart line, already handled above.

# Fix the AOT chart background color — keep lime
# (already fine: '#C0FF3860')

print("STEP 10: Color references cleaned up")

# ============================================================
# STEP 11: Update fade-in classes for correct section order
# ============================================================
# Revenue Overview should be fade-in-3 (was fade-in-4)
content = content.replace(
    '<div class="section fade-in fade-in-4">\n    <h2>Annual Revenue Overview</h2>',
    '<div class="section fade-in fade-in-3">\n    <h2>Annual Revenue Overview</h2>'
)
print("STEP 11: Fade-in animation order corrected")

# ============================================================
# Write back
# ============================================================
with open(FILE, 'w', encoding='utf-8') as f:
    f.write(content)

# Verify
lines = content.split('\n')
print(f"\nFile written: {len(lines)} lines")

# Verify balanced braces in script
script_start = content.find('<script>')
if script_start == -1:
    script_start = content.find("const D =")
script_section = content[script_start:]
opens = script_section.count('{')
closes = script_section.count('}')
print(f"Braces: {opens} open, {closes} close {'✓' if opens==closes else '⚠ MISMATCH'}")

# Check for remaining teal/orange references
for bad in ['#4ECDC4', '#A89F91', '#F5A623', '#EDBE5A', '#3A6BD4']:
    count = content.count(bad)
    if count > 0:
        print(f"WARNING: Found {count} remaining references to {bad}")
    else:
        print(f"OK: No references to {bad}")

print("\nDone. All corrections applied.")

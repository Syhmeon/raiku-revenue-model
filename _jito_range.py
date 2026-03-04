import json, urllib.request
out = []
for ep in [1, 50, 100, 200, 300, 350, 400, 450, 500, 550, 553]:
    try:
        body = json.dumps({"epoch": ep}).encode()
        req = urllib.request.Request(
            "https://kobe.mainnet.jito.network/api/v1/mev_rewards",
            data=body, headers={"Accept":"application/json","Content-Type":"application/json","User-Agent":"Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        if data.get("epoch") == ep:
            mev = int(data.get("total_network_mev_lamports", 0)) / 1e9
            out.append(f"Epoch {ep:>4}: {mev:>12.4f} SOL")
        else:
            out.append(f"Epoch {ep:>4}: returned {data.get('epoch')} instead")
    except Exception as e:
        out.append(f"Epoch {ep:>4}: ERROR {str(e)[:50]}")
f = open("_jito_range.log", "w"); f.write("\n".join(out)); f.close()

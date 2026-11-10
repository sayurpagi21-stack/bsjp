# -*- coding: utf-8 -*-
"""
BSJP Radar (lokal) - Beli Sore Jual Pagi
Pindai saham LQ45 (atau semua emiten IDX) dari Yahoo Finance, hitung skor BSJP,
tampilkan di http://localhost:8765. Refresh browser = data terbaru.

Cara pakai: klik dua kali file ini (atau Jalankan-BSJP.bat),
atau dari Command Prompt:  python bsjp.py
Tutup jendela hitamnya untuk berhenti.
Hanya pakai library bawaan Python, tidak perlu pip install.
"""
import json, math, os, sys, time, threading, urllib.request, urllib.error, webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8765
HOST = "0.0.0.0"   # bisa dibuka dari HP di WiFi yang sama. Ganti "127.0.0.1" kalau mau laptop saja.
WIB = timezone(timedelta(hours=7))
WORKERS = 8
QUICK_EVERY = 45          # detik: jarak minimal antar refresh cepat
FULL_EVERY = 30 * 60      # detik: pindai ulang semua emiten tiap 30 menit
PRE_LIQ = 1e9             # rata-rata transaksi >= Rp1 M ikut refresh cepat
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")

# Konstituen LQ45 periode 2 Nov 2026 - 29 Jan 2027 (sumber: pengumuman BEI via Kontan).
# Ganti daftar ini saat BEI merombak LQ45 (awal Feb, Mei, Agu, Nov).
LQ45 = """AADI ADMR ADRO AKRA AMMN AMRT ANTM ASII BBCA BBNI BBRI BBTN BMRI BRPT BUMI
CPIN CUAN DEWA EMTK ESSA EXCL GOTO HRTA ICBP INCO INDF INDY INKP ISAT ITMG JPFA
KLBF MAPI MBMA MDKA MEDC NCKL PGAS PGEO PTBA SMGR TLKM UNTR UNVR WIFI""".split()

# Daftar emiten IDX (962 kode, dari idx.co.id per 9 Nov 2026). Dipakai kalau
# daftar terbaru dari situs IDX tidak bisa diambil.
EMITEN = """MGLV TRUE LABA ARCI IPAC MASB BMHS FLMC NICL UVCR BUKA HAIS OILS GPSO MCOL RSGK RUNS SBMA CMNT GTSI IDEA KUAS BOBA MTEL DEPO BINO CMRY WGSH TAYS WMPP RMKE OBMD AVIA IPPE NASI BSML DRMA ADMR SEMA ASLC NETV BAUT ENAK NTBK SMKM STAA NANO BIKE WIRG SICO GOTO TLDN MTMH WINR IBOS OLIV ASHA SWID TRGU ARKO CHEM DEWI AXIO KRYA HATM RCCC GULA JARR AMMS RAFI KKES ELPI EURO KLIN TOOL BUAH CRAB MEDS COAL PRAY CBUT BELI MKTR OMED BSBK PDPP KDTN ZATA NINE MMIX PADA ISAP VTNY SOUL ELIT BEER CBPE SUNI CBRE WINE BMBL PEVE LAJU FWCT NAYZ IRSX PACK VAST CHIP HALO KING PGEO FUTR HILL BDKR PTMP SAGE TRON CUAN NSSS GTRA HAJJ PIPA NCKL MENN AWAN MBMA RAAM DOOH JATI TYRE MPXL SMIL KLAS MAXI VKTR RELF AMMN CRSN GRPM WIDI TGUK INET MAHA RMKO CNMA FOLK HBAT GRIA PPRI ERAL CYBR MUTU LMAX HUMI MSIE RSCH BABY AEGS IOTF KOCI PTPS BREN STRK KOKA LOPI UDNG RGAS MSTI IKPM AYAM SURI ASLI CGAS NICE MSJA SMLE ACRO MANG GRPH SMGA UNTD TOSK MPIX ALII MKAP MEJA LIVE HYGN BAIK VISI AREA MHKI ATLA DATA SOLA BATR SPRE PART GOLF ISEA BLES GUNA LABS DOSS NEST PTMR VERN DAAZ BOAT NAIK AADI CBDK OBAT MINE KAQI YUPI FORE MDLA DKHH ASPR PSAT COIN CDIA BLOG MERI CHEK PMUI EMAS PJHB RLCO SUPA WBSA JECX JELI EMMI BACH PRDL RANS AALI ABBA ABDA ABMM ACES ACST ADES ADHI ADMF ADMG ADRO AGII AGRO AGRS AHAP CASH TECH BSSR BSWD BTEK BTEL BTON BTPN BUDI MDIY KSIX RATU YOII HGII BRRC DGWG AIMS AISA AKKU AKPI AKRA AKSI ALDO ALKA ALMI ALTO AMAG AMFG AMIN AMRT ANJT ANTM APEX APIC APII APLI APLN ARGO ARII ARNA ARTA ARTI ARTO ASBI ASDM ASGR ASII ASJT ASMI ASRI ASRM ASSA ATIC AUTO BABP BACA BAJA BALI BAPA BATA BAYU BBCA BBHI BBKP BBLD BBMD BBNI BBRI BBRM BBTN BBYB BCAP BCIC BCIP BDMN BEKS BEST BFIN BGTG BHIT BIKA BIMA BINA BIPI BIPP BIRD BISI BJBR BJTM BKDP BKSL BKSW BLTA BLTZ BMAS BMRI BMSR BMTR BNBA BNBR BNGA BNII BNLI BOLT BPFI BPII BRAM BRMS BRNA BRPT BSDE BSIM BUKK BULL BUMI BUVA BVIC BWPT BYAN CANI CASS CEKA CENT CFIN CINT CITA CLPI CMNP CMPP CNKO COWL CPIN CPRO CSAP CTBN CTRA CTTH DART DEFI DEWA DGIK DILD DKFT DLTA DMAS DNAR DNET DOID DPNS DSFI DSNG DSSA DUTI DVLA DYAN ECII EKAD ELSA ELTY EMDE EMTK ENRG EPMT ERAA ERTX ESSA ESTI ETWA EXCL FAST FASW FISH FMII FORU FPNI GAMA GDST GDYR GEMA GEMS GGRM GIAA GJTL GLOB GMTD GOLD GOLL GPRA GSMF GTBO GWSA GZCO HADE HDFA HERO HEXA HITS HMSP HOME HOTL HRUM IATA IBFN IBST ICBP ICON IGAR IIKP IKAI IKBI IMAS IMJS IMPC INAF INAI INCI INCO INDF INDR INDS INDX INDY INKP INPC INPP INRU INTA INTD INTP IPOL ISAT ISSP ITMA ITMG JAWA JECC JIHD JKON JPFA JRPT JSMR JSPT JTPE KAEF KARW KBLI KBLM KBLV KBRI KDSI KIAS KICI KIJA KKGI KLBF KOBX KOIN KONI KOPI KPIG KRAS KREN LAPD LCGP LEAD LINK LION LMAS LMPI LMSH LPCK LPGI LPIN LPKR LPLI LPPF LPPS LRNA LSIP LTLS MAGP MAIN MAPI MAYA MBAP MBSS MBTO MCOR MDIA MDKA MDLN MDRN MEDC MEGA MERK META MFMI MGNA MICE MIDI MIKA MIRA MITI MKPI MLBI MLIA MLPL MLPT MMLP MNCN MPMX MPPA MRAT MREI MSKY MTDL MTFN MTLA MTSM MYOH MYOR MYTX NELY NIKL NIRO NISP NOBU NRCA OCAP OKAS OMRE PADI PALM PANR PANS PBRX PDES PEGE PGAS PGLI PICO PJAA PKPK PLAS PLIN PNBN PNBS PNIN PNLF PNSE POLY POOL PPRO PSAB PSDN PSKT PTBA PTIS PTPP PTRO PTSN PTSP PUDP PWON PYFA RAJA RALS RANC RBMS RDTX RELI RICY RIGS RIMO RODA ROTI RUIS SAFE SAME SCCO SCMA SCPI SDMU SDPC SDRA SGRO SHID SIDO SILO SIMA SIMP SIPD SKBM SKLT SKYB SMAR SMBR SMCB SMDM SMDR SMGR SMMA SMMT SMRA SMRU SMSM SOCI SONA SPMA SQMI SRAJ SRIL SRSN SRTG SSIA SSMS SSTM STAR STTP SUGI SULI SUPR TALF TARA TAXI TBIG TBLA TBMS TCID TELE TFCO TGKA TIFA TINS TIRA TIRT TKIM TLKM TMAS TMPO TOBA TOTL TOTO TOWR TPIA TPMA TRAM TRIL TRIM TRIO TRIS TRST TRUS TSPC ULTJ UNIC UNIT UNSP UNTR UNVR VICO VINS VIVA VOKS VRNA WAPO WEHA WICO WIIM WIKA WINS WOMF WSKT WTON YPAS YULE ZBRA SHIP CASA DAYA DPUM IDPR JGLE KINO MARI MKNT MTRA OASA POWR INCF WSBP PBSA PRDA BOGA BRIS PORT CARS MINA CLEO TAMU CSIS TGRA FIRE TOPS KMTR ARMY MAPB WOOD HRTA MABA HOKI MPOW MARK NASA MDKI BELL KIOS GMFI MTWI ZINC MCAS PPRE WEGE PSSI MORA DWGL PBID JMAS CAMP IPCM PCAR LCKM BOSS HELI JSKY INPS GHON TDPM DFAM NICK BTPS SPTO PRIM HEAL TRUK PZZA TUGU MSIN SWAT TNCA MAPA TCPI IPCC RISE BPTR POLL NFCX MGRO NUSA FILM ANDI LAND MOLI PANI DIGI CITY SAPX SURE HKMU MPRO DUCK GOOD SKRN YELO CAKK SATU SOSS DEAL POLA DIVA LUCK URBN SOTS ZONE PEHA FOOD BEEF POLI CLAY NATO JAYA COCO MTPS CPRI HRME POSA JAST FITT BOLA CCSI SFAN POLU KJEN KAYU ITIC PAMG IPTV BLUE ENVY EAST LIFE FUJI KOTA INOV ARKA SMKL HDIT KEEN BAPI TFAS GGRP OPMS NZIA SLIS PURE IRRA DMMX SINI WOWS ESIP TEBE KEJU PSGO AGAR IFSH REAL IFII PMJS UCID GLVA PGJO AMAR CSRA INDO AMOR TRIN DMND PURA PTPW TAMA IKAN AYLS DADA ASPI ESTA BESS AMAN CARE SAMF SBAT KBAG CBMF RONY CSMI BBSS BHAT EPAC UANG PGUN SOFA PPGL TOYS SGER TRJA PNGO SCNP BBSI KMDS PURI SOHO HOMI ROCK ENZO PLAN PTDU ATAP VICI PMMP WIFI FAPA DCII KETR DGNS UFOE BANK WMUU EDGE UNIQ BEBS SNLK ZYRX LFLO FIMP TAPG NPGF LUCY ADCP HOPE""".split()


# ---------------------------------------------------------------- data
def lan_ip():
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # tidak mengirim data, cuma cari IP lokal
        ip = s.getsockname()[0]; s.close(); return ip
    except Exception:
        return None


def http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def load_universe():
    url = ("https://www.idx.co.id/primary/StockData/GetSecuritiesStock"
           "?start=0&length=9999&code=&sector=&board=&language=id-id")
    try:
        j = http_json(url, timeout=8)
        codes = [d["Code"] for d in j.get("data", []) if d.get("Code")]
        if len(codes) > 800:
            return codes, "idx.co.id (terbaru)"
    except Exception:
        pass
    return EMITEN, "daftar bawaan (962 emiten)"


def wib_date(ts):
    return datetime.fromtimestamp(ts, WIB).strftime("%Y-%m-%d")


def fetch_chart(symbol, rng="6mo"):
    """Ambil OHLCV harian. Coba query1 lalu query2, dengan retry saat 429."""
    sym = urllib.request.quote(symbol)
    last = None
    for attempt in range(4):
        host = "query1" if attempt % 2 == 0 else "query2"
        url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym}?range={rng}&interval=1d"
        try:
            j = http_json(url)
            res = (j.get("chart") or {}).get("result")
            if not res:
                return None
            x = res[0]
            q = x["indicators"]["quote"][0]
            bars = []
            for i, t in enumerate(x.get("timestamp") or []):
                c, o, h, l = q["close"][i], q["open"][i], q["high"][i], q["low"][i]
                if c is None or o is None or h is None or l is None:
                    continue
                bars.append({"d": wib_date(t), "o": o, "h": h, "l": l, "c": c, "v": q["volume"][i] or 0})
            m = x.get("meta", {})
            return {"bars": bars, "name": m.get("longName") or m.get("shortName") or "",
                    "mt": m.get("regularMarketTime") or 0}
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 404:
                return None
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            last = e
            time.sleep(1 + attempt)
    raise RuntimeError(f"{symbol}: {last}")


def merge_bars(old, new):
    by = {b["d"]: b for b in old}
    for b in new:
        by[b["d"]] = b
    return [by[k] for k in sorted(by)][-130:]


# ---------------------------------------------------------------- analisis
def tick(p): return 1 if p < 200 else 2 if p < 500 else 5 if p < 2000 else 10 if p < 5000 else 25
def up(p): t = tick(p); return math.ceil(p / t - 1e-9) * t
def dn(p): t = tick(p); return math.floor(p / t + 1e-9) * t
def ara(p): return 0.35 if p <= 200 else 0.25 if p <= 5000 else 0.20
def avg(a): return sum(a) / len(a) if a else 0.0


def session_frac(mt, last_day):
    """Seberapa jauh sesi hari ini sudah berjalan (0-1)."""
    now = datetime.fromtimestamp(mt, WIB)
    if now.strftime("%Y-%m-%d") != last_day:
        return 1.0
    fri = now.weekday() == 4
    s1e = 11 * 60 + 30 if fri else 12 * 60
    s2s = 14 * 60 if fri else 13 * 60 + 30
    s2e = 15 * 60 + 50
    m = now.hour * 60 + now.minute
    tot = (s1e - 540) + (s2e - s2s)
    if m <= 540: el = 0
    elif m <= s1e: el = m - 540
    elif m <= s2s: el = s1e - 540
    else: el = (s1e - 540) + min(m, s2e) - s2s
    return max(0.05, min(1.0, el / tot))


def rsi(c, n=14):
    if len(c) <= n: return 50.0
    g = l = 0.0
    for i in range(1, n + 1):
        d = c[i] - c[i - 1]
        g += max(d, 0); l += max(-d, 0)
    g /= n; l /= n
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]
        g = (g * (n - 1) + max(d, 0)) / n
        l = (l * (n - 1) + max(-d, 0)) / n
    return 100.0 if l == 0 else 100 - 100 / (1 + g / l)


MIN_SC = 70   # batas kandidat BSJP (skor versi baru, hasil backtest 2025-2026)


def analyze(code, h, last_day, frac, ihsg_chg, ihsg_above=True):
    """Skor BSJP versi baru (hasil backtest) + skor lama sebagai pembanding + deteksi rebound."""
    b = h["bars"]
    if len(b) < 40:
        return None
    L, prev = b[-1], b[-2]
    live = L["d"] == last_day
    f = frac if live else 1.0
    C = [x["c"] for x in b]; V = [x["v"] for x in b]; n = len(b)
    c = L["c"]; chg = c / prev["c"] - 1 if prev["c"] else 0
    rng = L["h"] - L["l"]
    cp = (c - L["l"]) / rng if rng > 0 else (1.0 if chg > 0 else 0.5)
    vavg = avg(V[-21:-1]); vr = (L["v"] / f) / vavg if vavg > 0 else 0
    val = c * L["v"]; aval = avg([x["c"] * x["v"] for x in b[-21:-1]])
    ma5, ma20 = avg(C[-5:]), avg(C[-20:])
    R = rsi(C[-60:])
    hi20 = max(x["h"] for x in b[-21:-1])
    near_ara = chg >= ara(prev["c"]) - 0.02

    # --- historis versi baru: hari naik 1,5-8% + volume >= 1,5x -> pembukaan besoknya
    hs = []
    for j in range(21, n - 1):
        if not b[j - 1]["c"] or not b[j]["c"]: continue
        cj = b[j]["c"] / b[j - 1]["c"] - 1; vj = avg(V[j - 20:j])
        if 0.015 <= cj <= 0.08 and vj > 0 and b[j]["v"] / vj >= 1.5:
            hs.append(b[j + 1]["o"] / b[j]["c"] - 1)
    gu = (sum(1 for x in hs if x > 0) / len(hs)) if hs else 0
    bs = [len(hs), round(gu, 4), round(avg(hs), 4)] if hs else [0]

    # --- skor baru (maks 100)
    p_chg = 30 if 0.015 <= chg <= 0.08 else 12 if 0.005 <= chg < 0.015 else 10 if (chg > 0.08 and not near_ara) else 0
    p_vol = 30 if vr >= 2 else 20 if vr >= 1.5 else 8 if vr >= 1 else 0
    p_cp = 15 if 0.5 <= cp < 0.9 else 5 if (cp >= 0.9 or cp >= 0.3) else 0
    p_ih = 10 if ihsg_above else 0
    p_tr = 5 if c > ma20 else 0
    p_h = (10 if gu >= 0.6 else 5 if gu >= 0.5 else 0) if len(hs) >= 4 else 0
    sc = p_chg + p_vol + p_cp + p_ih + p_tr + p_h
    if near_ara: sc -= 15
    if ihsg_chg < -0.01: sc -= 5

    # --- skor lama (pembanding)
    o_cp = round(25 * min(1, max(0, (cp - 0.5) / 0.45)))
    o_chg = 20 if 0.015 <= chg <= 0.08 else 10 if 0.005 <= chg < 0.015 else 10 if (chg > 0.08 and not near_ara) else 4 if chg > 0 else 0
    o_vol = round(20 * min(1, max(0, (vr - 1) / 1.5)))
    o_tr = (5 if c > ma5 else 0) + (5 if ma5 > ma20 else 0) + (5 if c > ma20 else 0)
    o_r = 10 if 50 <= R <= 72 else 5 if 72 < R <= 80 else 4 if 45 <= R < 50 else 0
    sc_old = o_cp + o_chg + o_vol + o_tr + o_r + (5 if c >= hi20 else 0)
    if near_ara: sc_old -= 15
    if ihsg_chg < -0.01: sc_old -= 5

    liq = aval >= 5e9 and val >= 3e9 * f and live
    rb = R < 35 and c < ma20 * 0.95 and chg > 0.01 and cp >= 0.6
    return {"s": code, "n": (h.get("name") or "").replace("PT ", "").replace(" Tbk", "")[:40],
            "c": c, "o": L["o"], "h": L["h"], "l": L["l"], "chg": round(chg, 4), "cp": round(cp, 2),
            "vr": round(vr, 2), "val": round(val / 1e9, 1), "aval": round(aval / 1e9, 1),
            "ma5": round(ma5), "ma20": round(ma20), "rsi": round(R, 1), "brk": 1 if c >= hi20 else 0,
            "ara": 1 if near_ara else 0, "liq": 1 if liq else 0, "live": 1 if live else 0,
            "sc": max(0, round(sc)), "sco": max(0, round(sc_old)), "rb": 1 if (rb and liq) else 0,
            "p": [p_chg, p_vol, p_cp, p_ih, p_tr, p_h],
            "tp1": up(c * 1.01), "sl": dn(c * 0.97),
            "bs": bs, "sp": [round(x) for x in C[-30:]]}


# ---------------------------------------------------------------- state
S = {"hist": {}, "ihsg": None, "stocks": [], "asof": 0, "frac": 1.0, "day": "",
     "busy": False, "mode": "", "done": 0, "total": 0, "fails": 0,
     "last_full": 0, "last_update": 0, "msg": "", "universe": [], "src": "", "univ": "lq45"}
LOCK = threading.Lock()
LQ45_SET = set(LQ45)


def recompute():
    ih = S["hist"].get("^JKSE")
    if not ih or len(ih["bars"]) < 21:
        return
    jb = ih["bars"]; jc = [x["c"] for x in jb]
    last_day = jb[-1]["d"]
    mt = max((h.get("mt") or 0) for h in S["hist"].values())
    frac = session_frac(mt, last_day)
    ihsg = {"c": round(jc[-1], 2), "chg": round(jc[-1] / jc[-2] - 1, 4),
            "ma5": round(avg(jc[-5:])), "ma20": round(avg(jc[-20:])), "sp": [round(x) for x in jc[-30:]]}
    out = []
    for code, h in S["hist"].items():
        if code == "^JKSE": continue
        if S["univ"] == "lq45" and code not in LQ45_SET: continue
        try:
            r = analyze(code, h, last_day, frac, ihsg["chg"], ihsg["c"] >= ihsg["ma20"])
            if r: out.append(r)
        except Exception:
            pass
    out.sort(key=lambda r: (-r["liq"], -r["sc"]))
    with LOCK:
        S.update(stocks=out, ihsg=ihsg, asof=mt, frac=round(frac, 3), day=last_day, last_update=time.time())


def run_scan(full):
    with LOCK:
        if S["busy"]: return
        S["busy"] = True
    try:
        if S["univ"] == "lq45":
            codes = LQ45
            rng = "6mo" if full or any(c not in S["hist"] for c in LQ45) else "5d"
            full = rng == "6mo"
        elif full:
            if not S["universe"]:
                S["universe"], S["src"] = load_universe()
            codes = S["universe"]; rng = "6mo"
        else:
            codes = [c for c, h in S["hist"].items() if c != "^JKSE" and
                     avg([x["c"] * x["v"] for x in h["bars"][-21:-1]]) >= PRE_LIQ]
            rng = "5d"
        syms = ["^JKSE"] + [c + ".JK" for c in codes]
        S.update(mode="full" if full else "quick", done=0, total=len(syms), fails=0, msg="")
        with ThreadPoolExecutor(WORKERS) as ex:
            futs = {ex.submit(fetch_chart, s, "6mo" if s == "^JKSE" else rng): s for s in syms}
            for fu in as_completed(futs):
                s = futs[fu]; key = s.replace(".JK", "")
                try:
                    d = fu.result()
                    if d and d["bars"]:
                        old = S["hist"].get(key)
                        if old and not full:
                            d["bars"] = merge_bars(old["bars"], d["bars"])
                            d["name"] = d["name"] or old.get("name", "")
                        S["hist"][key] = d
                except Exception:
                    S["fails"] += 1
                S["done"] += 1
        recompute()
        if full: S["last_full"] = time.time()
        if S["fails"] > len(syms) * 0.3:
            S["msg"] = f"{S['fails']} saham gagal diambil (Yahoo mungkin membatasi). Coba refresh lagi 1-2 menit lagi."
    except Exception as e:
        S["msg"] = f"Gagal memindai: {e}"
    finally:
        S["busy"] = False


def maybe_refresh(force_full=False):
    if S["busy"]: return
    now = time.time()
    full = force_full or not S["hist"] or (S["univ"] == "all" and now - S["last_full"] > FULL_EVERY)
    if full or now - S["last_update"] > QUICK_EVERY:
        threading.Thread(target=run_scan, args=(full,), daemon=True).start()


# ---------------------------------------------------------------- server
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send(self, body, ctype, code=200):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        q = self.path.split("?")[1] if "?" in self.path else ""
        if path == "/":
            maybe_refresh()
            return self.send(PAGE, "text/html")
        if path == "/api/data":
            if "univ=all" in q and S["univ"] != "all":
                S["univ"] = "all"; S["last_update"] = 0; maybe_refresh(force_full=True)
            elif "univ=lq45" in q and S["univ"] != "lq45":
                S["univ"] = "lq45"; S["last_update"] = 0; recompute(); maybe_refresh()
            if "refresh=1" in q: maybe_refresh()
            if "full=1" in q: maybe_refresh(force_full=True)
            with LOCK:
                body = {k: S[k] for k in ("ihsg", "stocks", "asof", "frac", "day", "busy", "mode", "done",
                                          "total", "fails", "last_full", "last_update", "msg", "src", "univ")}
                ip = lan_ip(); body["lan"] = f"http://{ip}:{PORT}" if ip and HOST != "127.0.0.1" else ""
                body["universe"] = 45 if S["univ"] == "lq45" else len(S["universe"])
                if S["univ"] == "lq45": body["src"] = "LQ45 periode Nov 2026 - Jan 2027"
            return self.send(json.dumps(body, separators=(",", ":")), "application/json")
        self.send("Tidak ada", "text/plain", 404)


PAGE = r"""<!DOCTYPE html>
<html lang="id"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>BSJP Radar — Beli Sore Jual Pagi</title>
<link href="https://fonts.googleapis.com/css2?family=Archivo:wdth,wght@62..125,400..800&family=Figtree:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{--dusk:#1B2036;--dusk2:#232945;--dusk3:#2E3557;--line:#3A4268;--ink:#E9ECF5;--mute:#8F96B3;--sore:#F5B83D;--pagi:#8FD3E8;--up:#4FD18B;--down:#FF6B6B}
*{box-sizing:border-box}html{background:var(--dusk);color:var(--ink)}
body{margin:0;font:15px/1.55 Figtree,system-ui,sans-serif;-webkit-font-smoothing:antialiased}
.wrap{max-width:1120px;margin:0 auto;padding:28px 20px 64px}
h1,h2,h3{font-family:Archivo,sans-serif;margin:0;font-weight:700}
h1{font-size:clamp(30px,5vw,46px);font-stretch:112%;letter-spacing:-.01em;line-height:1.05}
h2{font-size:22px;font-stretch:105%;margin:44px 0 6px}
.sub{color:var(--mute);margin:8px 0 0;max-width:62ch}
.num{font-family:Archivo,sans-serif;font-variant-numeric:tabular-nums}
.up{color:var(--up)}.down{color:var(--down)}
:focus-visible{outline:2px solid var(--pagi);outline-offset:2px}
.top{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap}
button{font:600 14px Figtree,system-ui,sans-serif;border:0;border-radius:8px;padding:9px 14px;cursor:pointer}
.btn{background:var(--sore);color:#2a1d00}.btn2{background:var(--dusk3);color:var(--ink)}
button:disabled{opacity:.5;cursor:default}
.clock{margin-top:22px;background:linear-gradient(90deg,#3B2F45 0%,#2A2D4F 30%,#151931 55%,#151931 70%,#2B4057 100%);border:1px solid var(--line);border-radius:14px;padding:18px 18px 14px}
.track{position:relative;height:44px;margin-top:6px}
.seg{position:absolute;top:10px;height:24px;border-radius:6px;font:600 12px Figtree,system-ui,sans-serif;display:flex;align-items:center;padding:0 8px;white-space:nowrap;overflow:hidden}
.seg.buy{background:var(--sore);color:#2a1d00}.seg.sell{background:var(--pagi);color:#062633}
.seg.night{border:1px dashed #4a5280;color:var(--mute)}
.now{position:absolute;top:0;bottom:0;width:2px;background:var(--ink)}
.now span{position:absolute;top:-20px;left:50%;transform:translateX(-50%);font:600 11px Figtree,system-ui,sans-serif;background:var(--ink);color:var(--dusk);padding:1px 6px;border-radius:4px;white-space:nowrap}
.ticks{display:flex;justify-content:space-between;color:var(--mute);font-size:12px}
.status{display:flex;flex-wrap:wrap;gap:10px 28px;margin-top:14px;font-size:14px}
.status b{font-family:Archivo,sans-serif;font-weight:600}
.prog{margin-top:12px;height:6px;background:#141830;border-radius:3px;overflow:hidden;display:none}
.prog i{display:block;height:100%;background:var(--pagi);width:0;transition:width .4s}
.msg{margin-top:10px;color:var(--down);font-size:14px}
.market{margin-top:16px;display:grid;grid-template-columns:auto 1fr;gap:16px;align-items:center;background:var(--dusk2);border-radius:12px;padding:14px 18px;border-left:4px solid var(--down)}
.market.ok{border-left-color:var(--up)}.market p{margin:0;color:#C9CEE0}
.cands{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:14px;margin-top:16px}
.cand{background:var(--dusk2);border:1px solid var(--line);border-radius:12px;padding:16px 16px 14px;display:flex;flex-direction:column;gap:12px}
.cand.lead{grid-column:1/-1;background:linear-gradient(180deg,#2B2C48,#232945);border-color:#5b5480}
.chead{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.tk{font-family:Archivo,sans-serif;font-size:26px;font-weight:800;font-stretch:118%;line-height:1}
.cand.lead .tk{font-size:36px}.nm{color:var(--mute);font-size:13px;margin-top:4px}
.score{text-align:right}.score .num{font-size:30px;font-weight:700;line-height:1;color:var(--sore)}
.score small{display:block;color:var(--mute);font-size:12px}
.delta{font-size:12px;margin-left:4px}
.px{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}.px .num{font-size:22px;font-weight:600}
.plan{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--line);border-radius:8px;overflow:hidden}
.plan div{background:var(--dusk3);padding:8px 10px}.plan small{display:block;color:var(--mute);font-size:11.5px}
.plan .num{font-size:16px;font-weight:600}.plan .e .num{color:var(--sore)}.plan .t .num{color:var(--pagi)}.plan .s .num{color:var(--down)}
.facts{display:grid;grid-template-columns:repeat(3,1fr);gap:8px 12px;font-size:13px}
.facts small{display:block;color:var(--mute);font-size:11.5px}
.bar{height:6px;background:#1a1f35;border-radius:3px;margin-top:4px;position:relative}
.bar i{position:absolute;left:0;top:0;bottom:0;border-radius:3px;background:var(--sore)}
.hist{font-size:13px;color:#C9CEE0;background:#1f2440;border-radius:8px;padding:8px 10px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{font-size:12px;padding:2px 8px;border-radius:20px;background:#1f2440;color:#C9CEE0}
.chip.on{background:rgba(79,209,139,.15);color:var(--up)}.chip.warn{background:rgba(255,107,107,.14);color:var(--down)}
.lot{font-size:13px;color:var(--mute)}.lot b{color:var(--ink);font-family:Archivo,sans-serif}
.cand.lead .inner{display:grid;grid-template-columns:1.1fr 1fr;gap:18px}
.col{display:flex;flex-direction:column;gap:12px}
@media(max-width:760px){.cand.lead .inner{grid-template-columns:1fr}.plan{grid-template-columns:repeat(2,1fr)}}
.ctrl{display:flex;flex-wrap:wrap;gap:14px;align-items:center;margin-top:14px;background:var(--dusk2);border-radius:10px;padding:10px 14px;font-size:14px}
.ctrl input[type=number],.ctrl input[type=text]{width:150px;background:var(--dusk);color:var(--ink);border:1px solid var(--line);border-radius:6px;padding:6px 8px;font:inherit}
.ctrl label{display:flex;align-items:center;gap:8px;color:#C9CEE0}
.tablewrap{overflow-x:auto;margin-top:12px;border:1px solid var(--line);border-radius:10px;max-height:640px}
table{border-collapse:collapse;width:100%;font-size:13.5px;min-width:820px}
th,td{padding:8px 10px;text-align:right;white-space:nowrap}
th{position:sticky;top:0;background:var(--dusk3);color:#C9CEE0;font-weight:600;cursor:pointer;user-select:none}
th:first-child,td:first-child{text-align:left}
tbody tr{border-top:1px solid #2b3150}tbody tr:hover{background:#252b49}
td .tkc{font-family:Archivo,sans-serif;font-weight:700}td .muted{color:var(--mute);font-size:12px}
.pill{display:inline-block;min-width:34px;text-align:center;border-radius:5px;padding:1px 6px;font-weight:600;font-family:Archivo,sans-serif}
.rules{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px;margin-top:14px}
.rules div{background:var(--dusk2);border-radius:10px;padding:14px 16px}
.rules h3{font-size:15px;margin-bottom:6px}.rules ul{margin:0;padding-left:18px;color:#C9CEE0;font-size:14px}.rules li{margin:3px 0}
.foot{margin-top:36px;color:var(--mute);font-size:13px;max-width:78ch}
.empty{padding:18px;background:var(--dusk2);border-radius:10px;color:#C9CEE0}
@media(max-width:520px){.wrap{padding:20px 14px 48px}.seg{font-size:10px;padding:0 4px}.ticks{font-size:10px}.facts{grid-template-columns:repeat(2,1fr)}.cands{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<div class="top"><div><h1>BSJP Radar</h1>
<p class="sub">Saham IDX yang ditutup kuat sore ini dan secara historis sering lanjut naik besok pagi. Beli menjelang penutupan, jual di 30 menit pertama besok.</p></div>
<div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn" id="bRefresh">Perbarui data</button><label class="btn2" style="display:flex;align-items:center;gap:8px;border-radius:8px;padding:5px 10px;font-weight:600;font-size:14px">Daftar <select id="univ" style="background:var(--dusk);color:var(--ink);border:1px solid var(--line);border-radius:6px;padding:4px 6px;font:inherit"><option value="lq45">LQ45 (cepat)</option><option value="all">Semua emiten (1–2 menit)</option></select></label></div></div>

<section class="clock" aria-label="Jendela waktu BSJP">
 <div class="track" id="track"></div>
 <div class="ticks"><span>13:30</span><span>14:50 beli</span><span>15:50 tutup</span><span>malam</span><span>09:00 buka</span><span>09:30</span></div>
 <div class="status" id="status">Memuat…</div>
 <div class="prog" id="prog"><i></i></div>
 <div class="msg" id="msg"></div>
</section>
<div class="market" id="market" hidden></div>

<h2>Kandidat beli sore ini</h2>
<p class="sub">Saham likuid dengan skor 70 ke atas (rumus baru hasil backtest 2025–2026). Plan exit: jual di pre-opening / 15 menit pertama besok, jangan tunggu TP. Angka kecil di samping skor = perubahan sejak refresh sebelumnya.</p>
<div class="ctrl">
 <label for="modal">Modal per saham (Rp)</label><input type="number" id="modal" value="10000000" step="1000000" min="0">
 <label><input type="checkbox" id="auto" checked> Perbarui otomatis tiap 1 menit</label>
</div>
<div class="cands" id="cands"></div>

<h2>Kandidat rebound <span style="font-size:14px;color:var(--mute);font-weight:500">(percobaan)</span></h2>
<p class="sub">Saham yang sudah jenuh jual (RSI &lt; 35, lebih dari 5% di bawah MA20) dan hari ini mulai memantul. Backtest: 61% besoknya buka naik, rata-rata +0,22% bersih, tapi sampelnya kecil (187 kejadian). Risiko lebih tinggi, pakai lot kecil.</p>
<div class="cands" id="rebound"></div>

<h2>Semua saham yang dipindai</h2>
<p class="sub">Klik judul kolom untuk mengurutkan. Saham tidak likuid disembunyikan karena susah keluar besok pagi.</p>
<div class="ctrl"><label><input type="checkbox" id="showIlliq"> Tampilkan saham tidak likuid</label>
<input type="text" id="q" placeholder="Cari kode…" aria-label="Cari kode saham"></div>
<div class="tablewrap"><table id="tbl"><thead></thead><tbody></tbody></table></div>

<h2>Cara pakai sinyalnya</h2>
<div class="rules">
<div><h3 style="color:var(--sore)">Sore — beli</h3><ul><li>Cek ulang jam 15:00–15:45. Utamakan saham naik 1,5–8% dengan volume tinggi.</li><li>Hindari yang ditutup persis di pucuk: historisnya justru sering dibanting besok.</li><li>Jangan kejar: kalau harga sudah lewat Entry lebih dari 1%, lewati.</li></ul></div>
<div><h3 style="color:var(--pagi)">Pagi — jual</h3><ul><li>Baca laporan pagi 08:30 di Telegram. Kalau "waspada gap down", jual di pre-opening.</li><li>Pasang antrean jual pre-opening di +1% dari Entry.</li><li>Belum terjual sampai 09:15? Jual di harga pasar. Backtest: exit di pembukaan lebih baik daripada menunggu TP.</li></ul></div>
<div><h3 style="color:var(--down)">Batas rugi</h3><ul><li>Buka di bawah batas "Cut" (−3%)? Jual di menit pertama.</li><li>Risiko per transaksi idealnya ≤ 1–2% dari total modal.</li><li>Keunggulan rumus ini tipis (+0,1–0,3%/transaksi). Disiplin lebih penting daripada jumlah transaksi.</li></ul></div>
</div>
<h2>Cara skor dihitung (versi baru)</h2>
<div class="rules">
<div><h3>Kenaikan hari ini (30)</h3><ul><li>Penuh kalau naik +1,5% s/d +8%. Sinyal paling konsisten di backtest.</li></ul></div>
<div><h3>Volume (30)</h3><ul><li>Volume (diproyeksikan full day) ≥ 2× rata-rata 20 hari = penuh, ≥ 1,5× = 20.</li></ul></div>
<div><h3>Posisi close (15)</h3><ul><li>Penuh kalau close di 50–90% range. Close di pucuk cuma 5 poin karena historisnya buruk.</li></ul></div>
<div><h3>IHSG (10) + tren (5) + historis (10)</h3><ul><li>IHSG di atas MA20, harga di atas MA20, dan seberapa sering pola serupa di saham ini besoknya buka naik.</li></ul></div>
<div><h3>Hasil backtest</h3><ul><li>Feb 2025 – Nov 2026, 45 saham LQ45: skor ≥ 70 buka naik 61%, rata-rata +0,11% bersih per transaksi. Skor lama impas (−0,05%).</li></ul></div>
</div>
<p class="foot" id="foot"></p>
</div>
<script>
let D=null, prevSc={}, lastSc={}, lastAsof=0;
const fmt=(v,d=0)=>(+v).toLocaleString('id-ID',{minimumFractionDigits:d,maximumFractionDigits:d});
const pct=(v,d=1)=>(v>0?'+':'')+fmt(v*100,d)+'%';
const cls=v=>v>0?'up':v<0?'down':'';
const wib=ts=>{const d=new Date((ts+7*3600)*1000);return String(d.getUTCHours()).padStart(2,'0')+':'+String(d.getUTCMinutes()).padStart(2,'0')};
const tgl=s=>s?new Date(s+'T00:00:00Z').toLocaleDateString('id-ID',{weekday:'long',day:'numeric',month:'long',year:'numeric',timeZone:'UTC'}):'';
const MIN_SC=70, PARTS=['Kenaikan','Volume','Posisi close','IHSG>MA20','Tren','Historis'], MAXP=[30,30,15,10,5,10];
const $=id=>document.getElementById(id);

function spark(a,w=140,h=34){if(!a||a.length<2)return'';const mn=Math.min(...a),mx=Math.max(...a),r=mx-mn||1;
 const P=a.map((v,i)=>[(i/(a.length-1)*w),(h-2-(v-mn)/r*(h-4))]);const col=a.at(-1)>=a[0]?'var(--up)':'var(--down)';
 return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="Grafik 30 hari"><polyline fill="none" stroke="${col}" stroke-width="1.6" points="${P.map(p=>p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ')}"/><circle cx="${w}" cy="${P.at(-1)[1].toFixed(1)}" r="2.5" fill="${col}"/></svg>`}

function renderClock(){
 const t=$('track');t.innerHTML=`<div class="seg buy" style="left:36%;width:26%">Beli 14:50–15:49</div><div class="seg night" style="left:62.5%;width:21%">tahan semalam</div><div class="seg sell" style="left:84%;width:16%">Jual 09:00–09:30</div>`;
 if(!D||!D.asof)return;const d=new Date((D.asof+7*3600)*1000);const m=d.getUTCHours()*60+d.getUTCMinutes();
 if(m>=810&&m<=950){const n=document.createElement('div');n.className='now';n.style.left=((m-810)/140*62)+'%';n.innerHTML=`<span>data ${wib(D.asof)}</span>`;t.appendChild(n)}
}
function renderStatus(){
 const liq=D.stocks.filter(x=>x.liq).length;
 $('status').innerHTML=D.asof?`<span>Data per <b>${tgl(D.day)}, ${wib(D.asof)} WIB</b></span><span><b>${D.stocks.length}</b> dari ${D.universe} emiten terbaca, <b>${liq}</b> likuid</span><span>Sesi berjalan <b>${fmt(D.frac*100)}%</b></span>${D.busy?`<span>Memperbarui${D.univ==='lq45'?' LQ45':D.mode==='full'?' semua emiten':' saham likuid'}… <b>${D.done}/${D.total}</b></span>`:''}`
  :`<span>Memindai ${D.univ==='lq45'?'saham LQ45':'semua emiten'}… <b>${D.done}/${D.total}</b></span>`;
 const p=$('prog');p.style.display=D.busy?'block':'none';p.firstChild.style.width=(D.total?D.done/D.total*100:0)+'%';
 $('msg').textContent=D.msg||'';$('bRefresh').disabled=$('univ').disabled=D.busy;$('univ').value=D.univ;
}
function renderMarket(){const i=D.ihsg;if(!i)return;const el=$('market');el.hidden=false;const weak=i.c<i.ma20;el.classList.toggle('ok',!weak);
 el.innerHTML=`<div><div class="num" style="font-size:24px;font-weight:700">${fmt(i.c,2)}</div><div class="${cls(i.chg)}" style="font-size:13px">IHSG ${pct(i.chg,2)}</div></div><div>${spark(i.sp,220,40)}<p>${weak?`IHSG di bawah MA20 (${fmt(i.ma20)}). Tren belum pulih, pakai lot kecil dan disiplin di SL.`:`IHSG di atas MA20 (${fmt(i.ma20)}). Kondisi pasar mendukung, tetap disiplin di SL.`}</p></div>`}

function card(x,lead){
 const modal=+$('modal').value||0, lot=Math.floor(modal/(x.c*100)), risk=lot*100*(x.c-x.sl), gain=lot*100*(x.tp1-x.c), rr=(x.tp1-x.c)/Math.max(1,x.c-x.sl);
 const dlt=prevSc[x.s]!==undefined?x.sc-prevSc[x.s]:0;
 const chips=x.p.map((v,i)=>`<span class="chip ${v>=MAXP[i]*0.6?'on':''}">${PARTS[i]} ${v}/${MAXP[i]}</span>`).join('');
 const warn=[];if(x.c<100)warn.push('Harga < Rp100: 1 tick = '+pct(1/x.c,1));if(x.vr<0.6)warn.push('Volume masih sepi');if(x.bs[0]<4)warn.push('Data historis sedikit');if(x.cp>=.9)warn.push('Close di pucuk: historis kurang bagus');
 const hist=x.bs[0]>=1?`Dari <b>${x.bs[0]}</b> hari serupa di saham ini (naik 1,5–8% + volume ≥ 1,5×) dalam 6 bulan, <b>${fmt(x.bs[1]*100)}%</b> besoknya buka naik, rata-rata gap ${pct(x.bs[2],2)}.`:'Belum ada pola serupa di saham ini dalam 6 bulan terakhir.';
 const top=`<div class="chead"><div><div class="tk">${x.s}</div><div class="nm">${x.n}</div></div><div class="score"><span class="num">${x.sc}</span>${dlt?`<span class="delta ${cls(dlt)}">${dlt>0?'+':''}${dlt}</span>`:''}<small>skor BSJP · lama ${x.sco}</small></div></div>
 <div class="px"><span class="num">${fmt(x.c)}</span><span class="${cls(x.chg)}">${pct(x.chg)}</span>${spark(x.sp)}</div>
 <div class="plan"><div class="e"><small>Entry</small><span class="num">${fmt(x.c)}</span></div><div class="t"><small>Antre jual pre-open</small><span class="num">${fmt(x.tp1)}</span></div><div class="t"><small>Sisa: jual s/d</small><span class="num">09:15</span></div><div class="s"><small>Cut kalau buka ≤</small><span class="num">${fmt(x.sl)}</span></div></div>`;
 const bottom=`<div class="facts"><div><small>Posisi close</small>${fmt(x.cp*100)}% dari range<div class="bar"><i style="width:${x.cp*100}%"></i></div></div>
 <div><small>Volume vs rata-rata</small>${fmt(x.vr,2)}×<div class="bar"><i style="width:${Math.min(100,x.vr/2.5*100)}%"></i></div></div>
 <div><small>RSI 14</small>${fmt(x.rsi,1)}<div class="bar"><i style="width:${x.rsi}%"></i></div></div>
 <div><small>Transaksi hari ini</small>Rp${fmt(x.val,1)} M</div><div><small>MA5 / MA20</small>${fmt(x.ma5)} / ${fmt(x.ma20)}</div><div><small>Range hari ini</small>${fmt(x.l)}–${fmt(x.h)}</div></div>
 <div class="hist">${hist}</div>
 <div class="chips">${chips}${x.brk?'<span class="chip on">Tembus high 20 hari</span>':''}${warn.map(w=>`<span class="chip warn">${w}</span>`).join('')}</div>
 <div class="lot">${lot>0?`Modal ini dapat <b>${fmt(lot)} lot</b>. Kalau terjual di antrean pre-open: <b class="up">+Rp${fmt(gain)}</b>. Kalau kena cut: <b class="down">−Rp${fmt(risk)}</b>. Belum termasuk biaya ±0,4%.`:'Modal belum cukup untuk 1 lot.'}</div>`;
 return lead?`<article class="cand lead"><div class="inner"><div class="col">${top}</div><div class="col">${bottom}</div></div></article>`:`<article class="cand">${top}${bottom}</article>`}

function renderRebound(){if(!D||!D.asof){$('rebound').innerHTML='';return}
 const list=D.stocks.filter(x=>x.rb).sort((a,b)=>a.rsi-b.rsi);
 $('rebound').innerHTML=list.length?list.map(x=>`<article class="cand"><div class="chead"><div><div class="tk">${x.s}</div><div class="nm">${x.n}</div></div><div class="score"><span class="num" style="color:var(--pagi)">RSI ${fmt(x.rsi,0)}</span><small>${pct(x.c/x.ma20-1)} dari MA20</small></div></div><div class="px"><span class="num">${fmt(x.c)}</span><span class="${cls(x.chg)}">${pct(x.chg)}</span>${spark(x.sp)}</div><div class="plan"><div class="e"><small>Entry</small><span class="num">${fmt(x.c)}</span></div><div class="t"><small>Antre jual pre-open</small><span class="num">${fmt(x.tp1)}</span></div><div class="t"><small>Sisa: jual s/d</small><span class="num">09:15</span></div><div class="s"><small>Cut kalau buka ≤</small><span class="num">${fmt(x.sl)}</span></div></div></article>`).join(''):'<div class="empty">Belum ada saham oversold yang mulai memantul hari ini.</div>'}
function renderCands(){if(!D||!D.asof){$('cands').innerHTML='<div class="empty">Sedang memindai. Kandidat muncul begitu selesai.</div>';return}
 const list=D.stocks.filter(x=>x.liq&&x.sc>=MIN_SC).sort((a,b)=>b.sc-a.sc);
 $('cands').innerHTML=list.length?list.map((x,i)=>card(x,i===0)).join(''):'<div class="empty">Belum ada saham likuid yang lolos skor 70. Lebih baik tidak masuk daripada memaksa.</div>'}

const COLS=[['s','Saham'],['sc','Skor'],['sco','Skor lama'],['c','Harga'],['chg','Hari ini'],['cp','Close di range'],['vr','Vol ×'],['val','Transaksi (Rp M)'],['rsi','RSI'],['win','Historis buka naik'],['sl','Cut ≤']];
let sortK='sc',sortD=-1;
function renderTable(){if(!D)return;const show=$('showIlliq').checked,q=$('q').value.trim().toUpperCase(),t=$('tbl');
 t.querySelector('thead').innerHTML='<tr>'+COLS.map(([k,l])=>`<th data-k="${k}" scope="col" tabindex="0" aria-sort="${k===sortK?(sortD<0?'descending':'ascending'):'none'}">${l}${k===sortK?(sortD<0?' ▾':' ▴'):''}</th>`).join('')+'</tr>';
 const val=(x,k)=>k==='win'?(x.bs[0]?x.bs[1]:-1):x[k];
 const rows=D.stocks.filter(x=>(show||x.liq)&&(!q||x.s.includes(q))).sort((a,b)=>{const A=val(a,sortK),B=val(b,sortK);return(typeof A==='string'?A.localeCompare(B):A-B)*sortD}).slice(0,400);
 t.querySelector('tbody').innerHTML=rows.map(x=>{const sc=x.sc,bg=sc>=MIN_SC?'rgba(245,184,61,.2)':sc>=40?'rgba(143,211,232,.12)':'transparent',col=sc>=MIN_SC?'var(--sore)':sc>=40?'var(--pagi)':'var(--mute)';
  return `<tr><td><span class="tkc">${x.s}</span>${x.liq?'':' <span class="chip warn">'+(x.live?'tidak likuid':'tidak aktif hari ini')+'</span>'}<br><span class="muted">${x.n}</span></td><td><span class="pill" style="background:${bg};color:${col}">${sc}</span>${x.rb?' <span class="chip">rebound</span>':''}</td><td class="muted">${x.sco}</td><td class="num">${fmt(x.c)}</td><td class="${cls(x.chg)}">${pct(x.chg)}</td><td>${fmt(x.cp*100)}%</td><td>${fmt(x.vr,2)}</td><td>${fmt(x.val,1)}</td><td>${fmt(x.rsi,1)}</td><td>${x.bs[0]?fmt(x.bs[1]*100)+'% <span class="muted">('+x.bs[0]+'×)</span>':'–'}</td><td class="num">${fmt(x.sl)}</td></tr>`}).join('');
 t.querySelectorAll('th').forEach(th=>{const go=()=>{const k=th.dataset.k;if(k===sortK)sortD*=-1;else{sortK=k;sortD=k==='s'?1:-1}renderTable()};th.onclick=go;th.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();go()}}})}

function renderAll(){renderClock();renderStatus();renderMarket();renderCands();renderRebound();renderTable();
 $('foot').innerHTML=(D.static?`Halaman ini diperbarui otomatis oleh GitHub tiap ±10 menit di jam bursa (terakhir dibuat ${wib(D.built)} WIB). `:'')+`Sumber data: Yahoo Finance (bisa tertunda ±10–15 menit)${D.asof?', data terakhir '+wib(D.asof)+' WIB':''}. Daftar emiten: ${D.src||'-'}.${D.lan?` Buka dari HP (WiFi yang sama): <b>${D.lan}</b>.`:''} Sebelum 15:50 close, volume, dan skor masih bisa berubah. Statistik historis dari 6 bulan data harian; "sempat naik ≥1%" memakai high hari berikutnya, jadi belum tentu terjadi di jam pagi. Ini alat bantu analisis teknikal, bukan rekomendasi investasi.`}

let timer=null;
async function load(param=''){
 try{const r=await fetch('/api/data'+param,{cache:'no-store'});const j=await r.json();
  if(j.asof&&j.asof!==lastAsof){if(lastAsof)prevSc=lastSc;lastSc=Object.fromEntries(j.stocks.map(x=>[x.s,x.sc]));lastAsof=j.asof}
  D=j;renderAll();clearTimeout(timer);timer=setTimeout(()=>load(D.busy?'':($('auto').checked?'?refresh=1':'')),D.busy?2000:60000);
 }catch(e){$('msg').textContent='Tidak tersambung ke script. Pastikan jendela hitam BSJP masih terbuka.';clearTimeout(timer);timer=setTimeout(()=>load(),5000)}}
$('bRefresh').onclick=()=>load('?refresh=1');$('univ').onchange=e=>load('?univ='+e.target.value);
$('modal').addEventListener('input',renderCands);$('showIlliq').addEventListener('change',renderTable);$('q').addEventListener('input',renderTable);
load('?refresh=1');
</script></body></html>"""


def build_static(out):
    """Mode GitHub: pindai LQ45 sekali, tulis index.html + data.json ke folder out."""
    import os
    os.makedirs(out, exist_ok=True)
    run_scan(True)
    if not S["stocks"]:
        print("Gagal: tidak ada data dari Yahoo.", S["msg"]); sys.exit(1)
    body = {k: S[k] for k in ("ihsg", "stocks", "asof", "frac", "day", "fails", "msg", "univ")}
    body.update(busy=False, done=0, total=0, universe=len(LQ45), src="LQ45 periode Nov 2026 - Jan 2027",
                lan="", static=True, built=int(time.time()))
    page = (PAGE.replace("fetch('/api/data'+param,", "fetch('data.json?t='+Date.now(),")
                .replace('<label class="btn2" style="display:flex', '<label class="btn2" style="display:none')
                .replace("$('univ').value=D.univ;", ""))
    with open(os.path.join(out, "index.html"), "w", encoding="utf-8") as f: f.write(page)
    with open(os.path.join(out, "data.json"), "w", encoding="utf-8") as f: json.dump(body, f, separators=(",", ":"))
    print(f"OK: {len(S['stocks'])} saham, data {datetime.fromtimestamp(S['asof'], WIB):%Y-%m-%d %H:%M} WIB, {S['fails']} gagal")


def _rp(v):
    return f"{v:,.0f}".replace(",", ".")


def _pct(v, d=1):
    return (f"{v*100:+.{d}f}%").replace(".", ",")


def _num(v, d=2):
    return f"{v:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _footer(L):
    url = os.environ.get("PAGES_URL", "")
    if url:
        L.append("")
        L.append(f'<a href="{url}">Buka BSJP Radar</a> · ketik kode saham (mis. BBCA) untuk analisis')
    L.append("<i>Alat bantu analisis, bukan saran investasi.</i>")
    return "\n".join(L)


def build_report():
    """Laporan per jam / 15:30 / penutupan (HTML Telegram)."""
    now = datetime.now(WIB)
    st = S["stocks"]; ih = S["ihsg"]
    detail = now.hour == 15 and 10 <= now.minute < 50
    closed = now.hour > 15 or (now.hour == 15 and now.minute >= 50)
    liq = [x for x in st if x["liq"]]
    cand = sorted([x for x in liq if x["sc"] >= MIN_SC], key=lambda x: -x["sc"])
    reb = sorted([x for x in st if x.get("rb")], key=lambda x: x["rsi"])
    data_t = datetime.fromtimestamp(S["asof"], WIB).strftime("%H:%M") if S["asof"] else "-"
    L = ["⏰ <b>Waktunya siap-siap beli (BSJP)</b>" if detail else "🔔 <b>Rekap penutupan</b>" if closed else "📊 <b>Update pasar</b>",
         f"{now:%H:%M} WIB · data Yahoo per {data_t} WIB", ""]
    trend = "di atas MA20 ✅" if ih["c"] >= ih["ma20"] else "di bawah MA20 ⚠️"
    L.append(f"<b>IHSG</b> {_num(ih['c'])} ({_pct(ih['chg'], 2)}) · {trend}")
    L.append("")
    if cand:
        L.append(f"<b>Kandidat BSJP</b> (skor ≥ {MIN_SC}): {len(cand)}")
        for x in cand[:5]:
            if detail or closed:
                L.append(f"• <b>{x['s']}</b> {x['sc']} · {_rp(x['c'])} ({_pct(x['chg'])}) · vol {_num(x['vr'], 1)}×\n"
                         f"   Antre jual pre-open {_rp(x['tp1'])} · cut kalau buka ≤ {_rp(x['sl'])}")
            else:
                L.append(f"• <b>{x['s']}</b> skor {x['sc']} · {_rp(x['c'])} ({_pct(x['chg'])}) · vol {_num(x['vr'], 1)}×")
    else:
        L.append(f"<b>Kandidat BSJP:</b> belum ada yang skor ≥ {MIN_SC}.")
        near = sorted(liq, key=lambda x: -x["sc"])[:3]
        if near:
            L.append("Paling dekat: " + ", ".join(f"{x['s']} {x['sc']}" for x in near))
    if reb:
        L.append("")
        L.append("🔄 <b>Rebound (percobaan)</b>: " + ", ".join(f"{x['s']} {_pct(x['chg'])} (RSI {x['rsi']:.0f})" for x in reb[:4]))
    movers = sorted(liq, key=lambda x: -x["chg"])
    if movers:
        L.append("")
        L.append("📈 Naik: " + ", ".join(f"{x['s']} {_pct(x['chg'])}" for x in movers[:3]))
        L.append("📉 Turun: " + ", ".join(f"{x['s']} {_pct(x['chg'])}" for x in movers[::-1][:3]))
    if detail and cand:
        L.append("")
        L.append("Cek harga di Stockbit dulu. Lewati kalau sudah > +1% dari harga di atas, dan hindari yang close persis di pucuk.")
    if closed and cand:
        L.append("")
        L.append("Besok: baca laporan pagi 08:30, pasang antrean jual pre-opening, sisa jual sebelum 09:15.")
    return _footer(L)


# ---------------------------------------------------------------- laporan pagi
GLOBAL = [("^GSPC", "S&amp;P 500"), ("^IXIC", "Nasdaq"), ("^DJI", "Dow"), ("EIDO", "EIDO (saham RI di AS)"),
          ("^VIX", "VIX"), ("GC=F", "Emas"), ("HG=F", "Tembaga"), ("CL=F", "Minyak WTI"), ("IDR=X", "USD/IDR")]
LINKED = {"GC=F": ("Emas", "HRTA ANTM MDKA AMMN"), "HG=F": ("Tembaga", "AMMN MDKA"),
          "CL=F": ("Minyak", "MEDC AKRA PGAS"), "IDR=X": ("Rupiah melemah", "BBCA BBRI BMRI BBNI")}


def fetch_global():
    out = {}
    def one(sym):
        d = fetch_chart(sym, "5d")
        if d and len(d["bars"]) >= 2:
            c = [x["c"] for x in d["bars"]]
            out[sym] = {"c": c[-1], "chg": c[-1] / c[-2] - 1, "mt": d["mt"]}
    with ThreadPoolExecutor(6) as ex:
        list(ex.map(lambda s_: _safe(one, s_), [g[0] for g in GLOBAL]))
    return out


def _safe(fn, *a):
    try: return fn(*a)
    except Exception: return None


def global_verdict(G):
    e = G.get("EIDO", {}).get("chg"); sp = G.get("^GSPC", {}).get("chg")
    if (e is not None and e < -0.01) or (sp is not None and sp < -0.01):
        return "⚠️ <b>Waspada gap down.</b> Historis: kalau S&amp;P turun > 1%, IHSG cuma buka naik 35% hari. Kandidat BSJP kemarin sebaiknya dijual di pre-opening."
    if e is not None and e > 0.01:
        return "✅ <b>Global mendukung.</b> Historis: kalau EIDO naik > 1%, IHSG buka naik 88% hari."
    if sp is not None and sp > 0.01 and (e or 0) >= 0:
        return "✅ <b>Global mendukung.</b> Historis: kalau S&amp;P 500 naik > 1%, IHSG buka naik 90% hari."
    return "➖ <b>Netral.</b> Historis IHSG buka naik ±70–80% hari dalam kondisi ini, tapi kenaikannya kecil."


def build_morning(G):
    now = datetime.now(WIB)
    L = ["🌅 <b>Laporan pagi</b>", f"{now:%H:%M} WIB · sebelum pasar buka", ""]
    def g(sym, label, d=2, price=False):
        x = G.get(sym)
        if not x: return None
        return f"{label} {_num(x['c'], 0) + ' ' if price else ''}({_pct(x['chg'], d)})"
    us = [g("^GSPC", "S&amp;P 500"), g("^IXIC", "Nasdaq"), g("^DJI", "Dow")]
    L.append("<b>Wall Street semalam:</b> " + " · ".join(x for x in us if x))
    if G.get("EIDO"): L.append("<b>EIDO</b> (saham RI di AS): " + _pct(G["EIDO"]["chg"], 2))
    if G.get("^VIX"): L.append(f"VIX {_num(G['^VIX']['c'], 1)} ({_pct(G['^VIX']['chg'], 1)})")
    com = [g("GC=F", "Emas"), g("HG=F", "Tembaga"), g("CL=F", "Minyak"), g("IDR=X", "USD/IDR", price=True)]
    L.append("<b>Komoditas &amp; kurs:</b> " + " · ".join(x for x in com if x))
    L.append("")
    L.append(global_verdict(G))
    rel = []
    for sym, (name, codes) in LINKED.items():
        x = G.get(sym)
        if not x: continue
        if sym == "IDR=X":
            if x["chg"] > 0.005: rel.append(f"{name} {_pct(x['chg'])} → waspada: {codes.replace(' ', ', ')}")
        elif abs(x["chg"]) >= 0.01:
            arah = "naik" if x["chg"] > 0 else "turun"
            rel.append(f"{name} {arah} {_pct(x['chg'])} → {codes.replace(' ', ', ')}")
    if rel:
        L.append("")
        L.append("<b>Saham terkait yang perlu dipantau:</b>")
        L += ["• " + r for r in rel]
    st = S["stocks"]
    cand = sorted([x for x in st if x["liq"] and x["sc"] >= MIN_SC], key=lambda x: -x["sc"])
    if cand:
        L.append("")
        L.append("<b>Kandidat BSJP kemarin sore</b> (kalau kamu beli):")
        for x in cand[:5]:
            L.append(f"• <b>{x['s']}</b> close {_rp(x['c'])} → antre jual {_rp(x['tp1'])} · cut kalau buka ≤ {_rp(x['sl'])}")
    reb = [x for x in st if x.get("rb")]
    if reb:
        L.append("Rebound kemarin: " + ", ".join(f"{x['s']} (antre {_rp(x['tp1'])})" for x in reb[:4]))
    L.append("")
    L.append("Pasang antrean jual sebelum 08:58. Cek lanjutan dikirim sekitar 09:40.")
    return _footer(L)


def build_check():
    """Cek 09:40: pembukaan kandidat kemarin + saham yang kuat pagi ini (pakai data intraday 'hari ini')."""
    now = datetime.now(WIB)
    ih = S["ihsg"]; st = S["stocks"]
    data_t = datetime.fromtimestamp(S["asof"], WIB).strftime("%H:%M") if S["asof"] else "-"
    L = ["🔎 <b>Cek pagi</b>", f"{now:%H:%M} WIB · data Yahoo per {data_t} WIB", ""]
    jb = S["hist"]["^JKSE"]["bars"]
    if len(jb) >= 2:
        g = jb[-1]["o"] / jb[-2]["c"] - 1
        L.append(f"<b>IHSG</b> buka {_pct(g, 2)}, sekarang {_pct(ih['chg'], 2)}")
    # kandidat kemarin: hitung ulang skor pakai data sampai kemarin
    prev_day = jb[-2]["d"] if len(jb) >= 2 else ""
    ihc = jb[-2]["c"] / jb[-3]["c"] - 1 if len(jb) >= 3 else 0
    ih_above = jb[-2]["c"] >= avg([x["c"] for x in jb[-21:-1]])
    y = []
    for code, h in S["hist"].items():
        if code == "^JKSE" or len(h["bars"]) < 42 or h["bars"][-1]["d"] == prev_day: continue
        r = _safe(analyze, code, {"bars": h["bars"][:-1], "name": h.get("name")}, prev_day, 1.0, ihc, ih_above)
        if r and r["liq"] and (r["sc"] >= MIN_SC or r["rb"]):
            t = h["bars"][-1]
            y.append((r, t["o"] / r["c"] - 1, t["c"] / r["c"] - 1, t["c"] / t["o"] - 1))
    if y:
        L.append("")
        L.append("<b>Kandidat kemarin</b> (dari close kemarin):")
        for r, gap, now_, vs_o in sorted(y, key=lambda z: -z[0]["sc"])[:6]:
            tag = "✅ di atas open" if vs_o > 0 else "⚠️ di bawah open"
            L.append(f"• <b>{r['s']}</b>{' (rebound)' if r['rb'] and r['sc'] < MIN_SC else ''} buka {_pct(gap)}, sekarang {_pct(now_)} · {tag}")
    strong = []
    for x in st:
        if not x["liq"]: continue
        gap = x["o"] / (x["c"] / (1 + x["chg"])) - 1 if x["chg"] > -1 else 0
        vs_o = x["c"] / x["o"] - 1 if x["o"] else 0
        if 0 <= gap <= 0.04 and vs_o >= 0.005:
            strong.append((x, gap, vs_o))
    strong.sort(key=lambda z: -z[2])
    big = [x for x in st if x["liq"] and x["o"] / (x["c"] / (1 + x["chg"])) - 1 > 0.04]
    if strong:
        L.append("")
        L.append("<b>Kuat pagi ini</b> (gap 0–4%, bertahan di atas open):")
        for x, gap, vs_o in strong[:5]:
            L.append(f"• <b>{x['s']}</b> buka {_pct(gap)}, +{_num(vs_o * 100, 1)}% dari open, sekarang {_pct(x['chg'])}")
    if big:
        L.append("Gap besar > 4% (hindari kejar): " + ", ".join(x["s"] for x in big[:5]))
    L.append("")
    L.append("<i>Strategi beli pagi masih percobaan dan belum di-backtest. Pakai lot kecil, cut di bawah low 30 menit pertama.</i>")
    return _footer(L)


def send_telegram(text):
    token = os.environ["TELEGRAM_TOKEN"].strip(); chat = os.environ["TELEGRAM_CHAT_ID"].strip()
    data = json.dumps({"chat_id": chat, "text": text, "parse_mode": "HTML",
                       "disable_web_page_preview": True}).encode("utf-8")
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def telegram_report(mode="auto"):
    now = datetime.now(WIB)
    if mode == "auto":
        m = now.hour * 60 + now.minute
        mode = "pagi" if m < 9 * 60 else "cek" if 9 * 60 + 25 <= m < 10 * 60 else "jam"
    run_scan(True)
    if not S["stocks"]:
        print("Gagal: tidak ada data dari Yahoo.", S["msg"]); sys.exit(1)
    today = now.strftime("%Y-%m-%d")
    if mode in ("jam", "cek") and S["day"] != today and "--paksa" not in sys.argv:
        print(f"Bursa tidak buka hari ini (data terakhir {S['day']}). Tidak ada pesan dikirim."); return
    if mode == "pagi":
        msg = build_morning(fetch_global())
    elif mode == "cek":
        msg = build_check()
    else:
        msg = build_report()
    res = send_telegram(msg)
    print(f"Mode {mode}: " + ("terkirim" if res.get("ok") else f"gagal kirim: {res}"))


def main():
    if len(sys.argv) > 2 and sys.argv[1] == "--static":
        return build_static(sys.argv[2])
    if len(sys.argv) > 1 and sys.argv[1] == "--telegram":
        return telegram_report(sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] in ("pagi", "cek", "jam") else "auto")
    print("=" * 60)
    print("  BSJP Radar - Beli Sore Jual Pagi")
    print(f"  Buka di laptop : http://localhost:{PORT}")
    ip = lan_ip()
    if ip and HOST != "127.0.0.1":
        print(f"  Buka di HP     : http://{ip}:{PORT}  (HP harus di WiFi yang sama)")
    print("  Refresh browser untuk data terbaru.")
    print("  Tutup jendela ini untuk berhenti.")
    print("=" * 60)
    try:
        srv = ThreadingHTTPServer((HOST, PORT), H)
    except OSError:
        print(f"\nPort {PORT} sudah dipakai. Mungkin BSJP Radar sudah jalan di jendela lain.")
        webbrowser.open(f"http://localhost:{PORT}")
        input("Tekan Enter untuk menutup...")
        return
    maybe_refresh(force_full=True)
    threading.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()

    def progress():
        last = None
        while True:
            time.sleep(3)
            if S["busy"]:
                line = f"  Memindai ({'LQ45' if S['univ']=='lq45' else 'semua emiten'}): {S['done']}/{S['total']}"
                if line != last:
                    print(line); last = line
            elif last:
                n = len(S["stocks"]); liq = sum(1 for x in S["stocks"] if x["liq"])
                cand = sum(1 for x in S["stocks"] if x["liq"] and x["sc"] >= 55)
                print(f"  Selesai {datetime.now(WIB):%H:%M:%S} WIB: {n} saham terbaca, {liq} likuid, {cand} kandidat."
                      + (f"  ({S['fails']} gagal)" if S["fails"] else ""))
                last = None
    threading.Thread(target=progress, daemon=True).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

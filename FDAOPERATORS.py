# FDA.py – Scraper + Diff Reporter (OPERATORS mode + baseline seeding)
# -*- coding: utf-8 -*-
import os, re, time, json, ssl, smtplib
from typing import List, Dict
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import multiprocessing as mp
from email.message import EmailMessage
from selenium.common.exceptions import TimeoutException

# ===== URL =====
URL = "https://pertento.fda.moph.go.th/FDA_SEARCH_CENTER/PRODUCT/FRM_SEARCH_CMT.aspx"

# ===== คอนฟิก =====
OPERATORS = [
    "บริษัท อังกฤษตรางู (แอล.พี.) จำกัด",
    "บริษัท คัลเล่อร์คอส จำกัด",
    "บริษัท ซีโอเอสเอ็มเอเอ็กซ์ (ไทยแลนด์) จำกัด",
    "บริษัท อุตสาหกรรมมิตรมงคล จำกัด",
    "บริษัท ออกานิกส์ คอสเม่ จำกัด",
    "บริษัท พฤกษา แลบบอราเทอรี่ จำกัด",
    "บริษัท ไมลอทท์ แลบบอราทอรีส์ จำกัด",
    "บริษัท อินเตอร์เนชั่นแนล แลบบอราทอรีส์ จำกัด",
    "เอส แอนด์ เจ อินเตอร์เนชั่นแนล",
]
BRANDS: List[str] = []  # โหมดนี้ไม่ค้นยี่ห้อ

WORKERS   = 1
FAST_MODE = True
ALLOW_YY  = {"68"}

COLS = [
    "trade_name","cosmetic_name","notification_no","notification_year_be_last2",
    "notification_type","notification_status","approve_date","expire_date",
    "operator_name","foreign_mfr","contract_manufacturer","reference_for","skus",
]
TH_HEADERS = {
    "trade_name":"ชื่อการค้า","cosmetic_name":"ชื่อเครื่องสำอาง","notification_no":"เลขที่ใบรับจดแจ้ง",
    "notification_year_be_last2":"ปีที่จดแจ้ง","notification_type":"ประเภทการจดแจ้ง",
    "notification_status":"สถานะใบรับจดแจ้ง","approve_date":"วันที่อนุญาต","expire_date":"วันที่หมดอายุ",
    "operator_name":"ชื่อผู้ประกอบการ","foreign_mfr":"ชื่อและที่อยู่ผู้ผลิตต่างประเทศ",
    "contract_manufacturer":"ชื่อผู้ว่าจ้างผลิต","reference_for":"เลขอ้างอิงสำหรับ","skus":"SKUs",
}
BASE_HEADERS = [TH_HEADERS[c] for c in COLS if c in TH_HEADERS]

class DetailOpenError(Exception): ...
def log(msg): print(msg, flush=True)

# ---------- WebDriver ----------
def setup(headless=True):
    opt = Options()
    if headless: opt.add_argument("--headless=new")
    opt.add_argument("--window-size=1400,900")
    opt.add_argument("--disable-gpu"); opt.add_argument("--use-gl=swiftshader")
    opt.add_argument("--disable-gpu-compositing"); opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-extensions"); opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--disable-background-networking"); opt.add_argument("--disable-background-timer-throttling")
    opt.add_argument("--disable-renderer-backgrounding")
    opt.add_argument("--disable-features=Translate,MediaRouter,OptimizationHints,PaintHolding")
    opt.add_argument("--blink-settings=imagesEnabled=false")
    opt.add_argument("--lang=th-TH"); opt.add_argument("--remote-allow-origins=*")
    opt.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
    opt.add_experimental_option("excludeSwitches", ["enable-automation","enable-logging"])
    opt.add_experimental_option("useAutomationExtension", False)
    opt.page_load_strategy = "eager"
    opt.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2,
    })
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)
    drv.set_page_load_timeout(120)
    try:
        drv.execute_cdp_cmd("Network.enable", {})
        drv.execute_cdp_cmd("Network.setBlockedURLs", {"urls": ["*.png","*.jpg","*.jpeg","*.gif","*.webp","*.svg","*.woff","*.woff2","*.ttf","*.otf"]})
        drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return drv

def w_xpath(drv, xp, t=60):    return WebDriverWait(drv, t).until(EC.presence_of_element_located((By.XPATH, xp)))
def w_visible(drv, xp, t=60):  return WebDriverWait(drv, t).until(EC.visibility_of_element_located((By.XPATH, xp)))

def wait_ajax_idle(drv, timeout=60):
    def _idle(d):
        try:
            return d.execute_script("""
                try {
                  if (window.Telerik && Telerik.Web && Telerik.Web.UI && Telerik.Web.UI.RadAjaxManager) {
                      var m = Telerik.Web.UI.RadAjaxManager.getCurrent();
                      if (m && typeof m.get_isRequesting==='function') return !m.get_isRequesting();
                  }
                  if (window.Sys && Sys.WebForms && Sys.WebForms.PageRequestManager) {
                      var pr = Sys.WebForms.PageRequestManager.getInstance();
                      if (pr) return !pr.get_isInAsyncPostBack();
                  }
                } catch(e) {}
                return document.readyState === 'complete';
            """)
        except Exception:
            return True
    WebDriverWait(drv, timeout).until(lambda d: _idle(d))

def grid_present(drv) -> bool:
    return bool(
        drv.find_elements(By.XPATH, "//table[contains(@class,'rgMasterTable')]/tbody") or
        drv.find_elements(By.XPATH, "//td[contains(.,'No records to display')]") or
        drv.find_elements(By.XPATH, "//*[contains(@class,'validation') or contains(@class,'validator') or contains(@class,'error')]")
    )

def open_search_with_retries(drv, retries=4):
    for attempt in range(1, retries+1):
        drv.get(URL)
        try:
            w_visible(drv, "//*[@id='ContentPlaceHolder1_txt_oper']", t=30 if attempt == 1 else 45)
            w_visible(drv, "//*[@id='ContentPlaceHolder1_btn_sea_cmt']", t=10)
            return True
        except Exception:
            log(f"[open] attempt {attempt}: element not ready -> refresh")
            time.sleep(1.0 * attempt)
            try: drv.refresh(); wait_ajax_idle(drv, 45)
            except Exception: pass
    return False

def click_search(drv):
    btn = drv.find_element(By.ID, "ContentPlaceHolder1_btn_sea_cmt")
    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    time.sleep(0.2)
    drv.execute_script("arguments[0].click();", btn)
    wait_ajax_idle(drv, 60)

def wait_for_rows(drv, timeout=120) -> int:
    wait_ajax_idle(drv, min(60, timeout))
    WebDriverWait(drv, timeout).until(lambda d: grid_present(d))
    t0 = time.time()
    while True:
        rows = drv.find_elements(By.XPATH, "//table[contains(@class,'rgMasterTable')]/tbody/tr[count(td)>=2]")
        if rows: return len(rows)
        if drv.find_elements(By.XPATH, "//td[contains(.,'No records to display')]"): return 0
        drv.execute_script("window.scrollBy(0,200);"); drv.execute_script("window.scrollBy(0,-200);")
        time.sleep(0.25)
        if time.time() - t0 > timeout: raise TimeoutError("ตารางยังไม่โหลด ภายในเวลาที่กำหนด")

def page_val(drv) -> str:
    els = drv.find_elements(By.XPATH, "//input[contains(@class,'rgCurrentPage')]")
    return (els[0].get_attribute("value") or "").strip() if els else ""

def first_row_key(drv) -> str:
    try:
        row = drv.find_element(By.XPATH, "(//table[contains(@class,'rgMasterTable')]/tbody/tr[count(td)>=2])[1]")
    except Exception:
        return ""
    m = re.search(r"\b(\d{1,2})\D+(\d{1,2})\D+(\d{2,})\b", row.text or "")
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else (row.text or "").strip()

def next_exists(drv) -> bool:
    return bool(
        drv.find_elements(By.XPATH, "//input[contains(@class,'rgPageNext') and not(@disabled)]") or
        drv.find_elements(By.XPATH, "//a[contains(@class,'rgPageNext') and not(contains(@class,'rgDisabled'))]")
    )

def go_next(drv, retries: int = 3) -> bool:
    for attempt in range(1, retries + 1):
        before_key = first_row_key(drv); before_page = page_val(drv)
        btns = (drv.find_elements(By.XPATH, "//input[contains(@class,'rgPageNext') and not(@disabled)]")
                or drv.find_elements(By.XPATH, "//a[contains(@class,'rgPageNext') and not(contains(@class,'rgDisabled'))]"))
        if not btns: return False
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", btns[0]); time.sleep(0.2)
        drv.execute_script("arguments[0].click();", btns[0])
        try:
            WebDriverWait(drv, 45 if FAST_MODE else 60).until(
                lambda d: first_row_key(d) != before_key or page_val(d) != before_page
            )
        except Exception:
            if attempt < retries: continue
            return False
        wait_ajax_idle(drv, 60); wait_for_rows(drv)
        log(f"  -> ไปหน้า {page_val(drv)} แล้ว (attempt {attempt})")
        return True
    return False

# ---------- Utils ----------
def align_new_changes_strict(df_report, keep_status=True):
    status_col = "สถานะ"
    df = df_report.copy().rename(columns=TH_HEADERS)
    strict_cols = ([status_col] if (keep_status and status_col in df.columns) else []) + BASE_HEADERS
    for col in strict_cols:
        if col not in df.columns: df[col] = ""
    return df.reindex(columns=strict_cols)

def safe_name(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|\r\n]+","_", s).strip()
    return re.sub(r"\s+"," ", s)

def year_from_no(no: str) -> str:
    s = (no or "").strip()
    toks = [t for t in re.split(r"\D+", s) if t]
    if len(toks) >= 3: return toks[2][:2] if len(toks[2]) >= 2 else ""
    return ""

def valid_pos45(no: str) -> bool:
    s = (no or "").strip()
    toks = [t for t in re.split(r"\D+", s) if t]
    if len(toks) >= 3:
        seg3 = toks[2]
        return any(seg3.startswith(yy) for yy in ALLOW_YY)
    return False

NOTIF_RE = re.compile(r"\b(\d{1,2})\D+(\d{1,2})\D+(\d{2,})\b")
def notif_from_row_text(text: str) -> str:
    m = NOTIF_RE.search(text or "")
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""

# ---------- Search/operator ----------
def ensure_on_grid(drv, operator: str, yy2: str, max_back=2):
    for _ in range(max_back):
        if grid_present(drv): return
        try: drv.back(); wait_ajax_idle(drv, 60)
        except Exception: break
        if grid_present(drv): return
    drv.get(URL); wait_ajax_idle(drv, 60)
    fill_and_search(drv, operator, yy2)

def fill_and_search(drv, operator: str, yy2: str):
    if not open_search_with_retries(drv):
        raise RuntimeError("เปิดหน้า Search ไม่สำเร็จหลัง retry หลายครั้ง")
    for attempt in range(1, 4):
        try:
            op = drv.find_element(By.ID, "ContentPlaceHolder1_txt_oper")
            yy = drv.find_element(By.ID, "ContentPlaceHolder1_Txt_fdpdtno")
            brand_el = (drv.find_elements(By.ID, "ContentPlaceHolder1_txt_trade") or
                        drv.find_elements(By.ID, "ContentPlaceHolder1_txt_tradename") or [])
        except Exception:
            try: drv.get(URL); wait_ajax_idle(drv, 60)
            except Exception: pass
            continue
        try:
            drv.execute_script("arguments[0].value = '';", op)
            drv.execute_script("arguments[0].value = '';", yy)
            for el in brand_el:
                try: drv.execute_script("arguments[0].value='';", el)
                except: pass
        except Exception: pass
        time.sleep(0.05)
        try:
            op.send_keys(operator[:2]); yy.send_keys(yy2)
            drv.execute_script("arguments[0].value = arguments[1];", op, operator)
        except Exception: continue

        click_search(drv)
        try:
            n = wait_for_rows(drv, timeout=120 if attempt == 1 else 180)
            log(f"  -> โหลดตารางแล้ว {n} แถว (ปี {yy2})"); return
        except (TimeoutException, TimeoutError):
            log(f"[fill_and_search] Timeout (attempt {attempt}) -> refresh")
        except Exception as e:
            log(f"[fill_and_search] unexpected: {e}")
    raise RuntimeError(f"ค้นหาไม่สำเร็จหลายครั้ง: {operator} (ปี {yy2})")

# ---------- เปิดรายละเอียด ----------
def _txt_by_id(drv, _id: str) -> str:
    try:
        el = drv.find_element(By.ID, _id)
        txt = el.get_attribute("innerText") or el.text or ""
        return re.sub(r"\s+"," ", txt).strip()
    except:
        return ""

def open_detail_and_back(drv, link, operator: str, yy2: str) -> Dict[str, str]:
    base = drv.current_window_handle
    before = set(drv.window_handles)
    href = link.get_attribute("href") or ""
    m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
    if m:
        target, arg = m.group(1), m.group(2)
        js = """
        (function(target,arg){
          var f=document.forms[0];
          function ensure(n){var el=document.getElementsByName(n)[0]; if(!el){el=document.createElement('input');el.type='hidden';el.name=n;el.id=n;f.appendChild(el);} return el;}
          ensure('__EVENTTARGET').value   = target||'';
          ensure('__EVENTARGUMENT').value = arg||'';
          f.submit();
        })(arguments[0],arguments[1]);
        """
        drv.execute_script(js, target, arg)
    else:
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", link); time.sleep(0.2)
        drv.execute_script("arguments[0].click();", link)

    try:
        WebDriverWait(drv, 30).until(
            lambda d: len(d.window_handles) > len(before) or d.find_elements(By.ID, "ContentPlaceHolder1_lb_no_regnos")
        )
    except Exception:
        drv.execute_script("arguments[0].click();", link)
        WebDriverWait(drv, 30).until(
            lambda d: len(d.window_handles) > len(before) or d.find_elements(By.ID, "ContentPlaceHolder1_lb_no_regnos")
        )

    after = set(drv.window_handles)
    opened_new = len(after) > len(before)
    if opened_new: drv.switch_to.window((after - before).pop())

    try:
        wait_ajax_idle(drv, 60)
        w_xpath(drv, "//*[@id='ContentPlaceHolder1_lb_no_regnos']", 45 if FAST_MODE else 60)
        w_xpath(drv, "//*[@id='ContentPlaceHolder1_lb_status']", 45 if FAST_MODE else 60)
    except Exception as e:
        try:
            if opened_new: drv.close(); drv.switch_to.window(base)
        finally:
            ensure_on_grid(drv, operator, yy2)
        raise DetailOpenError(str(e))

    rec = {
        "notification_status": _txt_by_id(drv, "ContentPlaceHolder1_lb_status"),
        "notification_no": _txt_by_id(drv, "ContentPlaceHolder1_lb_no_regnos"),
        "notification_type": _txt_by_id(drv, "ContentPlaceHolder1_lb_type"),
        "trade_name": _txt_by_id(drv, "ContentPlaceHolder1_lb_trade_Tpop"),
        "cosmetic_name": _txt_by_id(drv, "ContentPlaceHolder1_lb_cosnm_Tpop"),
        "approve_date": _txt_by_id(drv, "ContentPlaceHolder1_lb_appdate"),
        "expire_date": _txt_by_id(drv, "ContentPlaceHolder1_lb_expdate"),
        "operator_name": _txt_by_id(drv, "ContentPlaceHolder1_lb_usernm_pop"),
        "foreign_mfr": _txt_by_id(drv, "ContentPlaceHolder1_lb_fac_pop"),
        "contract_manufacturer": _txt_by_id(drv, "ContentPlaceHolder1_lb_NAME_EMPLOYER"),
        "reference_for": _txt_by_id(drv, "ContentPlaceHolder1_lb_NO_pop"),
        "skus": "", "notification_year_be_last2": ""
    }
    rec["notification_year_be_last2"] = year_from_no(rec.get("notification_no",""))

    if opened_new: drv.close(); drv.switch_to.window(base)
    else: drv.back()
    wait_ajax_idle(drv, 60); ensure_on_grid(drv, operator, yy2); wait_for_rows(drv)
    log(f"     ✓ ดึงแล้ว: {rec.get('notification_no','')}")
    return rec

# ---------- Scrape (grid) ----------
def scrape_page(drv, operator: str, yy2: str) -> List[Dict]:
    out: List[Dict] = []
    wait_for_rows(drv); i = 1
    while True:
        rows = drv.find_elements(By.XPATH, "//table[contains(@class,'rgMasterTable')]/tbody/tr[count(td)>=2]")
        n = len(rows)
        if n == 0 or i > n: break
        try:
            row = drv.find_element(By.XPATH, f"(//table[contains(@class,'rgMasterTable')]/tbody/tr[count(td)>=2])[{i}]")
            txt = row.text or ""; no = notif_from_row_text(txt)
            if not no or year_from_no(no) != yy2: i += 1; continue
            link = row.find_element(By.XPATH, ".//a[contains(@href,'__doPostBack') and contains(.,'ดูข้อมูล')]")
            log(f"  -> CLICK ดูข้อมูล | เลขจดแจ้ง={no} | row={i}/{n}")
            rec = open_detail_and_back(drv, link, operator, yy2)
            if valid_pos45(rec.get("notification_no","")):
                rec["operator_name_query"] = operator; out.append(rec)
            i += 1
        except DetailOpenError as e:
            log(f"     ! เปิดรายละเอียดไม่สำเร็จ (ข้ามแถวนี้) : {str(e).splitlines()[0]}"); i += 1; continue
        except Exception as e:
            msg = getattr(e, "msg", str(e)); first = msg.splitlines()[0] if isinstance(msg, str) and msg else str(type(e).__name__)
            log(f"     ! ซิงค์ตารางใหม่ (i={i}/{n}) : {first}")
            wait_ajax_idle(drv, 60); ensure_on_grid(drv, operator, yy2); wait_for_rows(drv); time.sleep(0.2); continue
    return out

def scrape_operator(drv, operator: str) -> List[Dict]:
    log(f"Start (ผู้ประกอบการ): {operator}")
    results: List[Dict] = []
    for yy2 in sorted(ALLOW_YY):
        fill_and_search(drv, operator, yy2)
        if drv.find_elements(By.XPATH, "//td[contains(.,'No records to display')]"):
            log(f"  -> ปี {yy2}: ไม่มีข้อมูล"); continue
        while True:
            results.extend(scrape_page(drv, operator, yy2))
            if next_exists(drv) and go_next(drv): continue
            break
    log(f"Done (ผู้ประกอบการ): {operator} -> {len(results)} แถว")
    return results

# ---------- Seen storage & email ----------
def collect_operator_names(df: pd.DataFrame) -> set:
    cols = [c for c in df.columns if c in ("operator_name", "ชื่อผู้ประกอบการ")]
    if not cols: return set()
    col = cols[0]
    return set(str(x).strip() for x in df[col].dropna().astype(str).tolist() if str(x).strip())

def collect_items(df: pd.DataFrame) -> list:
    if df is None or df.empty: return []
    rev_map = {v: k for k, v in TH_HEADERS.items()}
    tmp = df.copy().rename(columns=rev_map)
    out = []
    for _, r in tmp.iterrows():
        row = {}
        for k in COLS:
            row[k] = (str(r[k]).strip() if (k in r and pd.notna(r[k])) else "")
        if row.get("notification_no") and not row.get("notification_year_be_last2"):
            row["notification_year_be_last2"] = year_from_no(row["notification_no"])
        out.append(row)
    return out

def load_seen_set(path: str) -> set:
    try:
        with open(path, "r", encoding="utf-8") as f: return set(json.load(f))
    except Exception: return set()

def save_seen_set(path: str, values: set):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(sorted(values), f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"save_seen_set error: {e}")

def send_email_with_attachments(subject: str, body: str, attachments: list):
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    use_tls = os.environ.get("SMTP_USE_TLS", "1") == "1"
    user = os.environ.get("SMTP_USER", "")
    pwd = os.environ.get("SMTP_PASS", "")
    to_list = [x.strip() for x in os.environ.get("SMTP_TO", "").split(",") if x.strip()]
    if not (host and port and user and pwd and to_list):
        log("SMTP env ไม่ครบ (HOST/PORT/USER/PASS/TO) – ข้ามการส่งอีเมล"); return

    msg = EmailMessage(); msg["Subject"] = subject; msg["From"] = user; msg["To"] = ", ".join(to_list)
    msg.set_content(body)
    for path in attachments or []:
        try:
            with open(path, "rb") as f: data = f.read()
            msg.add_attachment(data, maintype="text", subtype="csv", filename=os.path.basename(path))
        except Exception as e:
            log(f"แนบไฟล์ไม่สำเร็จ: {path} -> {e}")

    try:
        if use_tls and port == 587:
            context = ssl.create_default_context()
            with smtplib.SMTP(host, port, timeout=25) as server:
                server.ehlo(); server.starttls(context=context); server.ehlo()
                server.login(user, pwd); server.send_message(msg)
        elif port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=25) as server:
                server.login(user, pwd); server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=25) as server:
                server.login(user, pwd); server.send_message(msg)
        log("ส่งอีเมลสำเร็จ")
    except Exception as e:
        log(f"ส่งอีเมลไม่สำเร็จ: {e}")

# ---------- Baseline seeding ----------
def seed_seen_from_csvs(dirpath: str):
    """สร้าง baseline จากไฟล์ CSV ใน dirpath (อ่านได้ทั้งคอลัมน์ไทย/อังกฤษ)"""
    ops, items = set(), set()
    if not os.path.isdir(dirpath): return ops, items
    for fn in os.listdir(dirpath):
        if not fn.lower().endswith(".csv"): continue
        p = os.path.join(dirpath, fn)
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if "operator_name" in df.columns:
            ops |= set(str(x).strip() for x in df["operator_name"].dropna().astype(str))
        elif "ชื่อผู้ประกอบการ" in df.columns:
            ops |= set(str(x).strip() for x in df["ชื่อผู้ประกอบการ"].dropna().astype(str))
        col_no = "notification_no" if "notification_no" in df.columns else ("เลขที่ใบรับจดแจ้ง" if "เลขที่ใบรับจดแจ้ง" in df.columns else None)
        if col_no:
            items |= set(str(x).strip() for x in df[col_no].dropna().astype(str))
    return ops, items

# ---------- Runner ----------
def run_one_operator(op, headless=True, outdir="output_csv"):
    drv = setup(headless=headless)
    try:
        rows = scrape_operator(drv, op)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.reindex(columns=[c for c in COLS if c in df.columns]).rename(columns=TH_HEADERS)
        os.makedirs(outdir, exist_ok=True)
        out = os.path.join(outdir, f"{safe_name(op)}.csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        log(f"Saved: {out} ({len(df):,} แถว)")
        return out
    finally:
        drv.quit()

def main(headless=True, outdir="output_csv", workers=None):
    if workers is None: workers = WORKERS
    os.makedirs(outdir, exist_ok=True)

    SEEN_OP_PATH = os.path.join(outdir, "_operators_seen.json")
    SEEN_ITEM_PATH = os.path.join(outdir, "_items_seen.json")

    all_ops_current = set()
    all_items_current = []

    # ===== รอบ "ผู้ประกอบการ" =====
    if workers <= 1:
        drv = setup(headless=headless)
        try:
            for op in OPERATORS:
                rows = scrape_operator(drv, op)
                df = pd.DataFrame(rows)
                if not df.empty:
                    df = df.reindex(columns=[c for c in COLS if c in df.columns]).rename(columns=TH_HEADERS)
                out = os.path.join(outdir, f"{safe_name(op)}.csv")
                df.to_csv(out, index=False, encoding="utf-8-sig")
                log(f"Saved: {out} ({len(df):,} แถว)\n")
                all_ops_current |= collect_operator_names(df)
                all_items_current += collect_items(df)
        finally:
            drv.quit()
    else:
        with mp.Pool(processes=min(workers, len(OPERATORS), mp.cpu_count())) as pool:
            pool.starmap(run_one_operator, [(op, headless, outdir) for op in OPERATORS])
        for fn in os.listdir(outdir):
            if fn.lower().endswith(".csv") and not fn.startswith("new_") and not fn.startswith("BRAND__"):
                try:
                    df = pd.read_csv(os.path.join(outdir, fn))
                    all_ops_current |= collect_operator_names(df)
                    all_items_current += collect_items(df)
                except Exception:
                    pass

    # ===== Diff & Report =====
    seen_ops = load_seen_set(SEEN_OP_PATH)
    seen_item_nos = load_seen_set(SEEN_ITEM_PATH)

    # ถ้ายังไม่มี baseline ให้ seed จาก baseline/
    if not seen_ops and not seen_item_nos:
        base_dir = os.environ.get("FDA_BASELINE_DIR", "baseline")
        log(f"No seen baseline found → seeding from '{base_dir}' ...")
        seed_ops, seed_items = seed_seen_from_csvs(base_dir)
        if seed_ops or seed_items:
            save_seen_set(SEEN_OP_PATH, seed_ops)
            save_seen_set(SEEN_ITEM_PATH, seed_items)
            seen_ops, seen_item_nos = seed_ops, seed_items
            log(f"Seeded baseline: ops={len(seed_ops):,}, items={len(seed_items):,}")
        else:
            log(f"Baseline folder '{base_dir}' not found or empty.")

    by_operator_all = {}
    for it in all_items_current:
        by_operator_all.setdefault(it["operator_name"], []).append(it)

    current_item_nos = {it["notification_no"] for it in all_items_current if it.get("notification_no")}
    new_ops = sorted(all_ops_current - seen_ops)
    new_item_nos = current_item_nos - seen_item_nos

    new_ops_rows = []
    for op in new_ops:
        for it in by_operator_all.get(op, []):
            new_ops_rows.append({"สถานะ":"ผู้ประกอบการใหม่", **it})

    existing_ops_new_items_rows = []
    for op, items in by_operator_all.items():
        if op in new_ops: continue
        for it in items:
            if it["notification_no"] in new_item_nos:
                existing_ops_new_items_rows.append({"สถานะ":"สินค้าใหม่ (ผู้ประกอบการเดิม)", **it})

    report_rows = new_ops_rows + existing_ops_new_items_rows

    if report_rows:
        today = datetime.now().strftime("%Y-%m-%d")
        new_csv = os.path.join(outdir, f"new_changes_{today}.csv")
        df_report = pd.DataFrame(report_rows)
        if "notification_year_be_last2" not in df_report.columns and "notification_no" in df_report.columns:
            df_report["notification_year_be_last2"] = df_report["notification_no"].map(year_from_no)
        df_report = align_new_changes_strict(df_report, keep_status=True)
        df_report.to_csv(new_csv, index=False, encoding="utf-8-sig")

        save_seen_set(SEEN_OP_PATH, seen_ops | set(new_ops))
        save_seen_set(SEEN_ITEM_PATH, seen_item_nos | set(new_item_nos))

        # Email สรุป
        max_show = 20
        lines = []
        if new_ops:
            lines.append(f"ผู้ประกอบการใหม่: {len(new_ops)} ราย")
            for op in new_ops[:max_show]:
                ex = by_operator_all.get(op, [])[:3]
                samples = ", ".join([x.get("trade_name") or x.get("cosmetic_name") or x["notification_no"] for x in ex])
                lines.append(f"  - {op} (ตัวอย่างสินค้า: {samples})")
            if len(new_ops) > max_show:
                lines.append(f"  ... และอื่น ๆ อีก {len(new_ops)-max_show} ราย")
        if existing_ops_new_items_rows:
            from collections import defaultdict
            m = defaultdict(list)
            for r in existing_ops_new_items_rows: m[r["operator_name"]].append(r)
            lines.append(f"ผู้ประกอบการเดิมที่มีสินค้าใหม่: {len(m)} ราย")
            for op in list(m.keys())[:max_show]:
                ex = m[op][:3]
                samples = ", ".join([x.get("trade_name") or x.get("cosmetic_name") or x["notification_no"] for x in ex])
                lines.append(f"  - {op} (+{len(m[op])} รายการใหม่, ตัวอย่าง: {samples})")
            if len(m) > max_show:
                lines.append(f"  ... และอื่น ๆ อีก {len(m)-max_show} ราย")

        subject = "[FDA] สรุปความเปลี่ยนแปลง: ผู้ประกอบการใหม่ / สินค้าใหม่"
        body = f"""พบความเปลี่ยนแปลงจากรอบรันล่าสุด
{chr(10).join(lines)}

แนบไฟล์: {os.path.basename(new_csv)}
โฟลเดอร์เอาต์พุต: {os.path.abspath(outdir)}
"""
        send_email_with_attachments(subject, body, [new_csv])
        log(f"สร้าง {new_csv} และส่งอีเมลสรุปแล้ว")
    else:
        subject = "[FDA] รอบนี้ไม่มีผู้ประกอบการหรือสินค้าใหม่"
        body = f"""สรุปผลรอบรันล่าสุด:
- ไม่พบผู้ประกอบการใหม่
- ไม่พบสินค้าใหม่

โฟลเดอร์เอาต์พุต: {os.path.abspath(outdir)}
"""
        send_email_with_attachments(subject, body, [])
        log("ไม่มีผู้ประกอบการใหม่หรือสินค้าใหม่ → ส่งอีเมลแจ้งแล้ว")

if __name__ == "__main__":
    mp.freeze_support()
    main(headless=True, workers=WORKERS)
    time.sleep(0.2)

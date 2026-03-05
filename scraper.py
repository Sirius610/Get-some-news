import os
import json
import time
import requests
import pandas as pd
from datetime import datetime

# ================= 全局机密与配置 =================
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
CONFIG_DIR = "configs"
HISTORY_FILE = "history.txt" # 记忆账本文件
# ============================================

def load_history():
    """读取历史抓取记录"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return set(f.read().splitlines())
    return set()

def save_history(history_set):
    """保存历史抓取记录"""
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(history_set))

def get_feishu_token():
    if not FEISHU_APP_ID: return None
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        res = requests.post(url, json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}).json()
        return res.get("tenant_access_token")
    except:
        return None

def push_to_feishu(records, app_token, table_id):
    if not app_token or not table_id: return
    token = get_feishu_token()
    if not token: return
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    feishu_records = [{"fields": rec} for rec in records]
    
    try:
        res = requests.post(url, headers=headers, json={"records": feishu_records}).json()
        if res.get("code") == 0:
            print(f"  [√] 成功推送 {len(records)} 条数据到飞书！")
        else:
            print(f"  [x] 飞书推送报错: {res}")
    except Exception as e:
        print(f"  [x] 飞书推送网络异常: {e}")

def get_nested_data(data_dict, keys_list):
    temp = data_dict
    for key in keys_list:
        if isinstance(temp, dict): temp = temp.get(key, {})
        else: return []
    return temp if isinstance(temp, list) else []

def run_engine():
    today_str = datetime.now().strftime("%Y%m%d")
    all_results = []
    
    # 1. 挂载记忆账本
    history_set = load_history()
    new_items_count = 0
    
    if not os.path.exists(CONFIG_DIR):
        print(f"[!] 缺少 {CONFIG_DIR} 文件夹。")
        return

    for filename in os.listdir(CONFIG_DIR):
        if not filename.endswith(".json"): continue
        
        with open(os.path.join(CONFIG_DIR, filename), 'r', encoding='utf-8') as f:
            cfg = json.load(f)
            
        site_name = cfg.get("site_name", "未知站点")
        print(f"\n[*] 开始处理站点: {site_name}")
        
        req_cfg = cfg.get("request", {})
        rules = cfg.get("parse_rules", {})
        filters = cfg.get("filters", {})
        feishu_dest = cfg.get("feishu_destination", {})
        
        page_no = 1
        keep_running = True
        site_results = []
        
        while keep_running:
            if req_cfg.get("pagination_key"):
                req_cfg["payload"][req_cfg["pagination_key"]] = page_no
                
            try:
                if req_cfg.get("method", "GET").upper() == "POST":
                    res = requests.post(req_cfg["url"], json=req_cfg["payload"], headers=req_cfg.get("headers"), timeout=10)
                else:
                    res = requests.get(req_cfg["url"], params=req_cfg["payload"], headers=req_cfg.get("headers"), timeout=10)
                
                res.raise_for_status()
                json_data = res.json()
                
                items = get_nested_data(json_data, rules.get("list_path", []))
                if not items: break
                
                for item in items:
                    pub_date = str(item.get(rules.get("date_key"), ""))
                    date_short = pub_date[:8] if len(pub_date) >= 8 else ""
                    
                    if date_short and date_short < today_str:
                        keep_running = False
                        break
                        
                    if date_short != today_str: continue
                    
                    notice_desc = item.get(rules.get("notice_type_key"), "")
                    project_type = item.get(rules.get("project_type_key"), "")
                    title = item.get(rules.get("title_key"), "")
                    
                    if notice_desc not in filters.get("target_notice_types", []): continue
                    if project_type not in filters.get("target_project_types", []): continue
                    
                    # 2. 核心去重逻辑：生成数据指纹
                    data_fingerprint = f"{site_name}_{pub_date}_{title}"
                    if data_fingerprint in history_set:
                        continue # 如果在账本里见过，直接跳过！
                        
                    # 如果没见过，加入账本，并装入结果列表
                    history_set.add(data_fingerprint)
                    site_results.append({
                        "项目类型": project_type,
                        "项目名称": title,
                        "发布时间": pub_date,
                        "公告类型": notice_desc
                        # "来源站点": site_name
                    })
                    new_items_count += 1
                    
                page_no += 1
                if keep_running: time.sleep(1)
                
            except Exception as e:
                print(f"  [x] 抓取异常: {e}")
                break
                
        if site_results:
            print(f"  -> {site_name} 抓取到 {len(site_results)} 条【全新】数据。")
            all_results.extend(site_results)
            push_to_feishu(site_results, feishu_dest.get("app_token"), feishu_dest.get("table_id"))
        else:
            print(f"  -> {site_name} 今日无新的未推数据。")

    # 3. 任务结束，保存最新的账本
    save_history(history_set)

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_excel(f"全站新增数据_{today_str}.xlsx", index=False)
        print(f"\n[√] 引擎运行结束，共抓取并推送 {new_items_count} 条新数据。")
    else:
        print("\n[!] 引擎运行结束，没有发现需要推送的新数据。")

if __name__ == "__main__":
    run_engine()

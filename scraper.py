import os
import json
import time
import requests
import pandas as pd
from datetime import datetime

# ================= 全局机密 =================
# 从 GitHub Secrets 自动读取飞书机器人凭证
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
CONFIG_DIR = "configs"
# ============================================

def get_feishu_token():
    """获取飞书全局调用凭证"""
    if not FEISHU_APP_ID: return None
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        res = requests.post(url, json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}).json()
        return res.get("tenant_access_token")
    except:
        return None

def push_to_feishu(records, app_token, table_id):
    """根据传入的表信息，精准推送到对应的飞书表格"""
    if not app_token or not table_id:
        print("  [!] JSON 中未配置飞书目标表格，跳过推送。")
        return
        
    token = get_feishu_token()
    if not token:
        print("  [!] 飞书全局 Token 获取失败，请检查环境变量。")
        return
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    feishu_records = [{"fields": rec} for rec in records]
    
    try:
        res = requests.post(url, headers=headers, json={"records": feishu_records}).json()
        if res.get("code") == 0:
            print(f"  [√] 成功将 {len(records)} 条数据同步至指定的飞书表格！")
        else:
            print(f"  [x] 飞书推送报错 (请检查表头字段名): {res}")
    except Exception as e:
        print(f"  [x] 飞书推送网络异常: {e}")

def get_nested_data(data_dict, keys_list):
    """逐层安全提取 JSON 结构"""
    temp = data_dict
    for key in keys_list:
        if isinstance(temp, dict): temp = temp.get(key, {})
        else: return []
    return temp if isinstance(temp, list) else []

def run_engine():
    today_str = datetime.now().strftime("%Y%m%d")
    all_results = []
    
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
                    
                    site_results.append({
                        "项目类型": project_type,
                        "项目名称": title,
                        "发布时间": pub_date,
                        "公告类型": notice_desc
                        # "来源站点": site_name
                    })
                    
                page_no += 1
                if keep_running: time.sleep(1)
                
            except Exception as e:
                print(f"  [x] 抓取异常: {e}")
                break
                
        if site_results:
            print(f"  -> {site_name} 抓取到 {len(site_results)} 条今日数据。")
            all_results.extend(site_results)
            # 调用飞书推送 (从 JSON 中读取独立表信息)
            push_to_feishu(site_results, feishu_dest.get("app_token"), feishu_dest.get("table_id"))
        else:
            print(f"  -> {site_name} 今日无符合条件的新数据。")

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_excel(f"全站汇总更新_{today_str}.xlsx", index=False)
        print(f"\n[√] 引擎运行结束，共抓取 {len(all_results)} 条数据，总 Excel 已生成。")
    else:
        print("\n[!] 引擎运行结束，今日所有站点均无新数据。")

if __name__ == "__main__":
    run_engine()

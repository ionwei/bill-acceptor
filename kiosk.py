# /var/www/html/kiosk/kiosk.py (最終完整版 - 新增日誌清理)
import asyncio
import json
import base64
from aiohttp import web, ClientSession
from aiohttp.web import Response as web_Response
import websockets
import os
import sys
import mimetypes
import paho.mqtt.client as mqtt
import psutil
import socket
import serial
import serial.tools.list_ports
import requests
from datetime import datetime, timedelta
import csv
import shutil
import subprocess
import glob
import time

# --- 全域變數 ---
CONNECTED_CLIENTS = set()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG = {}
mqtt_message_queue = asyncio.Queue()
bill_acceptor = None
BILL_ACCEPTOR_PORT = None
BILL_ACCEPTOR_ENABLED = False
SETTINGS_PATH = os.path.join(BASE_DIR, 'kiosk_settings.json')
# --- 新增：日誌相關設定 ---
LOG_FILE_PATH = "/home/maho/kiosk_session.log"
MAX_LOG_SIZE_MB = 10 # 日誌檔案大小上限 (MB)
DEVICE_LOCK_STATE = "0"
QR_LOCk_STATE = "0"
CURRENT_API_URL = ""
device_id = None
hostname = None
# --- 新增：會員資訊 ---
current_member = None

# 交易日誌設定
LOG_DIR = os.path.join(BASE_DIR, "transaction_log")
os.makedirs(LOG_DIR, exist_ok=True)
SCREENSHOT_DIR = os.path.join(BASE_DIR, "screenshots")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# --- 硬體序列埠設定 ---
CH340_VID = 0x1a86; CH340_PID = 0x7523
CP210X_VID = 0x10c4; CP210X_PID = 0xea60
SERIAL_BAUDRATE = 9600

# --- 硬體狀態追蹤 ---
scanner_connected = False
acceptor_connected = False
bill_acceptor = None
BILL_ACCEPTOR_ENABLED = False

# --- 載入外部設定檔 ---
def load_config():
    global CONFIG
    config_path = os.path.join(BASE_DIR, 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f: CONFIG = json.load(f)
        print("[Info] config.json 設定檔已成功載入。")
        print(CONFIG)
    except Exception as e:
        print(f"[Fatal] 讀取設定檔時發生錯誤: {e}", file=sys.stderr)
        sys.exit(1)

# --- 手動提供檔案的函式 ---
async def handle_static_files(request):
    req_path = request.path
    if req_path == '/': req_path = '/index.html'
    abs_path = os.path.join(BASE_DIR, req_path.lstrip('/'))
    if not os.path.exists(abs_path) or not os.path.isfile(abs_path):
        return web_Response(text=f"<h1>404: 找不到檔案 {req_path}</h1>", status=404, content_type='text/html')
    try:
        content_type, _ = mimetypes.guess_type(abs_path)
        if content_type is None: content_type = 'application/octet-stream'
        with open(abs_path, 'rb') as f:
            return web_Response(body=f.read(), content_type=content_type)
    except Exception as e:
        print(f"[Fatal] 無法提供檔案 {req_path}: {e}", file=sys.stderr)
        return web_Response(text="<h1>500: 伺服器內部錯誤</h1>", status=500, content_type='text/html')

# 寫入交易日誌
def write_transaction_log(log_type, **kwargs):
    """
    統一日誌記錄函式，將所有記錄寫入同一個檔案
    
    Args:
        log_type: 日誌類型 ("LOGIN", "TRANSACTION", "DEPOSIT_COMPLETE")
        **kwargs: 根據不同類型傳入不同參數
    """
    today = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(LOG_DIR, f"{today}_unified.csv")  # 統一日誌檔案
    
    global CURRENT_API_URL

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 檢查檔案是否存在，如果不存在先寫表頭
    file_exists = os.path.exists(log_file)
    
    try:
        with open(log_file, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            if not file_exists:
                # 統一表頭，涵蓋所有可能的欄位
                writer.writerow([
                    "時間", "記錄類型", "會員帳號", "會員名稱", "USN", "PID", 
                    "交易ID", "金額", "狀態", "備註", "api_url"
                ])
                print(f"[LOG] 建立新的統一日誌檔案: {log_file}")
            
            if log_type == "LOGIN":
                # 會員登入記錄
                usn = kwargs.get('usn')
                pid = kwargs.get('pid')
                account = kwargs.get('account')
                name = kwargs.get('name')
                point = kwargs.get('point', 0)
                remark = f"會員 {account}({name}) 掃描QRCode登入"
                
                writer.writerow([
                    now, "會員登入", account, name, usn, pid, 
                    "", "", f'儲值前金額：{point}', remark, CURRENT_API_URL
                ])
                print(f"[LOG] 已記錄會員登入: {account}({name})")
                
            elif log_type == "TRANSACTION":
                # 交易明細記錄
                transaction_id = kwargs.get('transaction_id')
                usn = kwargs.get('usn')
                pid = kwargs.get('pid')
                amount = kwargs.get('amount')
                status = kwargs.get('status', 'SUCCESS')
                
                writer.writerow([
                    now, "交易明細", "", "", usn, pid, 
                    transaction_id, amount, status, f"儲值交易 {amount} 元", CURRENT_API_URL
                ])
                print(f"[LOG] 已記錄交易明細: {transaction_id}")
                
            elif log_type == "DEPOSIT_COMPLETE":
                # 儲值完成記錄
                usn = kwargs.get('usn')
                pid = kwargs.get('pid')
                account = kwargs.get('account')
                name = kwargs.get('name')
                total_amount = kwargs.get('total_amount', 0)
                point = kwargs.get('point', 0)
                remark = f"會員 {account}({name}) 完成儲值操作，本次總金額: {total_amount} 元，儲值後金額：{point}"
                
                writer.writerow([
                    now, "儲值完成", account, name, usn, pid, 
                    "", total_amount, "COMPLETED", remark, ""
                ])
                # 在儲值完成後加入空行
                writer.writerow([""] * 11)
                print(f"[LOG] 已記錄儲值完成: {account}({name}) - {total_amount}元")
                
            elif log_type == "QR_SCAN":
                # QRCode 掃描記錄
                qr_data = kwargs.get('qr_data')
                
                writer.writerow([
                    now, "QRCode 掃描", "", "", "", "", 
                    "", "", "", qr_data, ""
                ])
                print(f"[LOG] 已記錄 QRCode 掃描: {qr_data})")

            else:
                print(f"[ERROR] 未知的日誌類型: {log_type}")
                
    except Exception as e:
        print(f"[ERROR] 寫入統一日誌失敗: {e}")

# --- 保持原有調用方式兼容性的函式 ---
def write_original_transaction_log(transaction_id, usn, pid, amount, status="SUCCESS"):
    """
    保持原有的 write_transaction_log(transaction_id, usn, pid, amount, status) 調用方式
    """
    write_transaction_log("TRANSACTION", 
                         transaction_id=transaction_id, 
                         usn=usn, 
                         pid=pid, 
                         amount=amount, 
                         status=status)

# --- API 相關函式 ---
def get_device_id():
    try:
        did = CONFIG.get("device_id")
        if did:
            global device_id
            device_id = did
            return did
        else:
            print(f"⚠️ 警告: 無法從CONFIG獲取[device_id]。將使用預設值 '0000'。")
            return "機台"
    except Exception as e:
        print(f"❌ 獲取主機名稱時發生錯誤: {e}")
        return "0000"
    
device_id = get_device_id()

def api_get_member_info(usn, pid, did):
    """(fn=info) 同步執行會員驗證 API 呼叫"""
    api_url = CONFIG.get("api_base_url")
    params = {'fn': 'info', 'usn': usn, 'pid': pid, 'did': did}
    global CURRENT_API_URL
    try:
        print(f"[API] 正在呼叫會員驗證 API: {api_url} with params {params}")
        response = requests.get(api_url, params=params, timeout=10)
        CURRENT_API_URL = str(response.url)
        if response.status_code == 200:
            print("[API] 請求成功！")
            return response.json()
        else:
            print(f"[API] 請求失敗，狀態碼: {response.status_code}")
            return {"success": False, "message": f"API 錯誤 (Code: {response.status_code})"}
    except requests.exceptions.RequestException as e:
        print(f"[API] 網路請求時發生錯誤: {e}")
        return {"success": False, "message": "網路連線失敗"}

def api_recharge(usn, pid, did, amount):
    """(fn=recharge) 同步執行點數儲值 API 呼叫"""
    api_url = CONFIG.get("api_base_url")
    params = {'fn': 'recharge', 'usn': usn, 'pid': pid, 'did': did, 'recharge': amount}
    try:
        print(f"[API] 正在呼叫點數儲值 API: {api_url} with params {params}")
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code == 200:
            print("[API] 儲值請求成功！")
            return response.json()
        else:
            print(f"[API] 儲值請求失敗，狀態碼: {response.status_code}")
            return {"success": False, "message": f"API 錯誤 (Code: {response.status_code})"}
    except requests.exceptions.RequestException as e:
        print(f"[API] 網路請求時發生錯誤: {e}")
        return {"success": False, "message": "網路連線失敗"}

def api_recharge_allow(usn, pid, transaction_id, did):
    """(fn=recharge_allow) 同步執行儲值權限查詢 API 呼叫"""
    api_url = CONFIG.get("api_base_url")
    params = {'fn': 'recharge_allow', 'usn': usn, 'pid': pid, 'id': transaction_id, 'did': did}
    global CURRENT_API_URL
    try:
        print(f"[API] 正在呼叫儲值權限查詢 API: {api_url} with params {params}")
        response = requests.get(api_url, params=params, timeout=10)
        CURRENT_API_URL = str(response.url)
        if response.status_code == 200:
            print("[API] 儲值權限查詢成功！")
            return response.json()
        else:
            print(f"[API] 儲值權限查詢失敗，狀態碼: {response.status_code}")
            return {"success": False, "message": f"API 錯誤 (Code: {response.status_code})"}
    except requests.exceptions.RequestException as e:
        print(f"[API] 網路請求時發生錯誤: {e}")
        return {"success": False, "message": "網路連線失敗"}

def api_get_transaction_detail(usn, pid, strDate, endDate, did):
    api_url = CONFIG.get("api_base_url")
    params = {'fn': 'get_transaction_detail', 'usn': usn, 'pid': pid, 'strDate': strDate, 'endDate': endDate, 'did': did}
    try:
        print(f"[API] 正在呼叫交易明細查詢 API: {api_url} with params {params}")
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code == 200:
            print("[API] 交易明細查詢成功！")
            return response.json()
        else:
            print(f"[API] 交易明細查詢失敗，狀態碼: {response.status_code}")
            return {"success": False, "message": f"API 錯誤 (Code: {response.status_code})"}
    except requests.exceptions.RequestException as e:
        print(f"[API] 網路請求時發生錯誤: {e}")
        return {"success": False, "message": "網路連線失敗"}
    
# --- QRCode 資料處理函式 (共用邏輯) ---
async def handle_qr_code_data(qr_string):
    global DEVICE_LOCK_STATE, current_member, QR_LOCk_STATE
    if DEVICE_LOCK_STATE == "1" or QR_LOCk_STATE == "1":
        return
    
    # 如果有前一個會員，清除狀態
    if current_member:
        print(f"[Info] 清除前一個會員資訊: {current_member.get('acc')}")
        current_member = None
        disable_bill_acceptor()
    
    """解析 QRCode 字串，驗證會員並廣播結果"""
    print(f"[QRCode] 正在處理資料: {qr_string}")
    try:
        qr_data = json.loads(qr_string)
        print(qr_data)
        if qr_data.get("type") == "user" and qr_data.get("usn") and qr_data.get("pid"):
            usn, pid, did = qr_data.get("usn"), qr_data.get("pid"), get_device_id()
            loop = asyncio.get_running_loop()
            member_data = await loop.run_in_executor(None, api_get_member_info, usn, pid, did)

            if member_data and member_data.get("pld"):
                valid_data = member_data["pld"]
                valid_data.update({'usn': usn, 'pid': pid})
                print(valid_data)
                
                # 直接儲存到全域變數
                current_member = valid_data
                
                account = valid_data.get('acc', 'N/A')
                name = valid_data.get('name', 'N/A')
                point = valid_data.get('point', 0)
                write_transaction_log("LOGIN", usn=usn, pid=pid, account=account, name=name, point=point)
                await broadcast({"event": "member_info_valid", "data": valid_data})
                QR_LOCk_STATE = "1" 
                await asyncio.sleep(0.5)
            else:
                await broadcast({"event": "member_info_invalid", "message": member_data.get("message", "無效的會員資料")})

        elif qr_data.get("strDate") and qr_data.get("endDate") and qr_data.get("usn") and qr_data.get("pid"):
            usn, pid, strDate, endDate, did = qr_data.get("usn"), qr_data.get("pid"), qr_data.get("strDate"), qr_data.get("endDate"), get_device_id()
            loop = asyncio.get_running_loop()
            transaction_detail_data = await loop.run_in_executor(None, api_get_transaction_detail, usn, pid, strDate, endDate, did)
            print(transaction_detail_data)
            if transaction_detail_data and transaction_detail_data.get("pld"):
                transaction_data = transaction_detail_data["pld"]
                QR_LOCk_STATE = "1"
                await broadcast({"event": "transaction_detail", "data": transaction_data})
            elif transaction_detail_data["code"] == -12 :
                await broadcast({"event": "none_transaction_detail", "message": "查無此會員，請確認會員資料"})                
            else:
                await broadcast({"event": "none_transaction_detail", "message": transaction_detail_data.get("msg", "查無交易明細")})
        
        elif qr_data.get("type") == "kiosk_config":
            config_content = {
                k: base64.b64decode(v).decode("utf-8") if k != "type" else v
                for k, v in qr_data.items()
            }
            print(config_content)
            try:
                global CONFIG
                config_content.pop("type", None)
                CONFIG.update(config_content)
                with open(os.path.join(BASE_DIR, 'config.json'), 'w', encoding='utf-8') as f:
                    json.dump(CONFIG, f, ensure_ascii=False, indent=4)
                await broadcast({"event": "config_update_success", "message": "設定已更新"})
                print("[Config] 設定檔已更新")
            except Exception as e:
                print(f"[Config] 無法寫入設定檔: {e}")
                await broadcast({"event": "config_update_failed", "message": "無法寫入設定檔，請確認權限設定。"})

        else:
            await broadcast({"event": "member_info_invalid", "message": "無效的 QRCode 格式"})
    except (json.JSONDecodeError, TypeError):
        await broadcast({"event": "member_info_invalid", "message": "QRCode 解析失敗backend"})
    except Exception as e:
        print(f"[QRCode] 處理時發生未知錯誤: {e}")
        await broadcast({"event": "member_info_invalid", "message": "處理 QRCode 時發生錯誤"})

# 模擬
async def simulate_handle_qr_code_data(usn, pid, strDate, endDate, did):
    loop = asyncio.get_running_loop()
    transaction_detail_data = await loop.run_in_executor(None, api_get_transaction_detail, usn, pid, strDate, endDate, did)
    print(transaction_detail_data)
    if transaction_detail_data and transaction_detail_data.get("pld"):
        transaction_data = transaction_detail_data["pld"]
        await broadcast({"event": "transaction_detail", "data": transaction_data})
    elif transaction_detail_data["code"] == -12 :
        await broadcast({"event": "none_transaction_detail", "message": "查無此會員，請確認會員資料"})                
    else:
        await broadcast({"event": "none_transaction_detail", "message": transaction_detail_data.get("msg", "查無交易明細")})


# 儲存截圖
USER_ID = "1000"  # 您的使用者 ID
def capture_and_save(filename = None):
    now = datetime.now()

    # 建立日期資料夾 (YYYYMMDD)
    date_folder = os.path.join(SCREENSHOT_DIR, now.strftime("%Y%m%d"))
    os.makedirs(date_folder, exist_ok=True)

    # 檔名 = 時間 (HHMMSS.jpg)
    if not filename:
        filename = now.strftime("%H%M%S") + ".png"
    filepath = os.path.join(date_folder, filename)

    command = ["grim", filepath]

    my_env = os.environ.copy()
    my_env["XDG_RUNTIME_DIR"] = f"/run/user/{USER_ID}"
    # 根據您的測試結果，修正為 'wayland-1'
    my_env["WAYLAND_DISPLAY"] = "wayland-1"

    try:
        result = subprocess.run(
            command, 
            env=my_env, 
            check=True,
            capture_output=True,
            text=True
        )
        print("[Screenshot] 畫面擷取成功！")
        if result.stderr:
            print(f"[Screenshot] {result.stderr}")

    except FileNotFoundError:
        print("錯誤: 'grim' 指令不存在。")
    except subprocess.CalledProcessError as e:
        print(f"[Screenshot] 擷取畫面時 'grim' 指令執行失敗。")
        print(f"[Screenshot] {e.stderr}")

# 定期整理截圖檔案夾
def clean_old_screenshot_folders(days=7):
    if not os.path.exists(SCREENSHOT_DIR):
        return

    now = datetime.now()
    cutoff = now - timedelta(days=days)

    for folder_name in os.listdir(SCREENSHOT_DIR):
        folder_path = os.path.join(SCREENSHOT_DIR, folder_name)
        if os.path.isdir(folder_path):
            try:
                folder_date = datetime.strptime(folder_name, "%Y%m%d")
            except ValueError:
                continue

            if folder_date < cutoff:
                shutil.rmtree(folder_path)
                print(f"[{now}] 已刪除舊資料夾: {folder_path}")

# --- WebSocket 核心邏輯 ---
async def websocket_handler(websocket):
    CONNECTED_CLIENTS.add(websocket)
    print(f"[Info] 瀏覽器已連接。")
    try:
        async for message in websocket:
            print(f"[Recv] 收到來自瀏覽器的訊息: {message}")
            try:
                data = json.loads(message)
                action = data.get("action")
                loop = asyncio.get_running_loop()
                did = get_device_id()

                if action == "deposit_complete":
                    global QR_LOCk_STATE, current_member
                    total_deposit_data = data.get("data")
                    pid = total_deposit_data.get("pid")
                    points = total_deposit_data.get("points", 0)
                    points = int(points.replace(",", ""))
                    
                    # 1. 基本驗證：檢查 current_member
                    if not current_member or current_member.get('pid') != pid:
                        print(f"[Error] 會員驗證失敗: 期望 {pid}, 實際 {current_member.get('pid') if current_member else 'None'}")
                        await broadcast({"event": "recharge_failed", "message": "會員驗證失敗，請重新掃描QRCode"})
                        return
                    
                    # 2. 重新驗證會員資料（與 deposit_update 相同）
                    try:
                        usn = current_member.get('usn')
                        did = get_device_id()
                        fresh_member_data = await loop.run_in_executor(None, api_get_member_info, usn, pid, did)
                        
                        if not fresh_member_data or not fresh_member_data.get("pld"):
                            print(f"[Error] 完成儲值前會員資料驗證失敗")
                            await broadcast({"event": "recharge_failed", "message": "會員資料驗證失敗，請重新掃描QRCode"})
                            current_member = None
                            disable_bill_acceptor()
                            return
                        
                        fresh_data = fresh_member_data["pld"]
                        if (fresh_data.get('acc') != current_member.get('acc') or 
                            fresh_data.get('name') != current_member.get('name')):
                            print(f"[Error] 完成儲值時會員資料已變更")
                            await broadcast({"event": "recharge_failed", "message": "會員資料已變更，請重新掃描QRCode"})
                            current_member = None
                            disable_bill_acceptor()
                            return
                    
                    except Exception as e:
                        print(f"[Error] 完成儲值前驗證發生錯誤: {e}")
                        await broadcast({"event": "recharge_failed", "message": "系統驗證失敗，請重試"})
                        current_member = None
                        disable_bill_acceptor()
                        return
                    
                    # 3. 驗證通過後執行完成邏輯
                    account = current_member.get('acc', 'N/A')
                    name = current_member.get('name', 'N/A')
                    usn = current_member.get('usn')
                    
                    # 使用最新的會員資料計算
                    original_points = current_member.get('point', 0)  # 使用最新資料
                    new_points = fresh_data.get('point', 0)  # 使用最新資料
                    total_deposit = new_points - original_points
                    
                    write_transaction_log("DEPOSIT_COMPLETE", 
                                        usn=usn, 
                                        pid=pid, 
                                        account=account, 
                                        name=name,
                                        total_amount=total_deposit,
                                        point = new_points)
                    if mqtt_client and mqtt_client.is_connected():
                        mqtt_publish_topic = f"node/player/{pid}/info"
                        mqtt_data = {"points": new_points}
                        mqtt_client.publish(mqtt_publish_topic, json.dumps(mqtt_data))
                        print(f"[MQTT] 已發送到 {mqtt_publish_topic}: {mqtt_data}")
                    else:
                        print("[Error] MQTT 未初始化，無法發佈訊息")
                    
                    # 清除會員資訊
                    current_member = None
                    QR_LOCk_STATE = "0"
                    disable_bill_acceptor()
                    await broadcast({"event": "deposit_finalized", "message": "儲值完成！感謝您的使用。"})

                elif action == "indentity_confirm_timeout":
                    QR_LOCk_STATE = "0"
                    current_member = None
                    disable_bill_acceptor()

                elif action == "deposit_update":
                    deposit_data = data.get("data")
                    if deposit_data:
                        usn = deposit_data.get("usn")
                        pid = deposit_data.get("pid")
                        amount = deposit_data.get("total_amount")
                        
                        # 1. 比對current_member跟前端傳來的會員資料
                        if not current_member or not (current_member.get('usn') == usn and current_member.get('pid') == pid):
                            print(f"[Error] 儲值時會員驗證失敗: 期望 {usn}/{pid}, 實際 {current_member}")
                            await broadcast({"event": "recharge_failed", "message": "會員身份驗證失敗，儲值已取消"})
                            current_member = None
                            disable_bill_acceptor()
                            return
                        
                         # 2. 再請求一次API跟current_member比對
                        try:
                            did = get_device_id()
                            fresh_member_data = await loop.run_in_executor(None, api_get_member_info, usn, pid, did)
                            
                            if not fresh_member_data or not fresh_member_data.get("pld"):
                                print(f"[Error] 儲值前會員資料驗證失敗")
                                await broadcast({"event": "recharge_failed", "message": "會員資料驗證失敗，請重新掃描QRCode"})
                                current_member = None
                                disable_bill_acceptor()
                                return
                            
                            fresh_data = fresh_member_data["pld"]
                            if (fresh_data.get('acc') != current_member.get('acc') or 
                                fresh_data.get('name') != current_member.get('name')):
                                print(f"[Error] 會員資料已變更")
                                await broadcast({"event": "recharge_failed", "message": "會員資料已變更，請重新掃描QRCode"})
                                current_member = None
                                disable_bill_acceptor()
                                return
  
                        except Exception as e:
                            print(f"[Error] 儲值前驗證發生錯誤: {e}")
                            await broadcast({"event": "recharge_failed", "message": "系統驗證失敗，請重試"})
                            current_member = None
                            disable_bill_acceptor()
                            return
                        
                        # 3. 驗證通過後執行儲值
                        if usn and pid and amount > 0:
                            recharge_response = await loop.run_in_executor(None, api_recharge, usn, pid, did, amount)
                            print(f"[API] 儲值 API 回應: {recharge_response}")
                            
                            transaction_id = recharge_response.get('pld', {}).get('id')
                            if transaction_id:
                                print(f"[Logic] 取得交易 ID: {transaction_id}，準備確認儲值。")
                                allow_response = await loop.run_in_executor(None, api_recharge_allow, usn, pid, transaction_id, did)
                                print(f"[API] 儲值確認 API 回應: {allow_response}")
                                
                                if allow_response and allow_response.get("pld"):
                                    print(f"[Logic] 儲值成功。")
                                    write_original_transaction_log(transaction_id, usn, pid, amount, "SUCCESS")
                                    capture_and_save(f"{transaction_id}.png")
                                else:
                                    error_message = allow_response.get("message", "儲值驗證失敗，請聯繫客服。")
                                    await broadcast({"event": "recharge_failed", "message": error_message})
                                    print(f"[Logic] 儲值失敗，自動呼叫客服。")
                                    write_original_transaction_log(transaction_id, usn, pid, amount, "FAILED")
                                    current_member = None
                            else:
                                print("[Error] 儲值 API 未回傳交易 ID。")
                                await broadcast({"event": "recharge_failed", "message": "儲值失敗，無法取得交易序號。"})
                                current_member = None
                        else:
                            print("[Logic] 儲值金額為 0，不執行 API 呼叫。")
                            await broadcast({"event": "deposit_finalized", "message": "儲值完成！"})
                            current_member = None

                elif action == "bill_inserted_simulate":
                    bill_data = data.get("data")
                    amount = bill_data.get("amount")
                    if amount and amount > 0:
                        # 這裡直接模擬硬體的 broadcast
                        await broadcast({
                            "event": "bill_inserted",
                            "amount": amount
                        })
                
                elif action == "get_initial_ads":
                    print("[Logic] 收到前端請求，正在獲取目前廣告列表...")
                    try:
                        settings = {}
                        if os.path.exists(SETTINGS_PATH):
                            with open(SETTINGS_PATH, 'r', encoding='utf-8') as f: settings = json.load(f)
                        
                        durations = settings.get("Durations", {})
                        marquee_text = settings.get("MarqueeText", "歡迎光臨！")
                        
                        ad_list = []
                        for i in range(1, 6):
                            found_files = [f for f in os.listdir(BASE_DIR) if f.startswith(str(i) + '.')]
                            if found_files:
                                ad_list.append({"file": found_files[0], "duration": durations.get(str(i), 10)})
                        
                        await websocket.send(json.dumps({"event": "initial_data", "ads": ad_list, "marqueeText": marquee_text}))
                    except Exception as e:
                        print(f"[Error] 獲取初始資料時發生錯誤: {e}", file=sys.stderr)
                
                elif action == "user_validated":
                    enable_bill_acceptor()
                elif action == "get_device_lock_status":
                    await broadcast({"event": "device_shift_lock", "data": DEVICE_LOCK_STATE})
                elif action == "close_transaction_modal":
                    QR_LOCk_STATE = "0"
                elif action == "get_transaction_screenshot":
                    index_no = data.get("data")["IndexNo"]
                    row_date = data.get("data")["Date"]
                    now = datetime.now()
                    dt = dt = datetime.strptime(row_date, "%Y-%m-%d %H:%M:%S")

                    if (now - dt).days > 7:
                        await broadcast({"event": "none_transaction_detail", "message": "欲查詢的日期請設定在七日內"})
                        continue    
                    
                    dt = dt.strftime("%Y%m%d")
                    date_folder = os.path.join(SCREENSHOT_DIR, dt)
                    file_path = os.path.join(date_folder, f"{index_no}.png")

                    if not os.path.exists(file_path):
                        await broadcast({"event": "none_transaction_detail", "message": "查無此交易截圖"})
                        continue

                    with open(file_path, "rb") as f:
                        img_bytes = f.read()
                        img_b64 = base64.b64encode(img_bytes).decode()
                        # print(img_b64)
                        await websocket.send(json.dumps({"event": "screenshot_path", "image": f"data:image/png;base64,{img_b64}"}))

                elif action == "screenshot":
                    capture_and_save()

                elif action == "simulate_transaction_detail":
                    qr_data = data.get("data")
                    await handle_qr_code_data(qr_data)
                elif action == "simulate_show_deposit_modal":
                    qr_data = data.get("data")
                    await handle_qr_code_data(qr_data)
                    
            except Exception as e:
                print(f"[Error] 處理訊息時發生錯誤: {e}", file=sys.stderr)
    finally:
        CONNECTED_CLIENTS.remove(websocket)
        print("[Info] 一個瀏覽器已斷開連接。")

# --- 序列埠掃描器 (非阻塞修正版) ---
def find_ch340_port():
    ports = serial.tools.list_ports.comports()
    for port in ports:
        if port.vid == CH340_VID and port.pid == CH340_PID: return port.device
    return None

async def qrcode_scanner_loop():
    global scanner_connected, DEVICE_LOCK_STATE, QR_LOCk_STATE
    loop = asyncio.get_running_loop()
    ser = None
    while True:
        try:
            if ser is None or not ser.is_open:
                port_name = await loop.run_in_executor(None, find_ch340_port)
                if port_name:
                    ser = serial.Serial(port=port_name, baudrate=SERIAL_BAUDRATE, timeout=1)
                    # 清空緩衝區
                    ser.reset_input_buffer()
                    if not scanner_connected:
                        await send_system_notification("QRCode 掃描器已連接", "success")
                        scanner_connected = True
                else: await asyncio.sleep(5); continue
            line = await loop.run_in_executor(None, ser.readline)
            if line and DEVICE_LOCK_STATE == "0" and QR_LOCk_STATE == "0":
                if decoded_line := line.decode('utf-8').strip():
                    write_transaction_log("QR_SCAN", qr_data=decoded_line)
                    await handle_qr_code_data(decoded_line)
                    # 讀取完畢後清空緩衝區，避免殘留資料
                    ser.reset_input_buffer()
        except serial.SerialException:
            if ser and ser.is_open: ser.close()
            ser = None
            if scanner_connected:
                await send_system_notification("QRCode 掃描器連線中斷", "error")
                scanner_connected = False
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[Scanner] 監聽迴圈發生未知錯誤: {e}", file=sys.stderr)
            await asyncio.sleep(5)

# --- 通用通知函式 ---
async def broadcast(message_payload):
    if CONNECTED_CLIENTS:
        message_str = json.dumps(message_payload)
        await asyncio.gather(*[client.send(message_str) for client in CONNECTED_CLIENTS])

async def send_system_notification(message, level='info'):
    """標準化發送系統通知給前端"""
    print(f"[Notify:{level.upper()}] {message}")
    await broadcast({"event": "system_notification", "message": message, "level": level})

# --- 非同步下載廣告的任務 ---
async def download_ads_task(ad_list):
    print(f"[Downloader] 開始下載廣告，收到的資料: {ad_list}")
    media_download_url_base = CONFIG.get("media_download_url_base", "")
    if not media_download_url_base:
        print("[Error] 設定檔 config.json 中缺少 'media_download_url_base'，無法下載檔案。", file=sys.stderr)
        return

    total_files = len(ad_list)
    await broadcast({"event": "download_start", "total": total_files})
    
    new_durations = {ad['file'].split('.')[0]: ad['duration'] for ad in ad_list if 'file' in ad and 'duration' in ad}
    
    settings = {}
    if os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH, 'r', encoding='utf-8') as f: settings = json.load(f)
    settings["Durations"] = new_durations
    
    try:
        with open(SETTINGS_PATH, 'w', encoding='utf-8') as f: json.dump(settings, f, indent=4)
        print("[Downloader] 已更新 kiosk_settings.json")
    except Exception as e:
        print(f"[Error] 寫入 kiosk_settings.json 時發生錯誤: {e}", file=sys.stderr)

    filenames_to_keep = {ad['file'] for ad in ad_list}
    
    current_ad_files = []
    for i in range(1, 6):
        found = [f for f in os.listdir(BASE_DIR) if f.startswith(str(i) + '.')]
        if found: current_ad_files.extend(found)

    for local_file in current_ad_files:
        if local_file not in filenames_to_keep:
            try:
                os.remove(os.path.join(BASE_DIR, local_file))
                print(f"[Downloader] 已刪除過期檔案: {local_file}")
            except Exception as e:
                print(f"[Error] 刪除檔案 {local_file} 時發生錯誤: {e}", file=sys.stderr)
    
    downloaded_count = 0
    async with ClientSession() as session:
        for ad in ad_list:
            filename = ad['file']
            url = media_download_url_base + filename
            save_path = os.path.join(BASE_DIR, filename)
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(save_path, 'wb') as f: f.write(await response.read())
                        downloaded_count += 1
                        print(f"[Downloader] 成功下載: {filename} ({downloaded_count}/{total_files})")
                        await broadcast({"event": "download_progress", "file": filename, "count": downloaded_count, "total": total_files})
                    else:
                        print(f"[Error] 下載失敗 (狀態碼 {response.status}): {url}", file=sys.stderr)
            except Exception as e:
                print(f"[Error] 下載時發生網路錯誤 ({url}): {e}", file=sys.stderr)
    
    print("[Downloader] 所有廣告下載完成。")
    await broadcast({"event": "reload_ads", "ads": ad_list})

# --- 獲取系統資訊的函式 ---
def get_system_info():
    global hostname
    try:
        # s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.connect(("8.8.8.8", 80)); ip = s.getsockname()[0]; s.close()
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            internal_ip = s.getsockname()[0]
        last_three = internal_ip.split('.')[-1].zfill(3)
        hostname = socket.gethostname()
        ip = f"{hostname}-{last_three}"
    except Exception: ip = "N/A"
    try:
        temp = "N/A"
        for zone_path in glob.glob("/sys/class/thermal/thermal_zone*"):
            try:
                with open(f"{zone_path}/type") as f_type:
                    type_name = f_type.read().strip().lower()
                if "cpu" in type_name:  # 找 CPU zone
                    with open(f"{zone_path}/temp") as f_temp:
                        temp_milli = int(f_temp.read().strip())
                    temp = f"{temp_milli / 1000:.1f}"  # 轉成 °C
                    break
            except FileNotFoundError:
                continue
    except Exception: temp = "N/A"
    return { "ip": ip, "cpu_usage": psutil.cpu_percent(), "disk_usage": psutil.disk_usage('/').percent, "ram_usage": psutil.virtual_memory().percent, "cpu_temp": temp, "device_id" : CONFIG.get("device_id") }

# --- 定期回報系統資訊的背景任務 ---
async def system_info_task():
    while True:
        info = get_system_info()
        await broadcast({"event": "system_info", "data": info})
        await asyncio.sleep(5)

# --- 新增：日誌清理背景任務 ---
async def log_cleanup_task():
    """啟動時清理 LOG_DIR 下超過兩個月的日誌檔案"""
    RETAIN_DAYS = 60  # 近兩個月
    try:
        now = datetime.now()
        for filename in os.listdir(LOG_DIR):
            file_path = os.path.join(LOG_DIR, filename)
            if os.path.isfile(file_path):
                mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if now - mtime > timedelta(days=RETAIN_DAYS):
                    try:
                        os.remove(file_path)
                        print(f"[LogCleanup] 刪除過期日誌: {filename}")
                    except Exception as e:
                        print(f"[Error] 刪除 {filename} 失敗: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[Error] 清理日誌時發生錯誤: {e}", file=sys.stderr)

# --- MQTT 訊息佇列處理器 ---
async def mqtt_message_processor():
    """從佇列中取出 MQTT 訊息並在主執行緒中處理"""

    # --- 一次性計算好所有主題 ---
    dealer_sn = str(CONFIG.get("dealer_sn"))
    mqtt_topic_ads_base = CONFIG.get("mqtt_topic_ads", "kiosk/ads/update") + "/" + dealer_sn
    mqtt_topic_marquee_base = CONFIG.get("mqtt_topic_marquee", "kiosk/marquee/set") + "/" + dealer_sn
    mqtt_topic_notify_base = CONFIG.get("mqtt_topic_notify", "kiosk/system/notify") + "/" + dealer_sn
    mqtt_topic_updatePort_base = CONFIG.get("mqtt_topic_updatePort", "kiosk/port/update")
    mqtt_topic_updatePort = f"{mqtt_topic_updatePort_base}/{hostname}/{dealer_sn}"
    device_id = CONFIG.get("device_id", 6223)
    mqtt_topic_device_control = f"node/dealer/{dealer_sn}/shift-locked"

    # 修正：使用 list 而不是 tuple，並加入更多可能的主題格式
    TOPICS = {
        "ads": [
            # f"{mqtt_topic_ads_base}/port:{device_id}",  # 特定端口
            mqtt_topic_ads_base                           # 廣播
        ],
        "marquee": [
            # f"{mqtt_topic_marquee_base}/port:{device_id}",
            mqtt_topic_marquee_base
        ],
        "notify": [
            # f"{mqtt_topic_notify_base}/port:{device_id}",
            mqtt_topic_notify_base
        ],
        "device":[
            mqtt_topic_device_control
        ],
        "updatePort":[
            mqtt_topic_updatePort
        ]
    }

    while True:
        topic, payload_str = await mqtt_message_queue.get()
        print(f"[Processor] 從佇列中取得訊息: topic='{topic}', payload='{payload_str[:100]}...'")
        
        try:
            # 判斷 Ads 訊息 - 修正判斷邏輯
            if topic in TOPICS["ads"]:
                print(f"[MQTT] 處理廣告更新訊息...")
                try:
                    ad_list = json.loads(payload_str)
                    if isinstance(ad_list, list):
                        print(f"[MQTT] 收到 {len(ad_list)} 個廣告檔案")
                        await download_ads_task(ad_list)
                    else:
                        print(f"[MQTT] 廣告資料格式錯誤，預期為 list，收到: {type(ad_list)}")
                except json.JSONDecodeError as e:
                    print(f"[Error] 解析廣告 JSON 時發生錯誤: {e}")

            # 判斷 Marquee 訊息
            elif topic in TOPICS["marquee"]:
                print(f"[MQTT] 處理跑馬燈訊息: '{payload_str}'")
                try:
                    settings = {}
                    if os.path.exists(SETTINGS_PATH):
                        with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                            settings = json.load(f)
                    settings["MarqueeText"] = payload_str
                    with open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(settings, f, indent=4)
                    print(f"[MQTT] 已將跑馬燈文字儲存至 kiosk_settings.json")
                except Exception as e:
                    print(f"[Error] 儲存跑馬燈文字時發生錯誤: {e}", file=sys.stderr)
                await broadcast({"event": "update_marquee", "text": payload_str})

            # 判斷 Notify 訊息
            elif topic in TOPICS["notify"]:
                print(f"[MQTT] 處理通知訊息: '{payload_str}'")
                await broadcast({"event": "system_notification", "message": payload_str})
            
            elif topic in TOPICS["device"]:
                global DEVICE_LOCK_STATE
                DEVICE_LOCK_STATE = payload_str
                if payload_str == "1": disable_bill_acceptor()
                await broadcast({"event": "device_shift_lock", "data": DEVICE_LOCK_STATE})
            elif topic in TOPICS["updatePort"]:
                CONFIG["device_id"] = payload_str
                with open(os.path.join(BASE_DIR, 'config.json'), 'w', encoding='utf-8') as f:
                    json.dump(CONFIG, f, ensure_ascii=False, indent=4)
                get_device_id()
                print(f"[Update device_id]:已將機台名稱更新為 {payload_str}")
                if mqtt_client and mqtt_client.is_connected():
                    mqtt_data = {"status":"online", "did": payload_str}
                    mqtt_client.publish(f"device/{hostname}/status",  json.dumps(mqtt_data, ensure_ascii=False), retain=True)
                    print(f"[MQTT Publish] 發布主題：device/{hostname}/status, 發布訊息：{mqtt_data}")
                else:
                    print("[Error] MQTT 未初始化，無法發佈訊息")
            else:
                print(f"[MQTT] 收到未知主題的訊息: {topic}")

        except Exception as e:
            print(f"[Error] 處理佇列訊息時發生錯誤: {e}", file=sys.stderr)
        finally:
            mqtt_message_queue.task_done()

# --- MQTT 客戶端邏輯 ---
def on_mqtt_message(client, userdata, msg):
    """回呼函式：只負責將訊息放入佇列"""
    try:
        payload_str = msg.payload.decode("utf-8")
        loop = userdata['loop']
        loop.call_soon_threadsafe(mqtt_message_queue.put_nowait, (msg.topic, payload_str))
    except Exception as e:
        print(f"[Error] 放入佇列時發生錯誤: {e}", file=sys.stderr)

def wait_for_dns(hostname, timeout=30, retry_interval=2):
    """等待 DNS 解析可用"""
    print(f"[MQTT] 檢查 DNS 解析: {hostname}")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            socket.gethostbyname(hostname)
            print(f"[MQTT] DNS 解析成功")
            return True
        except socket.gaierror:
            print(f"[MQTT] DNS 尚未就緒，{retry_interval}秒後重試...")
            time.sleep(retry_interval)
    
    print(f"[MQTT] DNS 解析超時 ({timeout}秒)")
    return False

def setup_mqtt_client(loop):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    device_id = CONFIG.get("device_id", "機台")
    mqtt_data = {"status":"offline", "did": device_id}
    client.will_set(f"device/{hostname}/status", json.dumps(mqtt_data, ensure_ascii=False), retain=True)
    client.user_data_set({'loop': loop})
    client.on_message = on_mqtt_message
    client.on_disconnect = on_disconnect

    dealer_sn = str(CONFIG.get("dealer_sn"))
    mqtt_topic_ads_base = CONFIG.get("mqtt_topic_ads", "kiosk/ads/update") + "/" + dealer_sn
    mqtt_topic_marquee_base = CONFIG.get("mqtt_topic_marquee", "kiosk/marquee/set") + "/" + dealer_sn
    mqtt_topic_notify_base = CONFIG.get("mqtt_topic_notify", "kiosk/system/notify") + "/" + dealer_sn
    mqtt_topic_updatePort_base = CONFIG.get("mqtt_topic_updatePort", "kiosk/port/update")
    mqtt_topic_updatePort = f"{mqtt_topic_updatePort_base}/{hostname}/{dealer_sn}"
    mqtt_topic_device_control = f"node/dealer/{dealer_sn}/shift-locked"
    print(mqtt_topic_device_control)
    
    def on_connect(client, userdata, flags, reason_code, properties):
        """VERSION2 格式 - reason_code 是物件而非整數"""
        # 檢查是否連接失敗
        if reason_code.is_failure:
            print(f"[Error] MQTT 連接失敗: {reason_code}", file=sys.stderr)
            return
        
        # 連接成功
        # 裝置上限通知
        mqtt_data = {"status":"online", "did": device_id}
        client.publish(f"device/{hostname}/status",  json.dumps(mqtt_data, ensure_ascii=False), retain=True)
        print("[MQTT] 成功連接到 Broker")
        
        topics_to_subscribe = [
            (mqtt_topic_ads_base, 0),
            (mqtt_topic_marquee_base, 0),
            (mqtt_topic_notify_base, 0),
            (mqtt_topic_device_control, 0),
            (mqtt_topic_updatePort, 0)
        ]
        
        client.subscribe(topics_to_subscribe)
        print(f"[MQTT] 已訂閱主題: {[t[0] for t in topics_to_subscribe]}")
    
    client.on_connect = on_connect
    
    # 加入更詳細的除錯資訊
    def on_message_debug(client, userdata, msg):
        print(f"[MQTT Debug] 收到訊息 - 主題: '{msg.topic}', 內容長度: {len(msg.payload)} bytes")
        on_mqtt_message(client, userdata, msg)
    
    client.on_message = on_message_debug
    
    try:
        broker = CONFIG.get("mqtt_broker", "nmtw.lajioo.com")
        port = CONFIG.get("mqtt_port", 1883)
        user = CONFIG.get("mqtt_user")
        password = CONFIG.get("mqtt_password")

        # 等待 DNS 就緒
        if not wait_for_dns(broker, timeout=60, retry_interval=3):
            raise Exception(f"無法解析主機名稱: {broker}")
              
        if user and password: 
            client.username_pw_set(user, password)
            print(f"[MQTT] 使用認證連接到 {broker}:{port}")
        else:
            print(f"[MQTT] 匿名連接到 {broker}:{port}")
            
        client.connect(broker, port, 60)
        client.loop_start()
        print("[MQTT] 客戶端已啟動")
    except Exception as e:
        print(f"[Error] 無法啟動 MQTT 客戶端: {e}", file=sys.stderr)
        try:
            client.loop_start()
        except:
            pass
    return client

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    print(f"[MQTT] 斷線（reason_code={reason_code}），嘗試重新連線中...")
    loop = userdata.get('loop')
    if loop:
        asyncio.run_coroutine_threadsafe(reconnect_mqtt_loop(client, loop), loop)
    else:
        print("[MQTT] 找不到 asyncio loop，無法執行重連。")

# --- 重新連線邏輯 ---
async def reconnect_mqtt_loop(client, loop):
    delay = 1  # 初始延遲 
    MAX_DELAY = 120 # 最大延遲 (兩分鐘)
    
    print("[MQTT] 重連循環已啟動...")
    
    while True:
        try:
            # 使用 run_in_executor 來避免在 asyncio loop 中阻塞
            await loop.run_in_executor(None, sync_reconnect, client)
            print("[MQTT] 已成功重新連線並自動重新訂閱")
            # 重新啟動背景 loop
            client.loop_start()
            return 
        except Exception as e:
            print(f"[Error] MQTT 重連失敗 (將在 {delay} 秒後重試): {e}")
            await asyncio.sleep(delay)
            # 嘗試重新連現實間，指數退避
            delay = min(delay * 2, MAX_DELAY)

# 將 client.reconnect 包裝成一個同步函式，供 executor 呼叫
def sync_reconnect(client):
    """同步地呼叫 MQTT 客戶端的 reconnect 方法"""
    client.reconnect()

# --- 紙鈔機相關函式 ---
def find_cp210x_port():
    ports = serial.tools.list_ports.comports()
    for port in ports:
        if port.vid == CP210X_VID and port.pid == CP210X_PID: return port.device
    return None

def send_command(command):
    if bill_acceptor and bill_acceptor.is_open:
        try:
            bill_acceptor.write(bytes([command]))
            print(f"[Hardware] 已發送指令: {hex(command)}")
        except Exception as e:
            print(f"[Error] 發送指令時發生錯誤: {e}", file=sys.stderr)

def enable_bill_acceptor():
    global BILL_ACCEPTOR_ENABLED
    print("[Hardware] 啟用紙鈔機...")
    send_command(0x3E)
    BILL_ACCEPTOR_ENABLED = True

def disable_bill_acceptor():
    global BILL_ACCEPTOR_ENABLED
    print("[Hardware] 禁用紙鈔機...")
    send_command(0x5E)
    BILL_ACCEPTOR_ENABLED = False

def parse_bill_value(value_byte):
    return { 0x40: 100, 0x41: 200, 0x42: 500, 0x43: 1000, 0x44: 2000 }.get(value_byte, 0)

async def bill_acceptor_loop():
    global bill_acceptor, acceptor_connected, current_member
    loop = asyncio.get_running_loop()
    while True:
        try:
            if bill_acceptor is None or not bill_acceptor.is_open:
                port_name = await loop.run_in_executor(None, find_cp210x_port)
                if port_name:
                    bill_acceptor = serial.Serial(
                        port=port_name, baudrate=9600, bytesize=serial.EIGHTBITS,
                        parity=serial.PARITY_EVEN, stopbits=serial.STOPBITS_ONE, timeout=0.1
                    )
                    disable_bill_acceptor()
                    if not acceptor_connected:
                        await send_system_notification("紙鈔機已連接", "success")
                        acceptor_connected = True
                else: await asyncio.sleep(5); continue
            
            data_byte = await loop.run_in_executor(None, bill_acceptor.read, 1)
            if data_byte:
                command = data_byte[0]
                if command == 0x81:
                    await asyncio.sleep(0.05)
                    value_byte_data = await loop.run_in_executor(None, bill_acceptor.read, 1)
                    if value_byte_data:
                        amount = parse_bill_value(value_byte_data[0])
                        if BILL_ACCEPTOR_ENABLED and amount > 0:
                            # 簡化驗證：直接檢查 current_member
                            if not current_member:
                                print("[Error] 沒有登入的會員，拒絕收錢")
                                send_command(0x0F)  # 拒絕紙鈔
                                await send_system_notification("請先掃描會員QRCode", "error")
                                continue
                            
                            send_command(0x02)  # 接受紙鈔
                            print(f"[Hardware] 為會員 {current_member.get('acc')} 接受 {amount} 元紙鈔")
                            await broadcast({"event": "bill_inserted", "amount": amount})
                        else:
                            send_command(0x0F)
        except serial.SerialException:
            if bill_acceptor and bill_acceptor.is_open: bill_acceptor.close()
            bill_acceptor = None
            if acceptor_connected:
                # await send_system_notification("紙鈔機連線中斷", "error")
                acceptor_connected = False
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[BillAcceptor] 監聽迴圈發生未知錯誤: {e}", file=sys.stderr)
            await asyncio.sleep(5)

async def hardware_status_loop():
    previous_status = None
    while True:
        current_status = (scanner_connected, acceptor_connected)
        if current_status != previous_status and CONNECTED_CLIENTS:
            # 狀態改變或第一次檢查且有 client
            if not current_status[0] and not current_status[1]:
                msg = "QRCode 掃描器及紙鈔機連線中斷"
                await send_system_notification(msg, "error")
            elif not current_status[0] and current_status[1]:
                msg = "QRCode 掃描器連線中斷"
                await send_system_notification(msg, "error")
            elif current_status[0] and not current_status[1]:
                msg = "紙鈔機連線中斷"
                await send_system_notification(msg, "error")
            else:
                msg = "QRCode 掃描器及紙鈔機連線正常"
                await send_system_notification(msg, "success")
            previous_status = current_status
        await asyncio.sleep(1)

# MQTT斷線測試
async def test_disconnect_loop(client, interval=15):
    """每 interval 秒自動斷線一次，測試重連"""
    while True:
        await asyncio.sleep(interval)
        print(f"[Test] 自動斷線測試 ({interval}s 後)")
        try:
            client.disconnect()
        except Exception as e:
            print(f"[Test] 斷線時發生錯誤: {e}")

mqtt_client = None 
# --- 主程式 ---
async def main():
    global mqtt_client
    load_config()
    get_device_id()
    get_system_info()
    loop = asyncio.get_running_loop()
    mqtt_client = setup_mqtt_client(loop)
    clean_old_screenshot_folders(7)
    # asyncio.create_task(test_disconnect_loop(mqtt_client, interval=15)) # MQTT斷線測試

    
    # --- 關鍵修改：將日誌清理任務加入背景執行 ---
    background_tasks = [
        system_info_task(), 
        mqtt_message_processor(), 
        bill_acceptor_loop(),
        qrcode_scanner_loop(),
        log_cleanup_task(),
        hardware_status_loop()
    ]

    try:
        server = await websockets.serve(websocket_handler, "127.0.0.1", 8765)
        print(f"[OK] WebSocket 伺服器已啟動於 ws://127.0.0.1:8765")
        await asyncio.gather(*background_tasks)
    except Exception as e:
        print(f"[Fatal] 伺服器啟動失敗: {e}", file=sys.stderr)
    finally:
        if mqtt_client:
            mqtt_data = {"status":"offline", "did": device_id}
            mqtt_client.publish(f"device/{hostname}/status",  json.dumps(mqtt_data, ensure_ascii=False), retain=True)
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("[MQTT] 已釋放 MQTT 連線")

if __name__ == "__main__":
    try:
        print("--- 儲值機後端服務啟動 ---")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n--- 服務已手動關閉 ---")
import requests
import sqlite3
import os
from datetime import datetime, timezone, timedelta

# ==========================================
# 1. GitHub 비밀고(Secrets)에서 API 키 불러오기
# ==========================================
CLIENT_ID = os.environ.get("CHZZK_CLIENT_ID")
CLIENT_SECRET = os.environ.get("CHZZK_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("❌ 오류: API 키가 설정되지 않았습니다. GitHub Secrets를 확인하세요.")
    exit(1)

headers = {
    "X-CHZZK-CLIENT-ID": CLIENT_ID,
    "X-CHZZK-CLIENT-SECRET": CLIENT_SECRET,
    "Content-Type": "application/json"
}

# ==========================================
# 2. 한국 시간(KST) 설정 및 DB 세팅
# ==========================================
KST = timezone(timedelta(hours=9))

def setup_database():
    conn = sqlite3.connect("chzzk_radar.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS streamers (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            last_category TEXT,
            first_discovered_at TEXT,
            last_seen_live_at TEXT
        )
    ''')
    conn.commit()
    return conn

# ==========================================
# 3. 레이더 스캔 실행
# ==========================================
def scan_live_streamers(conn):
    cursor = conn.cursor()
    print("📡 치지직 레이더 스캔을 시작합니다...")

    url = "https://openapi.chzzk.naver.com/open/v1/lives?size=50"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print("❌ API 호출 에러!", response.text)
        return

    data = response.json()
    lives = data.get("content", {}).get("data",[])
    
    new_count = 0
    update_count = 0
    current_time = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')

    for live in lives:
        channel_id = live.get("channelId")
        channel_name = live.get("channelName")
        category = live.get("liveCategoryValue", "카테고리 없음")
        
        cursor.execute("SELECT * FROM streamers WHERE channel_id = ?", (channel_id,))
        exists = cursor.fetchone()
        
        if exists is None:
            cursor.execute('''
                INSERT INTO streamers (channel_id, channel_name, last_category, first_discovered_at, last_seen_live_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (channel_id, channel_name, category, current_time, current_time))
            new_count += 1
            print(f"✨ 뉴페이스 발견: {channel_name} ({category})")
        else:
            cursor.execute('''
                UPDATE streamers 
                SET channel_name = ?, last_category = ?, last_seen_live_at = ?
                WHERE channel_id = ?
            ''', (channel_name, category, current_time, channel_id))
            update_count += 1

    conn.commit()
    print("-" * 50)
    print(f"🎯 스캔 완료! 신규: {new_count}명 / 업데이트: {update_count}명")
    
    cursor.execute("SELECT COUNT(*) FROM streamers")
    total_count = cursor.fetchone()
    print(f"📚 현재 DB 누적 스트리머 수: {total_count}명")

if __name__ == "__main__":
    db_conn = setup_database()
    scan_live_streamers(db_conn)
    db_conn.close()

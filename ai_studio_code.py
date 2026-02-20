import requests
import sqlite3
import os
from datetime import datetime, timezone, timedelta

# ==========================================
# 1. API 키 불러오기 및 헤더 설정
# ==========================================
CLIENT_ID = os.environ.get("CHZZK_CLIENT_ID")
CLIENT_SECRET = os.environ.get("CHZZK_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("❌ 오류: API 키가 설정되지 않았습니다.")
    exit(1)

headers = {
    "Client-Id": CLIENT_ID,
    "Client-Secret": CLIENT_SECRET,
    "Content-Type": "application/json"
}

KST = timezone(timedelta(hours=9))

# ==========================================
# 2. DB 세팅
# ==========================================
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
# 3. 레이더 스캔 실행 (커서 기반 전체 수집)
# ==========================================
def scan_live_streamers(conn):
    cursor_db = conn.cursor()
    print("📡 전체 방송 목록 스캔을 시작합니다...")

    new_count = 0
    update_count = 0
    current_time = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    
    # 다음 페이지를 열기 위한 커서 초기화
    next_concurrent_user_count = None
    next_live_id = None
    
    page = 1
    max_pages = 100 # 안전장치 (최대 2000명 스캔 제한)

    while page <= max_pages:
        url = "https://openapi.chzzk.naver.com/open/v1/lives?size=20"
        
        # 이전 페이지에서 발급받은 커서가 있다면 URL 파라미터로 이어붙임
        if next_concurrent_user_count and next_live_id:
            url += f"&concurrentUserCount={next_concurrent_user_count}&liveId={next_live_id}"
            
        try:
            # 무한 대기 방지를 위한 5초 제한
            response = requests.get(url, headers=headers, timeout=5)
        except requests.exceptions.RequestException as e:
            print(f"❌ 통신 에러 (페이지 {page}): {e}")
            break

        if response.status_code != 200:
            print(f"❌ API 호출 에러 (페이지 {page}):", response.text)
            break

        data = response.json()
        content = data.get("content", {})
        lives = content.get("data", [])
        
        if not lives:
            print(f"✅ 더 이상 스캔할 방송이 없습니다. (종료 페이지: {page-1})")
            break

        for live in lives:
            channel_id = live.get("channelId")
            channel_name = live.get("channelName")
            category = live.get("liveCategoryValue", "카테고리 없음")
            
            cursor_db.execute("SELECT * FROM streamers WHERE channel_id = ?", (channel_id,))
            exists = cursor_db.fetchone()
            
            if exists is None:
                cursor_db.execute('''
                    INSERT INTO streamers (channel_id, channel_name, last_category, first_discovered_at, last_seen_live_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (channel_id, channel_name, category, current_time, current_time))
                new_count += 1
            else:
                cursor_db.execute('''
                    UPDATE streamers 
                    SET channel_name = ?, last_category = ?, last_seen_live_at = ?
                    WHERE channel_id = ?
                ''', (channel_name, category, current_time, channel_id))
                update_count += 1
                
        print(f"✔️ {page}페이지 수집 완료 ({len(lives)}명)")
        
        # 다음 페이지를 위한 커서(Cursor) 추출
        page_info = content.get("page", {})
        next_info = page_info.get("next")
        
        if next_info:
            next_concurrent_user_count = next_info.get("concurrentUserCount")
            next_live_id = next_info.get("liveId")
            page += 1
        else:
            print(f"✅ 마지막 페이지에 도달했습니다. (종료 페이지: {page})")
            break

    conn.commit()
    print("-" * 50)
    print(f"🎯 전체 스캔 완료 (신규: {new_count}명 / 업데이트: {update_count}명)")
    
    cursor_db.execute("SELECT COUNT(*) FROM streamers")
    total_count = cursor_db.fetchone()[0]
    print(f"📚 현재 DB 누적 스트리머 수: {total_count}명")

if __name__ == "__main__":
    db_conn = setup_database()
    scan_live_streamers(db_conn)
    db_conn.close()

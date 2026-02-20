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
# 3. 레이더 스캔 실행 (안전한 파라미터 인코딩 적용)
# ==========================================
def scan_live_streamers(conn):
    cursor_db = conn.cursor()
    print("📡 전체 방송 목록 스캔을 시작합니다 (안전한 파라미터 모드)...")

    new_count = 0
    update_count = 0
    current_time = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    
    next_cursor = None
    page = 1
    max_pages = 100 
    
    # 똑같은 페이지를 맴도는 무한 루프 방지용 방문 기록
    visited_cursors = set()

    while page <= max_pages:
        url = "https://openapi.chzzk.naver.com/open/v1/lives"
        
        # 파라미터를 딕셔너리로 묶어 requests에 맡김 (자동으로 완벽하게 URL 인코딩됨)
        params = {"size": 20}
        if next_cursor:
            params["next"] = next_cursor
            
        try:
            # url에 직접 붙이지 않고 params 인자 사용
            response = requests.get(url, headers=headers, params=params, timeout=5)
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
        
        # 다음 페이지 커서 확인
        page_info = content.get("page", {})
        next_cursor = page_info.get("next")
        
        # 커서 값이 없거나, 이미 방문했던 커서를 서버가 또 줬다면 종료
        if not next_cursor or next_cursor in visited_cursors:
            print(f"✅ 모든 목록 수집 완료. (최종 페이지: {page})")
            break
            
        # 정상적인 새 커서라면 방문 기록에 남기고 다음 페이지 진행
        visited_cursors.add(next_cursor)
        page += 1

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

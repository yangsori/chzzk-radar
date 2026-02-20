import requests
import sqlite3
import os
import time
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
    
    try:
        cursor.execute("ALTER TABLE streamers ADD COLUMN follower_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
        
    try:
        cursor.execute("ALTER TABLE streamers ADD COLUMN last_follower_updated_at TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    return conn

# ==========================================
# 3. 개별 채널 팔로워 수 조회 (안전한 타입 변환 적용)
# ==========================================
def get_follower_count(channel_id):
    url = "https://openapi.chzzk.naver.com/open/v1/channels"
    params = {"channelIds": channel_id}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            channels = data.get("content", {}).get("data", [])
            if channels:
                count = channels[0].get("followerCount")
                # 값이 아예 없거나 null일 경우를 방어하고, 무조건 정수형으로 변환
                return int(count) if count is not None else 0
    except Exception as e:
        print(f"⚠️ 팔로워 조회 에러 ({channel_id}): {e}")
    # 서버 오류 시 DB 덮어쓰기를 막기 위한 안전장치
    return None 

# ==========================================
# 4. 1단계: 생방송 레이더 (빈 데이터 필터링 적용)
# ==========================================
def scan_live_streamers(conn):
    cursor_db = conn.cursor()
    print("📡 1단계: 생방송 정밀 레이더 스캔을 시작합니다...")

    new_count = 0
    update_count = 0
    current_time_str = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    
    next_cursor = None
    page = 1
    max_pages = 200 
    visited_cursors = set()

    while page <= max_pages:
        url = "https://openapi.chzzk.naver.com/open/v1/lives"
        params = {"size": 20}
        if next_cursor:
            params["next"] = next_cursor
            
        try:
            response = requests.get(url, headers=headers, params=params, timeout=5)
        except requests.exceptions.RequestException:
            break

        if response.status_code != 200:
            break

        data = response.json()
        content = data.get("content", {})
        lives = content.get("data", [])
        
        if not lives:
            break

        for live in lives:
            channel_id = live.get("channelId")
            
            # 방어막 1: 채널 ID가 없거나 null이면 DB 에러가 나므로 건너뜀
            if not channel_id:
                continue
                
            # 방어막 2: 이름이나 카테고리가 null로 올 경우 기본 문자열로 대체 (None 오염 방지)
            channel_name = live.get("channelName") or "알 수 없는 채널"
            category = live.get("liveCategoryValue") or "카테고리 없음"
            
            cursor_db.execute("SELECT * FROM streamers WHERE channel_id = ?", (channel_id,))
            exists = cursor_db.fetchone()
            
            if exists is None:
                cursor_db.execute('''
                    INSERT INTO streamers 
                    (channel_id, channel_name, last_category, first_discovered_at, last_seen_live_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (channel_id, channel_name, category, current_time_str, current_time_str))
                new_count += 1
            else:
                cursor_db.execute('''
                    UPDATE streamers 
                    SET channel_name = ?, last_category = ?, last_seen_live_at = ?
                    WHERE channel_id = ?
                ''', (channel_name, category, current_time_str, channel_id))
                update_count += 1
                
        page_info = content.get("page", {})
        next_cursor = page_info.get("next")
        
        if not next_cursor or next_cursor in visited_cursors:
            break
            
        visited_cursors.add(next_cursor)
        page += 1

    conn.commit()
    print(f"✔️ 1단계 완료 - 신규 발견: {new_count}명 / 방송 정보 업데이트: {update_count}명")

# ==========================================
# 5. 2단계: DB 전체 팔로워 정산
# ==========================================
def update_all_followers_daily(conn):
    cursor_db = conn.cursor()
    print("📊 2단계: 전체 DB 대상 24시간 주기 팔로워 정산을 시작합니다...")
    
    current_time_dt = datetime.now(KST)
    current_time_str = current_time_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    cursor_db.execute("SELECT channel_id, channel_name, last_follower_updated_at FROM streamers")
    rows = cursor_db.fetchall()
    
    follower_update_count = 0
    fail_count = 0
    
    for row in rows:
        channel_id, channel_name, last_updated_str = row
        needs_update = False
        
        if not last_updated_str:
            needs_update = True
        else:
            try:
                last_updated_dt = datetime.strptime(last_updated_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=KST)
                if (current_time_dt - last_updated_dt) > timedelta(hours=24):
                    needs_update = True
            except ValueError:
                needs_update = True
                
        if needs_update:
            time.sleep(0.1) 
            follower_count = get_follower_count(channel_id)
            
            if follower_count is not None:
                cursor_db.execute('''
                    UPDATE streamers 
                    SET follower_count = ?, last_follower_updated_at = ?
                    WHERE channel_id = ?
                ''', (follower_count, current_time_str, channel_id))
                follower_update_count += 1
            else:
                fail_count += 1
            
            if (follower_update_count + fail_count) % 50 == 0:
                print(f"   ... {follower_update_count}명 갱신 완료 (통신 실패: {fail_count}명) ...")

    conn.commit()
    print(f"✔️ 2단계 완료 - 갱신 성공: {follower_update_count}명 / 갱신 지연(네트워크 오류): {fail_count}명")

if __name__ == "__main__":
    db_conn = setup_database()
    scan_live_streamers(db_conn)      
    update_all_followers_daily(db_conn) 
    
    cursor_db = db_conn.cursor()
    cursor_db.execute("SELECT COUNT(*) FROM streamers")
    total_count = cursor_db.fetchone()[0]
    print("-" * 50)
    print(f"📚 현재 DB 누적 스트리머 수: {total_count}명")
    
    db_conn.close()

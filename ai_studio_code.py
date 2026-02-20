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
# 2. DB 세팅 (스마트 업데이트를 위한 컬럼 자동 추가)
# ==========================================
def setup_database():
    conn = sqlite3.connect("chzzk_radar.db")
    cursor = conn.cursor()
    
    # 기본 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS streamers (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            last_category TEXT,
            first_discovered_at TEXT,
            last_seen_live_at TEXT
        )
    ''')
    
    # 팔로워 수 컬럼 추가 (이미 있으면 무시)
    try:
        cursor.execute("ALTER TABLE streamers ADD COLUMN follower_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
        
    # 마지막 팔로워 갱신 시간 컬럼 추가 (이미 있으면 무시)
    try:
        cursor.execute("ALTER TABLE streamers ADD COLUMN last_follower_updated_at TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    return conn

# ==========================================
# 3. 개별 채널 팔로워 수 조회 (안전한 파라미터 적용)
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
                return channels[0].get("followerCount", 0)
    except Exception as e:
        print(f"⚠️ 팔로워 조회 에러 ({channel_id}): {e}")
    return 0

# ==========================================
# 4. 레이더 스캔 실행 (커서 전체 스캔 + 24시간 스마트 팔로워 갱신)
# ==========================================
def scan_live_streamers(conn):
    cursor_db = conn.cursor()
    print("📡 스마트 스캔을 시작합니다 (전체 방송 + 24시간 주기 팔로워 수집)...")

    new_count = 0
    update_count = 0
    follower_update_count = 0
    
    current_time_dt = datetime.now(KST)
    current_time_str = current_time_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    next_cursor = None
    page = 1
    max_pages = 200 # 최대 4000명 수집 제한 (안전장치)
    visited_cursors = set()

    while page <= max_pages:
        url = "https://openapi.chzzk.naver.com/open/v1/lives"
        params = {"size": 20}
        if next_cursor:
            params["next"] = next_cursor
            
        try:
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
            
            # DB에서 이 스트리머의 마지막 팔로워 갱신 시간만 조회
            cursor_db.execute("SELECT last_follower_updated_at FROM streamers WHERE channel_id = ?", (channel_id,))
            row = cursor_db.fetchone()
            
            if row is None:
                # [상황 1] DB에 없는 뉴페이스: 즉시 팔로워 수 수집 후 저장
                time.sleep(0.1) 
                follower_count = get_follower_count(channel_id)
                
                cursor_db.execute('''
                    INSERT INTO streamers 
                    (channel_id, channel_name, last_category, first_discovered_at, last_seen_live_at, follower_count, last_follower_updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (channel_id, channel_name, category, current_time_str, current_time_str, follower_count, current_time_str))
                new_count += 1
            else:
                last_updated_str = row[0]
                needs_follower_update = False
                
                # [상황 2] 시간 계산: 업데이트된 적 없거나, 24시간이 지났는지 확인
                if not last_updated_str:
                    needs_follower_update = True
                else:
                    try:
                        last_updated_dt = datetime.strptime(last_updated_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=KST)
                        if (current_time_dt - last_updated_dt) > timedelta(hours=24):
                            needs_follower_update = True
                    except ValueError:
                        needs_follower_update = True
                
                if needs_follower_update:
                    # 24시간 경과됨: 팔로워 수 갱신 포함 업데이트
                    time.sleep(0.1)
                    follower_count = get_follower_count(channel_id)
                    cursor_db.execute('''
                        UPDATE streamers 
                        SET channel_name = ?, last_category = ?, last_seen_live_at = ?, follower_count = ?, last_follower_updated_at = ?
                        WHERE channel_id = ?
                    ''', (channel_name, category, current_time_str, follower_count, current_time_str, channel_id))
                    follower_update_count += 1
                else:
                    # 24시간 미만: 팔로워 수는 건너뛰고 기본 정보만 초고속 업데이트
                    cursor_db.execute('''
                        UPDATE streamers 
                        SET channel_name = ?, last_category = ?, last_seen_live_at = ?
                        WHERE channel_id = ?
                    ''', (channel_name, category, current_time_str, channel_id))
                
                update_count += 1
                
        print(f"✔️ {page}페이지 수집 완료 ({len(lives)}명)")
        
        page_info = content.get("page", {})
        next_cursor = page_info.get("next")
        
        # 커서 무한 루프 검증
        if not next_cursor or next_cursor in visited_cursors:
            print(f"✅ 모든 목록 수집 완료. (최종 페이지: {page})")
            break
            
        visited_cursors.add(next_cursor)
        page += 1

    conn.commit()
    print("-" * 50)
    print(f"🎯 완료 보고서 - 신규: {new_count}명 / 기본 업데이트: {update_count}명 / 팔로워 수 갱신: {follower_update_count}명")
    
    cursor_db.execute("SELECT COUNT(*) FROM streamers")
    total_count = cursor_db.fetchone()[0]
    print(f"📚 현재 DB 누적 스트리머 수: {total_count}명")

if __name__ == "__main__":
    db_conn = setup_database()
    scan_live_streamers(db_conn)
    db_conn.close()

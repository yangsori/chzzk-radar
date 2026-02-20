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

# 이전 단계에서 수정했던 올바른 인증 헤더
headers = {
    "Client-Id": CLIENT_ID,
    "Client-Secret": CLIENT_SECRET,
    "Content-Type": "application/json"
}

KST = timezone(timedelta(hours=9))

# ==========================================
# 2. DB 세팅 (팔로워 수 컬럼 추가)
# ==========================================
def setup_database():
    conn = sqlite3.connect("chzzk_radar.db")
    cursor = conn.cursor()
    
    # follower_count 컬럼이 포함된 새로운 테이블 구조
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS streamers (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            last_category TEXT,
            follower_count INTEGER,
            first_discovered_at TEXT,
            last_seen_live_at TEXT
        )
    ''')
    
    # 만약 예전 구조의 테이블이 이미 있다면, 팔로워 수 컬럼을 안전하게 추가
    try:
        cursor.execute("ALTER TABLE streamers ADD COLUMN follower_count INTEGER")
    except sqlite3.OperationalError:
        pass # 이미 컬럼이 존재하면 무시하고 넘어감
        
    conn.commit()
    return conn

# ==========================================
# 3. 개별 채널의 팔로워 수 조회 함수
# ==========================================
def get_follower_count(channel_id):
    # 채널 ID를 이용해 해당 스트리머의 상세 정보(팔로워 수 등)를 요청
    url = f"https://openapi.chzzk.naver.com/open/v1/channels?channelIds={channel_id}"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            channels = data.get("content", {}).get("data", [])
            if channels:
                return channels[0].get("followerCount", 0) # 팔로워 수 추출
    except Exception as e:
        print(f"⚠️ 팔로워 수 조회 실패 ({channel_id}): {e}")
    return 0

# ==========================================
# 4. 레이더 스캔 실행 (반복문 적용)
# ==========================================
def scan_live_streamers(conn):
    cursor = conn.cursor()
    print("📡 치지직 정밀 레이더 스캔을 시작합니다 (전체 페이지 & 팔로워 수 포함)...")

    new_count = 0
    update_count = 0
    current_time = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    
    page = 0 # 0페이지부터 시작
    max_pages = 50 # 무한 루프 방지를 위한 최대 페이지 설정 (20명 * 50페이지 = 1000명)

    while page < max_pages:
        url = f"https://openapi.chzzk.naver.com/open/v1/lives?size=20&page={page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ API 호출 에러 (페이지 {page}):", response.text)
            break

        data = response.json()
        lives = data.get("content", {}).get("data", [])
        
        # 더 이상 받아올 방송 데이터가 없으면 반복문 종료
        if not lives:
            print(f"✅ 더 이상 스캔할 방송이 없습니다. (종료 페이지: {page})")
            break

        for live in lives:
            channel_id = live.get("channelId")
            channel_name = live.get("channelName")
            category = live.get("liveCategoryValue", "카테고리 없음")
            
            # 팔로워 수 가져오기 (서버 과부하 방지를 위해 0.1초 휴식)
            follower_count = get_follower_count(channel_id)
            time.sleep(0.1) 
            
            cursor.execute("SELECT * FROM streamers WHERE channel_id = ?", (channel_id,))
            exists = cursor.fetchone()
            
            if exists is None:
                cursor.execute('''
                    INSERT INTO streamers (channel_id, channel_name, last_category, follower_count, first_discovered_at, last_seen_live_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (channel_id, channel_name, category, follower_count, current_time, current_time))
                new_count += 1
                print(f"✨ 뉴페이스 발견: {channel_name} ({category}) - 팔로워 {follower_count}명")
            else:
                cursor.execute('''
                    UPDATE streamers 
                    SET channel_name = ?, last_category = ?, follower_count = ?, last_seen_live_at = ?
                    WHERE channel_id = ?
                ''', (channel_name, category, follower_count, current_time, channel_id))
                update_count += 1

        # 다음 20개를 가져오기 위해 페이지 번호 1 증가
        page += 1 

    conn.commit()
    print("-" * 50)
    print(f"🎯 정밀 스캔 완료! 신규: {new_count}명 / 업데이트: {update_count}명")
    
    cursor.execute("SELECT COUNT(*) FROM streamers")
    total_count = cursor.fetchone()[0]
    print(f"📚 현재 DB 누적 스트리머 수: {total_count}명")

if __name__ == "__main__":
    db_conn = setup_database()
    scan_live_streamers(db_conn)
    db_conn.close()

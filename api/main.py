from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional

# Khởi tạo Ứng dụng API
app = FastAPI(
    title="Data Warehouse API cho OpenClaw",
    description="Hệ thống API cung cấp số liệu Báo cáo Tài chính P&L Tự động cho CEO",
    version="1.0.0"
)

# ==========================================
# CẤU HÌNH DATABASE
# ==========================================
DB_CONFIG = {
    "dbname": "BaoCao", "user": "postgres", 
    "password": "Vu123", "host": "localhost", "port": "5433"
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi kết nối Database: {str(e)}")

# ==========================================
# CÁC ENDPOINTS (ĐIỂM CUỐI GIAO TIẾP VỚI AI)
# ==========================================

@app.get("/", tags=["Hệ thống"])
def health_check():
    return {"status": "Trực tuyến", "message": "API Data Warehouse đang hoạt động tốt!"}

@app.get("/api/v1/reports/daily-summary", tags=["Báo cáo CEO"])
def get_daily_summary(
    ngay: Optional[str] = Query(None, description="Định dạng YYYY-MM-DD. Nếu để trống sẽ lấy 7 ngày gần nhất.")
):
    """Lấy báo cáo Tổng quan toàn Công ty (Gộp 3 sàn) theo ngày"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Xây dựng câu lệnh SQL
        sql = """
            SELECT 
                "Ngày" ::text,
                SUM("Tổng số đơn") AS "Tổng đơn",
                SUM("Doanh thu sau hủy") AS "Doanh thu thuần",
                SUM("SÀN TRẢ NBH") AS "Sàn trả (Payout)",
                SUM("Tổng phí ngoại sàn") AS "Tổng chi phí ngoại sàn",
                SUM("LỢI NHUẬN RÒNG") AS "Lợi Nhuận Ròng",
                ROUND((SUM("LỢI NHUẬN RÒNG") / NULLIF(SUM("Doanh thu sau hủy"), 0)) * 100, 2) AS "Biên LN (%)"
            FROM fact.vw_daily_pnl_master
        """
        
        # Nếu có truyền ngày thì lọc, không thì Group theo ngày giới hạn 7 ngày
        if ngay:
            sql += f" WHERE \"Ngày\" = '{ngay}'"
        
        sql += " GROUP BY \"Ngày\" ORDER BY \"Ngày\" DESC"
        
        if not ngay:
            sql += " LIMIT 7"

        cur.execute(sql)
        results = cur.fetchall()
        return {"status": "success", "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.get("/api/v1/reports/channel-details", tags=["Báo cáo Kế toán"])
def get_channel_details(
    ngay: str = Query(..., description="Bắt buộc nhập ngày muốn xem (YYYY-MM-DD)"),
    kenh: Optional[str] = Query(None, description="Lọc theo kênh: TikTok, Shopee, Nhanh.vn")
):
    """Xem chi tiết rành mạch từng đồng chi phí của từng Sàn / Shop trong 1 ngày"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        sql = """
            SELECT 
                "Kênh", "Shop",
                "Tổng số đơn", "Tỷ lệ hủy (%)", "Doanh thu sau hủy",
                "Phí cố định", "PHÍ THANH TOÁN", "PHÍ VXP", "Phí hoa hồng",
                "Phí quảng cáo", "Phí Booking", "Back office", "Giá vốn",
                "SÀN TRẢ NBH", "LỢI NHUẬN RÒNG", "Biên LN (%)"
            FROM fact.vw_daily_pnl_master
            WHERE "Ngày" = %s
        """
        params = [ngay]
        
        if kenh:
            sql += " AND \"Kênh\" = %s"
            params.append(kenh)
            
        sql += " ORDER BY \"LỢI NHUẬN RÒNG\" DESC"

        cur.execute(sql, params)
        results = cur.fetchall()
        return {"status": "success", "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()
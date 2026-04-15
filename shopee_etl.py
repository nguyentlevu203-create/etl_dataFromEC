import os
import re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
DB_CONFIG = {
    "dbname": "BaoCao", 
    "user": "postgres",
    "password": "Vu123",
    "host": "localhost",
    "port": "5433"
}

CHANNEL_SHOPEE_ID = 2 

# Thư mục Đơn hàng
INPUT_DIR = "data/shopee/orders/"
PROCESSED_DIR = "processed/shopee/orders/"

# Thư mục Quảng cáo
ADS_INPUT_DIR = "data/shopee/ads/"
ADS_PROCESSED_DIR = "processed/shopee/ads/"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(ADS_INPUT_DIR, exist_ok=True)
os.makedirs(ADS_PROCESSED_DIR, exist_ok=True)

# ==========================================
# 2. HÀM HỖ TRỢ
# ==========================================
def clean_code(val):
    if pd.isna(val): return ''
    s = str(val).strip().upper().replace("'", "").replace('"', "")
    if s in ('NAN', 'NONE', ''): return ''
    if 'E+' in s:
        try: return f"{float(s):.0f}"
        except: pass
    return s[:95]

def extract_money(series):
    if series is None or len(series) == 0: return 0
    clean_series = series.astype(str).str.replace(r'[^\d.-]', '', regex=True)
    return pd.to_numeric(clean_series, errors='coerce').fillna(0).astype(float)

def read_and_clean_file(filepath):
    """Đọc file và dò tìm Header chuẩn của Shopee"""
    if filepath.lower().endswith('.csv'):
        try: df = pd.read_csv(filepath, dtype=str) 
        except: df = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str) 
    else: 
        df = pd.read_excel(filepath, dtype=str)
    
    if not any('mã đơn' in str(c).lower() for c in df.columns):
        for i in range(min(15, len(df))):
            if any('mã đơn' in str(x).lower() for x in df.iloc[i].values):
                df.columns = df.iloc[i].values
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    df.columns = [str(c).replace('\ufeff', '').replace('\n', ' ').strip() for c in df.columns]
    return df.loc[:, ~df.columns.duplicated()].copy()

# ==========================================
# 3. TRANSFORM: ĐƠN HÀNG ĐA GIAN HÀNG
# ==========================================
def process_shopee_files():
    print("\n" + "="*60)
    print("🚀 ETL SHOPEE PROMAX - CHẾ ĐỘ ĐA GIAN HÀNG")
    print("="*60)
    
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(('.xlsx', '.xls', '.csv')) and not f.startswith('~')]
    
    aff_files = [os.path.join(INPUT_DIR, f) for f in files if 'affiliate' in f.lower() or 'hoa_hong' in f.lower()]
    pay_files = [os.path.join(INPUT_DIR, f) for f in files if 'tien_nhan' in f.lower() or 'thanh_toan' in f.lower()]
    main_files = [os.path.join(INPUT_DIR, f) for f in files if os.path.join(INPUT_DIR, f) not in aff_files and os.path.join(INPUT_DIR, f) not in pay_files]

    if not main_files: 
        print("⚠️ Không có file chi tiết đơn hàng mới.")
        return pd.DataFrame(), []

    # GOM DỮ LIỆU PHỤ TRỢ (AFFILIATE & PAYOUT)
    aff_map = {}
    for aff_f in aff_files:
        df_a = read_and_clean_file(aff_f)
        c_id_a = next((c for c in df_a.columns if 'mã đơn' in c.lower()), 'Mã đơn hàng')
        c_val_a = next((c for c in df_a.columns if 'chi phí' in c.lower() or 'hoa hồng' in c.lower()), 'Chi phí(₫)')
        if c_id_a in df_a.columns:
            df_a[c_id_a] = df_a[c_id_a].apply(clean_code)
            temp_map = df_a.groupby(c_id_a)[c_val_a].apply(lambda x: extract_money(x).sum()).to_dict()
            for k, v in temp_map.items(): aff_map[k] = aff_map.get(k, 0) + v

    pay_map = {}
    for pay_f in pay_files:
        df_p = read_and_clean_file(pay_f)
        p_col = next((c for c in df_p.columns if 'thanh toán' in c.lower() or 'doanh thu' in c.lower()), 'Tổng tiền đã thanh toán')
        c_id_p = next((c for c in df_p.columns if 'mã đơn' in c.lower()), 'Mã đơn hàng')
        loai_col = next((c for c in df_p.columns if 'đơn hàng / sản phẩm' in c.lower() or 'loại' in c.lower()), None)
        if loai_col:
            df_p = df_p[df_p[loai_col].astype(str).str.lower().str.contains('order|đơn hàng', na=False)].copy()
        if c_id_p in df_p.columns:
            df_p['mid'] = df_p[c_id_p].apply(clean_code)
            temp_map = df_p.groupby('mid')[p_col].apply(lambda x: extract_money(x).sum()).to_dict()
            for k, v in temp_map.items(): pay_map[k] = pay_map.get(k, 0) + v

    final_rows = []
    all_processed_files = main_files + aff_files + pay_files

    for main_f in main_files:
        filename = os.path.basename(main_f)
        # BÓC Tên Shop từ file
        name_no_ext = re.sub(r'\.(xlsx|xls|csv)$', '', filename, flags=re.IGNORECASE)
        shop_label = re.sub(r'chi_tiet_don_hang|don_hang|orders|\d{4}[-_]\d{2}[-_]\d{2}|\d{2}[-_]\d{2}[-_]\d{4}', '', name_no_ext, flags=re.IGNORECASE)
        shop_label = shop_label.strip(' _-').upper()
        if not shop_label: shop_label = 'SHOPEE'

        print(f"📦 Đọc file: {filename} -> Shop: [{shop_label}]")
        df = read_and_clean_file(main_f)
        
        c_id = next((c for c in df.columns if 'mã đơn' in c.lower()), 'Mã đơn hàng')
        c_st = next((c for c in df.columns if 'trạng thái' in c.lower()), 'Trạng thái đơn hàng')
        c_dt = next((c for c in df.columns if 'ngày đặt' in c.lower()), 'Ngày đặt hàng')
        c_bc = next((c for c in df.columns if 'sku phân loại' in c.lower() or 'mã vạch' in c.lower()), 'SKU phân loại hàng')

        df['raw_rev'] = (extract_money(df.get('Giá gốc', pd.Series(0))) * extract_money(df.get('Số lượng', pd.Series(1)))) \
                        - extract_money(df.get('Tổng số tiền được người bán trợ giá', pd.Series(0))) \
                        - extract_money(df.get('Mã giảm giá của Shop', pd.Series(0)))
        
        df['is_can'] = df[c_st].astype(str).str.strip() == 'Đã hủy'
        df['rev_val'] = df.apply(lambda x: 0 if x['is_can'] else x['raw_rev'], axis=1)

        order_sum = df.groupby(c_id, as_index=False).agg({'rev_val': 'sum', 'is_can': 'max'})

        for _, r in df.iterrows():
            oid = clean_code(r[c_id])
            if not oid: continue
            final_rows.append({
                'ext_id': oid, 'time': pd.to_datetime(r[c_dt], errors='coerce'), 'status': str(r[c_st])[:95],
                'is_can': bool(order_sum.loc[order_sum[c_id] == oid, 'is_can'].values[0]),
                'label': shop_label, 
                'rev': float(order_sum.loc[order_sum[c_id] == oid, 'rev_val'].values[0]),
                'paid': float(extract_money(pd.Series([r.get('Khách hàng thanh toán', 0)]))[0]),
                'f_fix': float(extract_money(pd.Series([r.get('Phí cố định', 0)]))[0]),
                'f_svc': float(extract_money(pd.Series([r.get('Phí dịch vụ', 0)]))[0]), 
                'f_pay': float(extract_money(pd.Series([r.get('Phí thanh toán', 0)]))[0]),
                'aff': float(aff_map.get(oid, 0)), 'payout': float(pay_map.get(oid, 0)),
                'product': str(r.get('Tên sản phẩm', '')), 'barcode': clean_code(r[c_bc]),
                'qty': int(extract_money(pd.Series([r.get('Số lượng', 1)]))[0])
            })
            
    return pd.DataFrame(final_rows), all_processed_files

# ==========================================
# 4. TRANSFORM: QUẢNG CÁO (+8% VAT & FIX NGÀY)
# ==========================================
def process_shopee_ads():
    print("\n" + "="*60)
    print("📢 ĐỌC QUẢNG CÁO TỪ TÊN FILE (FIX VAT & NGÀY)")
    print("="*60)
    ads_data = []
    files_to_move = []
    
    for filename in os.listdir(ADS_INPUT_DIR):
        if not filename.endswith(('.xlsx', '.xls', '.csv')) or filename.startswith('~'): continue

        try:
            # FIX NGÀY: Nhận diện YYYY-MM-DD hoặc DD-MM-YYYY
            date_match = re.search(r'\d{4}[-_]\d{2}[-_]\d{2}|\d{2}[-_]\d{2}[-_]\d{4}', filename)
            if not date_match: continue
                
            file_date_str = date_match.group()
            if re.match(r'^\d{4}', file_date_str): ad_date = pd.to_datetime(file_date_str).date()
            else: ad_date = pd.to_datetime(file_date_str, dayfirst=True).date()

            shop_label = filename.replace(file_date_str, '').replace('.xlsx', '').replace('.csv', '').replace('.xls', '').strip(' _-').upper()

            filepath = os.path.join(ADS_INPUT_DIR, filename)
            df = pd.read_csv(filepath) if filepath.lower().endswith('.csv') else pd.read_excel(filepath)
            
            # GOM CỘT CHI PHÍ (Né CPC/CPM rác)
            cost_cols = [c for c in df.columns if ('chi phí' in str(c).lower() or 'cost' in str(c).lower()) 
                         and not any(x in str(c).lower() for x in ['cpc', 'cpm', 'per', 'mỗi', 'lượt', 'tỷ lệ', 'rate', 'thuế'])]
            
            if cost_cols:
                raw_cost = sum(extract_money(df[c]).sum() for c in cost_cols)
                vat_cost = raw_cost * 1.08 # Thêm 8% VAT
                ads_data.append((ad_date, CHANNEL_SHOPEE_ID, shop_label, float(vat_cost)))
                files_to_move.append(filepath)
                print(f"✅ Ads Shop [{shop_label}] | Ngày: {ad_date} | Phí + VAT: {vat_cost:,.0f}đ")

        except Exception as e: print(f"⚠️ Lỗi Ads {filename}: {e}")
    return ads_data, files_to_move

# ==========================================
# 5. LOAD TO DB (UPSERT CHUẨN)
# ==========================================
def load_to_db(df_final, ads_data):
    conn = psycopg2.connect(**DB_CONFIG); cursor = conn.cursor()
    try:
        if not df_final.empty:
            orders = df_final.drop_duplicates(subset=['ext_id'])
            o_data = [(r['ext_id'], CHANNEL_SHOPEE_ID, r['time'], r['time'].date(), r['status'], r['is_can'], r['label'], r['rev'], r['paid'], r['f_fix'], r['f_svc'], r['f_pay'], r['aff'], r['payout']) for _, r in orders.iterrows()]
            
            q_ord = """INSERT INTO orders (external_order_id, channel_id, order_time, order_date, status, is_cancelled, label, seller_revenue, customer_paid, fixed_fee, service_fee, payment_fee, affiliate_amount, payout_actual)
                       VALUES %s ON CONFLICT (external_order_id, channel_id) DO UPDATE SET status=EXCLUDED.status, is_cancelled=EXCLUDED.is_cancelled, label=EXCLUDED.label, seller_revenue=EXCLUDED.seller_revenue, payout_actual=EXCLUDED.payout_actual, updated_at=CURRENT_TIMESTAMP RETURNING external_order_id, id;"""
            returned = execute_values(cursor, q_ord, o_data, fetch=True)
            mapping = {str(row[0]): int(row[1]) for row in returned}

            cursor.execute("SELECT barcode, cost FROM product_costs")
            costs = {str(r[0]): float(r[1]) for r in cursor.fetchall()}
            target_ids = tuple(mapping.values())
            if target_ids: cursor.execute("DELETE FROM order_items WHERE order_id IN %s", (target_ids,))
            
            item_vals = [(mapping[r['ext_id']], r['product'], '', r['barcode'], r['qty'], 0, 'quà tặng' in r['product'].lower(), sum(costs.get(p.strip(), 0) for p in str(r['barcode']).split('+'))) for _, r in df_final.iterrows() if r['ext_id'] in mapping]
            execute_values(cursor, "INSERT INTO order_items (order_id, product_name, product_code, barcode, quantity, price, is_gift, unit_cogs) VALUES %s", item_vals)

        if ads_data:
            cursor.execute("ALTER TABLE ads_costs ADD COLUMN IF NOT EXISTS shop_label TEXT;")
            cursor.execute("ALTER TABLE ads_costs DROP CONSTRAINT IF EXISTS ads_costs_unique_key; ALTER TABLE ads_costs ADD CONSTRAINT ads_costs_unique_key UNIQUE (date, channel_id, shop_label);")
            q_ads = "INSERT INTO ads_costs (date, channel_id, shop_label, cost) VALUES %s ON CONFLICT (date, channel_id, shop_label) DO UPDATE SET cost = EXCLUDED.cost;"
            execute_values(cursor, q_ads, ads_data)
            print("✅ Đã nạp Chi phí Quảng cáo.")
        
        conn.commit()
    except Exception as e: conn.rollback(); print(f"❌ LỖI DB: {e}")
    finally: cursor.close(); conn.close()

if __name__ == "__main__":
    res_df, order_files = process_shopee_files()
    ads_data, ads_files = process_shopee_ads()
    load_to_db(res_df, ads_data)
    for f in order_files + ads_files:
        if os.path.exists(f): os.replace(f, os.path.join(PROCESSED_DIR if f in order_files else ADS_PROCESSED_DIR, os.path.basename(f)))
    print("\n📁 HOÀN TẤT!")
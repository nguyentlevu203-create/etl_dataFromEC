import os
import shutil
import pandas as pd
import numpy as np
import psycopg2
import re
from psycopg2.extras import execute_values

# =====================================================================
# CẤU HÌNH HỆ THỐNG
# =====================================================================
DB_CONFIG = {
    "dbname": "BaoCao", "user": "postgres", 
    "password": "Vu123", "host": "localhost", "port": "5433"
}
CHANNEL_TIKTOK_ID = 3
BASE_IN = "data/tiktok/"
BASE_OUT = "processed/tiktok/"
DIRS = {
    "orders": "1_orders/", "creator": "2_affiliate_creator/",
    "partner": "3_affiliate_partner/", "paid": "4_settlement_paid/",
    "unpaid": "5_settlement_unpaid/", "ads_prod": "6_ads_product/",
    "ads_live": "7_ads_live/", "master": "8_master_data/"
}

for d in DIRS.values():
    os.makedirs(os.path.join(BASE_IN, d), exist_ok=True)
    os.makedirs(os.path.join(BASE_OUT, d), exist_ok=True)

def init_db():
    conn = psycopg2.connect(**DB_CONFIG); cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS fact;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact.tiktok_order_pnl (
                order_id TEXT PRIMARY KEY, order_date DATE, order_status TEXT, is_cancelled BOOLEAN,
                brand_group TEXT, total_sku_qty INT, fee_base_revenue NUMERIC(15,2), gmv_before_cancel NUMERIC(15,2),
                fixed_fee NUMERIC(15,2), payment_fee NUMERIC(15,2), vxp_fee NUMERIC(15,2), infrastructure_fee NUMERIC(15,2),
                commission_amount NUMERIC(15,2), booking_fee NUMERIC(15,2), return_shipping_fee NUMERIC(15,2), packaging_cost NUMERIC(15,2),
                backoffice_cost NUMERIC(15,2), cogs_amount NUMERIC(15,2), settlement_paid NUMERIC(15,2), settlement_unpaid NUMERIC(15,2),
                estimated_payout NUMERIC(15,2), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    finally: cur.close(); conn.close()

# =====================================================================
# HÀM HỖ TRỢ ĐỌC DỮ LIỆU
# =====================================================================
def get_valid_files(directory):
    if not os.path.exists(directory): return []
    return [f for f in os.listdir(directory) if (f.endswith('.xlsx') or f.endswith('.csv') or f.endswith('.xls')) and not f.startswith('~$')]

def extract_shop_label(filename):
    if 'LPM' in str(filename).upper(): return 'LPM'
    return 'SUA_CHUA'

def normalize_brand(x):
    if 'LPM' in str(x).upper(): return 'LPM'
    return 'SUA_CHUA'

def ext_money(series):
    if series is None: return pd.Series(0)
    def parse_val(x):
        x = str(x).strip()
        if not x or x.lower() in ['nan', 'none', 'nat']: return 0.0
        x = re.sub(r'[^\d.,-]', '', x)
        if '.' in x and ',' in x:
            if x.rfind('.') > x.rfind(','): x = x.replace(',', '')
            else: x = x.replace('.', '').replace(',', '.')
        elif '.' in x:
            parts = x.split('.')
            if len(parts) > 2 or len(parts[-1]) == 3: x = x.replace('.', '')
        elif ',' in x:
            parts = x.split(',')
            if len(parts) > 2 or len(parts[-1]) == 3: x = x.replace(',', '')
            else: x = x.replace(',', '.')
        try: return float(x)
        except: return 0.0
    return series.apply(parse_val)

def read_file_robust(filepath):
    df = pd.DataFrame()
    if str(filepath).lower().endswith('.csv'):
        try:
            df = pd.read_csv(filepath, header=None, low_memory=False, encoding='utf-8-sig', dtype=str)
            if not df.empty and len(df.columns) > 1: return df
        except: pass
    try:
        df = pd.read_excel(filepath, header=None, dtype=str)
        if not df.empty and len(df.columns) > 1: return df
    except: pass
    return pd.DataFrame()

def ultra_smart_load(filepath):
    df_raw = read_file_robust(filepath)
    if df_raw.empty: return pd.DataFrame()
        
    best_idx = -1
    for i in range(min(30, len(df_raw))):
        valid_cells = [str(x) for x in df_raw.iloc[i].values if pd.notna(x) and str(x).strip() not in ('', 'nan', 'none')]
        row_str = re.sub(r'\s+', ' ', " ".join(valid_cells).lower())
        if len(valid_cells) >= 3 and any(k in row_str for k in ['order/adjustment id', 'order id', 'mã đơn', 'id đơn', 'settlement', 'thanh toán', 'chi phí', 'thời gian', 'sku', 'phiên live', 'trạng thái', 'product name', 'tên sản phẩm', 'tên chiến dịch', 'tên phiên live']):
            best_idx = i
            break
    if best_idx == -1: return pd.DataFrame()

    header_row = [str(c).strip() for c in df_raw.iloc[best_idx].values]
    data_start_idx = best_idx + 1
    if data_start_idx < len(df_raw):
        row_2_str = re.sub(r'\s+', ' ', " ".join([str(x) for x in df_raw.iloc[data_start_idx].values if pd.notna(x)]).lower())
        if 'platform unique' in row_2_str or 'duy nhất' in row_2_str or 'description' in row_2_str or 'chú thích' in row_2_str or 'định dạng' in row_2_str:
            data_start_idx += 1  

    df = df_raw.iloc[data_start_idx:].copy()
    df.columns = header_row
    df = df.loc[:, ~df.columns.duplicated()]
    return df.loc[:, ~df.columns.str.contains('^Unnamed|^nan|^None', case=False, na=False)].copy()

def get_col(df_cols, keywords):
    for c in df_cols:
        c_clean = re.sub(r'\s+', ' ', str(c).lower().strip())
        if c_clean in keywords: return c
    for c in df_cols:
        c_clean = re.sub(r'\s+', ' ', str(c).lower().strip())
        for kw in keywords:
            if kw in c_clean: return c
    return None

# =====================================================================
# PIPELINE CHÍNH & HỆ THỐNG AUDIT LOG
# =====================================================================
def process_pipeline():
    print(f"\n{'='*60}\n🚀 TIKTOK ETL: V8 - TRUNG TÂM KIỂM SOÁT INPUT (AUDIT LOG)\n{'='*60}")
    
    # 🌟 BỘ LƯU TRỮ TRẠNG THÁI (Dùng để in bảng tổng kết cuối cùng)
    audit_log = {
        "orders": {"files": 0, "status": "Trống", "detail": ""},
        "creator": {"files": 0, "status": "Trống", "detail": ""},
        "partner": {"files": 0, "status": "Trống", "detail": ""},
        "paid": {"files": 0, "status": "Trống", "detail": ""},
        "unpaid": {"files": 0, "status": "Trống", "detail": ""},
        "ads_prod": {"files": 0, "status": "Trống", "detail": ""},
        "ads_live": {"files": 0, "status": "Trống", "detail": ""}
    }
    
    processed_files = []
    master_path = os.path.join(BASE_IN, DIRS['master'])

    # --- BƯỚC 0: ĐỌC DỮ LIỆU MASTER ---
    print("\n[0/7] 📂 ĐANG NẠP DỮ LIỆU MASTER (Giá vốn, Booking)...")
    cost_files = get_valid_files(master_path)
    cost_file = next((f for f in cost_files if 'giá von' in f.lower()), None)
    cost_map = {}
    if cost_file:
        df_cost = pd.read_excel(os.path.join(master_path, cost_file), dtype=str)
        c_ean = get_col(df_cost.columns, ['mã ean'])
        c_ma = next((c for c in df_cost.columns if str(c).strip().lower() in ['mã', 'mã sản phẩm', 'mã sp']), None)
        c_price = get_col(df_cost.columns, ['giá nhập'])
        if c_price:
            df_cost['price_clean'] = ext_money(df_cost[c_price])
            for _, row in df_cost.iterrows():
                if c_ma and pd.notna(row[c_ma]) and str(row[c_ma]).strip() != 'nan': cost_map[str(row[c_ma]).strip().upper()] = float(row['price_clean'])
                if c_ean and pd.notna(row[c_ean]) and str(row[c_ean]).strip() != 'nan': cost_map[str(row[c_ean]).strip().upper()] = float(row['price_clean'])

    cat_file = next((f for f in cost_files if 'nhóm hàng' in f.lower()), None)
    cat_map = {}
    if cat_file:
        df_cat = pd.read_excel(os.path.join(master_path, cat_file)) if cat_file.endswith('.xlsx') else pd.read_csv(os.path.join(master_path, cat_file))
        c_label = get_col(df_cat.columns, ['row labels', 'nhóm hàng', 'category']) or df_cat.columns[0]
        c_nhom = get_col(df_cat.columns, ['nhóm', 'brand', 'shop']) or df_cat.columns[1]
        cat_map = dict(zip(df_cat[c_label].astype(str).str.strip().str.lower(), df_cat[c_nhom].astype(str).str.strip()))

    book_file = next((f for f in cost_files if 'phí book' in f.lower()), None)
    book_map = {}
    if book_file:
        df_book = pd.read_excel(os.path.join(master_path, book_file)) if book_file.endswith('.xlsx') else pd.read_csv(os.path.join(master_path, book_file))
        c_tk = get_col(df_book.columns, ['tài khoản', 'account', 'creator']) or df_book.columns[0]
        c_hh = get_col(df_book.columns, ['hoa hồng', 'commission', 'rate']) or df_book.columns[1]
        book_map = dict(zip(df_book[c_tk].astype(str).str.strip().str.lower(), pd.to_numeric(df_book[c_hh], errors='coerce').fillna(0)))

    # --- BƯỚC 1: XỬ LÝ ĐƠN HÀNG ---
    print("\n[1/7] 📦 XỬ LÝ INPUT: 1_orders...")
    order_path = os.path.join(BASE_IN, DIRS['orders'])
    o_files = get_valid_files(order_path)
    audit_log["orders"]["files"] = len(o_files)
    
    if not o_files:
        print(f"   ❌ THẤT BẠI: Thư mục 1_orders trống. Dừng chương trình vì thiếu Đơn hàng gốc!")
        audit_log["orders"]["status"] = "Lỗi nghiêm trọng"
        audit_log["orders"]["detail"] = "Không có file đơn hàng nào để tạo bảng base."
        return pd.DataFrame(), [], processed_files, audit_log

    o_file = o_files[0]
    print(f"   -> Nạp file: {o_file}")
    df_o = ultra_smart_load(os.path.join(order_path, o_file))
    processed_files.append((order_path, o_file))

    co = {
        'id': get_col(df_o.columns, ['order id', 'mã đơn hàng', 'id đơn hàng', 'mã đơn']),
        'sku': get_col(df_o.columns, ['seller sku', 'sku người bán', 'sku', 'mã sku']),
        'status': get_col(df_o.columns, ['order status', 'trạng thái', 'tình trạng']),
        'time': get_col(df_o.columns, ['created time', 'thời gian tạo', 'thời gian đặt hàng', 'ngày tạo']),
        'qty': get_col(df_o.columns, ['quantity', 'số lượng', 'number of items', 'số mặt hàng', 'sl']),
        'price': get_col(df_o.columns, ['original price', 'giá gốc', 'sku unit original price', 'giá bán lẻ']),
        'discount': get_col(df_o.columns, ['seller discount', 'chiết khấu từ người bán', 'chiết khấu', 'giảm giá']),
        'gmv': get_col(df_o.columns, ['subtotal after discount', 'sau chiết khấu', 'tổng cộng', 'doanh thu']),
        'category': get_col(df_o.columns, ['sku\'s product category', 'product category', 'hạng mục sản phẩm', 'ngành hàng'])
    }
    
    if not co['id']: 
        print(f"   ❌ LỖI FILE: Không tìm thấy cột Mã Đơn Hàng trong file {o_file}.")
        audit_log["orders"]["status"] = "Lỗi định dạng"
        return pd.DataFrame(), [], processed_files, audit_log

    df_o[co['id']] = df_o[co['id']].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_o['qty_num'] = ext_money(df_o[co['qty']]) if co['qty'] else 1.0
    df_o['price_num'] = ext_money(df_o[co['price']]) if co['price'] else 0.0
    df_o['disc_num'] = ext_money(df_o[co['discount']]).abs() if co['discount'] else 0.0
    df_o['cat_clean'] = df_o[co['category']].astype(str).str.strip().str.lower() if co['category'] else ''
    df_o['brand_group'] = df_o['cat_clean'].map(cat_map).fillna('SUA_CHUA').apply(normalize_brand)
    df_o['fee_base_item'] = (df_o['price_num'] * df_o['qty_num']) - df_o['disc_num']
    df_o['gmv_item'] = ext_money(df_o[co['gmv']]) if co['gmv'] else 0.0
    
    if co['sku']:
        df_o['line_cogs'] = df_o[co['sku']].apply(lambda x: sum(cost_map.get(p.strip().upper(), 7000) for p in str(x).upper().split('+')) if '+' in str(x) else cost_map.get(str(x).strip().upper(), 7000)) * df_o['qty_num']
    else: df_o['line_cogs'] = 7000 * df_o['qty_num']

    agg_dict = {'brand_group': 'first', 'fee_base_item': 'sum', 'gmv_item': 'sum', 'line_cogs': 'sum', 'qty_num': 'sum'}
    if co['time']: agg_dict[co['time']] = 'first'
    if co['status']: agg_dict[co['status']] = 'first'

    fact_df = df_o.groupby(co['id']).agg(agg_dict).reset_index().rename(columns={co['id']: 'order_id'})
    if co['status']: fact_df.rename(columns={co['status']: 'order_status'}, inplace=True)
    else: fact_df['order_status'] = 'Unknown'
    if co['time']: fact_df['order_date'] = pd.to_datetime(fact_df[co['time']], format='mixed', dayfirst=True, errors='coerce').dt.date
    else: fact_df['order_date'] = None
    fact_df['is_cancelled'] = fact_df['order_status'].astype(str).str.contains('Hủy|Cancel', case=False, na=False)

    print(f"   ✅ OK: Ghi nhận {len(fact_df)} đơn hàng duy nhất.")
    audit_log["orders"]["status"] = "Thành công"
    audit_log["orders"]["detail"] = f"Nạp {len(fact_df)} mã đơn."

    # --- BƯỚC 2: AFFILIATE CREATOR ---
    print("\n[2/7] 🤝 XỬ LÝ INPUT: 2_affiliate_creator...")
    fact_df['comm_amt'] = 0.0
    fact_df['book_rate'] = 0.0 
    creator_path = os.path.join(BASE_IN, DIRS['creator'])
    c_files = get_valid_files(creator_path)
    audit_log["creator"]["files"] = len(c_files)
    
    if not c_files:
        print("   ⚠️ Trống: Không có file Affiliate Creator nào.")
    else:
        rows_added = 0
        for c_file in c_files:
            print(f"   -> Nạp file: {c_file}")
            df_c = ultra_smart_load(os.path.join(creator_path, c_file))
            processed_files.append((creator_path, c_file))
            cid = get_col(df_c.columns, ['order id', 'id đơn hàng', 'mã đơn hàng', 'mã đơn'])
            if cid:
                cst = get_col(df_c.columns, ['status', 'trạng thái'])
                c_est_std = get_col(df_c.columns, ['thanh toán hoa hồng tiêu chuẩn ước tính', 'ước tính'])
                c_est_ads = get_col(df_c.columns, ['thanh toán hoa hồng quảng cáo cửa hàng ước tính', 'quảng cáo'])
                c_acc = get_col(df_c.columns, ['tên người dùng nhà sáng tạo', 'creator username', 'tài khoản'])
                
                df_c[cid] = df_c[cid].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                est_std_val = ext_money(df_c[c_est_std]) if c_est_std else pd.Series(0, index=df_c.index)
                est_ads_val = ext_money(df_c[c_est_ads]) if c_est_ads else pd.Series(0, index=df_c.index)
                c_val = est_std_val + est_ads_val
                df_c['creator_commission_amount'] = np.where(df_c[cst].astype(str).str.contains('Không đủ điều kiện', case=False, na=False), 0, c_val) if cst else c_val
                
                if c_acc:
                    df_c['acc_clean'] = df_c[c_acc].astype(str).str.strip().str.lower()
                    df_c['mapped_book_rate'] = df_c['acc_clean'].map(book_map).fillna(0)
                else: df_c['mapped_book_rate'] = 0.0
                
                df_c_grouped = df_c.groupby(cid).agg({'creator_commission_amount': 'sum', 'mapped_book_rate': 'first'}).reset_index().rename(columns={cid: 'order_id'})
                fact_df = fact_df.merge(df_c_grouped, on='order_id', how='left')
                fact_df['comm_amt'] += fact_df['creator_commission_amount'].fillna(0)
                fact_df['book_rate'] = np.where(fact_df['mapped_book_rate'].notnull(), fact_df['mapped_book_rate'], fact_df['book_rate'])
                fact_df.drop(columns=['creator_commission_amount', 'mapped_book_rate'], inplace=True)
                rows_added += len(df_c_grouped)
            else:
                print(f"   ❌ LỖI: File {c_file} không có cột ID.")
        print(f"   ✅ OK: Ghép nối Hoa hồng Creator cho {rows_added} đơn.")
        audit_log["creator"]["status"] = "Thành công"
        audit_log["creator"]["detail"] = f"Khớp {rows_added} đơn."

    # --- BƯỚC 3: AFFILIATE PARTNER ---
    print("\n[3/7] 🏢 XỬ LÝ INPUT: 3_affiliate_partner...")
    partner_path = os.path.join(BASE_IN, DIRS['partner'])
    p_files = get_valid_files(partner_path)
    audit_log["partner"]["files"] = len(p_files)
    if not p_files:
        print("   ⚠️ Trống: Không có file Affiliate Partner nào.")
    else:
        rows_added = 0
        for p_file in p_files:
            print(f"   -> Nạp file: {p_file}")
            df_p = ultra_smart_load(os.path.join(partner_path, p_file))
            processed_files.append((partner_path, p_file))
            pid = get_col(df_p.columns, ['order id', 'id đơn hàng', 'mã đơn hàng', 'mã đơn'])
            p_act = get_col(df_p.columns, ['hoa hồng thực tế cho đối tác liên kết', 'hoa hồng thực tế'])
            if pid:
                df_p[pid] = df_p[pid].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                df_p['partner_commission_amount'] = ext_money(df_p[p_act]) if p_act else pd.Series(0, index=df_p.index)
                grp = df_p.groupby(pid)['partner_commission_amount'].sum().reset_index().rename(columns={pid: 'order_id'})
                fact_df = fact_df.merge(grp, on='order_id', how='left')
                fact_df['comm_amt'] += fact_df['partner_commission_amount'].fillna(0)
                fact_df.drop(columns=['partner_commission_amount'], inplace=True)
                rows_added += len(grp)
        print(f"   ✅ OK: Ghép nối Hoa hồng Partner cho {rows_added} đơn.")
        audit_log["partner"]["status"] = "Thành công"
        audit_log["partner"]["detail"] = f"Khớp {rows_added} đơn."

    # --- BƯỚC 4: SETTLEMENT PAID ---
    print("\n[4/7] 💰 XỬ LÝ INPUT: 4_settlement_paid...")
    fact_df['set_paid'] = 0.0
    paid_path = os.path.join(BASE_IN, DIRS['paid'])
    pd_files = get_valid_files(paid_path)
    audit_log["paid"]["files"] = len(pd_files)
    if not pd_files:
        print("   ⚠️ Trống: Không có file Paid nào. Dòng tiền sẽ bị khuyết.")
    else:
        total_paid_money = 0
        for pd_file in pd_files:
            print(f"   -> Nạp file: {pd_file}")
            df_pd = ultra_smart_load(os.path.join(paid_path, pd_file))
            processed_files.append((paid_path, pd_file))
            c_pd_id = get_col(df_pd.columns, ['order/adjustment id', 'mã đơn hàng/điều chỉnh', 'mã đơn hàng', 'order id'])
            c_rel_id = get_col(df_pd.columns, ['related order id', 'id đơn hàng liên quan'])
            pd_val = get_col(df_pd.columns, ['total settlement amount', 'tổng số tiền thanh toán', 'settlement amount', 'số tiền quyết toán'])
            if (c_pd_id or c_rel_id) and pd_val:
                if c_rel_id and c_pd_id:
                    df_pd['final_id'] = np.where(df_pd[c_rel_id].notna() & (df_pd[c_rel_id].astype(str).str.strip() != '') & (~df_pd[c_rel_id].astype(str).str.strip().str.lower().isin(['nan', 'none'])), df_pd[c_rel_id], df_pd[c_pd_id])
                else: df_pd['final_id'] = df_pd[c_rel_id] if c_rel_id else df_pd[c_pd_id]
                df_pd['final_id'] = df_pd['final_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                
                grp = df_pd.groupby('final_id')[pd_val].apply(lambda x: ext_money(x).sum()).reset_index().rename(columns={'final_id': 'order_id', pd_val: 'set_paid_val'})
                fact_df = fact_df.merge(grp, on='order_id', how='left')
                fact_df['set_paid'] += fact_df['set_paid_val'].fillna(0)
                total_paid_money += fact_df['set_paid_val'].sum()
                fact_df.drop(columns=['set_paid_val'], inplace=True)
            else:
                print(f"   ❌ LỖI: Không tìm thấy cột ID hoặc Tiền trong file {pd_file}.")
        print(f"   ✅ OK: Hút được {total_paid_money:,.0f} đ tiền Paid.")
        audit_log["paid"]["status"] = "Thành công"
        audit_log["paid"]["detail"] = f"{total_paid_money:,.0f} đ"

    # --- BƯỚC 5: SETTLEMENT UNPAID ---
    print("\n[5/7] ⏳ XỬ LÝ INPUT: 5_settlement_unpaid...")
    fact_df['set_unpaid'] = 0.0
    unpaid_path = os.path.join(BASE_IN, DIRS['unpaid'])
    up_files = get_valid_files(unpaid_path)
    audit_log["unpaid"]["files"] = len(up_files)
    if not up_files:
        print("   ⚠️ Trống: Không có file Unpaid nào.")
    else:
        total_unpaid_money = 0
        for up_file in up_files:
            print(f"   -> Nạp file: {up_file}")
            df_up = ultra_smart_load(os.path.join(unpaid_path, up_file))
            processed_files.append((unpaid_path, up_file))
            c_up_id = get_col(df_up.columns, ['order/adjustment id', 'mã đơn hàng/điều chỉnh', 'order id', 'mã đơn hàng', 'mã đơn'])
            c_rel_up_id = get_col(df_up.columns, ['related order id', 'id đơn hàng liên quan'])
            up_val = get_col(df_up.columns, ['total estimated settlement amount', 'est. settlement amount', 'estimated settlement amount', 'tổng số tiền dự kiến', 'số tiền quyết toán dự kiến', 'ước tính'])
            if (c_up_id or c_rel_up_id) and up_val:
                if c_rel_up_id and c_up_id:
                    df_up['final_id'] = np.where(df_up[c_rel_up_id].notna() & (df_up[c_rel_up_id].astype(str).str.strip() != '') & (~df_up[c_rel_up_id].astype(str).str.strip().str.lower().isin(['nan', 'none'])), df_up[c_rel_up_id], df_up[c_up_id])
                else: df_up['final_id'] = df_up[c_rel_up_id] if c_rel_up_id else df_up[c_up_id]
                df_up['final_id'] = df_up['final_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                
                grp = df_up.groupby('final_id')[up_val].apply(lambda x: ext_money(x).sum()).reset_index().rename(columns={'final_id': 'order_id', up_val: 'set_unpaid_val'})
                fact_df = fact_df.merge(grp, on='order_id', how='left')
                fact_df['set_unpaid'] += fact_df['set_unpaid_val'].fillna(0)
                total_unpaid_money += fact_df['set_unpaid_val'].sum()
                fact_df.drop(columns=['set_unpaid_val'], inplace=True)
            else:
                print(f"   ❌ LỖI: Không tìm thấy cột ID/Tiền trong {up_file}.")
        print(f"   ✅ OK: Ghi nhận {total_unpaid_money:,.0f} đ tiền Unpaid (Treo).")
        audit_log["unpaid"]["status"] = "Thành công"
        audit_log["unpaid"]["detail"] = f"{total_unpaid_money:,.0f} đ"

    # --- TÍNH TOÁN PHÍ (NỘI BỘ MÁY TÍNH) ---
    def calc_fees(r):
        is_lpm = (r['brand_group'] == 'LPM')
        rate_fixed = 0.158 if is_lpm else 0.1031
        rate_pack = 2000 if is_lpm else 3000
        standard_pack_cost = r['qty_num'] * rate_pack
        f_book = r['fee_base_item'] * r['book_rate']
        f_fix = r['fee_base_item'] * rate_fixed
        f_pay = r['fee_base_item'] * 0.05
        f_vxp = r['fee_base_item'] * 0.03
        f_infra = 3000
        f_cogs = r['line_cogs']
        f_pack = standard_pack_cost
        
        estimated_payout = r['fee_base_item'] - (f_fix + f_pay + f_vxp + f_infra + r['comm_amt'])
        
        if r['is_cancelled']: return pd.Series([0, 0, 0, 0, 30000, f_pack, 0, 0, f_book, 0])
        if r['set_paid'] != 0: tien_nhan_lai = r['set_paid']
        elif r['set_unpaid'] != 0: tien_nhan_lai = r['set_unpaid']
        else: tien_nhan_lai = estimated_payout
        f_bo = tien_nhan_lai * 0.15
        
        return pd.Series([f_fix, f_pay, f_vxp, f_infra, 0, f_pack, f_cogs, f_bo, f_book, estimated_payout])

    fact_df[['fixed_fee', 'payment_fee', 'vxp_fee', 'infra_fee', 'return_fee', 'pack_cost', 'cogs_amount', 'bo_cost', 'booking_fee', 'estimated_payout']] = fact_df.apply(calc_fees, axis=1)

    # --- BƯỚC 6 & 7: QUẢNG CÁO ADS PROD VÀ ADS LIVE ---
    ads_master_dict = {} 
    
    for idx, d_name in enumerate(['ads_prod', 'ads_live']):
        step_num = 6 + idx
        folder_label = "6_ads_product" if d_name == 'ads_prod' else "7_ads_live"
        print(f"\n[{step_num}/7] 📈 XỬ LÝ INPUT: {folder_label}...")
        
        a_path = os.path.join(BASE_IN, DIRS[d_name])
        a_files = get_valid_files(a_path)
        audit_log[d_name]["files"] = len(a_files)
        
        if not a_files:
            print(f"   ⚠️ Trống: Không có file {folder_label} nào.")
            continue
            
        total_folder_ads = 0
        for a_f in a_files:
            print(f"   -> Đang soi: {a_f}")
            ads_shop = extract_shop_label(a_f)
            
            date_match = re.search(r'(\d{4}[-_]\d{1,2}[-_]\d{1,2})', a_f)
            if not date_match: 
                print(f"      ⚠️ BỎ QUA: Không tìm thấy ngày YYYY-MM-DD trong tên file.")
                continue
            
            file_date = pd.to_datetime(date_match.group(1).replace('_', '-')).date()
            df_a = ultra_smart_load(os.path.join(a_path, a_f))
            processed_files.append((a_path, a_f))
            if df_a.empty: continue
            
            total_file_cost = 0
            target_col = None
            
            # TẦNG 1: Tìm theo tên cột
            for c in df_a.columns:
                if str(c).strip().lower() in ['chi phí', 'cost']:
                    target_col = c; break
            if not target_col:
                for c in df_a.columns:
                    c_clean = str(c).lower()
                    if ('chi phí' in c_clean or 'cost' in c_clean) and not any(x in c_clean for x in ['cpc', 'cpm', 'tỷ lệ', 'ròng', 'per', 'mỗi', 'hoàn', 'đơn hàng']):
                        target_col = c; break
            
            if target_col:
                total_file_cost = ext_money(df_a[target_col]).sum() * 1.10
            else:
                # TẦNG 2: Ép Vị trí RAW
                df_raw = read_file_robust(os.path.join(a_path, a_f))
                target_idx = 10 if d_name == 'ads_prod' else 5
                if not df_raw.empty and target_idx < len(df_raw.columns):
                    vals = df_raw.iloc[1:, target_idx]
                    total_file_cost = ext_money(vals).sum() * 1.10
            
            if total_file_cost > 0:
                key = (file_date, ads_shop)
                if key in ads_master_dict: ads_master_dict[key] += total_file_cost
                else: ads_master_dict[key] = total_file_cost
                total_folder_ads += total_file_cost
                print(f"      ✅ Ghi nhận: {total_file_cost:,.0f} đ")
            else:
                print(f"      ❌ Thất bại: Cột tiền trống hoặc sai cấu trúc.")
                
        print(f"   ✅ TỔNG KẾT {folder_label}: Nạp {total_folder_ads:,.0f} đ.")
        audit_log[d_name]["status"] = "Thành công"
        audit_log[d_name]["detail"] = f"{total_folder_ads:,.0f} đ"

    ads_data = [(d, CHANNEL_TIKTOK_ID, s, float(cost)) for (d, s), cost in ads_master_dict.items()]
    
    # 🎯 IN BẢNG TỔNG KẾT RA MÀN HÌNH TERMINAL 🎯
    print("\n============================================================")
    print("📊 BẢNG TỔNG KẾT TRẠNG THÁI KIỂM TOÁN (AUDIT REPORT)")
    print("============================================================")
    for folder, data in audit_log.items():
        icon = "✅" if data["status"] == "Thành công" else "⚠️" if data["status"] == "Trống" else "❌"
        # Canh lề đẹp mắt cho Terminal
        folder_pad = folder.ljust(12)
        files_str = f"({data['files']} file)".ljust(10)
        print(f"{icon} {folder_pad} | {files_str} | {data['status'].ljust(12)} | {data['detail']}")
    print("============================================================\n")

    return fact_df, ads_data, processed_files, audit_log

def load_to_db(fact_df, ads_data):
    conn = psycopg2.connect(**DB_CONFIG); cur = conn.cursor()
    try:
        if fact_df.empty:
            print("⚠️ BỎ QUA NẠP DB: Không có dữ liệu đơn hàng mới.")
        else:
            vals = []
            for _, r in fact_df.iterrows():
                if not str(r['order_id']).strip() or str(r['order_id']) == 'nan': continue
                vals.append((
                    str(r['order_id']), r['order_date'], str(r['order_status']), bool(r['is_cancelled']), str(r['brand_group']),
                    int(r['qty_num']), float(r['fee_base_item']), float(r['gmv_item']),
                    float(r['fixed_fee']), float(r['payment_fee']), float(r['vxp_fee']), float(r['infra_fee']),
                    float(r['comm_amt']), float(r['booking_fee']), float(r['return_fee']), float(r['pack_cost']), 
                    float(r['bo_cost']), float(r['cogs_amount']), float(r['set_paid']), float(r['set_unpaid']),
                    float(r['estimated_payout'])
                ))
            if vals:
                q = """
                    INSERT INTO fact.tiktok_order_pnl (
                        order_id, order_date, order_status, is_cancelled, brand_group, total_sku_qty, fee_base_revenue, gmv_before_cancel,
                        fixed_fee, payment_fee, vxp_fee, infrastructure_fee, commission_amount, booking_fee, return_shipping_fee, packaging_cost, backoffice_cost, cogs_amount,
                        settlement_paid, settlement_unpaid, estimated_payout
                    ) VALUES %s ON CONFLICT (order_id) DO UPDATE SET
                        order_date=EXCLUDED.order_date, order_status=EXCLUDED.order_status, is_cancelled=EXCLUDED.is_cancelled, 
                        brand_group=EXCLUDED.brand_group, total_sku_qty=EXCLUDED.total_sku_qty, fee_base_revenue=EXCLUDED.fee_base_revenue,
                        gmv_before_cancel=EXCLUDED.gmv_before_cancel, fixed_fee=EXCLUDED.fixed_fee, payment_fee=EXCLUDED.payment_fee,
                        vxp_fee=EXCLUDED.vxp_fee, infrastructure_fee=EXCLUDED.infrastructure_fee, commission_amount=EXCLUDED.commission_amount, 
                        booking_fee=EXCLUDED.booking_fee, return_shipping_fee=EXCLUDED.return_shipping_fee, packaging_cost=EXCLUDED.packaging_cost, 
                        backoffice_cost=EXCLUDED.backoffice_cost, cogs_amount=EXCLUDED.cogs_amount, settlement_paid=EXCLUDED.settlement_paid, 
                        settlement_unpaid=EXCLUDED.settlement_unpaid, estimated_payout=EXCLUDED.estimated_payout, updated_at=CURRENT_TIMESTAMP;
                """
                execute_values(cur, q, vals)

        if ads_data:
            q_ads = """
                INSERT INTO ads_costs (date, channel_id, shop_label, cost) VALUES %s
                ON CONFLICT (date, channel_id, shop_label) DO UPDATE SET cost = EXCLUDED.cost;
            """
            execute_values(cur, q_ads, ads_data)
            
        conn.commit(); print("💾 THÀNH CÔNG: Đã Save dữ liệu vào Database Postgres!")
    finally: cur.close(); conn.close()

if __name__ == "__main__":
    init_db()  
    fact_data, ads_data, processed_files, _ = process_pipeline()
    load_to_db(fact_data, ads_data)
    
    # Move processed files
    for path, f in processed_files:
        dest_folder = os.path.join(BASE_OUT, os.path.basename(os.path.normpath(path)))
        os.makedirs(dest_folder, exist_ok=True)
        try:
            shutil.move(os.path.join(path, f), os.path.join(dest_folder, f))
        except Exception as e:
            print(f"⚠️ Lỗi khi di chuyển file {f}: {e}")
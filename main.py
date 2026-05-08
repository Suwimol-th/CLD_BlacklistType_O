import os
import sys
import pandas as pd
from datetime import datetime
import configparser
import logging
import traceback
from mailer import send_summary_email

def get_app_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def main():
    app_path = get_app_path()
    
    # ---------------------------------------------------------
    # 1. ตั้งค่า Logging (สร้าง Folder 'log' และแยกไฟล์รายเดือน)
    # ---------------------------------------------------------
    log_dir = os.path.join(app_path, 'log')
    os.makedirs(log_dir, exist_ok=True)
    
    today = datetime.now()
    current_month = today.strftime('%Y%m')
    log_filename = os.path.join(log_dir, f'run_log_{current_month}.log')
    
    # ป้องกัน Log ซ้อนกันกรณีรันหลายรอบใน IDE
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8'
    )
    
    logging.info("==================================================")
    logging.info("START RUN TIME - Blacklist Processor Service")
    logging.info("==================================================")

    config_path = os.path.join(app_path, 'config.ini')
    if not os.path.exists(config_path):
        logging.error(f"ไม่พบไฟล์ตั้งค่า: {config_path}")
        print(f"Error: ไม่พบไฟล์ {config_path}")
        logging.info("END RUN TIME\n")
        return

    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')

    raw_base_dir = config['Paths']['base_source_dir']
    dest_dir = config['Paths']['dest_dir']
    dest_filename = config['Paths']['dest_filename']
    mail_conf = config['MailConfig']
    mail_temp = config['MailTemplates']
    signature = mail_conf['signature']
    
    year_str = today.strftime('%Y')
    today_str = today.strftime('%d/%m/%Y')
    yyyymm = today.strftime('%Y%m')
    yyyymmdd = today.strftime('%Y%m%d')
    
    actual_base_dir = raw_base_dir.replace('{year}', year_str)
    
    # ใช้ Path ตามที่คุณกิ๊ฟแก้ (อ่านจากโฟลเดอร์ YYYYMM โดยตรง)
    folder_path = os.path.join(actual_base_dir, yyyymm)
    source_filename = f"EXP_{yyyymmdd}.xlsx"
    source_filepath = os.path.join(folder_path, source_filename)
    dest_filepath = os.path.join(dest_dir, dest_filename)

    def send_mail(template_key, **kwargs):
        subject = mail_temp[f"{template_key}_subject"].format(date=today_str, **kwargs)
        body_raw = mail_temp[f"{template_key}_body"].format(date=today_str, **kwargs)
        full_body = f"{body_raw}\n\n--\n{signature}"
        
        logging.info(f"กำลังเตรียมส่งอีเมล หัวข้อ: {subject}")
        send_summary_email(mail_conf['smtp_server'], mail_conf['smtp_port'], 
                           mail_conf['mail_from'], mail_conf['mail_to'], mail_conf['mail_cc'], subject, full_body)

    logging.info(f"กำลังค้นหาไฟล์ต้นทางที่: {source_filepath}")
    if not os.path.exists(source_filepath):
        logging.warning(f"ไม่พบไฟล์ต้นทาง (Case 1)")
        send_mail('missing_source', source_path=source_filepath)
        logging.info("==================================================")
        logging.info("END RUN TIME\n")
        return

    logging.info("พบไฟล์ต้นทาง เริ่มกระบวนการอ่าน Sheet ต่างๆ")
    print(f"Processing: {source_filepath}")
    
    sheets_info = [(config['Sheets']['sheet1_name'], config['Sheets']['sheet1_remark']),
                   (config['Sheets']['sheet2_name'], config['Sheets']['sheet2_remark'])]
    
    total_source = 0
    all_data = []
    
    # กำหนด Header ปลายทาง (ตามที่คุณกิ๊ฟแก้ไข)
    DEST_HEADERS = ["ID/PASSPORT", "Title", "Name", "Surname", "Remark"]

    for sheet_name, remark in sheets_info:
        try:
            df = pd.read_excel(source_filepath, sheet_name=sheet_name)
            record_count = len(df)
            total_source += record_count
            logging.info(f"อ่านข้อมูล Sheet '{sheet_name}' สำเร็จ จำนวน: {record_count} รายการ")
            
            if not df.empty:
                df_out = pd.DataFrame(columns=DEST_HEADERS)
                
                # Mapping ข้อมูล (ต้นทาง -> ปลายทาง)
                if 'CITIZEN_ID' in df.columns: 
                    df_out['ID/PASSPORT'] = df['CITIZEN_ID']
                if 'TITLE' in df.columns: 
                    df_out['Title'] = df['TITLE'] 
                if 'FIRSTNAME_THAI' in df.columns: 
                    df_out['Name'] = df['FIRSTNAME_THAI']
                if 'LASTNAME_THAI' in df.columns: 
                    df_out['Surname'] = df['LASTNAME_THAI']
                
                df_out['Remark'] = remark
                all_data.append(df_out)
        except Exception as e:
            logging.error(f"เกิดข้อผิดพลาดในการอ่าน Sheet '{sheet_name}': {e}")
            print(f"Error reading sheet {sheet_name}: {e}")

    if total_source == 0 or not all_data:
        logging.warning("ไฟล์ต้นทางไม่มีข้อมูลใน Sheet ที่กำหนด (Case 2)")
        send_mail('empty_source', source_filename=source_filename)
        logging.info("==================================================")
        logging.info("END RUN TIME\n")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    
    # --- จุดที่ทำให้เป็น Text ทั้งหมด ---
    # แปลงทุกอย่างเป็น string และจัดการค่าว่างไม่ให้ขึ้นคำว่า 'nan'
    final_df = final_df.astype(str).replace('nan', '')
    total_dest = len(final_df)
    logging.info(f"รวมข้อมูลทั้งหมดเตรียมบันทึก จำนวนปลายทาง: {total_dest} รายการ")
    
    try:
        logging.info(f"กำลังบันทึกไฟล์ปลายทางที่: {dest_filepath}")
        if not os.path.exists(dest_dir): os.makedirs(dest_dir, exist_ok=True)
        
        # บันทึกไฟล์และเปลี่ยนชื่อ Sheet เป็น 'Person'
        final_df.to_excel(dest_filepath, index=False, sheet_name='Person')
        logging.info("บันทึกไฟล์ปลายทาง (Save Excel) สำเร็จ!")

        send_mail('success', source_filename=source_filename, total_source=total_source, 
                  total_dest=total_dest, dest_path=dest_filepath)
    except Exception as e:
        logging.error(f"บันทึกไฟล์ปลายทางล้มเหลว (Case 3): {e}")
        send_mail('error_dest', total_dest=total_dest, dest_path=dest_filepath, error_detail=str(e))
        
    logging.info("==================================================")
    logging.info("END RUN TIME")
    logging.info("==================================================\n")

if __name__ == "__main__":
    main()
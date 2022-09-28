import pymysql
import shift_check
from datetime import datetime
import datetime
import os
import time

str_time = datetime.datetime.now()
time.sleep(3)

def createFolder(directory,data):
    
    date_time=datetime.datetime.now()
    curtime1=date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2=date_time.strftime("%d-%m-%Y")
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        f= open("Log/"+str(curtime2)+".txt","a+")
        f.write(curtime1 +" "+ str(data) +"\r\n")
        f.close()
    except OSError:
        print ('Error: Creating directory. ' +  directory)

createFolder('Log/',"Startted ")



def function_call():

    try:
        db = pymysql.connect(host="localhost", user="root",passwd="", db="digital_factory_ent_v1_trail")
        cursor = db.cursor()
        db2 = pymysql.connect(host="localhost", user="root",passwd="", db="test_kepserver")
        cursor2 = db2.cursor()

    except Exception as e:
        createFolder('Log/',"Error connecting database : " + str(e))

    try:
        shift_check.shift_check()
        print(shift_check.company_date)
        print(f"{shift_check.act_date} 00:00:00")

        if shift_check.shift_end_flag == 1:
            createFolder('Log/', " Shift Changed to---Date--" +str(shift_check.act_date)+"-----shift--"+str(shift_check.act_shift))
            shift_check.shfit_end()
            shift_check.shift_end_flag = 0
            
    except Exception as e:
        createFolder('Log/', "Error in Shift_check---->>>"+str(e))


    try:
        cursor.execute("SELECT m.equipment_id,m.m_time_write,m.m_interlock_status,m.m_interlock_write,m.interlock_en_dis_address,m.Plc_stop_code_update_address,m.buffer_set_address,m.Plc_Stop_code_change_addrss,m.plc_shift_reset,m.downtime_buffer_address, m.cycle_time_address, m.down_time_buffer_reset_address, m.cycle_buffer_reset_address, m.shift_reset_address, m.Heart_beat_address,m.starting_string,m.starting_address,m.parameter,m.length,c.target,c.machine_status,c.product_code,c.current_stop_duration,c.actual,m.address,m.ip_address,m.port,d.product_id,m.equipment_code,m.equipment_name,c.machine_status AS machine_status,m.status AS machine_enable,m.equipment_id as equipment_id FROM current_production c INNER JOIN master_equipment m ON c.machine_id=m.equipment_id  INNER JOIN master_product d ON c.product_id=d.product_id  WHERE c.status = 0 AND m.parameter = '1'  ORDER BY c.machine_id")
        mc_rows = cursor.fetchall()
        for mc_row in mc_rows:

            # machine status of master_equipment
            machine_id = mc_row[0]
            machine_status_me = int(mc_row[20])
            cursor2.execute(f"SELECT * FROM test_kepserver.tii_1 where _NUMERICID={machine_id} and status='no'")
            print(f"SELECT * FROM test_kepserver.tii_1 where _NUMERICID={machine_id} and status='no'")
            rows = cursor2.fetchall()
            createFolder('Log/',f"length of rows : {len(rows)}")
            if len(rows) > 0 :

                for row in rows :
                    print(row)
                    # machine_status of kepserver ti_table . 
                    machine_status_ks = int(row[3])
                    timestamp = row[4]
                    # print(machine_status_ks)
                    if machine_status_ks == 0:
                        
                        sql = (f''' UPDATE current_production SET 
                                    shift_run_time = shift_run_time + IFNULL(TIMESTAMPDIFF(SECOND,current_run_time,'{row[4]}'),0), 
                                    date_time=now()
                                    WHERE machine_id = {machine_id} AND status = 0 ''')
                        cursor.execute(sql)
                        db.commit()

                        sql = (f''' UPDATE current_production SET 
                                    run_time = shift_run_time,
                                    current_run_time = '0000-00-00 00:00:00', 
                                    date_time=now()
                                    WHERE machine_id = {machine_id} AND status = 0 ''')
                        cursor.execute(sql)
                        db.commit()

                        # update cp set ms=0, csc = case when csbt = 0 then 1 else csc end,csbt = case when csbt = 0 then dt else csbt end,csd = now() - dt where mc_id=1
                        sql = (f''' UPDATE current_production SET 
                                    machine_status = 0 ,
                                    current_stop_code = CASE WHEN current_stop_begin_time = '0000-00-00 00:00:00' THEN 1 ELSE current_stop_code END,
                                    current_stop_begin_time = CASE WHEN current_stop_begin_time = '0000-00-00 00:00:00' THEN '{row[4]}' ELSE current_stop_begin_time END ,
                                    current_stop_duration = TIMESTAMPDIFF(SECOND,'{row[4]}',NOW()), 
                                    date_time=now()
                                    WHERE machine_id = {machine_id} AND status = 0 ''')
                        print(sql)
                        cursor.execute(sql)
                        db.commit()

                        # For breakdown popup .
                        sql = (f'''UPDATE current_production SET 
                                    breakdown_popup = CASE WHEN current_stop_duration > (SELECT m_minor_stoppage_time FROM master_equipment WHERE equipment_id = {machine_id}) 
                                    THEN 1 ELSE breakdown_popup END, 
                                    date_time=now()
                                    WHERE machine_id = {machine_id} ; 
                                    ''')
                        print(sql)
                        cursor.execute(sql)
                        db.commit()


                        # update ti set status=yes where id=1      
                        sql = (f''' UPDATE test_kepserver.tii_1 SET STATUS = 'yes' WHERE id = {row[0]} ''')
                        cursor2.execute(sql)
                        db2.commit()
                        createFolder('Log/',f"Machine_status from ks equals {machine_status_ks} and status change to yes in {row[0]}")
                        # print(f"Machine_status from ks equals {machine_status_ks} and datas aupdated ")

                    elif machine_status_ks == 1:

                        sql = (f''' UPDATE current_production SET 
                                    shift_stop_duration = shift_stop_duration + IFNULL(TIMESTAMPDIFF(SECOND,current_stop_begin_time,'{row[4]}'),0), 
                                    date_time=now()
                                    WHERE machine_id = {machine_id} AND status = 0 ''')
                        cursor.execute(sql)
                        db.commit()

                        sql = (f''' UPDATE current_production SET 
                                    loss_time_1 = shift_stop_duration, 
                                    date_time=now()
                                    WHERE machine_id = {machine_id} AND status = 0 ''')
                        cursor.execute(sql)
                        db.commit()

                        mill_date_shift_check = shift_check.date_shift_check(timestamp)
                        mill_date = mill_date_shift_check[0]
                        print(type(mill_date))
                        print(mill_date)
                        mill_shift = mill_date_shift_check[1]
                        print(type(mill_shift))

                        # insert closs (m_date,m_sh,mc_id,prod_id,op_id,sup_id,csc,csbt,csd) select m_date,m_sh,mc_id,prod_id,op_id,sup_id,csc,csbt,dt - csbt from cp where mc_id = 1 and csbt <> 0
                        sql = (f'''INSERT INTO current_loss 
                                    (machine_id,mill_date,mill_shift,product_id,operator_id,supervisor_id,current_stop_code,current_stop_begin_time,current_stop_duration)
                                    SELECT machine_id,'{mill_date} 00:00:00','{mill_shift}',product_id,operator1_id,supervisor_id,current_stop_code,current_stop_begin_time,TIMESTAMPDIFF(SECOND,current_stop_begin_time,'{row[4]}') 
                                    FROM current_production WHERE machine_id = {machine_id} AND status = 0 AND current_stop_begin_time <> '0000-00-00 00:00:00' ''')
                        print(sql)
                        cursor.execute(sql)
                        db.commit()

                        # update cp set ms=1, csc = 0,csbt = 0,csd = 0 where mc_id=1
                        sql = (f'''UPDATE current_production SET machine_status = 1,current_stop_code = 0,current_stop_begin_time = 0, current_stop_duration = 0 , current_run_time = '{row[4]}', date_time=now() WHERE machine_id = {machine_id} AND status = 0''')
                        cursor.execute(sql)
                        db.commit()

                        # update ti set status=yes where id=1
                        sql = (f''' UPDATE test_kepserver.tii_1 SET STATUS = 'yes' WHERE id = {row[0]} ''')
                        cursor2.execute(sql)
                        db2.commit()
                        createFolder('Log/',f"Machine_status from ks equals {machine_status_ks} and status change to yes in {row[0]}")
                        # print(f"Machine_status from ks equals {machine_status_ks} and datas aupdated ")
            else:
                createFolder ('Log/',f"records with status no : {len(rows)}. ")
                if machine_status_me == 0:
                    # update cp set csd = now() - csbt where mc_id=1
                    sql = (f''' UPDATE current_production SET current_stop_duration = IFNULL(TIMESTAMPDIFF(SECOND,current_stop_begin_time,NOW()),0) ,
                    loss_time_1 = TIMESTAMPDIFF(SECOND,current_stop_begin_time,NOW()) + shift_stop_duration, 
                    date_time=now() WHERE machine_id = {machine_id} ''')
                    cursor.execute(sql)
                    db.commit()
                    createFolder('Log/',f"Loss time updated .")
                elif machine_status_me == 1:
                    sql = (f''' UPDATE current_production SET current_run_duration = IFNULL(TIMESTAMPDIFF(SECOND,current_run_time,NOW()),0) ,
                    run_time = TIMESTAMPDIFF(SECOND,current_run_time,NOW()) + shift_run_time, 
                    date_time=now() WHERE machine_id = {machine_id} ''')
                    cursor.execute(sql)
                    db.commit()
                    createFolder('Log/',f"Run time updated .")

    except Exception as e :
        print(str(e))
        createFolder('Log/',"Error in function call : " + str(e))

# while True:

#     try:
#         db = pymysql.connect(host="localhost", user="root",passwd="", db="digital_factory_ent_v1_trail")
#         cursor = db.cursor()
#         db2 = pymysql.connect(host="localhost", user="root",passwd="", db="test_kepserver")
#         cursor2 = db2.cursor()

#     except Exception as e:
#         createFolder('Log/',"Error connecting database : " + str(e))
#     time.sleep(1)
#     function_call()

#     try:
#         shift_check.shift_check()
#         print(shift_check.company_date)
#         print(f"{shift_check.act_date} 00:00:00")

#         if shift_check.shift_end_flag == 1:
#             createFolder('Log/', " Shift Changed to---Date--" +str(shift_check.act_date)+"-----shift--"+str(shift_check.act_shift))
#             shift_check.shfit_end()
#             shift_check.shift_end_flag = 0
            
#     except Exception as e:
#         createFolder('Log/', "Error in Shift_check---->>>"+str(e))
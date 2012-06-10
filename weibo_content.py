#!/usr/local/bin/python3

#-*- coding: utf-8 -*-

import sys, getopt 
from weibopy.auth import OAuthHandler
from weibopy.api import API
from weibopy.error import WeibopError
import webbrowser
import pymysql
import time


DEFAULT_FETCH_USERS_NUMBER	= 1
DEFAULT_ONE_PAGE_COUNT		= 100
DEFAULT_CITY_CODE		= 11 # beijing

APP_KEY				= 1830868372
APP_SECRET			= """425d41c01a336ab667e4b92fc64812ac"""
BACK_URL			= "http://www.bubargain.com/backurl"

class Mode:
    FROM_DB     = 1
    FROM_NAME   = 2

class Logging:
    func_name = ''

    def __init__(self, func_name):
        self.func_name = func_name

    @staticmethod
    def get_logger(func_name):
        return Logging(func_name.upper())

    @staticmethod
    def timestamp():
        return time.strftime('%Y-%m-%d %X', time.localtime(time.time()))

    def info(self, content):
        print(Logging.timestamp() + "  INFO   [" + self.func_name  + "]: " + content)

    def warning(self, content):
        print(Logging.timestamp() + " WARNING [" + self.func_name  + "]: " + content)

    def error(self, content):
        print(Logging.timestamp() + "  ERROR  [" + self.func_name  + "]: " + content)


# global vars:
g_city_code 		= DEFAULT_CITY_CODE
g_one_page_count	= DEFAULT_ONE_PAGE_COUNT 
g_fetch_users_number	= DEFAULT_FETCH_USERS_NUMBER
g_stored_counter	= 0
g_mode			= Mode.FROM_DB
g_name			= ""
g_person_counter	= 0


def timestamp():
    return time.strftime('%Y-%m-%d %X ', time.localtime(time.time()))

def logging(content):
    print(timestamp() + content)

def do_auth():
    logging = Logging.get_logger('do_auth')
    auth = OAuthHandler(APP_KEY, APP_SECRET, BACK_URL)
    auth_url = auth.get_authorization_url()
    request_token_key = auth.request_token.key
    request_token_secret = auth.request_token.secret
    auth.set_request_token(request_token_key, request_token_secret)
    webbrowser.open(auth_url)
    verifier = input("Verifier: ").strip()
    access_token = auth.get_access_token(verifier)
    ATK = access_token.key
    ATS = access_token.secret
    auth.setAccessToken(ATK, ATS)
    api = API(auth)
    user = api.verify_credentials()
    logging.info("We are uing API from account: [uid = %s, name = %s]" % (user.id, user.screen_name))
    return api


def fetch_one_user_bilaterals(api, _uid):
    all_bilaterals = []
    bilaterals = api.show_bilateral(uid=_uid, count=g_one_page_count, page=1)
    bilaterals_number = len(bilaterals.users)
    logging("[FETCH_ONE]: Get %d bilaterals this time." % bilaterals_number)
    all_bilaterals.extend(get_bilaterals_data(bilaterals, bilaterals_number)) 
    bilaterals_total_number = bilaterals.total_number
    logging("[FETCH_ONE]: There are %d bilaterals of uid:%s " % (bilaterals_total_number, _uid))
    if (bilaterals_total_number >= bilaterals_number): 
        page_number = get_page_number(bilaterals_total_number, g_one_page_count)
        logging("[FETCH_ONE]: There are %d pages in total." % page_number)
        for p in range(2, page_number+1): # page 1 has been got
            bilaterals = api.show_bilateral(uid=_uid, count=g_one_page_count, page=p)
            bilaterals_number = len(bilaterals.users)
            logging("[FETCH_ONE]: Get %d bilaterals this time." % bilaterals_number)
            all_bilaterals.extend(get_bilaterals_data(bilaterals, bilaterals_number)) 
        #logging(all_bilaterals)
        return all_bilaterals
    else:
        logging("[FETCH_ERROR]: Error When fetch one user's bilaterals!!! =====>>>>>>> total_number: %d, one_page_count: %d" % (bilaterals_total_number, bilaterals_number)) 
        return False


def get_bilaterals_data(bilaterals, number):
    data = []
    for index in range(0, number):
        #logging("province = %s" % bilaterals.users[index]['province'])
        if (bilaterals.users[index]['province'] == str(g_city_code)):
            uid = bilaterals.users[index]['id']
            name = bilaterals.users[index]['name']
            logging("uid = %s    name = %s" % (uid, name))
            data.append((uid, name))
            #logging(data)
    #logging("[GET_DATA]: Get bilaterals data OK!! ====----====---->>> data: %s" % data)
    #logging("[GET_DATA]: Get bilaterals data OK!! ")
    return data


def get_page_number(total_number, page_number):
    if (total_number % page_number != 0):
        return int(total_number/page_number) + 1
    else:
        return int(total_number/page_number)


def is_exist(conn, uid):
    cursor = conn.cursor()
    sql = "select id from temp_users where uid = %s"
    param = uid
    n = cursor.execute(sql, param)
    if (0 == n):
        #logging("[CHECK_EXIST]: The user does not exist in temp, uid = %s" % uid)
        sql = "select id from users where uid = %s"
        n = cursor.execute(sql, param)
        if (0 == n):
            #logging("[CHECK_EXIST]: The user does not exist in users, uid = %s" % uid)
            cursor.close()
            return False
        elif (1 == n):
            #logging("[CHECK_EXIST]: Exist in users, uid = %s" % uid)
            cursor.close()
            return True
        else:
            logging("[CHECK_EXIST_ERROR]: Error Occured when check the uid = %s in users" % uid)
            cursor.close()
            conn.close()
            logging("[INFO]: Stored " + str(g_stored_counter) + " New Person In Total!")
            sys.exit(1)
    elif (1 == n):
        #logging("[CHECK_EXIST]: Exist in temp, uid = %s" % uid)
        cursor.close()
        return True
    else:
        logging("[CHECK_EXIST_ERROR]: Error Occured when check the uid = %s in temp" % uid)
        cursor.close()
        conn.close()
        logging("[INFO]: Stored " + str(g_stored_counter) + " New Person In Total!")
        sys.exit(1)


def reset_extended(conn, uid):
    cursor = conn.cursor()
    sql = "update users set extended='T' where uid = %s"
    param = uid
    n = cursor.execute(sql, param)
    if (n >= 0):
        logging("[RESET_EXTENDED]: Reset Extended Flag OK!")
        cursor.close()
        return True
    else:
        logging("[RESET_EXTENDED_ERROR]: Reset Extended Flag FAILED!!!")
        cursor.close()
        return False


def store_one_user_bilaterals(conn, bilaterals):
    global g_stored_counter
    cursor = conn.cursor()
    sql = "insert into temp_users (uid, nick_name) values(%s,%s)"
    for b in bilaterals:
        #logging("[STORE_BILATERALS]: one of them b: " + str(b))
        if (not is_exist(conn, b[0])):
            #logging("[STORE_BILATERALS]: This is a new user!!!")
            param = b
            n = cursor.execute(sql, param)
            if (1 == n):
                #logging("[STORE_ONE_BILATERALS]: Store bilateral uid = %s, name= %s OK!!" % (b[0], b[1]))
                g_stored_counter += 1
            else:
                logging("[STORE_ONE_BILATERALS_ERROR]: Error Occured when store the user of uid = %s, name= %s +++=================------>>>>>>>>>>><<<<<<<<<<<------===============" % (b[0], b[1]))
                cursor.close()
                return False
        else:
            pass
            #logging("[STORE_BILATERALS]: This user has been stored!!! uid = %s, name = %s" % (b[0], b[1]))
    cursor.close()
    return True



def fetch_users(conn):
    if (Mode.FROM_DB == g_mode):
        logging("[FETCH_USERS_DB]: DB MODE!!! ")
        sql = "select uid from users limit %s"
        param = int(g_fetch_users_number)
    elif (Mode.FROM_NAME == g_mode):
        return [(g_name,)]
        #logging("[FETCH_USERS_NAME]: NAME MODE!!! ")
        #sql = "select uid from users where nick_name = %s"
        #param = g_name
    else:
        logging("[FETCH_USERS_ERROR]: MODE IS NOT EXIST!!! ====================<><><><><><><><><><>==================== ")
        return False
    cursor = conn.cursor()
    n = cursor.execute(sql, param)
    if (Mode.FROM_DB == g_mode and g_fetch_users_number == n):
        logging("[FETCH_USERS_DB]: Fetch %d users Successfully" % n)
        uids = cursor.fetchall()
        cursor.close()
        logging("[FETCH_USERS_DB]: To Process Users: " + str(uids))
        return uids
    elif (Mode.FROM_DB == g_mode and n >= 0):
        logging("[FETCH_USERS_DB]: There is less than %d users, Fetched %d users Successfully" % (g_fetch_users_number, n))
        uids = cursor.fetchall()
        cursor.close()
        return uids
    elif (Mode.FROM_NAME == g_mode and 1 == n):
        logging("[FETCH_USERS_NAME]: Fetched user: %s Successfully!" % g_name)
        uid = cursor.fetchone()
        logging("[FETCH_USERS_NAME]: name: %s    uid: %s" % (g_name, str(uid)))
        cursor.close()
        return [uid]
    elif (0 == n):
        logging("[FETCH_USER_WARNING]: NO SUCH USER in DB!")
        cursor.close()
        return False
    else:
        logging("[FETCH_USERS_ERROR]: Database Operation ERROR!! n = %d" % n)
        cursor.close()
        return False
        

def fetch_store_one_user_weibo(conn, api, uid):
    logging = Logging.get_logger('fetch_store_one_user_weibo')
    fetch_result = fetch_one_user_weibo(api, uid)
    logging.info("fetch_result: %s" % fetch_result)
    if (False == fetch_result):
        logging.error("ERROR Occured when fetching weibo")
        return False
    else:
        logging.info("Fetch weibo of uid: %s OK!!" % uid)
        if (False == store_one_user_weibo(conn, fetch_result)):
            logging.error("ERROR Occured when storing weibo!")
            return False
        else:
            logging.info("Store weibo of uid: %s OK!!" % uid)
            return True


def fetch_store_weibo(conn, api, uids):
    global g_person_counter
    logging = Logging.get_logger('fetch_store_weibo')
    logging.info("uids: %s" % str(uids))
    for uid in uids:
        g_person_counter += 1
        logging.info("----------=-=-=-=-=-=-=-=-=-=========================--==-=-=-=-=->.>.>.>.>.>.>>>>>> person: %d START!!" % g_person_counter)
        if (True == fetch_store_one_user_weibo(conn, api, uid[0])):
            logging.info("-----------=-=-=-=-=-=-=-=-=-==========================---=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=>>>>>>>>>>> person: %d END!!" % g_person_counter)
        else:
            logging.error("Error Occured when process the person: %d   uid: %s", (g_person_counter, uid[0]))
            return False 
    logging.info("Fetch and Store %d persons Successfully!" % g_person_counter)
    return True

def fetch_weibo_to_database(conn):
    logging = Logging.get_logger('fetch_weibo_to_database')
    fetch_users_result = fetch_users(conn)
    if (False == fetch_users_result):
        logging.error("Error Occured When Fetching Users!!")
        logging.info("Stored " + str(g_stored_counter) + " New Person In Total!")
        sys.exit(1)
    else:
        logging.info("Fetch users OK!!")
        uids = fetch_users_result
    logging.info("Start to do Auth!!! ==============>>>>> ^_^")
    api = do_auth()
    logging.info("Done Auth!!! ==============>>>>> ^_^")
    #bilaterals = fetch_bilaterals(api, uids)
    if (True == fetch_store_weibo(conn, api, uids)):
        logging.info("Store All weibo Successfully!!!")
        return True
    else:
        logging.error("Store All weibo Failed!!!")
        return False




def main():
    global g_one_page_count, g_fetch_users_number, g_mode, g_name
    try:
        opts,args = getopt.getopt(sys.argv[1:],"p:c:u:n:")
        for op,value in opts:
            if op == "-p":
                g_one_page_count = int(value)
            elif op == "-u":
                g_fetch_users_number = int(value)
            elif op == "-n":
                g_name = str(value)
                logging(g_name)
                g_mode = Mode.FROM_NAME
        print(opts)  
        print(args) 
    except getopt.GetoptError:
        logging("[ERROR]: Params are not defined well!")
        logging("[INFO]: Stored " + str(g_stored_counter) + " New Person In Total!")
        sys.exit(1)

    logging("START")
    conn = pymysql.connect(host="ec2-204-236-172-73.us-west-1.compute.amazonaws.com", user="root", passwd="RooT", db="spider", charset="utf8")
    fetch_weibo_to_database(conn)
    conn.close()
    logging("[INFO]: Stored " + str(g_stored_counter) + " New Person In Total!")
    logging("END")




if __name__ == "__main__":
    main()



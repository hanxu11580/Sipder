import datetime
import requests
import execjs
from configs import connect_db
from threading import Timer


def get_js():
    with open('./js/main.js', 'r', encoding='UTF-8') as f1:
        line = f1.readline()
        js_main = ''
        while line:
            js_main += line
            line = f1.readline()
    with open('./js/pako-min.js', 'r', encoding='UTF-8') as f2:
        line = f2.readline()
        js_pako = ''
        while line:
            js_pako += line
            line = f2.readline()
    return js_pako, js_main


def time_grasp(start_time=None, end_time=None):
    """
        获得给定起始时间和结束时间：
            start_time 默认为 本地时间
            end_time 默认为 start_time + 5天
        :param start_time: 年月入时分秒 '2020-01-01 12:12:12'
        :return:start_time, end_time (string)
    """
    if start_time is not None:
        start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:00:00')
    else:
        start_time = datetime.datetime.now()
    # url 最多5天
    if end_time is not None:
        end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:00:00').strftime('%Y-%m-%d %H:00:00')
    else:
        end_time = (start_time + datetime.timedelta(days=5)).strftime('%Y-%m-%d %H:00:00')
    start_time = start_time.strftime('%Y-%m-%d %H:00:00')
    return start_time, end_time


def request_url(forecast_time, end_time):
    """"
        请求数据并获得有效数据, 小于10000的数据 URL 无数据
        :param forecast_time: 起始时间
        :param end_time: 结束时间
        :return: 获得每一小时的预测数据
    """
    content_list = []
    while datetime.datetime.strptime(forecast_time, '%Y-%m-%d %H:%M:%S') <= datetime.datetime.strptime(end_time,'%Y-%m-%d %H:%M:%S'):
        forecast_time = (datetime.datetime.strptime(forecast_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        forecast_time_0 = str(forecast_time)[:10].replace('-', '')
        forecast_time_1 = str(forecast_time)[:16].replace('-', '').replace(':', '').replace(' ', '')
        city_url = 'https://map.zq12369.com/data//gzip/{}/aqi/{}-cityaqidata.txt'.format(forecast_time_0, forecast_time_1)
        res = requests.get(city_url)
        if res.status_code == 200 and len(res.text) > 10000:
            content_list.append(res.content.decode('utf-8'))
            print('url:{}请求成功'.format(city_url))

        else:
            content_list.append(None)
            print('url:{}暂无数据'.format(city_url))
    return content_list


def data_handle(content_list):
    air_zq_city = []
    air_zq_forecast_data = []
    for content in content_list:
        if content is not None:
            js_pako, js_main = get_js()
            js = js_pako + js_main
            exec = execjs.compile(js)

            str_data = exec.call('decode', content)
            for info in str_data.split('|')[1:]:
                info = info.replace(',,', ',')
                data = info.split(',')
                air_zq_city.append((data[1], data[2], data[3], data[4]))  # 省 市 经纬度
                air_zq_forecast_data.append(data)
    return air_zq_city, air_zq_forecast_data


def sql_insert(city_info, forecast_info):
    """
        主键重复的更新， 否则插入
        :param city_info:
        :param forecast_info:
        :return:
    """
    cout1 = cout2 = 0
    conn, cursor = connect_db()
    city_info = list(set(city_info))

    for city in city_info:
        sql = "insert  into air_zq_city (province, city, longitude, latitude) values ('{}', '{}', '{}', '{}') on duplicate key update longitude=values (longitude), latitude=values (latitude)".format(city[0], city[1], city[2], city[3])
        sql1 = "insert  into air_zq_city (province, city, longitude, latitude) values ('{}', '{}', '{}', '{}') on duplicate key update longitude=values (longitude), latitude=values (latitude)".format(city[0], city[1]+"市", city[2], city[3])
        sql_area_code = "update air_zq_city zq join air_state_city state on state.city_name=\'{}\' set zq.area_code=state.unique_area_code where zq.city=\'{}\'".format(city[1],city[1])
        sql_area_code1 = "update air_zq_city zq join air_state_city state on state.city_name=\'{}\' set zq.area_code=state.unique_area_code where zq.city=\'{}\'".format(city[1]+"市",city[1]+"市")
        print('air_zq_city插入中')
        if '盟' in city[1] or '地区' in city[1]:
            cursor.execute(sql)
            cursor.execute(sql_area_code)

        else:
            if len(city[1]) >= 3 and '州' in city[1]:
                cursor.execute(sql)
                cursor.execute(sql_area_code)
            else:
                if city[1] == '克州' or city[1] == '博州' or city[1] == '巴州':
                    cursor.execute(sql)
                    cursor.execute(sql_area_code)
                else:
                    # 如果不是上面这几种情况那么直接加上 市
                    cursor.execute(sql1)
                    cursor.execute(sql_area_code1)


        conn.commit()
        cout1 += 1

    for forecast in forecast_info:
        print('forecast插入中')
        if len(forecast) < 18:
            forecast.append(None)

        sql = "insert into air_zq_city_forecast_data (forecast_time, province, city, longitude,latitude,aqi,pm25,pm10,so2,no2,co,o3,temp10,temp11,temp12,temp13,temp14,temp15) values ('{}', '{}', '{}', '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}') on duplicate key update aqi=values (aqi), pm25=values (pm25), pm10=values (pm10), so2=values (so2), no2=values (no2), co=values (co), o3=values (o3), temp10=values (temp10),temp11=values (temp11), temp12=values (temp12), temp13=values (temp13), temp14=values (temp14), temp15=values (temp15)".format(forecast[0], forecast[1], forecast[2], forecast[3], forecast[4], forecast[5],forecast[6], forecast[7], forecast[8], forecast[9], forecast[10], forecast[11], forecast[12],forecast[13], forecast[14], forecast[15], forecast[16], forecast[17])
        sql1 = "insert into air_zq_city_forecast_data (forecast_time, province, city, longitude,latitude,aqi,pm25,pm10,so2,no2,co,o3,temp10,temp11,temp12,temp13,temp14,temp15) values ('{}', '{}', '{}', '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}') on duplicate key update aqi=values (aqi), pm25=values (pm25), pm10=values (pm10), so2=values (so2), no2=values (no2), co=values (co), o3=values (o3), temp10=values (temp10),temp11=values (temp11), temp12=values (temp12), temp13=values (temp13), temp14=values (temp14), temp15=values (temp15)".format(forecast[0], forecast[1], forecast[2]+"市", forecast[3], forecast[4], forecast[5],forecast[6], forecast[7], forecast[8], forecast[9], forecast[10], forecast[11], forecast[12],forecast[13], forecast[14], forecast[15], forecast[16], forecast[17])
        sql_area_code = "update air_zq_city_forecast_data zq join air_state_city state on state.city_name=\'{}\' set zq.area_code=state.unique_area_code where zq.city=\'{}\'".format(forecast[2],forecast[2])
        sql_area_code1 = "update air_zq_city_forecast_data zq join air_state_city state on state.city_name=\'{}\' set zq.area_code=state.unique_area_code where zq.city=\'{}\'".format(forecast[2]+"市", forecast[2]+"市")
        if '盟' in forecast[2] or '地区' in forecast[2]:
            cursor.execute(sql)
            cursor.execute(sql_area_code)

        else:
            if len(forecast[2]) >= 3 and '州' in forecast[2]:
                cursor.execute(sql)
                cursor.execute(sql_area_code)
            else:
                if forecast[2] == '克州' or forecast[2] == '博州' or forecast[2] == '巴州':
                    cursor.execute(sql)
                    cursor.execute(sql_area_code)
                else:
                    # 如果不是上面这几种情况那么直接加上 市
                    cursor.execute(sql1)
                    cursor.execute(sql_area_code1)


        conn.commit()
        cout2 += 1


    cursor.close()
    print('>>>成功插入{}条city_info数据(含更新)<<<'.format(cout1))
    print('>>>成功插入{}条forecast_info数据(含更新)<<<'.format(cout2))


def spider(start_time=None, end_time=None):
    """
    :param start_time:开始时间  string
    :param end_time: 结束时间   string
    :return:
    """
    start_time, end_time = time_grasp(start_time, end_time)
    content_list = request_url(start_time, end_time)
    city_info, forecast_info = data_handle(content_list)
    sql_insert(city_info, forecast_info)






def int2str(start_time, end_time):
    start_time = str(start_time)
    end_time = str(end_time)
    start_time = '{year}-{month}-{day} {hour}:00:00'.format(year=start_time[:4], month=start_time[4:6],
                                                            day=start_time[6:8], hour=start_time[8:])
    end_time = '{year}-{month}-{day} {hour}:00:00'.format(year=end_time[:4], month=end_time[4:6],
                                                          day=end_time[6:8], hour=end_time[8:])
    return start_time, end_time







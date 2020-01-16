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
    for content in content_list:
        if content is not None:
            air_zq_city = []
            air_zq_forecast_data = []

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

    for city in city_info:
        print('air_zq_city插入中')
        cursor.execute("insert  into air_zq_city (province, city, longitude, latitude) values ('{}', '{}', '{}', '{}') on duplicate key update longitude=values (longitude), latitude=values (latitude)".format(city[0], city[1], city[2],city[3]))
        conn.commit()
        cout1 += 1

    for forecast in forecast_info:
        print('forecast插入中')
        if len(forecast) < 18:
            forecast.append(None)
        cursor.execute("insert  into air_zq_city_forecast_data (forecast_time, province, city, temp1,temp2,temp3,temp4,temp5,temp6,temp7,temp8,temp9,temp10,"
                       "temp11,temp12,temp13,temp14,temp15) values ('{}', '{}', '{}', '{}','{}','{}','{}','{}','{}','{}',"
                       "'{}','{}','{}','{}','{}','{}','{}','{}') on duplicate key update temp3=values (temp3), "
                       "temp4=values (temp4), temp5=values (temp5), temp6=values (temp6), temp7=values (temp7), temp8=values (temp8), temp9=values (temp9), temp10=values (temp10),"
                       "temp11=values (temp11), temp12=values (temp12), temp13=values (temp13), temp14=values (temp14), temp15=values (temp15)".format(
            forecast[0], forecast[1], forecast[2], forecast[3], forecast[4], forecast[5],
            forecast[6], forecast[7], forecast[8], forecast[9], forecast[10], forecast[11], forecast[12],
            forecast[13], forecast[14], forecast[15], forecast[16], forecast[17]))
        conn.commit()
        cout2 += 1
    cursor.close()
    print('>>>成功插入{}条数据(含更新)<<<'.format(cout1))
    print('>>>成功插入{}条数据(含更新)<<<'.format(cout2))


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







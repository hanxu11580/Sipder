from utils.tools import spider, int2str
from flask import Flask
from flask_restful import Api, Resource
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
api = Api(app)


class SpiderApi(Resource):
    def get(self):
        return spider()


class SpiderApiTime(Resource):
    def get(self, start_time, end_time):
        # 2020010612 to '2020-01-06 12:00:00'
        start_time, end_time = int2str(start_time, end_time)
        spider(start_time, end_time)


api.add_resource(SpiderApi, '/')
api.add_resource(SpiderApiTime, '/<int:start_time>/<int:end_time>')


if __name__ == '__main__':
    """
        每隔1小时，爬取一次5天数据
    """
    # scheduler = BackgroundScheduler()
    #     # # interval 时间间隔
    #     # scheduler.add_job(spider, 'interval', seconds=3600)
    #     # scheduler.start()
    #     # app.run()

    spider()















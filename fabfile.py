#coding: utf8
import time
from fabric.api import *
from fabric.colors import *


env.hosts = ['221.235.53.169']
env.user = 'huolibi'
env.password = 'HLbi12@('


def git_file():
    local("git add .")
    local("git commit -m fab")
    local("git push origin master")
    time.sleep(5)
    with cd("/home/huolibi/local/hbgj_statistics"):
        run("git pull origin master")
        result = run("pychecker time_job_excute/*.py")
        red(result)
# def make_targz_one_by_one(source_dir):
#     for root, dirs, files in os.walk(source_dir):
#         for f in files:
#             file_type = f.split(".")
#             if len(file_type) == 2:
#                 if file_type[1] == "sh":
#                     with lcd("D:\\前端\\新建文件夹\\bin"):
#                         run("dos2unix.exe " + f)
#




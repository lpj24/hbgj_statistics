#coding: utf8
import time
from fabric.api import *
from fabric.colors import *

env.hosts = ['192.168.182.166']
env.user = 'lpj'
env.password = '123'


def git_file():
    local("git add .")
    local("git commit -m fab")
    local("git push origin master")
    with cd("/home/lpj/Public/hbgj_statistics"):
        run("git pull origin master")
        result = run("pychecker time_job_excute/*.py")
        time.sleep(3)
        green(result)




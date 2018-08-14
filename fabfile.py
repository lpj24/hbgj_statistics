# -*- coding: utf-8 -*-

from fabric.api import *


env.hosts = ['221.235.53.169']
env.user = 'huolibi'
env.password = 'HLbi12@('


def check_job():
    from monitor import task
    task.check_execute_job()


def check_py():
    check_job()
    local("pychecker time_job_excute/*.py")


def deploy():
    import uuid
    commit_info = uuid.uuid1()
    check_py()
    local("git add .")
    local("git commit -m " + str(commit_info))
    local("git push origin master")
    # with cd("/home/huolibi/local/hbgj_statistics/"):
    #     run("git stash")
    #     run("git stash clear")
    #     run("/home/huolibi/.local/bin/git pull origin master")
    #     run("pychecker time_job_excute/*.py")



if __name__ == '__main__':
    s = '你好hello'
    a = s[0]
    import sys
    print sys.getdefaultencoding()
    print sys.stdin.encoding
    print isinstance(a, str)
    print unicode(a, 'gbk')
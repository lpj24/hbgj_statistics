#coding: utf8
import tarfile, os
from fabric.api import *
from fabric.colors import *


env.hosts = ['192.168.182.166']
env.user = 'lpj'
env.password = '123'

#
# def test():
#     print os.path.pardir
#
#
# def make_targz_one_by_one(output_filename, source_dir):
#     tar = tarfile.open(output_filename, "w:gz")
#     for root, dirs, files in os.walk(source_dir):
#         for f in files:
#             file_type = f.split(".")
#             if len(file_type) == 2:
#                 if file_type[1] == "sh":
#                     with lcd("D:\\前端\\新建文件夹\\bin"):
#                         run("dos2unix.exe " + f)
#                 path_file = os.path.join(root, f)
#                 tar.add(path_file)
#     tar.close()
#
#
# def make_tar_gz():
#     output_filename = "hbgj_statistics.tar.gz"
#     source_dir = os.path.join(os.path.pardir, "hbgj_statistics")
#     make_targz_one_by_one(output_filename, source_dir)
#     # with tarfile.open("hbgj_statistics.tar.gz", "w:gz") as tar:
#     #     tar.add("C:\\Users\\Administrator\\PycharmProjects\\hbgj_statistics",
#     #             arcname=os.path.basename("C:\\Users\\Administrator\\PycharmProjects\\hbgj_statistics"))


def git_file():
    local("git add .")
    local("git commit -m fab")
    local("git push origin master")
    # with cd("/home/lpj/Public/hbgj_statistics"):
    #     run("git pull origin master")


if __name__ == "__main__":
    # dest_path = 'hbgj_statistics.tar.gz'
    # fullPath, projectName = os.path.split(os.getcwd())
    # out = tarfile.TarFile.open(dest_path, 'w:gz')
    # out.add("hbgj_statistics")
    # out.close()
    make_tar_gz()


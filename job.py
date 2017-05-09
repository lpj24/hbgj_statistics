# -*-coding:utf-8 -*-
import requests

from pdfminer.pdfinterp import PDFResourceManager, process_pdf

from pdfminer.converter import TextConverter

from pdfminer.layout import LAParams

from io import StringIO

from io import open


def count_words_at_url(url):
    resp = requests.get(url)
    return len(resp.text.split())

if __name__ == "__main__":
    # print count_words_at_url("http://www.cnblogs.com/gudaojuanma/p/Python-RQ-Job.html")

    # def readPDF(pdffile):
    #     # 存储共享资源
    #     rsrcmgr = PDFResourceManager()
    #     retstr = StringIO()
    #     laparams = LAParams()
    #     device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    #     process_pdf(rsrcmgr, device, pdffile)
    #     device.close()
    #     content = retstr.getvalue()
    #     return content
    #
    # pdffile = open("E:\\pdf\\pdfbook\\geli.PDF", "r")
    # print readPDF(pdffile)
    import os
    a = os.popen("pdf2txt.py E:\\pdf\\pdfbook\\geli.PDF")
    print a

#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Function:
【未解决】将不可拷贝复制的PDF中的表格数据导出并转换为xml格式数据
http://www.crifan.com/non_copy_pdf_table_data_export_to_xml
 
Author:     Crifan Li
Version:    2014-01-27
Contact:    http://www.crifan.com/about/me
"""
 
import os
import sys
import codecs
from BeautifulSoup import BeautifulSoup;
 
def pdf_table_to_xml():
    """Extract data from HTML which is generated from PDF using pdftohtml, then saved to xml"""
    srcHtmlFolder = "D:\\tmp\\tmp_dev_root\\virutalbox\\ubuntu\\win7_to_ubuntu\\pdf_to_html_withTable"
    htmlFilenameList = []
    baseFilename = "hart183WithTable-"
    fileSuffix = ".html"
 
    #create output file
    outputXmlFilename = "GeneratedHartIdCompanyXml.xml";
    # 'a+': read,write,append
    # 'w' : clear before, then write
    #outputXmlFp = codecs.open(outputXmlFilename, 'w')
    outputXmlFp = codecs.open(outputXmlFilename, 'w', "UTF-8")
 
    #generate html file list to process
    # hart183WithTable-9.html to hart183WithTable-34.html
    for pageNum in range(9, 35):
        fullFilename = baseFilename + str(pageNum) + fileSuffix
        #print "fullFilename=",fullFilename;
        # fullFilename= hart183WithTable-9.html
        # fullFilename= hart183WithTable-10.html
        #fullFilename= hart183WithTable-34.html
        fullFile = os.path.join(srcHtmlFolder, fullFilename)
        #print "fullFile=",fullFile
         
        srcHtmlFp = open(fullFile)
        #print "srcHtmlFp=",srcHtmlFp
        srcHtml = srcHtmlFp.read()
        #print "srcHtml=",srcHtml
 
        foundAllFt = []
        paraLineNum = 0
         
        soup = BeautifulSoup(srcHtml, fromEncoding="UTF-8")
        #hart183WithTable-9.html
        # <P style="position:absolute;top:744px;left:108px;white-space:nowrap" class="ft05">0304&#160;</P>
        # <P style="position:absolute;top:744px;left:245px;white-space:nowrap" class="ft05">NEWTHERMOX&#160;</P>
        # <P style="position:absolute;top:744px;left:535px;white-space:nowrap" class="ft05">Ametek&#160;</P>
        # <P style="position:absolute;top:766px;left:108px;white-space:nowrap" class="ft05">0A01&#160;</P>
        # <P style="position:absolute;top:766px;left:245px;white-space:nowrap" class="ft05">TRI20&#160;</P>
        # <P style="position:absolute;top:766px;left:535px;white-space:nowrap" class="ft05">Brooks&#160;Instrument&#160;</P>
        foundAllFt05 = soup.findAll(name="p", attrs={"class":"ft05"})
        #print "foundAllFt05=",foundAllFt05
        ft05Len = len(foundAllFt05)
        print "ft05Len=",ft05Len
         
        #hart183WithTable-10.html
        # <P style="position:absolute;top:181px;left:81px;white-space:nowrap" class="ft03">1109&#160;</P>
        # <P style="position:absolute;top:181px;left:218px;white-space:nowrap" class="ft03">DELTBS/Deltabar&#160;S&#160;</P>
        # <P style="position:absolute;top:181px;left:508px;white-space:nowrap" class="ft03">Endress&#160;&amp;&#160;Hauser&#160;</P>
        # <P style="position:absolute;top:204px;left:81px;white-space:nowrap" class="ft03">110A&#160;</P>
        # <P style="position:absolute;top:204px;left:218px;white-space:nowrap" class="ft03">FMU231/FMU13x&#160;</P>
        # <P style="position:absolute;top:204px;left:508px;white-space:nowrap" class="ft03">Endress&#160;&amp;&#160;Hauser&#160;</P>
         
        #hart183WithTable-34.html
        # <P style="position:absolute;top:181px;left:81px;white-space:nowrap" class="ft03">E183&#160;</P>
        # <P style="position:absolute;top:181px;left:218px;white-space:nowrap" class="ft03">Radar&#160;Lvl&#160;Transmitter&#160;</P>
        # <P style="position:absolute;top:181px;left:508px;white-space:nowrap" class="ft03">FUTURE&#160;INSTRUMENT&#160;</P>
        # <P style="position:absolute;top:204px;left:81px;white-space:nowrap" class="ft03">E184&#160;</P>
        # <P style="position:absolute;top:204px;left:218px;white-space:nowrap" class="ft03">EA10S&#160;</P>
        # <P style="position:absolute;top:204px;left:508px;white-space:nowrap" class="ft03">MOTOYAMA&#160;</P>
        foundAllFt03 = soup.findAll(name="p", attrs={"class":"ft03"})
        #print "foundAllFt03=",foundAllFt03
        ft03Len = len(foundAllFt03)
        print "ft03Len=",ft03Len
 
        if((ft05Len > 1) and (0 == (ft05Len % 3))):
            print "+++ ft05 is real table data for ",fullFile
            paraLineNum = ft05Len
            foundAllFt = foundAllFt05
        elif((ft03Len > 1) and (0 == (ft03Len % 3))):
            print "+++ ft03 real table data for ",fullFile
            paraLineNum = ft03Len
            foundAllFt = foundAllFt03
        else:
            print "--- Not found valid table data for ",fullFile
            sys.exit(-2)
         
        #real start extrat data
        totalRowNum = paraLineNum/3
        print "totalRowNum=",totalRowNum
        for rowIdx in range(totalRowNum):
            def postProcessStr(origStr):
                """do some post process for input str"""
                processedStr = origStr.replace("&#160;", " ")
                #processedStr = processedStr.replace("&amp;", "&")
                processedStr = processedStr.strip()
                return processedStr
                 
            hartCodeSoup = foundAllFt[rowIdx*3 + 0]
            hartCodeUni = unicode(hartCodeSoup.string)
            hartCodeUni = postProcessStr(hartCodeUni)
 
            hartDescSoup = foundAllFt[rowIdx*3 + 1]
            hartDescUni = unicode(hartDescSoup.string)
            hartDescUni = postProcessStr(hartDescUni)
 
            hartNameSoup = foundAllFt[rowIdx*3 + 2]
            hartNameUni = unicode(hartNameSoup.string)
            hartNameUni = postProcessStr(hartNameUni)
 
            #   <HartCompany Code="3701" Name="Yokogawa" Description="YEWFLO"/>
            xmlLineStr = '  <HartCompany Code="' + hartCodeUni + '" Name="' + hartNameUni + '" Description="' + hartDescUni + '"/>' + '\n'
            #print "xmlLineStr=",xmlLineStr
 
            #save data
            outputXmlFp.write(xmlLineStr)
 
    #save and close output file
    outputXmlFp.flush()
    outputXmlFp.close()
 
if __name__ == "__main__":
    pdf_table_to_xml();
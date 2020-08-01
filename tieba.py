import requests
from lxml import etree
import ast
import json
import pymysql as msql


class TiebaSpider:
    def __init__(self, name):
        self.flag = 0
        self.name = name
        self.url_temp = "https://tieba.baidu.com/f?kw=" + name + "&ie=utf-8&pn={}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
        }

    def start_url(self):     # 1. url
        start_url = self.url_temp.format(0)
        return start_url

    def parse_url(self, url):   # 2. 发送请求 获取响应
        response = requests.get(url, self.headers)
        html_str = response.content.decode()
        return html_str

    def get_content_list(self, html_str):   # 3.提取数据
        html = etree.HTML(html_str)
        li_str = "//li[@class=' j_thread_list clearfix']" if self.flag !=0 else "//li[@class=' j_thread_list thread_top j_thread_list clearfix']"
        li_list = html.xpath(li_str)
        content_list = []
        for li in li_list:
            # xpath-> 字典
            data = str(li.xpath('./@data-field')[0]) + ''
            data = data.replace('null','None')
            data = data.replace('true', 'True')
            data = ast.literal_eval(data)
            item = {}
            item['id'] = data['id']
            item['user'] = data['author_nickname'] if data['author_nickname'] !=None else data['author_name']
            item['renum'] = data['reply_num']
            item['is_top'] = 'False' if data['is_top'] == None else 'True'
            item['is_good'] = 'False' if data['is_good'] == None else 'True'
            item['title'] = str(li.xpath("./div/div[2]/div[1]/div[1]/a/text()")[0]) + ''
            content_list.append(item)
            


        # 第一页的直接返回即可
        if self.flag == 0:
            return content_list
        # 提取下一页url地址
        next_url = "https:" + html.xpath("//a[@class='next pagination-item ']/@href")[0] if len(html.xpath("//a[@class='next pagination-item ']/@href")) > 0 else None
        return content_list, next_url

#     def save_html_str(self, content_list):   # 4. 保存
#         with open('tieba.json', 'w') as f:
#             json.dump(content_list, f)
    
    def create(self):
        conn = msql.connect("localhost", "root", "123456", "test")#连接数据库 
     
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS Information") #删除掉已有的EMPLOYER表
    
        sql = """CREATE TABLE Information (
                ID BIGINT PRIMARY KEY,
                USER  CHAR(255),
                RENUM INT(20),
                IS_TOP CHAR(255),
                IS_GOOD CHAR(255),
                TITLE LONGTEXT)"""
     
        cursor.execute(sql)
        conn.close()
        
    def insert(self,value):
        conn = msql.connect("localhost", "root", "123456", "test")
 
        cursor = conn.cursor()
        sql = "INSERT INTO Information(ID,USER,RENUM,IS_TOP,IS_GOOD,TITLE) VALUES (%s, %s,  %s,%s,%s,%s)"
        try:
            cursor.execute(sql,value)
            conn.commit()
            print('插入数据成功')
        except:
            conn.rollback()
            print("插入数据失败")
        conn.close()       

    def run(self):
        next_url = self.start_url()
        self.create()
        while next_url is not None:
            # 2. 发送请求 获取响应
            html_str = self.parse_url(next_url)
            # 3. 提取数据
            if self.flag == 0:
                con = self.get_content_list(html_str)
                self.flag = 1

            content_list, next_url = self.get_content_list(html_str)
            content_list.extend(con)
                        
            print(content_list,'\n')

            for content in content_list:
                l=[]
                for key ,values in content.items():
                    l.append(values)
                self.insert(l)
            

#             # 4. 保存
#             self.save_html_str(content_list)
  
            
            
if __name__ == "__main__":
    tieba = TiebaSpider("新型冠状病毒")
    tieba.run()
# coding=utf-8
"""
created on:2017.1.16
author:totoro
target:优化爬取微博电影评论用户
method:selenium
"""
from selenium import webdriver
import re
import time
import random
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

"""
整体思路：
1.从电影列表里读取每一部电影的名字，存为key
2.自动登录微博，进入电影的热门搜索界面
3.获取每一页的博文用户＋博文的评论ID
4.遍历每一个评论ID获取评论用户
"""


# 调用FireFox浏览器
browser = webdriver.Firefox()
# 存储评论ID的列表
comment_id = []
# 每部电影的用户数量
usernum = 0


# 读取电影名
def read_name(path,charset="utf-8"):
    """
    从txt文件中按行读取电影名
    :param path:txt文件的存放路径
    :parameter charset:编码格式
    :return lines:txt文件的每一行即每个电影名
    """
    with open(path) as f:
        lines = [line.strip().decode(charset) for line in f.readlines()]
    f.close()
    return lines


# 进行自动登录
def login_weibo(username,password):
    """
    进行微博的自动输入用户名和密码
    :param username:用户名
    :param password:密码
    """
    try:
        # 输入用户名／密码进行登陆　
        print u'准备登陆Weibo.cn网站...'
        browser.get("http://login.weibo.cn/login/")
        elem_user = browser.find_element_by_name("mobile")
        elem_user.send_keys(username)
        elem_pwd = browser.find_element_by_xpath("/html/body/div[2]/form/div/input[2]")
        elem_pwd.send_keys(password)
        # 暂停时间输入验证码　
        time.sleep(30)
    except Exception, e:
        print "Error:", e
    finally:
        print u'End LoginWeibo!!\n\n'


# 获取翻页所需页码
def get_page_num(url):
    """
    获取翻页所需的页码
    :param url: 首页
    :return: n:页码
    """
    browser.get(url)
    html = browser.page_source
    # 当前页面为Unicode编码，需要转化成String形式
    html = str(html)
    num = re.findall('1/(\d*?)页<', html)
    n = num[0] if num else 1
    return n


# 获取电影的实时博文用户（包含100页）
def get_realtime_blog_user(key):
    """
    获取实时博文的用户
    :param key: 电影名
    :parameter usernum: 用户数量
    :return: username: 用户名
    """
    try:
        each_url = "http://weibo.cn/search/mblog?hideSearchFrame=&keyword=" + key + "&page=%d"
        for a in range(100):
            comment_url = each_url % (a + 1)
            for b in range(5):
                browser.get(comment_url)
                html = browser.page_source
                second = random.randint(4, 9)
                time.sleep(second)
                if not '抱歉，未找到' in html:
                    break
            html = str(html)
            time.sleep(2)
            username = re.findall('class="nk" href=.*?>(.*?)</a>', html)
            for i in range(len(username)):
                print username[i]
                with open('/home/totoro/codes/PycharmProjects/Seeing/Data/' + key + '.txt', 'a') as f:
                    f.write(username[i] + '\n')
                global usernum
                usernum = usernum+1
                if usernum == 5000:
                    break
    except Exception, e:
        print "Error:", e

# 获取电影的热门博文用户＋评论ID
def get_blog_user(key):
    """
    获取热门博文的用户及其评论网址的ID
    :param key: 电影名
    :parameter page_num: 热门博文的页数
    :parameter url: 搜索热门首页
    :parameter　comment_url: 热门每一页
    :parameter id:博文评论id
    :return: username: 用户名
    """
    try:
        url = "http://weibo.cn/search/mblog?hideSearchFrame=&keyword=" + key + "&sort=hot"
        page_num = get_page_num(url)
        each_url = "http://weibo.cn/search/mblog?hideSearchFrame=&keyword=" + key + "&sort=hot&page=%d"
        for a in range(int(page_num)):
            comment_url = each_url % (a + 1)
            for b in range(5):
                browser.get(comment_url)
                html = browser.page_source
                second = random.randint(4, 9)
                time.sleep(second)
                if '抱歉，未找到' not in html:
                    break
            html = str(html)
            time.sleep(2)
            username = re.findall('class="nk" href=.*?>(.*?)</a>', html)
            id = re.findall('id="M_(.*?)"',html)
            for i in range(len(username)):
                comment_id.append(id[i])
                # print username[i]
                # with open('/home/totoro/codes/PycharmProjects/Seeing/Data/' + key + '.txt', 'a') as f:
                #     f.write(username[i] + '\n')
                global usernum
                usernum = usernum+1
                if usernum == 5000:
                    break
    except Exception, e:
        print "Error:", e

# 获取评论用户
def get_comment_user(comment_id):
    """
    获取评论中的用户
    :param comment_id:博文评论id
    :return: username: 用户名
    """
    try:
        for a in range(len(comment_id)):
            url = "http://weibo.cn/comment/"+comment_id[a]
            browser.get(url)
            html = browser.page_source
            if "还没有人针对这条微博发表评论" in html:
                continue
            else:
                page_num = get_page_num(url)
                each_url = url + "?page=%d"
                for i in range(int(page_num)):
                    comment_url = each_url % (i+1)
                    for b in range(5):
                        browser.get(comment_url)
                        comment_html = browser.page_source
                        second = random.randint(4, 9)
                        time.sleep(second)
                        if '抱歉，未找到' not in html:
                            break
                    comment_html = str(comment_html)
                    time.sleep(2)
                    username = re.findall('/u/.*?">(.*?)</a>', comment_html)
                    for c in range(len(username)):
                        print username[c]
                        with open('/home/totoro/codes/PycharmProjects/Seeing/Data/' + key + '.txt', 'a') as f:
                            f.write(username[c] + '\n')
                        global usernum
                        usernum = usernum + 1
                        if usernum == 5000:
                            break
    except Exception, e:
        print "Error:", e


if __name__ == '__main__':
    # 定义变量
    username = 'pqwsb72145@163.com'  # 输入用户名
    password = 'pachong'  # 输入密码
    login_weibo(username, password)
    key = '电影你的名字'
    get_realtime_blog_user(key)
    get_blog_user(key)
    get_comment_user(comment_id)